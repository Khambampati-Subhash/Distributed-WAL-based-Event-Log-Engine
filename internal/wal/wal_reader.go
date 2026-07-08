package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// WALReader reads records back out of the log by offset.
//
// It opens its OWN read-only handle to the file and reads with ReadAt, which
// is positional: it reads at an absolute byte position and never touches a
// shared file cursor. That is what lets many readers run concurrently with a
// writer that is appending at the end — no lock is held during disk I/O.
//
// To turn an offset into a byte position it consults the writer's in-memory
// index (via PositionOf). That lookup briefly takes the writer's lock because
// the index slice can be reallocated by an in-flight append; but the lock is
// released before the (slow) disk read happens.
type WALReader struct {
	writer WALWriterInterface // source of the offset -> byte-position index
	file   *os.File           // own read-only handle; safe for concurrent ReadAt
	offset uint64             // cursor: the next offset Read() will return
	table  *crc32.Table       // CRC32C polynomial, for verifying reads
}

// NewWALReader opens a read-only handle to the same log file the writer owns.
// The writer is passed in so the reader can translate offsets into positions.
func NewWALReader(filename string, writer WALWriterInterface) (*WALReader, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("wal: open for read %q: %w", filename, err)
	}
	return &WALReader{
		writer: writer,
		file:   file,
		table:  crc32.MakeTable(crc32.Castagnoli),
	}, nil
}

// ReadAt returns the payload stored at the given offset. This is the core
// primitive: offset -> position -> bytes. It is stateless (doesn't move the
// cursor) and safe to call from many goroutines at once.
//
// Phase 2: the CRC32C is recomputed and verified on EVERY read, not just at
// startup. Bit-rot can strike a record after the engine has booted and indexed
// it; verifying on read means we never silently hand a consumer corrupt bytes.
// CRC32C is hardware-accelerated, so this costs almost nothing.
//
// If the offset has not been written yet it returns io.EOF, which a consumer
// can read as "you are caught up, nothing new yet". A *CorruptionError is
// returned if the stored CRC does not match the recomputed one.
func (r *WALReader) ReadAt(offset uint64) ([]byte, error) {
	// 1. offset -> byte position, using the writer's index.
	pos, ok := r.writer.PositionOf(offset)
	if !ok {
		return nil, io.EOF // offset is past the end of the log
	}

	// 2. Read the length field (4 bytes).
	lenBuf := make([]byte, lengthSize+crcSize)
	if _, err := r.file.ReadAt(lenBuf, pos); err != nil {
		return nil, fmt.Errorf("wal: read header + crc length at offset %d (pos %d): %w", offset, pos, err)
	}
	length := binary.BigEndian.Uint32(lenBuf[:4])

	storedCRC := binary.BigEndian.Uint32(lenBuf[4:])

	// 4. Read exactly <length> payload bytes after the length+crc header.
	payload := make([]byte, length)
	if _, err := r.file.ReadAt(payload, pos+recordHeaderSize); err != nil {
		return nil, fmt.Errorf("wal: read payload at offset %d (pos %d): %w", offset, pos, err)
	}

	// 5. Verify CRC32C over (length || payload).
	computedCRC := crc32.Checksum(lenBuf, r.table)
	computedCRC = crc32.Update(computedCRC, r.table, payload)
	if computedCRC != storedCRC {
		return nil, &CorruptionError{
			Offset:   pos,
			Expected: storedCRC,
			Got:      computedCRC,
			Reason:   fmt.Sprintf("crc mismatch reading offset %d", offset),
		}
	}

	return payload, nil
}

// Read returns the record at the cursor, then advances the cursor by one.
// This is the convenient "consumer" view: call Read() repeatedly to walk the
// log front-to-back. It also returns the offset that was read, so the caller
// can persist its progress. Returns io.EOF once the cursor reaches the end.
func (r *WALReader) Read() (data []byte, offset uint64, err error) {
	data, err = r.ReadAt(r.offset)
	if err != nil {
		return nil, r.offset, err
	}
	offset = r.offset
	r.offset++
	return data, offset, nil
}

// Seek moves the cursor so the next Read() starts at the given offset. A
// crashed consumer uses this to resume from the offset it last persisted.
func (r *WALReader) Seek(offset uint64) {
	r.offset = offset
}

// Close releases the reader's file handle. It does not affect the writer.
func (r *WALReader) Close() error {
	return r.file.Close()
}
