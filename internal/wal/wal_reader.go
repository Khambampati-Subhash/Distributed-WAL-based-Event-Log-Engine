package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/inmemorystore"
)

// WALReader reads records back out of the log by offset.
//
// It opens its OWN read-only handle to the file and reads with ReadAt, which
// is positional: it reads at an absolute byte position and never touches a
// shared file cursor. That is what lets many readers run concurrently with a
// writer that is appending at the end — no lock is held during disk I/O.
//
// Offset-to-position lookups go through the InMemoryStore directly (taking
// only an RLock), so readers never contend with the writer's mutex.
type WALReader struct {
	store  inmemorystore.InMemoryStoreInterface
	file   *os.File
	offset uint64
	table  *crc32.Table
}

// NewWALReader opens a read-only handle to the same log file the writer owns.
// The store provides offset-to-position lookups without going through the writer.
func NewWALReader(filename string, store inmemorystore.InMemoryStoreInterface) (*WALReader, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("wal: open for read %q: %w", filename, err)
	}
	return &WALReader{
		store: store,
		file:  file,
		table: crc32.MakeTable(crc32.Castagnoli),
	}, nil
}

// ReadAt returns the payload stored at the given offset. This is the core
// primitive: offset -> position -> bytes. It is stateless (doesn't move the
// cursor) and safe to call from many goroutines at once.
//
// The CRC32C is recomputed and verified on EVERY read, not just at startup.
// Bit-rot can strike a record after the engine has booted and indexed it;
// verifying on read means we never silently hand a consumer corrupt bytes.
//
// If the offset has not been written yet it returns io.EOF. A *CorruptionError
// is returned if the stored CRC does not match the recomputed one.
func (r *WALReader) ReadAt(offset uint64) ([]byte, error) {
	pos, ok := r.store.Get(offset)
	if !ok {
		return nil, io.EOF
	}

	lenBuf := make([]byte, LengthSize+CrcSize)
	if _, err := r.file.ReadAt(lenBuf, pos); err != nil {
		return nil, fmt.Errorf("wal: read header at offset %d (pos %d): %w", offset, pos, err)
	}
	length := binary.BigEndian.Uint32(lenBuf[:LengthSize])
	storedCRC := binary.BigEndian.Uint32(lenBuf[LengthSize:])

	payload := make([]byte, length)
	if _, err := r.file.ReadAt(payload, pos+RecordHeaderSize); err != nil {
		return nil, fmt.Errorf("wal: read payload at offset %d (pos %d): %w", offset, pos, err)
	}

	computedCRC := crc32.Checksum(lenBuf[:LengthSize], r.table)
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
// Returns io.EOF once the cursor reaches the end.
func (r *WALReader) Read() (data []byte, offset uint64, err error) {
	data, err = r.ReadAt(r.offset)
	if err != nil {
		return nil, r.offset, err
	}
	offset = r.offset
	r.offset++
	return data, offset, nil
}

// Seek moves the cursor so the next Read() starts at the given offset.
func (r *WALReader) Seek(offset uint64) {
	r.offset = offset
}

// Close releases the reader's file handle.
func (r *WALReader) Close() error {
	return r.file.Close()
}
