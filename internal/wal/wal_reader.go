package wal

import (
	"encoding/binary"
	"fmt"
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
	writer *WALWriter // source of the offset -> byte-position index
	file   *os.File   // own read-only handle; safe for concurrent ReadAt
	offset uint64     // cursor: the next offset Read() will return
}

// NewWALReader opens a read-only handle to the same log file the writer owns.
// The writer is passed in so the reader can translate offsets into positions.
func NewWALReader(filename string, writer *WALWriter) (*WALReader, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("wal: open for read %q: %w", filename, err)
	}
	return &WALReader{writer: writer, file: file}, nil
}

// ReadAt returns the payload stored at the given offset. This is the core
// primitive: offset -> position -> bytes. It is stateless (doesn't move the
// cursor) and safe to call from many goroutines at once.
//
// If the offset has not been written yet it returns io.EOF, which a consumer
// can read as "you are caught up, nothing new yet".
func (r *WALReader) ReadAt(offset uint64) ([]byte, error) {
	// 1. offset -> byte position, using the writer's index.
	pos, ok := r.writer.PositionOf(offset)
	if !ok {
		return nil, io.EOF // offset is past the end of the log
	}

	// 2. Read the 4-byte length prefix at that position.
	header := make([]byte, headerSize)
	if _, err := r.file.ReadAt(header, pos); err != nil {
		return nil, fmt.Errorf("wal: read header at offset %d (pos %d): %w", offset, pos, err)
	}
	length := binary.BigEndian.Uint32(header)

	// 3. Read exactly <length> payload bytes immediately after the prefix.
	payload := make([]byte, length)
	if _, err := r.file.ReadAt(payload, pos+headerSize); err != nil {
		return nil, fmt.Errorf("wal: read payload at offset %d (pos %d): %w", offset, pos, err)
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
