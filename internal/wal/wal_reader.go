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
// Because the index is sparse (only every Nth offset is a checkpoint), a read
// finds its target by taking the nearest checkpoint at or below the offset
// (store.Floor) and scanning records forward from there until it reaches the
// target — at most a checkpoint interval of records.
type WALReader struct {
	store  inmemorystore.InMemoryStoreInterface
	file   *os.File
	offset uint64
	table  *crc32.Table
}

// NewWALReader opens a read-only handle to the same log file the writer owns.
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

// ReadAt returns the payload stored at the given offset. It seeks to the nearest
// checkpoint at/below the offset, then scans records forward to the target. It
// is stateless (doesn't move a shared cursor) and safe to call concurrently.
//
// The CRC32C is recomputed and verified on the target record on EVERY read —
// bit-rot can strike after the engine has booted, and (because startup no longer
// full-scans) read time is now the point where corruption is caught. Skipped
// records on the way to the target are framed by their length only.
//
// Returns io.EOF if the offset has not been written yet, or a *CorruptionError
// if the target record's stored CRC does not match.
func (r *WALReader) ReadAt(offset uint64) ([]byte, error) {
	cur, pos, ok := r.store.Floor(offset)
	if !ok {
		cur, pos = 0, 0
	}

	header := make([]byte, RecordHeaderSize)
	for {
		if _, err := r.file.ReadAt(header, pos); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil, io.EOF // scanned past the head: offset not written yet
			}
			return nil, fmt.Errorf("wal: read header at offset %d (pos %d): %w", offset, pos, err)
		}
		length := binary.BigEndian.Uint32(header[:LengthSize])
		if int64(length) > int64(MaxRecordSize) {
			return nil, &CorruptionError{
				Offset: pos,
				Reason: fmt.Sprintf("length out of bounds: %d", length),
			}
		}

		if cur == offset {
			storedCRC := binary.BigEndian.Uint32(header[LengthSize:])
			payload := make([]byte, length)
			if _, err := r.file.ReadAt(payload, pos+RecordHeaderSize); err != nil {
				return nil, fmt.Errorf("wal: read payload at offset %d (pos %d): %w", offset, pos, err)
			}
			computedCRC := crc32.Checksum(header[:LengthSize], r.table)
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

		pos += RecordHeaderSize + int64(length)
		cur++
	}
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
