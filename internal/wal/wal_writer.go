package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/inmemorystore"
)

// On-disk record format (repeated, append-only):
//
//	[ length : 4-byte big-endian uint32 ]
//	[ crc    : 4-byte big-endian CRC32C of (length || payload) ]
//	[ payload : <length> bytes ]
const (
	LengthSize       = 4
	CrcSize          = 4
	RecordHeaderSize = LengthSize + CrcSize
	MaxRecordSize    = 64 * 1024 * 1024 // 64MB sanity cap
	NthIndex         = 10
)

// CorruptionError indicates detected bit-rot or corruption in the log.
type CorruptionError struct {
	Offset   int64  // byte position in file
	Expected uint32 // expected CRC
	Got      uint32 // actual CRC computed from bytes
	Reason   string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("wal: corruption at byte %d: %s (crc: expected %08x, got %08x)", e.Offset, e.Reason, e.Expected, e.Got)
}

type WALWriter struct {
	mu    sync.Mutex
	file  *os.File
	table *crc32.Table
	size  int64
	store inmemorystore.InMemoryStoreInterface
}

// NewWalWriter opens (or creates) the log file and rebuilds the in-memory
// index from whatever is already on disk, so the log survives a restart.
// The caller provides the InMemoryStore that will hold offset-to-position
// mappings; this decouples the writer from index file management.
func NewWalWriter(filename string, store inmemorystore.InMemoryStoreInterface) (*WALWriter, error) {
	_, statErr := os.Stat(filename)
	isNew := os.IsNotExist(statErr)

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", filename, err)
	}

	if isNew {
		if err := fsyncDir(filepath.Dir(filename)); err != nil {
			file.Close()
			return nil, fmt.Errorf("wal: fsync dir for %q: %w", filename, err)
		}
	}

	w := &WALWriter{
		file:  file,
		table: crc32.MakeTable(crc32.Castagnoli),
		store: store,
	}
	if err := w.rebuildIndex(); err != nil {
		file.Close()
		return nil, err
	}

	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("wal: size after recovery: %w", err)
	}
	w.size = size
	return w, nil
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Write appends one opaque record and returns the offset it was stored at.
func (w *WALWriter) Write(data []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(data) > MaxRecordSize {
		return 0, fmt.Errorf("wal: payload too large (%d > %d)", len(data), MaxRecordSize)
	}

	pos, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("wal: seek end: %w", err)
	}

	var lenBuf [LengthSize]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	crcChecksum := crc32.Checksum(lenBuf[:], w.table)
	crcChecksum = crc32.Update(crcChecksum, w.table, data)

	var crcBuf [CrcSize]byte
	binary.BigEndian.PutUint32(crcBuf[:], crcChecksum)

	buf := make([]byte, 0, RecordHeaderSize+len(data))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, crcBuf[:]...)
	buf = append(buf, data...)

	if _, err := w.file.Write(buf); err != nil {
		return 0, fmt.Errorf("wal: write record: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("wal: fsync: %w", err)
	}

	offset := uint64(w.store.Len())

	if err := w.store.WriteIndex(offset, uint64(pos)); err != nil {
		return 0, err
	}
	w.store.Put(offset, pos)
	w.size += RecordHeaderSize + int64(len(data))
	return offset, nil
}

// Size returns the current size of the segment file in bytes.
func (w *WALWriter) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// NextOffset returns the offset the next Write will be assigned.
func (w *WALWriter) NextOffset() uint64 {
	return uint64(w.store.Len())
}

// Close closes the underlying WAL file. The caller is responsible for
// closing the InMemoryStore separately.
func (w *WALWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// rebuildIndex scans the WAL file on startup and reconstructs the in-memory
// index. Also restores the sparse index counter so on-disk index entries
// continue at the correct interval after restart.
func (w *WALWriter) rebuildIndex() error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek start: %w", err)
	}

	lenBuf := make([]byte, LengthSize)
	crcBuf := make([]byte, CrcSize)
	var pos int64
	var recordCount uint32

	for {
		_, err := io.ReadFull(w.file, lenBuf)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			return w.truncateTorn(pos)
		}
		if err != nil {
			return fmt.Errorf("wal: read length at %d: %w", pos, err)
		}

		length := int64(binary.BigEndian.Uint32(lenBuf))

		if length < 0 || length > int64(MaxRecordSize) {
			return &CorruptionError{
				Offset: pos,
				Reason: fmt.Sprintf("length out of bounds: %d", length),
			}
		}

		_, err = io.ReadFull(w.file, crcBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return w.truncateTorn(pos)
		}
		if err != nil {
			return fmt.Errorf("wal: read crc at %d: %w", pos, err)
		}
		storedCRC := binary.BigEndian.Uint32(crcBuf)

		payload := make([]byte, length)
		_, err = io.ReadFull(w.file, payload)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return w.truncateTorn(pos)
		}
		if err != nil {
			return fmt.Errorf("wal: read payload at %d: %w", pos, err)
		}

		computedCRC := crc32.Checksum(lenBuf, w.table)
		computedCRC = crc32.Update(computedCRC, w.table, payload)

		if computedCRC != storedCRC {
			return &CorruptionError{
				Offset:   pos,
				Expected: storedCRC,
				Got:      computedCRC,
				Reason:   "crc mismatch (bit-rot or disk corruption)",
			}
		}

		offset := uint64(w.store.Len())
		w.store.Put(offset, pos)
		recordCount++
		pos += RecordHeaderSize + length
	}

	w.store.ResetCounter(recordCount)

	_, err := w.file.Seek(0, io.SeekEnd)
	return err
}

func (w *WALWriter) truncateTorn(validEnd int64) error {
	if err := w.file.Truncate(validEnd); err != nil {
		return fmt.Errorf("wal: truncate torn record at %d: %w", validEnd, err)
	}
	_, err := w.file.Seek(0, io.SeekEnd)
	return err
}
