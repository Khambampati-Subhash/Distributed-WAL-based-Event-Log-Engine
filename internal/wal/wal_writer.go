package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/inmemorystore"
)

// On-disk record format (repeated, append-only):
//
//	[ length : 4-byte big-endian uint32 ]
//	[ crc    : 4-byte big-endian CRC32C of (length || payload) ]
//	[ payload : <length> bytes ]
const (
	lengthSize       = 4
	crcSize          = 4
	recordHeaderSize = lengthSize + crcSize
	maxRecordSize    = 64 * 1024 * 1024 // 64MB sanity cap
	nthIndex         = 10
)

// CorruptionError indicates detected bit-rot or corruption in the log.
type CorruptionError struct {
	Offset   int64  // byte position in file
	Expected uint32 // expected CRC
	Got      uint32 // actual CRC computed from bytes
	Reason   string // "length out of bounds", "crc mismatch", etc.
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("wal: corruption at byte %d: %s (crc: expected %08x, got %08x)", e.Offset, e.Reason, e.Expected, e.Got)
}

type WALWriter struct {
	Mu            sync.Mutex // serializes producers: one append at a time
	WalFile       *os.File   // the open append-only file (opened once, held here)
	table         *crc32.Table
	size          int64
	inMemoryStore *inmemorystore.InMemoryStore
}

type WALWriterInterface interface {
	Write(data []byte) (uint64, error)
	Size() int64
	PositionOf(offset uint64) (int64, bool)
	NextOffset() uint64
	Close() error
}

// NewWalWriter opens (or creates) the log file and rebuilds the in-memory
// index from whatever is already on disk, so the log survives a restart.
func NewWalWriter(filename string) (*WALWriter, error) {
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

	fileNameSplit := strings.Split(filename, ".")
	indexFileName := strings.Join([]string{fileNameSplit[0], "index"}, ".")

	store, err := inmemorystore.NewInMemoryStore(indexFileName, nthIndex)
	if err != nil {
		file.Close()
		return nil, err
	}

	w := &WALWriter{
		WalFile:       file,
		table:         crc32.MakeTable(crc32.Castagnoli),
		inMemoryStore: store,
	}
	if err := w.rebuildIndex(); err != nil {
		file.Close()
		store.Close()
		return nil, err
	}

	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		store.Close()
		return nil, fmt.Errorf("wal: size after recovery: %w", err)
	}
	w.size = size
	return w, nil
}

// fsyncDir flushes a directory's own metadata to stable storage.
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
	w.Mu.Lock()
	defer w.Mu.Unlock()

	if len(data) > maxRecordSize {
		return 0, fmt.Errorf("wal: payload too large (%d > %d)", len(data), maxRecordSize)
	}

	pos, err := w.WalFile.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("wal: seek end: %w", err)
	}

	var lenBuf [lengthSize]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	crcChecksum := crc32.Checksum(lenBuf[:], w.table)
	crcChecksum = crc32.Update(crcChecksum, w.table, data)

	var crcBuf [crcSize]byte
	binary.BigEndian.PutUint32(crcBuf[:], crcChecksum)

	var totalBytes []byte
	totalBytes = append(totalBytes, lenBuf[:]...)
	totalBytes = append(totalBytes, crcBuf[:]...)
	totalBytes = append(totalBytes, data[:]...)

	if _, err := w.WalFile.Write(totalBytes); err != nil {
		return 0, fmt.Errorf("wal: write header + crc + total payload: %w", err)
	}

	if err := w.WalFile.Sync(); err != nil {
		return 0, fmt.Errorf("wal: fsync: %w", err)
	}

	offset := uint64(w.inMemoryStore.Len())

	err = w.inMemoryStore.WriteIndex(offset, uint64(pos))
	if err != nil {
		return 0, err
	}
	w.inMemoryStore.Put(offset, pos)
	w.size += recordHeaderSize + int64(len(data))
	return offset, nil
}

// Size returns the current size of the segment file in bytes.
func (w *WALWriter) Size() int64 {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	return w.size
}

// PositionOf returns the byte position where the record at offset begins.
func (w *WALWriter) PositionOf(offset uint64) (int64, bool) {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	return w.inMemoryStore.Get(offset)
}

// NextOffset returns the offset the next Write will be assigned.
func (w *WALWriter) NextOffset() uint64 {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	return uint64(w.inMemoryStore.Len())
}

// Close closes the underlying file and index.
func (w *WALWriter) Close() error {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	w.inMemoryStore.Close()
	return w.WalFile.Close()
}

// rebuildIndex scans the WAL file on startup and reconstructs the in-memory index.
func (w *WALWriter) rebuildIndex() error {
	if _, err := w.WalFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek start: %w", err)
	}

	lenBuf := make([]byte, lengthSize)
	crcBuf := make([]byte, crcSize)
	var pos int64

	for {
		_, err := io.ReadFull(w.WalFile, lenBuf)
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

		if length < 0 || length > int64(maxRecordSize) {
			return &CorruptionError{
				Offset: pos,
				Reason: fmt.Sprintf("length out of bounds: %d", length),
			}
		}

		_, err = io.ReadFull(w.WalFile, crcBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return w.truncateTorn(pos)
		}
		if err != nil {
			return fmt.Errorf("wal: read crc at %d: %w", pos, err)
		}
		storedCRC := binary.BigEndian.Uint32(crcBuf)

		payload := make([]byte, length)
		_, err = io.ReadFull(w.WalFile, payload)
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

		offset := uint64(w.inMemoryStore.Len())
		w.inMemoryStore.Put(offset, pos)
		pos += recordHeaderSize + length
	}

	_, err := w.WalFile.Seek(0, io.SeekEnd)
	return err
}

// truncateTorn cuts off a half-written record left by a crash.
func (w *WALWriter) truncateTorn(validEnd int64) error {
	if err := w.WalFile.Truncate(validEnd); err != nil {
		return fmt.Errorf("wal: truncate torn record at %d: %w", validEnd, err)
	}
	_, err := w.WalFile.Seek(0, io.SeekEnd)
	return err
}
