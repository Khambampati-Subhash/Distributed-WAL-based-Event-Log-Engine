package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/checksum"
	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/inmemorystore"
)

// On-disk record format (repeated, append-only):
//
//	[ length   : 4-byte big-endian uint32 ]
//	[ checksum : Checksum.Size() bytes over (length || payload) ]
//	[ payload  : <length> bytes ]
//
// The checksum width is not fixed here: it comes from the pluggable
// checksum.Checksum, so LengthSize is the only constant part of the header.
const (
	LengthSize    = 4
	MaxRecordSize = 64 * 1024 * 1024 // 64MB sanity cap
	NthIndex      = 10
)

// CorruptionError indicates detected bit-rot or corruption in the log.
type CorruptionError struct {
	Offset   int64  // byte position in file
	Expected []byte // checksum stored on disk
	Got      []byte // checksum recomputed from the bytes
	Reason   string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("wal: corruption at byte %d: %s (checksum: expected %x, got %x)", e.Offset, e.Reason, e.Expected, e.Got)
}

type WALWriter struct {
	mu         sync.Mutex
	file       *os.File
	csum       checksum.Checksum
	headerSize int // LengthSize + csum.Size()
	size       int64
	nextOffset uint64 // offset the next Write will assign
	store      inmemorystore.InMemoryStoreInterface
}

// NewWalWriter opens (or creates) the log file and recovers its head offset from
// the sparse checkpoint index rather than a full scan: it loads the checkpoints,
// jumps to the last one, and scans only the short tail after it to find the true
// head and truncate any torn final record. The caller provides the store and the
// checksum algorithm (which fixes the on-disk header width for this log).
func NewWalWriter(filename string, store inmemorystore.InMemoryStoreInterface, csum checksum.Checksum) (*WALWriter, error) {
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
		file:       file,
		csum:       csum,
		headerSize: LengthSize + csum.Size(),
		store:      store,
	}
	if err := w.recover(); err != nil {
		file.Close()
		return nil, err
	}
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

	sum := w.csum.Compute(lenBuf[:], data) // checksum over length || payload

	buf := make([]byte, 0, w.headerSize+len(data))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, sum...)
	buf = append(buf, data...)

	if _, err := w.file.Write(buf); err != nil {
		return 0, fmt.Errorf("wal: write record: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("wal: fsync: %w", err)
	}

	offset := w.nextOffset
	if err := w.store.Checkpoint(offset, pos); err != nil {
		return 0, err
	}
	w.nextOffset++
	w.size += int64(w.headerSize) + int64(len(data))
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
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextOffset
}

// Close closes the underlying WAL file. The caller is responsible for
// closing the InMemoryStore separately.
func (w *WALWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// recover reconstructs the head offset after a restart. It loads the sparse
// checkpoints, positions at the last one, and scans forward only over the tail
// (at most a checkpoint interval of records) to count records up to the head and
// truncate a torn final record. No CRC is verified here — checkpoints are a
// speed hint, and integrity is checked on every read instead.
func (w *WALWriter) recover() error {
	if err := w.store.LoadCheckpoints(); err != nil {
		return err
	}

	var pos int64
	var off uint64
	if cpOff, cpPos, ok := w.store.Floor(^uint64(0)); ok {
		off, pos = cpOff, cpPos
	}

	info, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("wal: stat during recovery: %w", err)
	}
	fileSize := info.Size()

	lenBuf := make([]byte, LengthSize)
	for {
		if pos == fileSize {
			break // clean end, exactly on a record boundary
		}
		if pos+int64(w.headerSize) > fileSize {
			// A header started but the file ends before it completes: torn tail.
			if err := w.file.Truncate(pos); err != nil {
				return fmt.Errorf("wal: truncate torn header at %d: %w", pos, err)
			}
			break
		}
		if _, err := w.file.ReadAt(lenBuf, pos); err != nil {
			return fmt.Errorf("wal: read length at %d: %w", pos, err)
		}
		length := int64(binary.BigEndian.Uint32(lenBuf))
		if length > int64(MaxRecordSize) {
			return &CorruptionError{
				Offset: pos,
				Reason: fmt.Sprintf("length out of bounds: %d", length),
			}
		}
		recEnd := pos + int64(w.headerSize) + length
		if recEnd > fileSize {
			// The payload is incomplete: torn tail. Truncate to the last good record.
			if err := w.file.Truncate(pos); err != nil {
				return fmt.Errorf("wal: truncate torn payload at %d: %w", pos, err)
			}
			break
		}
		pos = recEnd
		off++
	}

	w.nextOffset = off
	w.size = pos
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("wal: seek end after recovery: %w", err)
	}
	return nil
}
