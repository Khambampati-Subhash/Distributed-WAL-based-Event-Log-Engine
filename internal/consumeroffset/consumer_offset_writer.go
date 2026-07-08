package offset

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// OffsetWriter persists a single consumer's committed offset to its own file.
//
// Why this exists: a consumer reads the log and tracks "I'm at record N". If it
// crashes, that number is gone unless we wrote it down. OffsetWriter writes it
// down durably so the consumer can resume exactly where it left off.
//
// The offset is stored as 8 raw bytes (a big-endian uint64). To avoid leaving a
// half-written file behind after a crash, we write to a temporary file, fsync
// it, then atomically rename it over the real file — rename is atomic on POSIX,
// so a reader always sees either the old offset or the new one, never garbage.
type OffsetWriter struct {
	path           string
	mu             sync.Mutex
	nthOffset      uint32
	previousOffset uint64
}

func NewOffsetWriter(path string, nthOffset uint32) *OffsetWriter {
	return &OffsetWriter{path: path, nthOffset: nthOffset}
}

// instead of storing each offset in file everytime we can batch it
func (w *OffsetWriter) BatchWrite(offset uint64) error {
	if offset-w.previousOffset == uint64(w.nthOffset) {
		w.previousOffset = offset
		return w.Write(offset)
	}
	return nil
}

// Write durably persists offset as the consumer's committed position.
func (w *OffsetWriter) Write(offset uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	tmp := w.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("offset: open tmp %q: %w", tmp, err)
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], offset)
	if _, err := f.Write(buf[:]); err != nil {
		f.Close()
		return fmt.Errorf("offset: write: %w", err)
	}
	// fsync the data, then close, then atomically swap it into place.
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("offset: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("offset: close tmp: %w", err)
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return fmt.Errorf("offset: rename %q -> %q: %w", tmp, w.path, err)
	}
	if err := fsyncDir(filepath.Dir(w.path)); err != nil {
		return fmt.Errorf("error while fsync the dir: %s", err)
	}
	return nil
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir) // open the directory read-only
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync() // fsync the directory fd
}
