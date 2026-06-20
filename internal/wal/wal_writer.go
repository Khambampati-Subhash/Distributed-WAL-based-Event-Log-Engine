package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// On-disk record format (repeated, append-only):
//
//	[ length : 4-byte big-endian uint32 ][ payload : <length> bytes ]
//
// There is NO position written into the file. The offset->position index
// lives in memory (Index) and is rebuilt by scanning the file once on startup.
const headerSize = 4

type WALWriter struct {
	Mu    sync.Mutex // serializes producers: one append at a time
	File  *os.File   // the open append-only file (opened once, held here)
	Index []int64    // Index[offset] = byte position where that record starts
}

// NewWalWriter opens (or creates) the log file and rebuilds the in-memory
// index from whatever is already on disk, so the log survives a restart.
func NewWalWriter(filename string) (*WALWriter, error) {
	// Did the file already exist? If we're about to create it, the new
	// directory entry must itself be fsynced (see below) or a crash could
	// lose the file's name even though its data was synced.
	_, statErr := os.Stat(filename)
	isNew := os.IsNotExist(statErr)

	// O_APPEND => every Write lands at the end of the file.
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", filename, err)
	}

	// fsync the parent directory so the newly-created log file's directory
	// entry is durable. fsyncing the file persists its data + inode but NOT
	// the directory that names it; without this a crash right after creation
	// could leave the file nameless (effectively gone).
	if isNew {
		if err := fsyncDir(filepath.Dir(filename)); err != nil {
			file.Close()
			return nil, fmt.Errorf("wal: fsync dir for %q: %w", filename, err)
		}
	}

	w := &WALWriter{File: file}
	if err := w.rebuildIndex(); err != nil {
		file.Close()
		return nil, err
	}
	return w, nil
}

// fsyncDir flushes a directory's own metadata to stable storage. This is what
// makes a create/rename of a file *inside* that directory durable: fsyncing a
// file does not persist the directory entry that names it.
func fsyncDir(dir string) error {
	d, err := os.Open(dir) // open the directory read-only
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync() // fsync the directory fd
}

// Write appends one opaque record and returns the offset it was stored at.
func (w *WALWriter) Write(data []byte) (uint64, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	// Where this record will start = current end of file.
	pos, err := w.File.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("wal: seek end: %w", err)
	}

	var header [headerSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))

	if _, err := w.File.Write(header[:]); err != nil {
		return 0, fmt.Errorf("wal: write header: %w", err)
	}
	if _, err := w.File.Write(data); err != nil {
		return 0, fmt.Errorf("wal: write payload: %w", err)
	}

	// fsync — the "ahead" in write-ahead log. Until this returns the bytes
	// only live in the OS page cache; a power loss would lose them. We
	// acknowledge success ONLY after the data is durable on disk.
	if err := w.File.Sync(); err != nil {
		return 0, fmt.Errorf("wal: fsync: %w", err)
	}

	offset := uint64(len(w.Index)) // offset is just "which record # is this"
	w.Index = append(w.Index, pos)
	return offset, nil
}

// PositionOf returns the byte position where the record at offset begins,
// so the reader can seek straight to it. ok=false if offset doesn't exist.
func (w *WALWriter) PositionOf(offset uint64) (int64, bool) {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	if offset >= uint64(len(w.Index)) {
		return 0, false
	}
	return w.Index[offset], true
}

// Close closes the underlying file.
func (w *WALWriter) Close() error {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	return w.File.Close()
}

// rebuildIndex scans the file once on startup and reconstructs Index.
// The file is the source of truth; the index is a derived lookup table.
func (w *WALWriter) rebuildIndex() error {
	if _, err := w.File.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek start: %w", err)
	}

	header := make([]byte, headerSize)
	var pos int64
	for {
		_, err := io.ReadFull(w.File, header)
		if err == io.EOF {
			break // clean end: every whole record was read
		}
		if err == io.ErrUnexpectedEOF {
			return w.truncateTorn(pos) // crash left a partial header
		}
		if err != nil {
			return fmt.Errorf("wal: read header at %d: %w", pos, err)
		}

		length := int64(binary.BigEndian.Uint32(header))
		if _, err := io.CopyN(io.Discard, w.File, length); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return w.truncateTorn(pos) // crash left a partial payload
			}
			return fmt.Errorf("wal: read payload at %d: %w", pos, err)
		}

		w.Index = append(w.Index, pos)
		pos += headerSize + length
	}

	_, err := w.File.Seek(0, io.SeekEnd)
	return err
}

// truncateTorn cuts off a half-written record left by a crash, so the log
// only ever contains whole, durable records.
func (w *WALWriter) truncateTorn(validEnd int64) error {
	if err := w.File.Truncate(validEnd); err != nil {
		return fmt.Errorf("wal: truncate torn record at %d: %w", validEnd, err)
	}
	_, err := w.File.Seek(0, io.SeekEnd)
	return err
}
