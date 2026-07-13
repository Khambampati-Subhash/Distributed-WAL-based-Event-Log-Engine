package inmemorystore

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type InMemoryStore struct {
	Index     map[uint64]int64
	IndexFile *os.File
}

func NewInMemoryStore(indexFileName string) (*InMemoryStore, error) {
	// Did the file already exist? If we're about to create it, the new
	// directory entry must itself be fsynced (see below) or a crash could
	// lose the file's name even though its data was synced.
	_, statErr := os.Stat(indexFileName)
	isNew := os.IsNotExist(statErr)
	indexfile, err := os.OpenFile(indexFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open for index file %q: %w", indexFileName, err)
	}

	// fsync the parent directory so the newly-created log file's directory
	// entry is durable. fsyncing the file persists its data + inode but NOT
	// the directory that names it; without this a crash right after creation
	// could leave the file nameless (effectively gone).
	if isNew {
		if err := fsyncDir(filepath.Dir(indexFileName)); err != nil {
			indexfile.Close()
			return nil, fmt.Errorf("indexFile: fsync dir for %q: %w", indexFileName, err)
		}
	}
	inMemoryStore := &InMemoryStore{Index: make(map[uint64]int64), IndexFile: indexfile}
	if err := inMemoryStore.rebuildIndex(); err != nil {
		indexfile.Close()
		return nil, err
	}

	return inMemoryStore, nil
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

func (inMemoryStore *InMemoryStore) Get(offset uint64) int64 {
	return 0
}

func (inMemoryStore *InMemoryStore) Store(value int64) error {
	return nil
}

func (inMemoryStore *InMemoryStore) rebuildIndex() error {
	if _, err := inMemoryStore.IndexFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek start: %w", err)
	}

	data := make([]byte, 8)
	var pos int64

	for {
		// Read the length field (4 bytes).
		_, err := io.ReadFull(inMemoryStore.IndexFile, data)
		if err == io.EOF {
			break // clean end
		}
		if err != nil {
			return fmt.Errorf("index File : read length at %d: %w", pos, err)
		}

		storedOffsetValue := binary.BigEndian.Uint32(data[0:4])

		storedBytePosition := binary.BigEndian.Uint32(data[4:8])

		inMemoryStore.Index[uint64(storedOffsetValue)] = int64(storedBytePosition)

	}
	_, err := inMemoryStore.IndexFile.Seek(0, io.SeekEnd)
	return err
}
