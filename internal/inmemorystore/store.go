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
	n         uint32
	nthIndex  uint32
}

func NewInMemoryStore(indexFileName string, nthIndex uint32) (*InMemoryStore, error) {
	_, statErr := os.Stat(indexFileName)
	isNew := os.IsNotExist(statErr)
	indexfile, err := os.OpenFile(indexFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open for index file %q: %w", indexFileName, err)
	}

	if isNew {
		if err := fsyncDir(filepath.Dir(indexFileName)); err != nil {
			indexfile.Close()
			return nil, fmt.Errorf("indexFile: fsync dir for %q: %w", indexFileName, err)
		}
	}
	store := &InMemoryStore{
		Index:     make(map[uint64]int64),
		IndexFile: indexfile,
		nthIndex:  nthIndex,
	}
	if err := store.rebuildIndex(); err != nil {
		indexfile.Close()
		return nil, err
	}

	return store, nil
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *InMemoryStore) Get(offset uint64) (int64, bool) {
	pos, ok := s.Index[offset]
	return pos, ok
}

func (s *InMemoryStore) Put(offset uint64, position int64) {
	s.Index[offset] = position
}

func (s *InMemoryStore) Len() int {
	return len(s.Index)
}

func (s *InMemoryStore) WriteIndex(offset, position uint64) error {
	if s.n == s.nthIndex {
		s.n = 0
		var buf [16]byte
		binary.BigEndian.PutUint64(buf[0:8], offset)
		binary.BigEndian.PutUint64(buf[8:16], position)

		if _, err := s.IndexFile.Write(buf[:]); err != nil {
			return fmt.Errorf("wal: write index + byte position: %w", err)
		}
		if err := s.IndexFile.Sync(); err != nil {
			return fmt.Errorf("wal: fsync index+byte position: %w", err)
		}
	}
	s.n++
	return nil
}

func (s *InMemoryStore) Close() error {
	if s.IndexFile != nil {
		return s.IndexFile.Close()
	}
	return nil
}

func (s *InMemoryStore) rebuildIndex() error {
	if _, err := s.IndexFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek start: %w", err)
	}

	buf := make([]byte, 16)

	for {
		_, err := io.ReadFull(s.IndexFile, buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("index file: read entry: %w", err)
		}

		storedOffset := binary.BigEndian.Uint64(buf[0:8])
		storedBytePosition := binary.BigEndian.Uint64(buf[8:16])
		s.Index[storedOffset] = int64(storedBytePosition)
	}
	_, err := s.IndexFile.Seek(0, io.SeekEnd)
	return err
}
