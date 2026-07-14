package inmemorystore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type InMemoryStore struct {
	mu        sync.RWMutex
	index     map[uint64]int64
	indexFile *os.File
	n         uint32
	nthIndex  uint32
}

func NewInMemoryStore(indexFileName string, nthIndex uint32) (*InMemoryStore, error) {
	_, statErr := os.Stat(indexFileName)
	isNew := os.IsNotExist(statErr)
	indexfile, err := os.OpenFile(indexFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("inmemorystore: open %q: %w", indexFileName, err)
	}

	if isNew {
		if err := fsyncDir(filepath.Dir(indexFileName)); err != nil {
			indexfile.Close()
			return nil, fmt.Errorf("inmemorystore: fsync dir for %q: %w", indexFileName, err)
		}
	}
	return &InMemoryStore{
		index:     make(map[uint64]int64),
		indexFile: indexfile,
		nthIndex:  nthIndex,
	}, nil
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	pos, ok := s.index[offset]
	return pos, ok
}

func (s *InMemoryStore) Put(offset uint64, position int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index[offset] = position
}

func (s *InMemoryStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.index)
}

func (s *InMemoryStore) WriteIndex(offset, position uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n == s.nthIndex {
		s.n = 0
		var buf [16]byte
		binary.BigEndian.PutUint64(buf[0:8], offset)
		binary.BigEndian.PutUint64(buf[8:16], position)

		if _, err := s.indexFile.Write(buf[:]); err != nil {
			return fmt.Errorf("inmemorystore: write index entry: %w", err)
		}
		if err := s.indexFile.Sync(); err != nil {
			return fmt.Errorf("inmemorystore: fsync index: %w", err)
		}
	}
	s.n++
	return nil
}

func (s *InMemoryStore) ResetCounter(totalRecords uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n = totalRecords % (s.nthIndex + 1)
}

func (s *InMemoryStore) Close() error {
	if s.indexFile != nil {
		return s.indexFile.Close()
	}
	return nil
}
