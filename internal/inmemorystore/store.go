package inmemorystore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// indexEntryBytes is the on-disk size of one checkpoint: two big-endian uint64s
// (offset, byte position).
const indexEntryBytes = 16

// InMemoryStore holds a SPARSE set of offset→position checkpoints, one for every
// Nth record, mirrored to an on-disk .index file. It is not a full map of every
// offset: recovery loads only these checkpoints (cheap, no full WAL scan, no CRC
// verification), and a reader finds any offset by taking the nearest checkpoint
// at or below it (Floor) and scanning the WAL forward from there.
//
// The checkpoints are kept sorted by offset (they are appended in ascending
// order), so Floor is a binary search.
type InMemoryStore struct {
	mu        sync.RWMutex
	offsets   []uint64 // checkpoint offsets, ascending
	positions []int64  // byte position of the record at offsets[i]
	index     map[uint64]int64
	indexFile *os.File
	n         uint32 // records seen since the last checkpoint was written
	nthIndex  uint32 // write a checkpoint every nthIndex records
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

// LoadCheckpoints reads every checkpoint from the on-disk .index file into
// memory. It performs NO CRC verification — checkpoints are a fast recovery hint,
// and record integrity is verified on read instead. A torn trailing entry (from
// a crash mid-write) is truncated so future appends stay aligned.
func (s *InMemoryStore) LoadCheckpoints() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, err := s.indexFile.Stat()
	if err != nil {
		return fmt.Errorf("inmemorystore: stat index: %w", err)
	}
	size := info.Size()
	aligned := size - (size % indexEntryBytes)
	if aligned != size {
		// A crash left a partial checkpoint at the tail; drop it so the next
		// append lands on a clean 16-byte boundary.
		if err := s.indexFile.Truncate(aligned); err != nil {
			return fmt.Errorf("inmemorystore: truncate torn index: %w", err)
		}
	}

	count := int(aligned / indexEntryBytes)
	s.offsets = make([]uint64, 0, count)
	s.positions = make([]int64, 0, count)
	s.index = make(map[uint64]int64, count)
	if count == 0 {
		return nil
	}

	buf := make([]byte, aligned)
	if _, err := s.indexFile.ReadAt(buf, 0); err != nil {
		return fmt.Errorf("inmemorystore: read index: %w", err)
	}
	for i := 0; i < count; i++ {
		off := binary.BigEndian.Uint64(buf[i*indexEntryBytes:])
		pos := int64(binary.BigEndian.Uint64(buf[i*indexEntryBytes+8:]))
		s.offsets = append(s.offsets, off)
		s.positions = append(s.positions, pos)
		s.index[off] = pos
	}
	return nil
}

// Floor returns the checkpoint with the greatest offset <= the target, i.e. the
// closest indexed starting point to scan forward from. ok is false when no
// checkpoint is at or below the target (the caller should start from offset 0,
// byte position 0).
func (s *InMemoryStore) Floor(offset uint64) (checkpointOffset uint64, position int64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// First index whose offset is strictly greater than the target; the one
	// before it is the floor.
	i := sort.Search(len(s.offsets), func(i int) bool { return s.offsets[i] > offset })
	if i == 0 {
		return 0, 0, false
	}
	return s.offsets[i-1], s.positions[i-1], true
}

// Get returns the byte position of an offset only if it is itself a checkpoint.
// Most offsets are not checkpoints; use Floor + a forward scan for those.
func (s *InMemoryStore) Get(offset uint64) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pos, ok := s.index[offset]
	return pos, ok
}

// Checkpoint records that the record at the given offset lives at the given byte
// position. It is called once per appended record; only every nthIndex-th call
// actually persists a checkpoint (to disk, fsynced) and adds it to the in-memory
// set. The rest are counted and dropped — that is what makes the index sparse.
func (s *InMemoryStore) Checkpoint(offset uint64, position int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.n++
	if s.nthIndex > 0 && s.n < s.nthIndex {
		return nil
	}
	s.n = 0

	var buf [indexEntryBytes]byte
	binary.BigEndian.PutUint64(buf[0:8], offset)
	binary.BigEndian.PutUint64(buf[8:16], uint64(position))
	if _, err := s.indexFile.Write(buf[:]); err != nil {
		return fmt.Errorf("inmemorystore: write checkpoint: %w", err)
	}
	if err := s.indexFile.Sync(); err != nil {
		return fmt.Errorf("inmemorystore: fsync index: %w", err)
	}
	s.offsets = append(s.offsets, offset)
	s.positions = append(s.positions, position)
	s.index[offset] = position
	return nil
}

func (s *InMemoryStore) Close() error {
	if s.indexFile != nil {
		return s.indexFile.Close()
	}
	return nil
}
