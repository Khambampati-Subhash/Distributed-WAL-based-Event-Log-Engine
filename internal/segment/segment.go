// Package segment turns the single-file WAL into a set of segment files.
//
// A log is a directory of segment files named by their BASE OFFSET (the global
// offset of the first record they contain), zero-padded to 20 digits:
//
//	<dir>/00000000000000000000.log   # offsets 0 .. (base + count - 1)
//	<dir>/00000000000000001000.log   # next segment
//	<dir>/00000000000000002000.log   # active segment (being appended)
//
// Each Segment is a thin wrapper over a wal.WALWriter (the per-file primitive
// that already does durable append, CRC32C, and recovery). The Segment adds the
// base offset and translates between GLOBAL offsets (what callers use) and
// LOCAL slots (the record's position within this one file):
//
//	localSlot = globalOffset - baseOffset
//
// The record format is unchanged from Phase 2: [len:4][crc:4][payload:N].
package segment

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/inmemorystore"
	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/wal"
)

const segmentSuffix = ".log"

const baseOffsetDigits = 20

// Segment is one segment file plus the global base offset of its first record.
type Segment struct {
	baseOffset uint64
	writer     *wal.WALWriter
	reader     *wal.WALReader
	store      *inmemorystore.InMemoryStore
	path       string

	mu         sync.Mutex
	lastAppend time.Time

	readers sync.WaitGroup
}

// SegmentFileName returns the canonical filename for a segment with the given
// base offset, e.g. base 1000 -> "00000000000000001000.log".
func SegmentFileName(baseOffset uint64) string {
	return fmt.Sprintf("%0*d%s", baseOffsetDigits, baseOffset, segmentSuffix)
}

// indexFileName derives the .index path from a .log path.
func indexFileName(logPath string) string {
	return strings.TrimSuffix(logPath, segmentSuffix) + ".index"
}

// NewSegment opens (or creates) the segment file for baseOffset inside dir,
// recovering its index from disk via the underlying WALWriter.
func NewSegment(dir string, baseOffset uint64) (*Segment, error) {
	path := filepath.Join(dir, SegmentFileName(baseOffset))
	idxPath := indexFileName(path)

	store, err := inmemorystore.NewInMemoryStore(idxPath, wal.NthIndex)
	if err != nil {
		return nil, err
	}

	writer, err := wal.NewWalWriter(path, store)
	if err != nil {
		store.Close()
		return nil, err
	}
	reader, err := wal.NewWALReader(path, store)
	if err != nil {
		writer.Close()
		store.Close()
		return nil, err
	}

	lastAppend := time.Now()
	if info, statErr := os.Stat(path); statErr == nil {
		lastAppend = info.ModTime()
	}

	return &Segment{
		baseOffset: baseOffset,
		writer:     writer,
		reader:     reader,
		store:      store,
		path:       path,
		lastAppend: lastAppend,
	}, nil
}

func (s *Segment) BaseOffset() uint64 { return s.baseOffset }

func (s *Segment) NextOffset() uint64 {
	return s.baseOffset + s.writer.NextOffset()
}

func (s *Segment) Count() uint64 { return s.writer.NextOffset() }

func (s *Segment) Size() int64 { return s.writer.Size() }

func (s *Segment) Contains(globalOffset uint64) bool {
	return globalOffset >= s.baseOffset && globalOffset < s.NextOffset()
}

func (s *Segment) Append(data []byte, now time.Time) (uint64, error) {
	localSlot, err := s.writer.Write(data)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	s.lastAppend = now
	s.mu.Unlock()
	return s.baseOffset + localSlot, nil
}

func (s *Segment) LastAppend() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAppend
}

func (s *Segment) acquireRead() { s.readers.Add(1) }

func (s *Segment) releaseRead() { s.readers.Done() }

func (s *Segment) ReadAt(globalOffset uint64) ([]byte, error) {
	if globalOffset < s.baseOffset {
		return nil, fmt.Errorf("segment: offset %d below base %d", globalOffset, s.baseOffset)
	}
	localSlot := globalOffset - s.baseOffset
	return s.reader.ReadAt(localSlot)
}

func (s *Segment) waitReaders() { s.readers.Wait() }

func (s *Segment) Path() string { return s.path }

func (s *Segment) Close() error {
	rerr := s.reader.Close()
	werr := s.writer.Close()
	serr := s.store.Close()
	if werr != nil {
		return werr
	}
	if rerr != nil {
		return rerr
	}
	return serr
}
