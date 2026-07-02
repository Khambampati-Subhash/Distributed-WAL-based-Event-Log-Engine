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
	"sync"
	"time"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/wal"
)

// segmentSuffix is appended to the zero-padded base offset to form a filename.
const segmentSuffix = ".log"

// baseOffsetDigits is the zero-padding width. 20 digits holds any uint64
// (max uint64 is 20 digits), so filenames sort lexicographically by base offset.
const baseOffsetDigits = 20

// Segment is one segment file plus the global base offset of its first record.
type Segment struct {
	baseOffset uint64         // global offset of the first record in this file
	writer     *wal.WALWriter // the underlying durable, CRC-checked file
	reader     *wal.WALReader // read handle into the same file
	path       string         // full path to the .log file

	// Retention (Phase 4) state:
	mu         sync.Mutex // guards lastAppend
	lastAppend time.Time  // wall-clock time of the most recent append; used
	//                           to decide when the whole segment ages out.
	//                           Initialized from file mtime on open, then updated
	//                           to the current time on each append.
	readers sync.WaitGroup // in-flight readers; retention waits for this to drain
	//                        before closing+deleting the file (fd/unlink safety)
}

// SegmentFileName returns the canonical filename for a segment with the given
// base offset, e.g. base 1000 -> "00000000000000001000.log".
func SegmentFileName(baseOffset uint64) string {
	return fmt.Sprintf("%0*d%s", baseOffsetDigits, baseOffset, segmentSuffix)
}

// NewSegment opens (or creates) the segment file for baseOffset inside dir,
// recovering its index from disk via the underlying WALWriter.
func NewSegment(dir string, baseOffset uint64) (*Segment, error) {
	path := filepath.Join(dir, SegmentFileName(baseOffset))

	writer, err := wal.NewWalWriter(path)
	if err != nil {
		return nil, err
	}
	reader, err := wal.NewWALReader(path, writer)
	if err != nil {
		writer.Close()
		return nil, err
	}

	// Seed lastAppend from the file's mtime (cold-start fallback). While the
	// engine runs, Append updates it to the current time; but after a restart
	// our in-memory time is gone, so the file's mtime is the best proxy for
	// "when was this segment last written".
	lastAppend := time.Now()
	if info, statErr := os.Stat(path); statErr == nil {
		lastAppend = info.ModTime()
	}

	return &Segment{
		baseOffset: baseOffset,
		writer:     writer,
		reader:     reader,
		path:       path,
		lastAppend: lastAppend,
	}, nil
}

// BaseOffset is the global offset of this segment's first record.
func (s *Segment) BaseOffset() uint64 { return s.baseOffset }

// NextOffset is the global offset the next appended record would get, i.e.
// baseOffset + number of records currently in this segment. It also marks the
// exclusive upper bound of the global offsets this segment owns.
func (s *Segment) NextOffset() uint64 {
	return s.baseOffset + s.writer.NextOffset()
}

// Count is the number of records currently stored in this segment.
func (s *Segment) Count() uint64 { return s.writer.NextOffset() }

// Size is the segment file's current size in bytes (for roll decisions).
func (s *Segment) Size() int64 { return s.writer.Size() }

// Contains reports whether the given GLOBAL offset lives in this segment.
func (s *Segment) Contains(globalOffset uint64) bool {
	return globalOffset >= s.baseOffset && globalOffset < s.NextOffset()
}

// Append writes one opaque record and returns the GLOBAL offset it was stored
// at (baseOffset + local slot). now is the current wall-clock time (injected so
// tests can control segment age); it becomes this segment's last-append time.
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

// LastAppend returns the wall-clock time of the most recent append (or, after a
// cold start with no appends yet, the file's mtime). Retention compares this
// against the retention window to decide if the whole segment has aged out.
func (s *Segment) LastAppend() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAppend
}

// acquireRead marks a reader as in-flight so retention won't Close() the file
// out from under it. MUST be called by the manager while holding the manager
// lock, so it is atomic with the segment still being a member of the live set.
// The caller must call releaseRead when done.
func (s *Segment) acquireRead() { s.readers.Add(1) }

// releaseRead marks an in-flight reader as finished.
func (s *Segment) releaseRead() { s.readers.Done() }

// ReadAt returns the payload at the given GLOBAL offset. It translates the
// global offset to this segment's local slot before reading. The caller is
// responsible for having acquired a read reference (see acquireRead) so the
// underlying file is not closed mid-read.
func (s *Segment) ReadAt(globalOffset uint64) ([]byte, error) {
	if globalOffset < s.baseOffset {
		return nil, fmt.Errorf("segment: offset %d below base %d", globalOffset, s.baseOffset)
	}
	localSlot := globalOffset - s.baseOffset
	return s.reader.ReadAt(localSlot)
}

// waitReaders blocks until all in-flight readers have released. Retention calls
// this AFTER detaching the segment from the live set (so no new readers can
// acquire it) and BEFORE Close()+Remove(), guaranteeing no read is cut off.
func (s *Segment) waitReaders() { s.readers.Wait() }

// Path returns the segment file's full path (used by retention to delete it).
func (s *Segment) Path() string { return s.path }

// Close releases the segment's file handles.
func (s *Segment) Close() error {
	rerr := s.reader.Close()
	werr := s.writer.Close()
	if werr != nil {
		return werr
	}
	return rerr
}
