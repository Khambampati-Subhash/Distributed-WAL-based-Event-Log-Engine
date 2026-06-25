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
	"path/filepath"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/wal"
)

// segmentSuffix is appended to the zero-padded base offset to form a filename.
const segmentSuffix = ".log"

// baseOffsetDigits is the zero-padding width. 20 digits holds any uint64
// (max uint64 is 20 digits), so filenames sort lexicographically by base offset.
const baseOffsetDigits = 20

// Segment is one segment file plus the global base offset of its first record.
type Segment struct {
	baseOffset uint64          // global offset of the first record in this file
	writer     *wal.WALWriter  // the underlying durable, CRC-checked file
	reader     *wal.WALReader  // read handle into the same file
	path       string          // full path to the .log file
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

	return &Segment{
		baseOffset: baseOffset,
		writer:     writer,
		reader:     reader,
		path:       path,
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
// at (baseOffset + local slot).
func (s *Segment) Append(data []byte) (uint64, error) {
	localSlot, err := s.writer.Write(data)
	if err != nil {
		return 0, err
	}
	return s.baseOffset + localSlot, nil
}

// ReadAt returns the payload at the given GLOBAL offset. It translates the
// global offset to this segment's local slot before reading.
func (s *Segment) ReadAt(globalOffset uint64) ([]byte, error) {
	if globalOffset < s.baseOffset {
		return nil, fmt.Errorf("segment: offset %d below base %d", globalOffset, s.baseOffset)
	}
	localSlot := globalOffset - s.baseOffset
	return s.reader.ReadAt(localSlot)
}

// Close releases the segment's file handles.
func (s *Segment) Close() error {
	rerr := s.reader.Close()
	werr := s.writer.Close()
	if werr != nil {
		return werr
	}
	return rerr
}
