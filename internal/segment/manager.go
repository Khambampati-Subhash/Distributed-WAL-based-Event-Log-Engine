package segment

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DefaultMaxSegmentBytes is the roll threshold if Config leaves it unset.
const DefaultMaxSegmentBytes = 1 << 30 // 1 GiB

// Config controls the log directory and segment behavior. It is set once at
// startup (the user configures it when constructing the manager).
type Config struct {
	Dir             string // directory holding the .log segment files
	MaxSegmentBytes int64  // roll to a new segment once the active one exceeds this
}

// Manager owns the ordered set of segments that together form one logical log.
// Appends go to the single ACTIVE (last) segment; when it exceeds the size
// threshold it is sealed and a new active segment begins. Reads are routed to
// whichever segment owns the requested offset.
type Manager struct {
	mu       sync.RWMutex // guards segments + active; RWMutex so reads don't serialize
	cfg      Config
	segments []*Segment // ordered by base offset; last element is active
}

// Open creates or reopens a segmented log in cfg.Dir. It discovers any existing
// segment files, rebuilds their indexes (CRC-verified, via the WAL layer), and
// marks the highest-base segment active. An empty/new directory starts with a
// single segment at base offset 0.
func Open(cfg Config) (*Manager, error) {
	if cfg.MaxSegmentBytes <= 0 {
		cfg.MaxSegmentBytes = DefaultMaxSegmentBytes
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("segment: mkdir %q: %w", cfg.Dir, err)
	}

	m := &Manager{cfg: cfg}

	bases, err := discoverBaseOffsets(cfg.Dir)
	if err != nil {
		return nil, err
	}

	if len(bases) == 0 {
		// Fresh log: one empty segment starting at offset 0.
		seg, err := NewSegment(cfg.Dir, 0)
		if err != nil {
			return nil, err
		}
		m.segments = []*Segment{seg}
		return m, nil
	}

	// Reopen every existing segment in base-offset order; each rebuilds its
	// own index from disk. The last one (highest base) becomes active.
	for _, base := range bases {
		seg, err := NewSegment(cfg.Dir, base)
		if err != nil {
			m.closeAll()
			return nil, err
		}
		m.segments = append(m.segments, seg)
	}
	return m, nil
}

// discoverBaseOffsets lists *.log files in dir and parses their base offsets,
// returning them sorted ascending.
func discoverBaseOffsets(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("segment: read dir %q: %w", dir, err)
	}
	var bases []uint64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, segmentSuffix) {
			continue
		}
		baseStr := strings.TrimSuffix(name, segmentSuffix)
		base, err := strconv.ParseUint(baseStr, 10, 64)
		if err != nil {
			// Not a segment file we recognize; skip rather than fail.
			continue
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

// active returns the current writable segment (the last one). Caller holds mu.
func (m *Manager) active() *Segment {
	return m.segments[len(m.segments)-1]
}

// Append writes one opaque record to the active segment and returns its global
// offset, rolling to a new segment first if the active one is full.
func (m *Manager) Append(data []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Roll BEFORE writing if the active segment is already at/over the limit.
	// (Rolling before, not after, keeps each segment <= MaxSegmentBytes + one record.)
	if m.active().Size() >= m.cfg.MaxSegmentBytes {
		if err := m.roll(); err != nil {
			return 0, err
		}
	}
	return m.active().Append(data)
}

// roll seals the active segment and starts a new one whose base offset is the
// next global offset. Caller holds mu.
func (m *Manager) roll() error {
	nextBase := m.active().NextOffset()
	seg, err := NewSegment(m.cfg.Dir, nextBase)
	if err != nil {
		return fmt.Errorf("segment: roll to base %d: %w", nextBase, err)
	}
	m.segments = append(m.segments, seg)
	return nil
}

// ReadAt returns the payload at the given global offset by routing to the
// segment that owns it. Returns io.EOF if the offset has not been written yet.
func (m *Manager) ReadAt(offset uint64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seg := m.segmentFor(offset)
	if seg == nil {
		return nil, io.EOF // beyond the end of the log (or below earliest)
	}
	return seg.ReadAt(offset)
}

// segmentFor returns the segment owning the given global offset, or nil if no
// segment contains it. Caller holds (at least) RLock.
//
// Segments are sorted by base offset, so we binary-search for the last segment
// whose base <= offset, then confirm the offset is within its range.
func (m *Manager) segmentFor(offset uint64) *Segment {
	// Find the rightmost segment with baseOffset <= offset.
	i := sort.Search(len(m.segments), func(i int) bool {
		return m.segments[i].BaseOffset() > offset
	}) - 1
	if i < 0 {
		return nil // offset is below the earliest segment's base
	}
	seg := m.segments[i]
	if !seg.Contains(offset) {
		return nil // within the gap past this segment but not yet written
	}
	return seg
}

// NextOffset is the global offset the next Append will assign (== total records
// across all segments, assuming offset 0 still exists).
func (m *Manager) NextOffset() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.active().NextOffset()
}

// EarliestOffset is the lowest global offset still stored (base of the first
// segment). Useful once retention can delete the earliest segments.
func (m *Manager) EarliestOffset() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.segments[0].BaseOffset()
}

// SegmentCount returns how many segment files currently make up the log.
func (m *Manager) SegmentCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.segments)
}

// Close closes every segment.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeAll()
}

// closeAll closes all segments, returning the first error. Caller holds mu.
func (m *Manager) closeAll() error {
	var firstErr error
	for _, seg := range m.segments {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
