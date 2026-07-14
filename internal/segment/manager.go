package segment

import (
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults applied when Config leaves a field unset.
const (
	DefaultMaxSegmentBytes  = 1 << 30 // 1 GiB
	DefaultRetention        = 7 * 24 * time.Hour
	DefaultCheckInterval    = 1 * time.Minute
	DefaultMaxDeletesPerRun = 0 // 0 == unlimited
)

// Config controls the log directory and segment behavior. It is set once at
// startup (the user configures it when constructing the manager).
type Config struct {
	Dir             string // directory holding the .log segment files
	MaxSegmentBytes int64  // roll to a new segment once the active one exceeds this

	// Retention (Phase 4). Zero values fall back to the Default* constants.
	Retention        time.Duration // delete segments whose last-append is older than this
	CheckInterval    time.Duration // how often the retention goroutine runs
	MaxDeletesPerRun int           // cap deletes per run (0 = unlimited) to smooth I/O
	DisableRetention bool          // if true, no background retention goroutine starts

	// clock is injectable for deterministic tests; nil means time.Now.
	clock func() time.Time
}

func (c *Config) now() time.Time {
	if c.clock != nil {
		return c.clock()
	}
	return time.Now()
}

// WithClock sets an injectable time source (for demos/tests that need to
// control segment age without waiting real time). Returns the config for
// chaining. In production, leave it unset and time.Now is used.
func (c Config) WithClock(clock func() time.Time) Config {
	c.clock = clock
	return c
}

// RetentionMetrics holds counters an operator can inspect to see whether
// retention is healthy (all fields read/written atomically).
type RetentionMetrics struct {
	SegmentsDeleted atomic.Int64 // total segments successfully removed
	BytesReclaimed  atomic.Int64 // total bytes freed
	DeleteErrors    atomic.Int64 // failed os.Remove attempts (the "something's wrong" signal)
	Runs            atomic.Int64 // how many times the retention loop has run
	LastRunUnixNano atomic.Int64 // wall-clock of the last run (liveness)
}

// OffsetOutOfRetentionError means the requested offset was already deleted by
// retention — the consumer fell behind the retention window. It carries the
// earliest offset still available so the caller can reset to it (loudly, not
// silently: the caller KNOWS it skipped Earliest-Requested records).
type OffsetOutOfRetentionError struct {
	Requested uint64 // the offset the consumer asked for
	Earliest  uint64 // the earliest offset still stored
}

func (e *OffsetOutOfRetentionError) Error() string {
	return fmt.Sprintf("segment: offset %d out of retention; earliest available is %d (skipped %d records)",
		e.Requested, e.Earliest, e.Earliest-e.Requested)
}

// Manager owns the ordered set of segments that together form one logical log.
// Appends go to the single ACTIVE (last) segment; when it exceeds the size
// threshold it is sealed and a new active segment begins. Reads are routed to
// whichever segment owns the requested offset.
type Manager struct {
	mu       sync.RWMutex // guards segments + active; RWMutex so reads don't serialize
	cfg      Config
	segments []*Segment // ordered by base offset; last element is active

	Metrics RetentionMetrics // observability for the retention loop

	stop     chan struct{}  // closed by Close() to stop the retention goroutine
	stopOnce sync.Once      // ensures the stop channel is closed at most once
	wg       sync.WaitGroup // waits for the retention goroutine to exit on Close()
}

type ManagerInterface interface {
	Append(data []byte) (uint64, error)
	ReadAt(offset uint64) ([]byte, error)
	NextOffset() uint64
	EarliestOffset() uint64
	SegmentCount() int
	Close() error
	RunRetention()
}

// Open creates or reopens a segmented log in cfg.Dir. It discovers any existing
// segment files, rebuilds their indexes (CRC-verified, via the WAL layer), and
// marks the highest-base segment active. An empty/new directory starts with a
// single segment at base offset 0.
func Open(cfg Config) (*Manager, error) {
	if cfg.MaxSegmentBytes <= 0 {
		cfg.MaxSegmentBytes = DefaultMaxSegmentBytes
	}
	if cfg.Retention <= 0 {
		cfg.Retention = DefaultRetention
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = DefaultCheckInterval
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("segment: mkdir %q: %w", cfg.Dir, err)
	}

	m := &Manager{cfg: cfg, stop: make(chan struct{})}

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
	} else {
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
	}

	if !cfg.DisableRetention {
		m.wg.Add(1)
		go m.retentionLoop()
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
	return m.active().Append(data, m.cfg.now())
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
//
// We hold the lock only long enough to pick the segment, then release it before
// the (slow) disk read — so readers don't block the writer or each other on I/O.
// The chosen Segment's own file handle is safe for concurrent positional reads.
func (m *Manager) ReadAt(offset uint64) ([]byte, error) {
	m.mu.RLock()
	seg := m.segmentFor(offset)
	if seg != nil {
		// Acquire a read reference while still holding the lock, so it is
		// atomic with the segment being a live member. Retention detaches
		// under the same lock and then waits for readers to drain, so it can
		// never Close() this file while we hold the reference.
		seg.acquireRead()
	}
	earliest := m.segments[0].BaseOffset()
	m.mu.RUnlock()

	if seg == nil {
		// Two very different "no segment" cases:
		if offset < earliest {
			// The data this consumer wants was already DELETED by retention.
			// This is NOT "caught up" — the consumer fell behind the window.
			// Surface it loudly so the caller can reset (see ErrOffsetOutOfRetention).
			return nil, &OffsetOutOfRetentionError{Requested: offset, Earliest: earliest}
		}
		return nil, io.EOF // offset is at/beyond the head: legitimately nothing yet
	}
	defer seg.releaseRead()
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

// retentionLoop runs in the background, deleting aged-out segments on a ticker
// until Close() stops it. Each tick runs one RunRetention pass.
func (m *Manager) retentionLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
			m.RunRetention()
		}
	}
}

// RunRetention performs one retention pass: delete every segment whose data has
// aged past cfg.Retention, oldest-first, but never the active segment. It is
// exported so tests can trigger a deterministic pass without waiting for the
// ticker. Safe to call concurrently with Append/ReadAt.
//
// The work is split in two phases to avoid blocking the core:
//
//	Phase A (fast, under lock): decide which segments are victims and DETACH
//	  them from the live slice — pure in-memory pointer surgery.
//	Phase B (slow, NO lock):    wait for in-flight readers to drain, then
//	  Close()+os.Remove() each victim. Disk I/O happens with the lock released,
//	  so producers/consumers are never blocked on deletion.
func (m *Manager) RunRetention() {
	m.Metrics.Runs.Add(1)
	m.Metrics.LastRunUnixNano.Store(m.cfg.now().UnixNano())

	cutoff := m.cfg.now().Add(-m.cfg.Retention)

	// ---- Phase A: decide + detach (fast, under lock) ----
	m.mu.Lock()
	// Segments are ordered oldest-first; the active (last) segment is never a
	// victim. Walk the prefix and collect those whose last-append < cutoff.
	// Stop at the first segment that is NOT expired: because segments are
	// append-time ordered, nothing after a live segment can be older.
	var victims []*Segment
	limit := m.cfg.MaxDeletesPerRun
	for i := 0; i < len(m.segments)-1; i++ { // len-1 => never the active segment
		seg := m.segments[i]
		if seg.LastAppend().After(cutoff) {
			break // this and all later segments are still within the window
		}
		if limit > 0 && len(victims) >= limit {
			break // respect the per-run cap to smooth I/O
		}
		victims = append(victims, seg)
	}
	if len(victims) > 0 {
		// Detach the victim prefix in one shot; readers can no longer route to them.
		m.segments = m.segments[len(victims):]
	}
	m.mu.Unlock()

	// ---- Phase B: act (slow, NO lock held) ----
	for _, seg := range victims {
		seg.waitReaders() // let any in-flight ReadAt finish (no read is cut off)
		size := seg.Size()
		if err := seg.Close(); err != nil {
			m.Metrics.DeleteErrors.Add(1)
			log.Printf("segment: retention close %s: %v", seg.Path(), err)
			continue
		}
		if err := os.Remove(seg.Path()); err != nil {
			m.Metrics.DeleteErrors.Add(1)
			log.Printf("segment: retention remove %s: %v", seg.Path(), err)
			continue
		}
		if err := os.Remove(seg.IndexPath()); err != nil && !os.IsNotExist(err) {
			m.Metrics.DeleteErrors.Add(1)
			log.Printf("segment: retention remove index %s: %v", seg.IndexPath(), err)
		}
		m.Metrics.SegmentsDeleted.Add(1)
		m.Metrics.BytesReclaimed.Add(size)
	}
}

// Close stops the retention goroutine and closes every segment.
func (m *Manager) Close() error {
	m.stopOnce.Do(func() { close(m.stop) })
	m.wg.Wait() // ensure retention isn't mutating segments while we close them

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
