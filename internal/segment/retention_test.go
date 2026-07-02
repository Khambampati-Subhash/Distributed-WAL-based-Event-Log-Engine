package segment

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is an injectable, goroutine-safe clock so retention tests are
// deterministic — we advance time by hand instead of waiting real days.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// retentionConfig builds a Config with retention manually driven (no background
// ticker) and a fake clock, so tests call RunRetention() explicitly.
func retentionConfig(dir string, clk *fakeClock) Config {
	return Config{
		Dir:              dir,
		MaxSegmentBytes:  32, // tiny → each few records roll a new segment
		Retention:        24 * time.Hour,
		DisableRetention: true, // we drive RunRetention() by hand
		clock:            clk.Now,
	}
}

// TestRetentionDeletesAgedSegments proves old segments (disk + memory) are
// removed once their last-append is past the retention window, and that the
// active segment is never deleted.
func TestRetentionDeletesAgedSegments(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_000_000, 0)}
	dir := t.TempDir()
	m, err := Open(retentionConfig(dir, clk))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// Append a batch "in the past" so it rolls into several segments.
	for i := 0; i < 8; i++ {
		if _, err := m.Append([]byte(fmt.Sprintf("old-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	segsBefore := m.SegmentCount()
	if segsBefore < 2 {
		t.Fatalf("need multiple segments to test retention, got %d", segsBefore)
	}

	// Jump 48h into the future, then append one fresh record (lands in a new
	// active segment stamped "now"). All earlier segments are now > 24h old.
	clk.Advance(48 * time.Hour)
	if _, err := m.Append([]byte("fresh")); err != nil {
		t.Fatal(err)
	}

	m.RunRetention()

	// Only the active (fresh) segment should survive.
	if got := m.SegmentCount(); got != 1 {
		t.Fatalf("expected 1 surviving segment (the active one), got %d", got)
	}
	if m.Metrics.SegmentsDeleted.Load() == 0 {
		t.Fatal("expected SegmentsDeleted > 0")
	}
	if m.Metrics.DeleteErrors.Load() != 0 {
		t.Fatalf("unexpected delete errors: %d", m.Metrics.DeleteErrors.Load())
	}

	// The deleted segments must be gone from DISK too, not just memory.
	entries, _ := os.ReadDir(dir)
	logFiles := 0
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".log" {
			logFiles++
		}
	}
	if logFiles != 1 {
		t.Fatalf("expected 1 .log file on disk, found %d", logFiles)
	}
}

// TestRetentionNeverDeletesActive ensures that even if the active segment is
// "old", it is never deleted (we're still writing to it).
func TestRetentionNeverDeletesActive(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_000_000, 0)}
	cfg := retentionConfig(t.TempDir(), clk)
	cfg.MaxSegmentBytes = 1 << 20 // large → everything stays in ONE (active) segment
	m, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	m.Append([]byte("only"))
	clk.Advance(72 * time.Hour) // active segment is now "ancient"
	m.RunRetention()

	if got := m.SegmentCount(); got != 1 {
		t.Fatalf("active segment must never be deleted; count=%d", got)
	}
	if got := m.Metrics.SegmentsDeleted.Load(); got != 0 {
		t.Fatalf("nothing should be deleted; SegmentsDeleted=%d", got)
	}
}

// TestStaleOffsetLoudReset proves a consumer whose offset was deleted gets a
// loud, typed error (not a silent EOF), and can reset to the earliest offset.
func TestStaleOffsetLoudReset(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_000_000, 0)}
	dir := t.TempDir()
	m, err := Open(retentionConfig(dir, clk))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	for i := 0; i < 8; i++ {
		m.Append([]byte(fmt.Sprintf("v%d", i)))
	}
	clk.Advance(48 * time.Hour)
	m.Append([]byte("fresh")) // fresh active segment
	m.RunRetention()          // deletes the old ones

	earliest := m.EarliestOffset()
	if earliest == 0 {
		t.Fatal("expected earliest offset to advance past 0 after retention")
	}

	// A consumer still sitting at offset 0 must get a LOUD error, not io.EOF.
	r := m.NewReader()
	r.Seek(0)
	_, _, err = r.Next()
	var oore *OffsetOutOfRetentionError
	if !errors.As(err, &oore) {
		t.Fatalf("expected *OffsetOutOfRetentionError at deleted offset, got %T: %v", err, err)
	}
	t.Logf("consumer correctly told it fell behind: %v", oore)

	// After resetting, it can read from the earliest surviving offset.
	reset := r.ResetToEarliest()
	if reset != earliest {
		t.Fatalf("reset to %d, want earliest %d", reset, earliest)
	}
	data, off, err := r.Next()
	if err != nil {
		t.Fatalf("read after reset failed: %v", err)
	}
	if off != earliest {
		t.Fatalf("post-reset read at offset %d, want %d", off, earliest)
	}
	_ = data
}

// TestRetentionConcurrentWithReadsAndWrites is the -race guard: retention runs
// repeatedly while producers append and consumers read. It must never race or
// cut off an in-flight read (thanks to the reader refcount).
func TestRetentionConcurrentWithReadsAndWrites(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_000_000, 0)}
	cfg := retentionConfig(t.TempDir(), clk)
	m, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Producer: append continuously, advancing the clock so old segments age out.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !stop.Load(); i++ {
			m.Append([]byte(fmt.Sprintf("event-%d", i)))
			clk.Advance(time.Hour) // each append moves time forward
		}
	}()

	// Readers: continuously read at the current earliest offset (racing with
	// retention deleting it). A stale-offset error is fine; a race/panic is not.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				off := m.EarliestOffset()
				_, _ = m.ReadAt(off) // may succeed, EOF, or OffsetOutOfRetention — all OK
			}
		}()
	}

	// Retention: run passes in a tight loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			m.RunRetention()
		}
	}()

	time.Sleep(200 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	// Sanity: the log is still functional and bounded (retention actually ran).
	if m.Metrics.Runs.Load() == 0 {
		t.Fatal("retention never ran")
	}
	t.Logf("deleted %d segments, %d errors, %d runs",
		m.Metrics.SegmentsDeleted.Load(), m.Metrics.DeleteErrors.Load(), m.Metrics.Runs.Load())
}
