// Command retention demonstrates Phase 4: time-based retention of segments.
//
// It appends enough events to roll several segment files, advances a (fake)
// clock past the retention window, runs a retention pass, and shows:
//   - old segment files deleted from disk + memory (never the active one)
//   - metrics: segments deleted, bytes reclaimed
//   - a consumer sitting on a now-deleted offset getting a LOUD reset, not a
//     silent EOF, then resuming from the earliest surviving offset
//
// The clock is injected so we don't have to wait 7 real days.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/segment"
)

// fakeClock lets the demo fast-forward time.
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

func main() {
	dir, err := os.MkdirTemp("", "wal-retention-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	cfg := segment.Config{
		Dir:              dir,
		MaxSegmentBytes:  40,             // tiny → a handful of events per segment
		Retention:        24 * time.Hour, // keep only ~1 day
		DisableRetention: true,           // drive RunRetention() by hand for the demo
	}.WithClock(clk.Now)

	m, err := segment.Open(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close()

	// ---- append events that roll across several segments ----
	fmt.Println("appending 12 events (old)...")
	for i := 0; i < 12; i++ {
		if _, err := m.Append([]byte(fmt.Sprintf("event-%d", i))); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("  -> %d segment files, earliest offset=%d, head=%d\n",
		m.SegmentCount(), m.EarliestOffset(), m.NextOffset())

	// ---- time passes; append one fresh event into a new active segment ----
	fmt.Println("\nadvancing clock 48h, appending 1 fresh event...")
	clk.Advance(48 * time.Hour)
	m.Append([]byte("fresh-event"))

	// ---- run retention ----
	fmt.Println("\nrunning retention (delete segments older than 24h)...")
	m.RunRetention()
	fmt.Printf("  -> segments now=%d, earliest offset=%d\n", m.SegmentCount(), m.EarliestOffset())
	fmt.Printf("  -> metrics: deleted=%d, bytesReclaimed=%d, errors=%d\n",
		m.Metrics.SegmentsDeleted.Load(),
		m.Metrics.BytesReclaimed.Load(),
		m.Metrics.DeleteErrors.Load())

	// ---- a consumer still at offset 0 fell behind: LOUD reset, not silent EOF ----
	fmt.Println("\na slow consumer still sitting at offset 0 tries to read...")
	r := m.NewReader()
	r.Seek(0)
	_, _, err = r.Next()
	var oore *segment.OffsetOutOfRetentionError
	if errors.As(err, &oore) {
		fmt.Printf("  -> LOUD signal: %v\n", oore)
		reset := r.ResetToEarliest()
		fmt.Printf("  -> consumer resets to earliest offset=%d and resumes\n", reset)
		data, off, err := r.Next()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  -> read offset=%d event=%q\n", off, data)
	} else {
		fmt.Printf("  (unexpected: %v)\n", err)
	}

	fmt.Println("\nRetention demo complete.")
}
