package segment

import (
	"fmt"
	"testing"
)

// tinyConfig uses a very small segment size so a handful of records forces
// multiple rolls — that's what we want to exercise.
func tinyConfig(dir string) Config {
	return Config{Dir: dir, MaxSegmentBytes: 32} // ~1-2 small records per segment
}

// TestRollsIntoMultipleSegments proves appends roll across many files and that
// every offset is readable from whichever segment owns it.
func TestRollsIntoMultipleSegments(t *testing.T) {
	m, err := Open(tinyConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	const n = 10
	for i := 0; i < n; i++ {
		off, err := m.Append([]byte(fmt.Sprintf("event-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if off != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, off)
		}
	}

	// With a 32-byte cap and ~15-byte records, we must have rolled several times.
	if m.SegmentCount() < 2 {
		t.Fatalf("expected multiple segments, got %d", m.SegmentCount())
	}
	t.Logf("10 records spread across %d segments", m.SegmentCount())

	// Every offset must read back correctly regardless of which segment it's in.
	for i := 0; i < n; i++ {
		data, err := m.ReadAt(uint64(i))
		if err != nil {
			t.Fatalf("read offset %d: %v", i, err)
		}
		want := fmt.Sprintf("event-%d", i)
		if string(data) != want {
			t.Fatalf("offset %d: got %q want %q", i, data, want)
		}
	}
}

// TestRecoverManySegments proves a restart rediscovers all segment files,
// rebuilds their indexes, and continues appending at the correct next offset.
func TestRecoverManySegments(t *testing.T) {
	dir := t.TempDir()

	m, err := Open(tinyConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err := m.Append([]byte(fmt.Sprintf("rec-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	segCountBefore := m.SegmentCount()
	m.Close()

	// Reopen the SAME directory.
	m2, err := Open(tinyConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	if m2.SegmentCount() != segCountBefore {
		t.Fatalf("recovery: segment count %d != %d before", m2.SegmentCount(), segCountBefore)
	}
	if m2.NextOffset() != 6 {
		t.Fatalf("recovery: next offset %d, want 6", m2.NextOffset())
	}

	// Old data is still readable...
	data, err := m2.ReadAt(0)
	if err != nil || string(data) != "rec-0" {
		t.Fatalf("recovered read offset 0: %q %v", data, err)
	}

	// ...and new appends continue from offset 6.
	off, err := m2.Append([]byte("rec-6"))
	if err != nil {
		t.Fatal(err)
	}
	if off != 6 {
		t.Fatalf("post-recovery append got offset %d, want 6", off)
	}
}

// TestReadPastEndIsEOF confirms an unwritten offset returns io.EOF.
func TestReadPastEndIsEOF(t *testing.T) {
	m, err := Open(tinyConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.Append([]byte("only-one"))

	if _, err := m.ReadAt(99); err == nil {
		t.Fatal("expected error reading unwritten offset 99, got nil")
	}
}

// TestSegmentFileName checks the zero-padded base-offset naming.
func TestSegmentFileName(t *testing.T) {
	cases := map[uint64]string{
		0:    "00000000000000000000.log",
		1000: "00000000000000001000.log",
	}
	for base, want := range cases {
		if got := SegmentFileName(base); got != want {
			t.Fatalf("SegmentFileName(%d) = %q, want %q", base, got, want)
		}
	}
}
