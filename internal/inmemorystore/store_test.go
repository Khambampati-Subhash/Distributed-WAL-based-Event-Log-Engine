package inmemorystore

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func newStore(t *testing.T, nth uint32) (*InMemoryStore, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seg.index")
	s, err := NewInMemoryStore(path, nth)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.LoadCheckpoints(); err != nil {
		t.Fatal(err)
	}
	return s, path
}

func TestCheckpointCadence(t *testing.T) {
	s, _ := newStore(t, 3) // checkpoint every 3rd record
	defer s.Close()

	// 9 records → checkpoints at offsets 2, 5, 8 (the 3rd, 6th, 9th call).
	for off := uint64(0); off < 9; off++ {
		if err := s.Checkpoint(off, int64(off*100)); err != nil {
			t.Fatal(err)
		}
	}
	want := []uint64{2, 5, 8}
	if len(s.offsets) != len(want) {
		t.Fatalf("got %d checkpoints %v, want %v", len(s.offsets), s.offsets, want)
	}
	for i, w := range want {
		if s.offsets[i] != w {
			t.Fatalf("checkpoint %d = %d, want %d", i, s.offsets[i], w)
		}
	}
}

func TestFloor(t *testing.T) {
	s, _ := newStore(t, 3)
	defer s.Close()
	for off := uint64(0); off < 9; off++ {
		if err := s.Checkpoint(off, int64(off*100)); err != nil {
			t.Fatal(err)
		}
	}
	// checkpoints at 2(pos200), 5(pos500), 8(pos800)
	cases := []struct {
		target  uint64
		wantOff uint64
		wantPos int64
		wantOK  bool
	}{
		{0, 0, 0, false},    // below first checkpoint
		{1, 0, 0, false},    // still below
		{2, 2, 200, true},   // exactly the first checkpoint
		{4, 2, 200, true},   // between 2 and 5 → floor is 2
		{5, 5, 500, true},   // exactly
		{7, 5, 500, true},   // between 5 and 8
		{100, 8, 800, true}, // above last → floor is last
	}
	for _, c := range cases {
		off, pos, ok := s.Floor(c.target)
		if ok != c.wantOK || (ok && (off != c.wantOff || pos != c.wantPos)) {
			t.Errorf("Floor(%d) = (%d,%d,%v), want (%d,%d,%v)",
				c.target, off, pos, ok, c.wantOff, c.wantPos, c.wantOK)
		}
	}
}

func TestCheckpointsPersistAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.index")
	s1, err := NewInMemoryStore(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.LoadCheckpoints(); err != nil {
		t.Fatal(err)
	}
	for off := uint64(0); off < 6; off++ {
		if err := s1.Checkpoint(off, int64(off*10)); err != nil {
			t.Fatal(err)
		}
	}
	s1.Close()

	// Reopen: checkpoints must survive on disk and reload identically.
	s2, err := NewInMemoryStore(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if err := s2.LoadCheckpoints(); err != nil {
		t.Fatal(err)
	}
	off, pos, ok := s2.Floor(100)
	if !ok || off != 5 || pos != 50 {
		t.Fatalf("after reload, Floor(100) = (%d,%d,%v), want (5,50,true)", off, pos, ok)
	}
}

// TestNonMonotonicIndexTruncated covers the edge case where — because the index
// is written without its own fsync — a crash could leave a partially-flushed or
// garbage entry in the middle. LoadCheckpoints must keep only the valid strictly
// ascending prefix and self-heal the file, so Floor never returns a bogus entry.
func TestNonMonotonicIndexTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.index")

	// Hand-write three entries; the third goes BACKWARDS in offset (corruption).
	entries := []struct {
		off uint64
		pos int64
	}{
		{10, 100},
		{20, 200},
		{15, 150}, // non-monotonic: must be discarded along with anything after
	}
	var raw []byte
	for _, e := range entries {
		var b [indexEntryBytes]byte
		binary.BigEndian.PutUint64(b[0:8], e.off)
		binary.BigEndian.PutUint64(b[8:16], uint64(e.pos))
		raw = append(raw, b[:]...)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := NewInMemoryStore(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.LoadCheckpoints(); err != nil {
		t.Fatal(err)
	}

	if len(s.offsets) != 2 {
		t.Fatalf("expected valid prefix of 2 checkpoints, got %d (%v)", len(s.offsets), s.offsets)
	}
	off, pos, ok := s.Floor(1000)
	if !ok || off != 20 || pos != 200 {
		t.Fatalf("Floor(1000) = (%d,%d,%v), want (20,200,true)", off, pos, ok)
	}
	// The file must be self-healed to exactly the valid prefix.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 2*indexEntryBytes {
		t.Fatalf("index file size %d, want %d after self-heal", info.Size(), 2*indexEntryBytes)
	}
}

func TestTornIndexEntryTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.index")
	s1, err := NewInMemoryStore(path, 1) // checkpoint every record
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.LoadCheckpoints(); err != nil {
		t.Fatal(err)
	}
	if err := s1.Checkpoint(0, 0); err != nil {
		t.Fatal(err)
	}
	if err := s1.Checkpoint(1, 100); err != nil {
		t.Fatal(err)
	}
	s1.Close()

	// Simulate a crash mid-write: append 5 stray bytes (a partial 16-byte entry).
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := NewInMemoryStore(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if err := s2.LoadCheckpoints(); err != nil {
		t.Fatal(err)
	}
	// The two whole checkpoints survive; the torn tail is dropped.
	if len(s2.offsets) != 2 {
		t.Fatalf("expected 2 checkpoints after torn-tail truncation, got %d", len(s2.offsets))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size()%indexEntryBytes != 0 {
		t.Fatalf("index file size %d not aligned to %d after load", info.Size(), indexEntryBytes)
	}
}
