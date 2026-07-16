package inmemorystore

import (
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
