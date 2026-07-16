package consumeroffset

import (
	"path/filepath"
	"testing"
)

func TestWriteReadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "consumer.offset")
	w := NewOffsetWriter(path, 1)
	if err := w.Write(42); err != nil {
		t.Fatal(err)
	}
	got, err := NewOffsetReader(path).Read()
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("read back %d, want 42", got)
	}
}

func TestReadMissingFileIsZero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "never-written.offset")
	got, err := NewOffsetReader(path).Read()
	if err != nil {
		t.Fatalf("missing file should not error, got %v", err)
	}
	if got != 0 {
		t.Fatalf("missing file should read as 0, got %d", got)
	}
}

// TestBatchWriteFlushesOnJump is the regression test for the fixed bug: with the
// old exact-equality check, an offset that advanced past the boundary in a jump
// would never persist. With >=, it flushes.
func TestBatchWriteFlushesOnJump(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch.offset")
	w := NewOffsetWriter(path, 5) // flush every 5 records of progress

	// Advance in a jump of 8 (past the boundary of 5) on the very first commit.
	if err := w.BatchWrite(8); err != nil {
		t.Fatal(err)
	}
	got, err := NewOffsetReader(path).Read()
	if err != nil {
		t.Fatal(err)
	}
	if got != 8 {
		t.Fatalf("batch write should have flushed offset 8, got %d", got)
	}
}

func TestBatchWriteSkipsUntilThreshold(t *testing.T) {
	path := filepath.Join(t.TempDir(), "batch.offset")
	w := NewOffsetWriter(path, 10)

	// Below the threshold: nothing should be written yet, so the file is absent
	// and the reader returns 0.
	for off := uint64(1); off < 10; off++ {
		if err := w.BatchWrite(off); err != nil {
			t.Fatal(err)
		}
	}
	got, err := NewOffsetReader(path).Read()
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("no flush expected below threshold, but read %d", got)
	}

	// Crossing the threshold flushes.
	if err := w.BatchWrite(10); err != nil {
		t.Fatal(err)
	}
	got, err = NewOffsetReader(path).Read()
	if err != nil {
		t.Fatal(err)
	}
	if got != 10 {
		t.Fatalf("expected flush at offset 10, got %d", got)
	}
}
