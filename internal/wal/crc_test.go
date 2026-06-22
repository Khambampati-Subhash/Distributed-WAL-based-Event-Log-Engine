package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// writeRecords is a helper: open a fresh log, append the given payloads, close.
func writeRecords(t *testing.T, path string, payloads ...string) {
	t.Helper()
	w, err := NewWalWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range payloads {
		if _, err := w.Write([]byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestCRCDetectsBitRot is the headline Phase-2 test: it MANUFACTURES corruption
// by flipping a byte in the middle of a complete record, then proves the engine
// refuses to load it (instead of silently returning bad data).
func TestCRCDetectsBitRot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	writeRecords(t, path, "hello", "world", "third")

	// Corrupt a byte deep in the file (inside a payload, not the torn tail).
	// Record layout: [len:4][crc:4]["hello"] = 13 bytes; flip a byte at offset 30,
	// which lands inside the second/third record's payload region.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], 30); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF // flip all bits in this byte
	if _, err := f.WriteAt(b[:], 30); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Reopening must FAIL with a CorruptionError — not succeed silently.
	_, err = NewWalWriter(path)
	if err == nil {
		t.Fatal("expected corruption to be detected on reopen, got nil error")
	}
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CorruptionError, got %T: %v", err, err)
	}
	t.Logf("correctly detected corruption: %v", ce)
}

// TestTornTailIsTruncatedNotCorruption proves the OTHER branch of the decision
// tree: a partial record at EOF (a crash mid-write) is treated as a torn tail —
// truncated, no error — while the valid records before it survive.
func TestTornTailIsTruncatedNotCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	writeRecords(t, path, "alpha", "beta", "gamma")

	// Simulate a crash mid-write: chop the last few bytes off the file so the
	// final record is incomplete.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	// Reopen: should succeed (torn tail truncated), keeping the 2 whole records.
	w, err := NewWalWriter(path)
	if err != nil {
		t.Fatalf("torn tail should be truncated cleanly, got error: %v", err)
	}
	defer w.Close()
	if got := w.NextOffset(); got != 2 {
		t.Fatalf("expected 2 surviving records after torn-tail truncation, got %d", got)
	}
}

// TestCRCMatchesOnCleanRoundTrip is the happy path: write then read back, CRC
// must match (no false positives).
func TestCRCMatchesOnCleanRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	w, err := NewWalWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write([]byte("payload-with-crc")); err != nil {
		t.Fatal(err)
	}

	r, err := NewWALReader(path, w)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	data, err := r.ReadAt(0)
	if err != nil {
		t.Fatalf("clean read should not error, got: %v", err)
	}
	if string(data) != "payload-with-crc" {
		t.Fatalf("got %q", data)
	}
}

// TestReadAtDetectsRuntimeCorruption proves CRC is verified on EVERY read, not
// just at startup: we corrupt the file AFTER the engine has booted and indexed
// it, then read — the read must surface a CorruptionError.
func TestReadAtDetectsRuntimeCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	w, err := NewWalWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write([]byte("good-data-here")); err != nil {
		t.Fatal(err)
	}

	// Corrupt the payload on disk while the engine is running (index already built).
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	var b [1]byte
	f.ReadAt(b[:], 10) // inside the payload
	b[0] ^= 0xFF
	f.WriteAt(b[:], 10)
	f.Close()

	r, err := NewWALReader(path, w)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, err = r.ReadAt(0)
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CorruptionError on runtime read, got %T: %v", err, err)
	}
	t.Logf("runtime read correctly detected corruption: %v", ce)
}
