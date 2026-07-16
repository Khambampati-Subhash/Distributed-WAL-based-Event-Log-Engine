package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/inmemorystore"
)

func writeRecords(t *testing.T, path string, payloads ...string) {
	t.Helper()
	w, store := newTestWriter(t, path)
	for _, p := range payloads {
		if _, err := w.Write([]byte(p)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	store.Close()
}

// TestCRCDetectsBitRotOnRead verifies the post-checkpoint-recovery guarantee:
// startup no longer full-scans the WAL (it trusts the sparse checkpoints), so
// bit-rot in a complete record is caught on READ rather than at reopen. The
// reopen itself succeeds; the corruption surfaces when the record is read.
func TestCRCDetectsBitRotOnRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	writeRecords(t, path, "hello", "world", "third")

	// Corrupt a byte inside the third record (offset 2). Each record is 13 bytes
	// (8 header + 5 payload); record 2 begins at byte 26, so byte 30 lands in it.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], 30); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], 30); err != nil {
		t.Fatal(err)
	}
	f.Close()

	idxPath := path + ".index"
	store, err := inmemorystore.NewInMemoryStore(idxPath, NthIndex)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Reopen now succeeds — recovery no longer verifies CRCs.
	if _, err := NewWalWriter(path, store); err != nil {
		t.Fatalf("reopen should succeed (recovery does not CRC-scan), got: %v", err)
	}

	r, err := NewWALReader(path, store)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, err = r.ReadAt(2)
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CorruptionError reading corrupted offset 2, got %T: %v", err, err)
	}
	t.Logf("correctly detected corruption on read: %v", ce)
}

func TestTornTailIsTruncatedNotCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	writeRecords(t, path, "alpha", "beta", "gamma")

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	w, store := newTestWriter(t, path)
	defer w.Close()
	defer store.Close()
	if got := w.NextOffset(); got != 2 {
		t.Fatalf("expected 2 surviving records after torn-tail truncation, got %d", got)
	}
}

func TestCRCMatchesOnCleanRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	w, store := newTestWriter(t, path)
	defer w.Close()
	defer store.Close()
	if _, err := w.Write([]byte("payload-with-crc")); err != nil {
		t.Fatal(err)
	}

	r, err := NewWALReader(path, store)
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

func TestReadAtDetectsRuntimeCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	w, store := newTestWriter(t, path)
	defer w.Close()
	defer store.Close()
	if _, err := w.Write([]byte("good-data-here")); err != nil {
		t.Fatal(err)
	}

	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	var b [1]byte
	f.ReadAt(b[:], 10)
	b[0] ^= 0xFF
	f.WriteAt(b[:], 10)
	f.Close()

	r, err := NewWALReader(path, store)
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
