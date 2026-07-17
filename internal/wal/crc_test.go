package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/checksum"
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
	if _, err := NewWalWriter(path, store, testChecksum()); err != nil {
		t.Fatalf("reopen should succeed (recovery does not CRC-scan), got: %v", err)
	}

	r, err := NewWALReader(path, store, testChecksum())
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

	r, err := NewWALReader(path, store, testChecksum())
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

// TestSwappableChecksumSHA256 proves the checksum is decoupled: the WAL works
// unchanged with SHA-256 (a 32-byte header instead of CRC32C's 4), and still
// round-trips, recovers, and detects corruption — all via the Checksum interface.
func TestSwappableChecksumSHA256(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	idxPath := path + ".index"
	sha := checksum.NewSHA256()

	store, err := inmemorystore.NewInMemoryStore(idxPath, NthIndex)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWalWriter(path, store, sha)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"alpha", "beta", "gamma"} {
		if _, err := w.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	store.Close()

	// Reopen with SHA-256 and read back — recovery must parse the 32-byte header.
	store2, err := inmemorystore.NewInMemoryStore(idxPath, NthIndex)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	w2, err := NewWalWriter(path, store2, sha)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if got := w2.NextOffset(); got != 3 {
		t.Fatalf("recovered NextOffset = %d, want 3", got)
	}

	r, err := NewWALReader(path, store2, sha)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := r.ReadAt(1)
	if err != nil {
		t.Fatalf("SHA-256 read should succeed, got: %v", err)
	}
	if string(got) != "beta" {
		t.Fatalf("read %q, want %q", got, "beta")
	}

	// Corrupt a payload byte and confirm the SHA-256 checksum catches it on read.
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	// Record 0 begins at 0; its payload starts after a 4+32 = 36-byte header.
	var b [1]byte
	f.ReadAt(b[:], 37)
	b[0] ^= 0xFF
	f.WriteAt(b[:], 37)
	f.Close()

	if _, err := r.ReadAt(0); err == nil {
		t.Fatal("expected SHA-256 to detect the corrupted record on read")
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

	r, err := NewWALReader(path, store, testChecksum())
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
