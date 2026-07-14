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

func TestCRCDetectsBitRot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")
	writeRecords(t, path, "hello", "world", "third")

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
	_, err = NewWalWriter(path, store)
	if err == nil {
		t.Fatal("expected corruption to be detected on reopen, got nil error")
	}
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CorruptionError, got %T: %v", err, err)
	}
	t.Logf("correctly detected corruption: %v", ce)
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
