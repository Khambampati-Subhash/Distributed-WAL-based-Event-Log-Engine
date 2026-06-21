package wal

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
)

// TestWriteReadRecover covers the single-threaded happy path plus recovery:
// append two records, read them back, hit EOF past the end, then reopen and
// confirm the index is rebuilt from disk.
func TestWriteReadRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")

	w, err := NewWalWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	o0, err := w.Write([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	o1, err := w.Write([]byte("world"))
	if err != nil {
		t.Fatal(err)
	}
	if o0 != 0 || o1 != 1 {
		t.Fatalf("offsets should be 0,1 got %d,%d", o0, o1)
	}

	r, err := NewWALReader(path, w)
	if err != nil {
		t.Fatal(err)
	}
	got0, err := r.ReadAt(0)
	if err != nil {
		t.Fatal(err)
	}
	got1, err := r.ReadAt(1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got0, []byte("hello")) || !bytes.Equal(got1, []byte("world")) {
		t.Fatalf("read back wrong data: %q %q", got0, got1)
	}
	if _, err := r.ReadAt(2); err != io.EOF {
		t.Fatalf("expected EOF past end, got %v", err)
	}
	r.Close()
	w.Close()

	// Reopen: index must be rebuilt by scanning the file on disk.
	w2, err := NewWalWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if got := w2.NextOffset(); got != 2 {
		t.Fatalf("recovery should rebuild 2 records, got %d", got)
	}
}

// TestConcurrentProducersAndConsumers is the safety guard for the concurrency
// model. Run it with the race detector:
//
//	go test -race ./internal/wal/
//
// Many producers append at once; many readers read independently at the same
// time. It asserts (a) every appended record gets a unique offset in [0, total)
// and (b) every record is readable with intact bytes. The race detector proves
// there are no unsynchronized memory accesses.
func TestConcurrentProducersAndConsumers(t *testing.T) {
	const (
		producers         = 8
		eventsPerProducer = 250
		readers           = 8
	)
	total := producers * eventsPerProducer

	path := filepath.Join(t.TempDir(), "events.log")
	w, err := NewWalWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// --- concurrent producers ---
	// Collect every offset returned so we can prove they're unique and dense.
	offsets := make([]uint64, total)
	var idx int64
	var mu sync.Mutex // guards idx -> position in offsets slice
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < eventsPerProducer; i++ {
				payload := []byte(fmt.Sprintf("p%d-event-%d", pid, i))
				off, err := w.Write(payload)
				if err != nil {
					t.Errorf("producer %d write: %v", pid, err)
					return
				}
				mu.Lock()
				offsets[idx] = off
				idx++
				mu.Unlock()
			}
		}(p)
	}

	// --- concurrent readers, running WHILE producers append ---
	// Each reader gets its OWN WALReader (own handle, own cursor) and walks
	// the log from 0 until it catches up to the current head.
	var readWg sync.WaitGroup
	for rdr := 0; rdr < readers; rdr++ {
		readWg.Add(1)
		go func(rid int) {
			defer readWg.Done()
			reader, err := NewWALReader(path, w)
			if err != nil {
				t.Errorf("reader %d open: %v", rid, err)
				return
			}
			defer reader.Close()
			var seen uint64
			for seen < uint64(total) {
				data, off, err := reader.Read()
				if err == io.EOF {
					continue // caught up to head; more may arrive
				}
				if err != nil {
					t.Errorf("reader %d: %v", rid, err)
					return
				}
				if len(data) == 0 {
					t.Errorf("reader %d: empty payload at offset %d", rid, off)
					return
				}
				seen = off + 1
			}
		}(rdr)
	}

	wg.Wait()     // all producers done
	readWg.Wait() // all readers caught up

	// --- correctness: offsets are exactly the set {0,1,...,total-1} ---
	if w.NextOffset() != uint64(total) {
		t.Fatalf("expected %d records, got %d", total, w.NextOffset())
	}
	seen := make([]bool, total)
	for _, off := range offsets {
		if off >= uint64(total) {
			t.Fatalf("offset %d out of range [0,%d)", off, total)
		}
		if seen[off] {
			t.Fatalf("duplicate offset %d — appends were not serialized!", off)
		}
		seen[off] = true
	}

	// --- every record is readable with non-empty bytes ---
	r, err := NewWALReader(path, w)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for off := uint64(0); off < uint64(total); off++ {
		data, err := r.ReadAt(off)
		if err != nil {
			t.Fatalf("read offset %d: %v", off, err)
		}
		if len(data) == 0 {
			t.Fatalf("offset %d has empty payload", off)
		}
	}
}
