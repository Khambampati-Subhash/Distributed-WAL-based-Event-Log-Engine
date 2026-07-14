package wal

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/inmemorystore"
)

func newTestWriter(t *testing.T, path string) (*WALWriter, *inmemorystore.InMemoryStore) {
	t.Helper()
	idxPath := path + ".index"
	store, err := inmemorystore.NewInMemoryStore(idxPath, NthIndex)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewWalWriter(path, store)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	return w, store
}

func TestWriteReadRecover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.log")

	w, store := newTestWriter(t, path)
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

	r, err := NewWALReader(path, store)
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
	store.Close()

	w2, store2 := newTestWriter(t, path)
	defer w2.Close()
	defer store2.Close()
	if got := w2.NextOffset(); got != 2 {
		t.Fatalf("recovery should rebuild 2 records, got %d", got)
	}
}

func TestConcurrentProducersAndConsumers(t *testing.T) {
	const (
		producers         = 8
		eventsPerProducer = 250
		readers           = 8
	)
	total := producers * eventsPerProducer

	path := filepath.Join(t.TempDir(), "events.log")
	w, store := newTestWriter(t, path)
	defer w.Close()
	defer store.Close()

	offsets := make([]uint64, total)
	var idx int64
	var mu sync.Mutex
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

	var readWg sync.WaitGroup
	for rdr := 0; rdr < readers; rdr++ {
		readWg.Add(1)
		go func(rid int) {
			defer readWg.Done()
			reader, err := NewWALReader(path, store)
			if err != nil {
				t.Errorf("reader %d open: %v", rid, err)
				return
			}
			defer reader.Close()
			var seen uint64
			for seen < uint64(total) {
				data, off, err := reader.Read()
				if err == io.EOF {
					continue
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

	wg.Wait()
	readWg.Wait()

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

	r, err := NewWALReader(path, store)
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
