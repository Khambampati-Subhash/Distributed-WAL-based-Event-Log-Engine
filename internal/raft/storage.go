package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PersistentState is the trio Raft MUST have on stable storage before it replies
// to any RPC: the current term, who it voted for this term, and the log. Without
// them, a restarted node could vote twice in one term or forget committed entries
// — both violate Raft's safety guarantees.
type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID
	Log         []LogEntry
}

// Storage persists and restores a node's PersistentState. It is an interface so
// tests can use fast in-memory storage while production uses a durable file — the
// same node logic drives both.
type Storage interface {
	Save(state PersistentState) error
	Load() (state PersistentState, ok bool, err error)
}

// MemoryStorage keeps state in RAM. A "crash + restart" in a test is modeled by
// dropping the Raft node but KEEPING its MemoryStorage, then building a fresh
// node from it — exactly what a real restart sees on disk.
type MemoryStorage struct {
	mu    sync.Mutex
	saved *PersistentState
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) Save(state PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := state
	cp.Log = append([]LogEntry(nil), state.Log...) // snapshot; the caller mutates its log
	m.saved = &cp
	return nil
}

func (m *MemoryStorage) Load() (PersistentState, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saved == nil {
		return PersistentState{}, false, nil
	}
	cp := *m.saved
	cp.Log = append([]LogEntry(nil), m.saved.Log...)
	return cp, true, nil
}

// FileStorage persists state to a single file, replaced atomically (tmp + fsync +
// rename + parent-dir fsync) — the same durability recipe the consumer-offset
// writer uses, so a crash mid-write can never leave a torn state file. It
// rewrites the whole state on each Save; snapshots (step 4) will bound its size.
type FileStorage struct {
	mu   sync.Mutex
	path string
}

func NewFileStorage(path string) *FileStorage { return &FileStorage{path: path} }

func (f *FileStorage) Save(state PersistentState) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		return fmt.Errorf("raft: encode state: %w", err)
	}

	tmp := f.path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raft: open tmp %q: %w", tmp, err)
	}
	if _, err := file.Write(buf.Bytes()); err != nil {
		file.Close()
		return fmt.Errorf("raft: write state: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("raft: fsync state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("raft: close tmp: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("raft: rename state: %w", err)
	}
	return fsyncDir(filepath.Dir(f.path))
}

func (f *FileStorage) Load() (PersistentState, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return PersistentState{}, false, nil
	}
	if err != nil {
		return PersistentState{}, false, fmt.Errorf("raft: read state %q: %w", f.path, err)
	}
	var state PersistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&state); err != nil {
		return PersistentState{}, false, fmt.Errorf("raft: decode state: %w", err)
	}
	return state, true, nil
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
