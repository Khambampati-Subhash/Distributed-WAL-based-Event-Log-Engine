package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/checksum"
	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/inmemorystore"
)

// On-disk record format (repeated, append-only):
//
//	[ length   : 4-byte big-endian uint32 ]
//	[ checksum : Checksum.Size() bytes over (length || payload) ]
//	[ payload  : <length> bytes ]
//
// The checksum width is not fixed here: it comes from the pluggable
// checksum.Checksum, so LengthSize is the only constant part of the header.
const (
	LengthSize    = 4
	MaxRecordSize = 64 * 1024 * 1024 // 64MB sanity cap
	NthIndex      = 10
)

// CorruptionError indicates detected bit-rot or corruption in the log.
type CorruptionError struct {
	Offset   int64  // byte position in file
	Expected []byte // checksum stored on disk
	Got      []byte // checksum recomputed from the bytes
	Reason   string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("wal: corruption at byte %d: %s (checksum: expected %x, got %x)", e.Offset, e.Reason, e.Expected, e.Got)
}

// pendingCheckpoint is a record that has been appended to the file but whose
// index checkpoint must wait until an fsync has made its bytes durable.
type pendingCheckpoint struct {
	seq    uint64
	offset uint64
	pos    int64
}

type WALWriter struct {
	// mu guards the append path and all fields below; cond is broadcast on mu
	// whenever a flush finishes so waiters can re-check their durability.
	mu         sync.Mutex
	cond       *sync.Cond
	file       *os.File
	csum       checksum.Checksum
	headerSize int // LengthSize + csum.Size()
	size       int64
	nextOffset uint64 // offset the next Write will assign

	// Group commit: writeSeq is bumped per appended record; syncedSeq is the
	// highest seq an fsync has made durable. Records in [syncedSeq+1, writeSeq]
	// are in the page cache but not yet fsynced. pending holds their checkpoints
	// until they are durable. syncing is true while one goroutine (the "leader")
	// is running the fsync; others wait on cond and get covered by it.
	writeSeq  uint64
	syncedSeq uint64
	syncing   bool
	syncErr   error
	pending   []pendingCheckpoint

	store inmemorystore.InMemoryStoreInterface
}

// NewWalWriter opens (or creates) the log file and recovers its head offset from
// the sparse checkpoint index rather than a full scan: it loads the checkpoints,
// jumps to the last one, and scans only the short tail after it to find the true
// head and truncate any torn final record. The caller provides the store and the
// checksum algorithm (which fixes the on-disk header width for this log).
func NewWalWriter(filename string, store inmemorystore.InMemoryStoreInterface, csum checksum.Checksum) (*WALWriter, error) {
	_, statErr := os.Stat(filename)
	isNew := os.IsNotExist(statErr)

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", filename, err)
	}

	if isNew {
		if err := fsyncDir(filepath.Dir(filename)); err != nil {
			file.Close()
			return nil, fmt.Errorf("wal: fsync dir for %q: %w", filename, err)
		}
	}

	w := &WALWriter{
		file:       file,
		csum:       csum,
		headerSize: LengthSize + csum.Size(),
		store:      store,
	}
	w.cond = sync.NewCond(&w.mu)
	if err := w.recover(); err != nil {
		file.Close()
		return nil, err
	}
	return w, nil
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Write appends one opaque record and returns the offset it was stored at. It
// returns only after an fsync has made the record durable, so "Write returned"
// still means "on stable storage" — but concurrent writers share that fsync
// (group commit), so throughput no longer costs one fsync per record.
//
// Write is Reserve followed by Commit. Callers that need concurrency across the
// fsync (e.g. the segment Manager, which must not hold its own lock during the
// slow fsync) call the two phases separately.
func (w *WALWriter) Write(data []byte) (uint64, error) {
	offset, seq, err := w.Reserve(data)
	if err != nil {
		return 0, err
	}
	if err := w.Commit(seq); err != nil {
		return 0, err
	}
	return offset, nil
}

// Reserve is phase 1: it appends the record bytes to the page cache and assigns
// the offset. It is fast (no fsync) and fully serialized, so offsets are dense
// and ordered. The returned seq is passed to Commit to make the record durable.
// The record is NOT durable until Commit returns.
func (w *WALWriter) Reserve(data []byte) (offset uint64, seq uint64, err error) {
	if len(data) > MaxRecordSize {
		return 0, 0, fmt.Errorf("wal: payload too large (%d > %d)", len(data), MaxRecordSize)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	pos := w.size

	var lenBuf [LengthSize]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	sum := w.csum.Compute(lenBuf[:], data) // checksum over length || payload

	buf := make([]byte, 0, w.headerSize+len(data))
	buf = append(buf, lenBuf[:]...)
	buf = append(buf, sum...)
	buf = append(buf, data...)

	// The file is O_APPEND, so the write lands at the end regardless of any cursor.
	if _, err := w.file.Write(buf); err != nil {
		return 0, 0, fmt.Errorf("wal: write record: %w", err)
	}

	offset = w.nextOffset
	w.nextOffset++
	w.size += int64(len(buf))
	w.writeSeq++
	seq = w.writeSeq
	w.pending = append(w.pending, pendingCheckpoint{seq: seq, offset: offset, pos: pos})
	return offset, seq, nil
}

// Commit is phase 2: it makes the record reserved at seq durable via a
// group-committed fsync (coalesced with concurrent commits) and returns only
// once it is on stable storage.
func (w *WALWriter) Commit(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.commitLocked(seq)
}

// commitLocked makes the record at seq durable and returns; it is called and
// returns with w.mu held. This is the group-commit core:
//
//   - If our record is already durable (a previous batch flushed it), return.
//   - If another goroutine is mid-fsync, WAIT on cond — when it finishes it will
//     very likely have covered us, so we return without fsyncing ourselves. This
//     is what makes appends coalesce: dozens of waiters ride one fsync.
//   - Otherwise become the leader: fsync once (mu released during the syscall so
//     others keep appending), then mark everything up to that point durable and
//     record its index checkpoints.
func (w *WALWriter) commitLocked(seq uint64) error {
	for {
		if w.syncErr != nil {
			return w.syncErr
		}
		if w.syncedSeq >= seq {
			return nil // already durable — covered by someone else's fsync
		}
		if w.syncing {
			w.cond.Wait() // a flush is in flight; wait and re-check (usually covered)
			continue
		}

		// Become the leader for this batch.
		w.syncing = true
		target := w.writeSeq // everything appended so far becomes durable after Sync
		w.mu.Unlock()
		serr := w.file.Sync()
		w.mu.Lock()
		w.syncing = false

		if serr != nil {
			w.syncErr = fmt.Errorf("wal: fsync: %w", serr)
			w.cond.Broadcast()
			return w.syncErr
		}
		if target > w.syncedSeq {
			w.syncedSeq = target
		}
		// Log bytes up to target are now durable. ONLY NOW record their index
		// checkpoints — the index must never reference data that isn't on stable
		// storage (write-ahead ordering: log first, index after). Applied here,
		// under mu and in leader order, so checkpoints stay strictly ascending.
		// A checkpoint write failing is non-fatal: the record is already durable
		// and the index is a rebuildable hint.
		drain := 0
		for drain < len(w.pending) && w.pending[drain].seq <= target {
			_ = w.store.Checkpoint(w.pending[drain].offset, w.pending[drain].pos)
			drain++
		}
		w.pending = w.pending[drain:]
		w.cond.Broadcast() // wake waiters: their records are now durable
		// Loop: syncedSeq >= seq now, so the next iteration returns nil.
	}
}

// Size returns the current size of the segment file in bytes.
func (w *WALWriter) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// NextOffset returns the offset the next Write will be assigned.
func (w *WALWriter) NextOffset() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextOffset
}

// Close flushes anything still buffered (a clean shutdown must lose nothing),
// records the remaining checkpoints, then closes the file. Normally every
// returned Write is already durable, so the final Sync is a no-op. The caller is
// responsible for closing the InMemoryStore separately and must not call Write
// concurrently with Close.
func (w *WALWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writeSeq > w.syncedSeq && w.syncErr == nil {
		if err := w.file.Sync(); err != nil {
			w.file.Close()
			return fmt.Errorf("wal: fsync on close: %w", err)
		}
		for _, cp := range w.pending {
			_ = w.store.Checkpoint(cp.offset, cp.pos)
		}
		w.pending = nil
		w.syncedSeq = w.writeSeq
	}
	return w.file.Close()
}

// recover reconstructs the head offset after a restart. It loads the sparse
// checkpoints, positions at the last VALID one, and scans forward only over the
// tail (at most a checkpoint interval of records) to count records up to the head
// and truncate a torn final record. No CRC is verified here — checkpoints are a
// speed hint, and integrity is checked on every read instead.
func (w *WALWriter) recover() error {
	if err := w.store.LoadCheckpoints(); err != nil {
		return err
	}

	info, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("wal: stat during recovery: %w", err)
	}
	fileSize := info.Size()

	// Defensive guard: trust the last checkpoint only if it points at a record
	// that can actually fit inside the durable log. A checkpoint at or past EOF
	// (which batching bugs or reordered, un-fsynced index writes could in theory
	// produce) is discarded and we fall back to a full scan from offset 0. Without
	// this, a bogus position would make the tail scan truncate-grow the file.
	var pos int64
	var off uint64
	if cpOff, cpPos, ok := w.store.Floor(^uint64(0)); ok && cpPos+int64(w.headerSize) <= fileSize {
		off, pos = cpOff, cpPos
	}

	lenBuf := make([]byte, LengthSize)
	for {
		if pos == fileSize {
			break // clean end, exactly on a record boundary
		}
		if pos+int64(w.headerSize) > fileSize {
			// A header started but the file ends before it completes: torn tail.
			if err := w.file.Truncate(pos); err != nil {
				return fmt.Errorf("wal: truncate torn header at %d: %w", pos, err)
			}
			break
		}
		if _, err := w.file.ReadAt(lenBuf, pos); err != nil {
			return fmt.Errorf("wal: read length at %d: %w", pos, err)
		}
		length := int64(binary.BigEndian.Uint32(lenBuf))
		if length > int64(MaxRecordSize) {
			return &CorruptionError{
				Offset: pos,
				Reason: fmt.Sprintf("length out of bounds: %d", length),
			}
		}
		recEnd := pos + int64(w.headerSize) + length
		if recEnd > fileSize {
			// The payload is incomplete: torn tail. Truncate to the last good record.
			if err := w.file.Truncate(pos); err != nil {
				return fmt.Errorf("wal: truncate torn payload at %d: %w", pos, err)
			}
			break
		}
		pos = recEnd
		off++
	}

	w.nextOffset = off
	w.size = pos
	return nil
}
