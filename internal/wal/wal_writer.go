package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// On-disk record format (repeated, append-only):
//
//	[ length : 4-byte big-endian uint32 ]
//	[ crc    : 4-byte big-endian CRC32C of (length || payload) ]
//	[ payload : <length> bytes ]
//
// Phase 2 adds CRC32C (Castagnoli) for integrity detection. The CRC is computed
// over the length field AND the payload (not just the payload), so a bit-flip in
// the length itself — which would cause mis-framing — is also caught.
//
// CRC32C (not IEEE/standard CRC32) is chosen because:
//  1. Kafka uses it (mirrors the real thing)
//  2. Hardware-accelerated on modern x86/ARM (single CPU instruction), so zero cost
//  3. Better error detection properties for storage (detects more burst errors)
//  4. Go: hash/crc32.MakeTable(crc32.Castagnoli) provides the polynomial
//
// Why CRC, not SHA256 or MD5?
//   - SHA/MD5 are cryptographic hashes (hard to forge), but FAR more expensive
//   - CRC is non-cryptographic but designed for bit-flip detection (fast, sufficient)
//   - Per-record CRC is about "did my disk corrupt this", not "did an attacker forge it"
//   - At 694 events/sec (your real workload), SHA per-record adds unacceptable latency
//
// The CRC field itself is NOT checksummed (a checksum can't checksum itself).
const (
	lengthSize       = 4
	crcSize          = 4
	recordHeaderSize = lengthSize + crcSize
	maxRecordSize    = 64 * 1024 * 1024 // 64MB sanity cap
	nthIndex         = 10
)

// CorruptionError indicates detected bit-rot or corruption in the log.
type CorruptionError struct {
	Offset   int64  // byte position in file
	Expected uint32 // expected CRC
	Got      uint32 // actual CRC computed from bytes
	Reason   string // "length out of bounds", "crc mismatch", etc.
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("wal: corruption at byte %d: %s (crc: expected %08x, got %08x)", e.Offset, e.Reason, e.Expected, e.Got)
}

type WALWriter struct {
	Mu        sync.Mutex // serializes producers: one append at a time
	WalFile   *os.File   // the open append-only file (opened once, held here)
	IndexFile *os.File
	Index     []int64      // Index[offset] = byte position where that record starts
	table     *crc32.Table // CRC32C (Castagnoli) polynomial, reused for all computations
	size      int64        // current file size in bytes (tracked in-memory, no stat per write)
	n         uint32
}

type WALWriterInterface interface {
	Write(data []byte) (uint64, error)
	Size() int64
	PositionOf(offset uint64) (int64, bool)
	NextOffset() uint64
	Close() error
}

// NewWalWriter opens (or creates) the log file and rebuilds the in-memory
// index from whatever is already on disk, so the log survives a restart.
func NewWalWriter(filename string) (*WALWriter, error) {
	// Did the file already exist? If we're about to create it, the new
	// directory entry must itself be fsynced (see below) or a crash could
	// lose the file's name even though its data was synced.
	_, statErr := os.Stat(filename)
	isNew := os.IsNotExist(statErr)

	// O_APPEND => every Write lands at the end of the file.
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", filename, err)
	}

	fileNameSplit := strings.Split(filename, ".")
	indexFileName := strings.Join([]string{fileNameSplit[0], "index"}, ".")

	_, statErr = os.Stat(indexFileName)
	isNew = os.IsNotExist(statErr)

	indexfile, err := os.OpenFile(indexFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open for index file %q: %w", indexFileName, err)
	}

	// fsync the parent directory so the newly-created log file's directory
	// entry is durable. fsyncing the file persists its data + inode but NOT
	// the directory that names it; without this a crash right after creation
	// could leave the file nameless (effectively gone).
	if isNew {
		if err := fsyncDir(filepath.Dir(filename)); err != nil {
			file.Close()
			return nil, fmt.Errorf("wal: fsync dir for %q: %w", filename, err)
		}
	}

	w := &WALWriter{
		WalFile:   file,
		IndexFile: indexfile,
		table:     crc32.MakeTable(crc32.Castagnoli),
	}
	if err := w.rebuildIndex(); err != nil {
		file.Close()
		return nil, err
	}

	// Record the current file size (end position after recovery) so the
	// segment manager can decide when to roll without stat-ing every write.
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("wal: size after recovery: %w", err)
	}
	w.size = size
	return w, nil
}

// fsyncDir flushes a directory's own metadata to stable storage. This is what
// makes a create/rename of a file *inside* that directory durable: fsyncing a
// file does not persist the directory entry that names it.
func fsyncDir(dir string) error {
	d, err := os.Open(dir) // open the directory read-only
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync() // fsync the directory fd
}

func (w *WALWriter) IndexWriter(index, position uint64) error {
	if w.n == nthIndex {
		w.n = 0
		var indexValue [8]byte
		binary.BigEndian.PutUint64(indexValue[:], index)
		var bytePositionValue [8]byte
		binary.BigEndian.PutUint64(bytePositionValue[:], position)
		var final []byte
		final = append(final, indexValue[:]...)
		final = append(final, bytePositionValue[:]...)

		if _, err := w.IndexFile.Write(final); err != nil {
			return fmt.Errorf("wal: write index + byte position: %w", err)
		}

		// fsync — data is only durable after this returns.
		if err := w.IndexFile.Sync(); err != nil {
			return fmt.Errorf("wal: fsync index+byte position: %w", err)
		}

	}
	w.n++
	return nil
}

// Write appends one opaque record and returns the offset it was stored at.
func (w *WALWriter) Write(data []byte) (uint64, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()

	// Sanity check: refuse to store records that are unreasonably large.
	// This cap distinguishes "honest corruption in length field" from "torn tail".
	if len(data) > maxRecordSize {
		return 0, fmt.Errorf("wal: payload too large (%d > %d)", len(data), maxRecordSize)
	}

	// Current end of file = the byte position where this record will start.
	pos, err := w.WalFile.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("wal: seek end: %w", err)
	}

	// Build the length header (4 bytes, big-endian).
	var lenBuf [lengthSize]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	// Compute CRC32C over (length || payload). The CRC itself is NOT included.
	crcChecksum := crc32.Checksum(lenBuf[:], w.table)
	crcChecksum = crc32.Update(crcChecksum, w.table, data)

	// Build the CRC header (4 bytes, big-endian).
	var crcBuf [crcSize]byte
	binary.BigEndian.PutUint32(crcBuf[:], crcChecksum)

	var totalBytes []byte
	totalBytes = append(totalBytes, lenBuf[:]...)
	totalBytes = append(totalBytes, crcBuf[:]...)
	totalBytes = append(totalBytes, data[:]...)

	// Write: [ len:4 ][ crc:4 ][ payload:N ]
	if _, err := w.WalFile.Write(totalBytes); err != nil {
		return 0, fmt.Errorf("wal: write header + crc + total payload: %w", err)
	}

	// fsync — data is only durable after this returns.
	if err := w.WalFile.Sync(); err != nil {
		return 0, fmt.Errorf("wal: fsync: %w", err)
	}

	offset := uint64(len(w.Index))

	err = w.IndexWriter(offset, uint64(pos))
	if err != nil {
		return 0, err
	}
	w.Index = append(w.Index, pos)
	w.size += recordHeaderSize + int64(len(data)) // track bytes written
	return offset, nil
}

// Size returns the current size of the segment file in bytes. The segment
// manager uses this to decide when to roll to a new segment.
func (w *WALWriter) Size() int64 {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	return w.size
}

// PositionOf returns the byte position where the record at offset begins,
// so the reader can seek straight to it. ok=false if offset doesn't exist.
func (w *WALWriter) PositionOf(offset uint64) (int64, bool) {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	if offset >= uint64(len(w.Index)) {
		return 0, false
	}
	return w.Index[offset], true
}

// NextOffset returns the offset the next Write will be assigned, i.e. the
// number of records currently in the log. Used to compute consumer lag
// (lag = NextOffset - consumer's current offset).
func (w *WALWriter) NextOffset() uint64 {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	return uint64(len(w.Index))
}

// Close closes the underlying file.
func (w *WALWriter) Close() error {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	return w.WalFile.Close()
}

// rebuildIndex scans the file once on startup and reconstructs Index.
// Phase 2: also verifies CRC32C for every record and applies a 3-way decision tree:
//
//  1. Torn tail (partial record at EOF) → truncate, keep valid records
//  2. Corrupt length (out of bounds)   → return CorruptionError, stop
//  3. CRC mismatch (bit-rot in complete record) → return CorruptionError, stop
//
// The rationale: torn tails are expected after crashes and safe to truncate.
// Bit-rot is unexpected and requires operator intervention (investigate disk,
// consider if more files are corrupt, etc). Never silently skip corrupt records.
func (w *WALWriter) rebuildIndex() error {
	if _, err := w.WalFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek start: %w", err)
	}

	lenBuf := make([]byte, lengthSize)
	crcBuf := make([]byte, crcSize)
	var pos int64
	var nn uint32

	for {
		// Read the length field (4 bytes).
		_, err := io.ReadFull(w.WalFile, lenBuf)
		if err == io.EOF {
			break // clean end
		}
		if err == io.ErrUnexpectedEOF {
			return w.truncateTorn(pos) // partial header — torn tail
		}
		if err != nil {
			return fmt.Errorf("wal: read length at %d: %w", pos, err)
		}

		length := int64(binary.BigEndian.Uint32(lenBuf))

		// Sanity check: a corrupted length field might claim an impossible size.
		// If so, this is NOT a torn tail (we read the full 4 bytes); it's corruption.
		if length < 0 || length > int64(maxRecordSize) {
			return &CorruptionError{
				Offset: pos,
				Reason: fmt.Sprintf("length out of bounds: %d", length),
			}
		}

		// Read the CRC field (4 bytes).
		_, err = io.ReadFull(w.WalFile, crcBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return w.truncateTorn(pos) // partial record — torn tail
		}
		if err != nil {
			return fmt.Errorf("wal: read crc at %d: %w", pos, err)
		}
		storedCRC := binary.BigEndian.Uint32(crcBuf)

		// Read the payload.
		payload := make([]byte, length)
		_, err = io.ReadFull(w.WalFile, payload)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return w.truncateTorn(pos) // partial payload — torn tail
		}
		if err != nil {
			return fmt.Errorf("wal: read payload at %d: %w", pos, err)
		}

		// Verify CRC32C over (length || payload).
		computedCRC := crc32.Checksum(lenBuf, w.table)
		computedCRC = crc32.Update(computedCRC, w.table, payload)

		if computedCRC != storedCRC {
			return &CorruptionError{
				Offset:   pos,
				Expected: storedCRC,
				Got:      computedCRC,
				Reason:   "crc mismatch (bit-rot or disk corruption)",
			}
		}

		if nn == w.n {
			w.Index = append(w.Index, pos)
			nn = 0
		}
		pos += recordHeaderSize + length
		nn++
	}

	_, err := w.WalFile.Seek(0, io.SeekEnd)
	return err
}

// truncateTorn cuts off a half-written record left by a crash, so the log
// only ever contains whole, durable records.
func (w *WALWriter) truncateTorn(validEnd int64) error {
	if err := w.WalFile.Truncate(validEnd); err != nil {
		return fmt.Errorf("wal: truncate torn record at %d: %w", validEnd, err)
	}
	_, err := w.WalFile.Seek(0, io.SeekEnd)
	return err
}
