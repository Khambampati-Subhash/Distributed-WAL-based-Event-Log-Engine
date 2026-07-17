// Package checksum provides pluggable integrity algorithms for the WAL.
//
// The WAL depends on the Checksum interface, not on a concrete algorithm, so
// CRC32C can be swapped for SHA-256 (or anything else) purely by construction —
// the Strategy pattern / dependency inversion. To add an algorithm, implement
// Checksum; nothing in the WAL changes.
//
// Important: the checksum WIDTH is part of the on-disk record format (the header
// is [length][checksum], and the checksum is Size() bytes). A log written with a
// 4-byte CRC therefore cannot be read back by a 32-byte SHA-256 reader. Choose
// one algorithm per log via segment.Config.Checksum; the default is CRC32C.
package checksum

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
)

// Checksum computes a fixed-width integrity check over one or more byte slices,
// treated as a single concatenated message (so callers can pass length and
// payload separately without allocating a joined buffer).
//
// Implementations must be safe for concurrent use: the WAL shares one instance
// across producer and reader goroutines.
type Checksum interface {
	// Compute returns the checksum of parts concatenated in order. The result is
	// always exactly Size() bytes long.
	Compute(parts ...[]byte) []byte
	// Size is the width of a Compute result in bytes (4 for CRC32, 32 for SHA-256).
	Size() int
	// Name identifies the algorithm, e.g. "crc32c" or "sha256".
	Name() string
}

// CRC32C is the Castagnoli CRC32: hardware-accelerated, 4 bytes, the default.
// It detects random disk faults cheaply — it is not a defense against tampering.
type CRC32C struct {
	table *crc32.Table
}

func NewCRC32C() *CRC32C {
	return &CRC32C{table: crc32.MakeTable(crc32.Castagnoli)}
}

func (c *CRC32C) Compute(parts ...[]byte) []byte {
	var sum uint32
	for _, p := range parts {
		sum = crc32.Update(sum, c.table, p)
	}
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, sum)
	return out
}

func (c *CRC32C) Size() int    { return 4 }
func (c *CRC32C) Name() string { return "crc32c" }

// SHA256 is a cryptographic checksum: 32 bytes, far stronger but much slower than
// CRC. Use it when you need to detect deliberate tampering, not just bit-rot.
type SHA256 struct{}

func NewSHA256() *SHA256 { return &SHA256{} }

func (SHA256) Compute(parts ...[]byte) []byte {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum(nil)
}

func (SHA256) Size() int    { return 32 }
func (SHA256) Name() string { return "sha256" }
