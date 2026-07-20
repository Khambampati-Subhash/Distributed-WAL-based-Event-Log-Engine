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
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"hash/adler32"
	"hash/crc32"
	"hash/crc64"
	"hash/fnv"
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

// hashChecksum adapts any hash.Hash constructor to the Checksum interface. Most
// algorithms need nothing more than "make a fresh hasher, write the parts, sum",
// so they share this one wrapper instead of a bespoke type each.
type hashChecksum struct {
	name    string
	size    int
	newHash func() hash.Hash
}

func (h hashChecksum) Compute(parts ...[]byte) []byte {
	hh := h.newHash()
	for _, p := range parts {
		hh.Write(p)
	}
	return hh.Sum(nil)
}

func (h hashChecksum) Size() int    { return h.size }
func (h hashChecksum) Name() string { return h.name }

// The algorithms below span the tradeoff space, widening from 4 to 20 bytes and
// moving from cheap-but-weak to strong-but-costly. All are standard library, so
// adding them keeps the module dependency-free. Each fixes a different on-disk
// header width (see the package doc); pick one per log via segment.Config.

// NewCRC32IEEE is the "classic" CRC32 (IEEE 802.3 polynomial, used by zlib and
// gzip): 4 bytes. Unlike CRC32C it is not universally hardware-accelerated.
func NewCRC32IEEE() Checksum {
	t := crc32.MakeTable(crc32.IEEE)
	return hashChecksum{"crc32-ieee", 4, func() hash.Hash { return crc32.New(t) }}
}

// NewAdler32 is Adler-32 (used by zlib): 4 bytes, faster than a CRC but with
// weaker error detection, especially on short inputs — a cautionary contrast.
func NewAdler32() Checksum {
	return hashChecksum{"adler32", 4, func() hash.Hash { return adler32.New() }}
}

// NewCRC64ECMA is CRC-64 (ECMA polynomial): 8 bytes, stronger collision
// resistance than a 32-bit CRC at twice the width.
func NewCRC64ECMA() Checksum {
	t := crc64.MakeTable(crc64.ECMA)
	return hashChecksum{"crc64-ecma", 8, func() hash.Hash { return crc64.New(t) }}
}

// NewFNV1a64 is the 64-bit FNV-1a non-cryptographic hash: 8 bytes, very fast,
// commonly used for hash tables rather than integrity — included for comparison.
func NewFNV1a64() Checksum {
	return hashChecksum{"fnv1a-64", 8, func() hash.Hash { return fnv.New64a() }}
}

// NewMD5 is the MD5 digest: 16 bytes. Cryptographically broken for collision
// resistance, but still a fine (if heavy) integrity checksum.
func NewMD5() Checksum {
	return hashChecksum{"md5", 16, func() hash.Hash { return md5.New() }}
}

// NewSHA1 is the SHA-1 digest: 20 bytes. Also collision-broken, but sits between
// MD5 and SHA-256 in cost and width.
func NewSHA1() Checksum {
	return hashChecksum{"sha1", 20, func() hash.Hash { return sha1.New() }}
}
