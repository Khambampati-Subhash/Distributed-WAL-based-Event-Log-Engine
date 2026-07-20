package checksum

import (
	"bytes"
	"testing"
)

// algorithms under test; add new implementations here to get the shared contract
// checks for free.
func algorithms() []Checksum {
	return []Checksum{
		NewCRC32C(),
		NewCRC32IEEE(),
		NewAdler32(),
		NewCRC64ECMA(),
		NewFNV1a64(),
		NewMD5(),
		NewSHA1(),
		NewSHA256(),
	}
}

func TestSizeMatchesOutput(t *testing.T) {
	for _, a := range algorithms() {
		got := a.Compute([]byte("hello world"))
		if len(got) != a.Size() {
			t.Errorf("%s: Compute returned %d bytes, Size() says %d", a.Name(), len(got), a.Size())
		}
	}
}

func TestDeterministic(t *testing.T) {
	for _, a := range algorithms() {
		x := a.Compute([]byte("abc"), []byte("def"))
		y := a.Compute([]byte("abc"), []byte("def"))
		if !bytes.Equal(x, y) {
			t.Errorf("%s: not deterministic", a.Name())
		}
	}
}

// TestPartsEqualConcatenation is the property the WAL relies on: checksumming
// (length, payload) as two parts must equal checksumming their concatenation.
func TestPartsEqualConcatenation(t *testing.T) {
	for _, a := range algorithms() {
		parts := a.Compute([]byte("length"), []byte("payload"))
		joined := a.Compute([]byte("lengthpayload"))
		if !bytes.Equal(parts, joined) {
			t.Errorf("%s: multi-part checksum != concatenated checksum", a.Name())
		}
	}
}

func TestDetectsChange(t *testing.T) {
	for _, a := range algorithms() {
		good := a.Compute([]byte("the quick brown fox"))
		bad := a.Compute([]byte("the quick brown box")) // one byte differs
		if bytes.Equal(good, bad) {
			t.Errorf("%s: checksum did not change for different input", a.Name())
		}
	}
}

func TestKnownSizes(t *testing.T) {
	cases := []struct {
		c    Checksum
		size int
	}{
		{NewCRC32C(), 4},
		{NewCRC32IEEE(), 4},
		{NewAdler32(), 4},
		{NewCRC64ECMA(), 8},
		{NewFNV1a64(), 8},
		{NewMD5(), 16},
		{NewSHA1(), 20},
		{NewSHA256(), 32},
	}
	for _, tc := range cases {
		if tc.c.Size() != tc.size {
			t.Errorf("%s size = %d, want %d", tc.c.Name(), tc.c.Size(), tc.size)
		}
	}
}

// TestUniqueNames guards against copy-paste mistakes: every algorithm must have a
// distinct Name (the dashboards and reports key off it).
func TestUniqueNames(t *testing.T) {
	seen := map[string]bool{}
	for _, a := range algorithms() {
		if seen[a.Name()] {
			t.Errorf("duplicate algorithm name %q", a.Name())
		}
		seen[a.Name()] = true
	}
}
