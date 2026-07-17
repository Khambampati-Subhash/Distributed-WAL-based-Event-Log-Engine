package checksum

import (
	"bytes"
	"testing"
)

// algorithms under test; add new implementations here to get the shared contract
// checks for free.
func algorithms() []Checksum {
	return []Checksum{NewCRC32C(), NewSHA256()}
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
	if NewCRC32C().Size() != 4 {
		t.Errorf("CRC32C size should be 4, got %d", NewCRC32C().Size())
	}
	if NewSHA256().Size() != 32 {
		t.Errorf("SHA256 size should be 32, got %d", NewSHA256().Size())
	}
}
