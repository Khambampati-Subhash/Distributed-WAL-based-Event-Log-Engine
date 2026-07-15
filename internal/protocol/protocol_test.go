package protocol

import (
	"bytes"
	"io"
	"testing"
)

func TestRequestRoundTrip(t *testing.T) {
	var buf bytes.Buffer

	req := &Request{Op: OpProduce, Payload: []byte("test-data")}
	if err := WriteRequest(&buf, req); err != nil {
		t.Fatal(err)
	}

	got, err := ReadRequest(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Op != req.Op || !bytes.Equal(got.Payload, req.Payload) {
		t.Fatalf("request roundtrip mismatch: got op=%d payload=%q", got.Op, got.Payload)
	}
}

func TestResponseRoundTrip(t *testing.T) {
	var buf bytes.Buffer

	resp := &Response{Status: StatusOK, Payload: []byte("result")}
	if err := WriteResponse(&buf, resp); err != nil {
		t.Fatal(err)
	}

	got, err := ReadResponse(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != resp.Status || !bytes.Equal(got.Payload, resp.Payload) {
		t.Fatalf("response roundtrip mismatch: got status=%d payload=%q", got.Status, got.Payload)
	}
}

func TestReadRequestEmptyIsEOF(t *testing.T) {
	var buf bytes.Buffer
	_, err := ReadRequest(&buf)
	if err != io.EOF {
		t.Fatalf("expected EOF on empty read, got %v", err)
	}
}

func TestReadRequestRejectsOversizePayload(t *testing.T) {
	// Header claiming a payload larger than MaxPayloadSize must be rejected
	// rather than trusted (a corrupt or hostile length must not OOM the reader).
	var buf bytes.Buffer
	header := []byte{OpProduce, 0xFF, 0xFF, 0xFF, 0xFF} // length ~4 GiB
	buf.Write(header)
	if _, err := ReadRequest(&buf); err == nil {
		t.Fatal("expected oversize payload to be rejected")
	}
}
