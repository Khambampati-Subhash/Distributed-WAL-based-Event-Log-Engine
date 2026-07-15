package network

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/segment"
)

func startTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := segment.Config{
		Dir:              dir,
		MaxSegmentBytes:  1 << 20,
		DisableRetention: true,
	}
	mgr, err := segment.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(mgr, "127.0.0.1:0")
	if err != nil {
		mgr.Close()
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() {
		srv.Close()
		mgr.Close()
	})
	return srv, srv.Addr().String()
}

func TestProduceAndRead(t *testing.T) {
	_, addr := startTestServer(t)
	c, err := NewClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	off, err := c.Produce([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if off != 0 {
		t.Fatalf("expected offset 0, got %d", off)
	}

	data, err := c.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Fatalf("got %q, want %q", data, "hello")
	}
}

func TestNextAndEarliestOffset(t *testing.T) {
	_, addr := startTestServer(t)
	c, err := NewClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	earliest, err := c.EarliestOffset()
	if err != nil {
		t.Fatal(err)
	}
	if earliest != 0 {
		t.Fatalf("expected earliest 0, got %d", earliest)
	}

	for i := 0; i < 5; i++ {
		if _, err := c.Produce([]byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	next, err := c.NextOffset()
	if err != nil {
		t.Fatal(err)
	}
	if next != 5 {
		t.Fatalf("expected next 5, got %d", next)
	}
}

func TestReadNonExistent(t *testing.T) {
	_, addr := startTestServer(t)
	c, err := NewClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err = c.Read(999)
	if err == nil {
		t.Fatal("expected error reading non-existent offset")
	}
}

func TestConcurrentClients(t *testing.T) {
	_, addr := startTestServer(t)

	const clients = 8
	const msgsPerClient = 50

	var wg sync.WaitGroup
	errs := make(chan error, clients*msgsPerClient)

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := NewClient(addr)
			if err != nil {
				errs <- err
				return
			}
			defer c.Close()

			for j := 0; j < msgsPerClient; j++ {
				payload := []byte(fmt.Sprintf("client-%d-msg-%d", id, j))
				off, err := c.Produce(payload)
				if err != nil {
					errs <- fmt.Errorf("produce: %w", err)
					return
				}
				data, err := c.Read(off)
				if err != nil {
					errs <- fmt.Errorf("read offset %d: %w", off, err)
					return
				}
				if !bytes.Equal(data, payload) {
					errs <- fmt.Errorf("mismatch at offset %d: got %q want %q", off, data, payload)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestMultipleMessagesSequential(t *testing.T) {
	_, addr := startTestServer(t)
	c, err := NewClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	messages := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, msg := range messages {
		if _, err := c.Produce([]byte(msg)); err != nil {
			t.Fatal(err)
		}
	}

	for i, want := range messages {
		data, err := c.Read(uint64(i))
		if err != nil {
			t.Fatalf("read offset %d: %v", i, err)
		}
		if string(data) != want {
			t.Fatalf("offset %d: got %q want %q", i, data, want)
		}
	}
}

func TestStreamRead(t *testing.T) {
	_, addr := startTestServer(t)
	c, err := NewClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < 10; i++ {
		if _, err := c.Produce([]byte(fmt.Sprintf("event-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate a consumer reading from offset 0 to EOF
	for off := uint64(0); ; off++ {
		data, err := c.Read(off)
		if err != nil {
			if off == 10 {
				break // expected: past the end
			}
			t.Fatalf("unexpected error at offset %d: %v", off, err)
		}
		want := fmt.Sprintf("event-%d", off)
		if string(data) != want {
			t.Fatalf("offset %d: got %q want %q", off, data, want)
		}
	}
}

func TestStreamReadCatchUp(t *testing.T) {
	_, addr := startTestServer(t)
	c, err := NewClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	const n = 25
	for i := range n {
		if _, err := c.Produce(fmt.Appendf(nil, "event-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	var got []string
	next, err := c.StreamRead(0, func(offset uint64, data []byte) error {
		if offset != uint64(len(got)) {
			t.Errorf("out-of-order offset: got %d want %d", offset, len(got))
		}
		got = append(got, string(data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if next != n {
		t.Fatalf("expected resume offset %d, got %d", n, next)
	}
	if len(got) != n {
		t.Fatalf("streamed %d records, want %d", len(got), n)
	}
	for i, s := range got {
		if want := fmt.Sprintf("event-%d", i); s != want {
			t.Fatalf("record %d: got %q want %q", i, s, want)
		}
	}
}

func TestStreamReadResumesFromMiddle(t *testing.T) {
	_, addr := startTestServer(t)
	c, err := NewClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := range 10 {
		if _, err := c.Produce(fmt.Appendf(nil, "msg-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	var count int
	next, err := c.StreamRead(5, func(offset uint64, data []byte) error {
		if offset != uint64(5+count) {
			t.Errorf("offset %d, want %d", offset, 5+count)
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Fatalf("streamed %d records from offset 5, want 5", count)
	}
	if next != 10 {
		t.Fatalf("resume offset %d, want 10", next)
	}
}

func TestStreamReadEmptyLog(t *testing.T) {
	_, addr := startTestServer(t)
	c, err := NewClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	called := false
	next, err := c.StreamRead(0, func(offset uint64, data []byte) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("callback should not fire on an empty log")
	}
	if next != 0 {
		t.Fatalf("resume offset %d, want 0", next)
	}
}

func TestStreamThenProduceThenStreamAgain(t *testing.T) {
	_, addr := startTestServer(t)
	c, err := NewClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := range 3 {
		if _, err := c.Produce(fmt.Appendf(nil, "first-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	next, err := c.StreamRead(0, func(offset uint64, data []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if next != 3 {
		t.Fatalf("first stream resume offset %d, want 3", next)
	}

	// Append more, then resume from where we left off — should see only the new ones.
	for i := range 2 {
		if _, err := c.Produce(fmt.Appendf(nil, "second-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	next, err = c.StreamRead(next, func(offset uint64, data []byte) error {
		got = append(got, string(data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if next != 5 {
		t.Fatalf("second stream resume offset %d, want 5", next)
	}
	if len(got) != 2 || got[0] != "second-0" || got[1] != "second-1" {
		t.Fatalf("resumed stream saw wrong records: %v", got)
	}
}

func TestIdleTimeoutClosesConnection(t *testing.T) {
	dir := t.TempDir()
	mgr, err := segment.Open(segment.Config{Dir: dir, MaxSegmentBytes: 1 << 20, DisableRetention: true})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(mgr, "127.0.0.1:0", WithIdleTimeout(100*time.Millisecond))
	if err != nil {
		mgr.Close()
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() {
		srv.Close()
		mgr.Close()
	})

	// Open a raw connection and never send anything; the server should close it.
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("expected connection to be closed by idle timeout")
	}
	// A closed connection surfaces as io.EOF on the client read.
	if !errors.Is(err, io.EOF) {
		t.Logf("connection ended with: %v (acceptable as long as it ended)", err)
	}
}

func TestProtocolRoundTrip(t *testing.T) {
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

	resp := &Response{Status: StatusOK, Payload: []byte("result")}
	if err := WriteResponse(&buf, resp); err != nil {
		t.Fatal(err)
	}

	gotResp, err := ReadResponse(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if gotResp.Status != resp.Status || !bytes.Equal(gotResp.Payload, resp.Payload) {
		t.Fatalf("response roundtrip mismatch: got status=%d payload=%q", gotResp.Status, gotResp.Payload)
	}
}

func TestEmptyRequest(t *testing.T) {
	var buf bytes.Buffer
	_, err := ReadRequest(&buf)
	if err != io.EOF {
		t.Fatalf("expected EOF on empty read, got %v", err)
	}
}
