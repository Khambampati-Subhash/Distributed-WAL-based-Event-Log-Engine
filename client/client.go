// Package client is the public, remote client for the WAL event-log server.
//
// It is the one package an external program imports to produce and consume
// events over TCP. It depends only on the wire protocol, not the storage
// engine, so a producer or consumer app stays lightweight:
//
//	c, err := client.New("log-host:9876")
//	if err != nil { ... }
//	defer c.Close()
//
//	off, _ := c.Produce([]byte("hello"))          // -> 0
//	data, _ := c.Read(0)                            // -> "hello"
//	next, _ := c.StreamRead(0, func(offset uint64, data []byte) error {
//	    fmt.Printf("[%d] %s\n", offset, data)
//	    return nil
//	})
//
// A Client is safe for concurrent use: each call is a serialized request/reply
// round-trip on the single connection.
package client

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/protocol"
)

type Client struct {
	mu   sync.Mutex
	conn net.Conn
}

// New dials the server at addr (host:port) and returns a connected client.
func New(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", addr, err)
	}
	return &Client{conn: conn}, nil
}

// Produce appends one record and returns the offset it was stored at.
func (c *Client) Produce(data []byte) (uint64, error) {
	resp, err := c.roundTrip(&protocol.Request{Op: protocol.OpProduce, Payload: data})
	if err != nil {
		return 0, err
	}
	if len(resp.Payload) != 8 {
		return 0, fmt.Errorf("produce: unexpected response length %d", len(resp.Payload))
	}
	return binary.BigEndian.Uint64(resp.Payload), nil
}

// Read returns the record stored at the given offset.
func (c *Client) Read(offset uint64) ([]byte, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], offset)
	resp, err := c.roundTrip(&protocol.Request{Op: protocol.OpRead, Payload: buf[:]})
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// StreamRead asks the server to push every record from startOffset up to the
// current head in one round-trip. fn is called for each record in order as its
// frame arrives (records are streamed sequentially, so the offset is tracked
// client-side). It returns the next offset to resume from once caught up — call
// StreamRead(next, ...) again later to pick up records appended since.
//
// If fn returns an error, streaming stops and that error is returned. A server
// error frame (e.g. out-of-retention) is returned as an error.
func (c *Client) StreamRead(startOffset uint64, fn func(offset uint64, data []byte) error) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], startOffset)
	if err := protocol.WriteRequest(c.conn, &protocol.Request{Op: protocol.OpStreamRead, Payload: buf[:]}); err != nil {
		return startOffset, err
	}

	next := startOffset
	for {
		resp, err := protocol.ReadResponse(c.conn)
		if err != nil {
			return next, err
		}
		switch resp.Status {
		case protocol.StatusStreamEnd:
			return next, nil
		case protocol.StatusError:
			return next, fmt.Errorf("server error: %s", string(resp.Payload))
		case protocol.StatusOK:
			if err := fn(next, resp.Payload); err != nil {
				return next, err
			}
			next++
		default:
			return next, fmt.Errorf("stream: unknown status %d", resp.Status)
		}
	}
}

// NextOffset returns the offset the next produced record will be assigned.
func (c *Client) NextOffset() (uint64, error) {
	resp, err := c.roundTrip(&protocol.Request{Op: protocol.OpNextOffset})
	if err != nil {
		return 0, err
	}
	if len(resp.Payload) != 8 {
		return 0, fmt.Errorf("next-offset: unexpected response length %d", len(resp.Payload))
	}
	return binary.BigEndian.Uint64(resp.Payload), nil
}

// EarliestOffset returns the lowest offset still stored (retention floor).
func (c *Client) EarliestOffset() (uint64, error) {
	resp, err := c.roundTrip(&protocol.Request{Op: protocol.OpEarliestOffset})
	if err != nil {
		return 0, err
	}
	if len(resp.Payload) != 8 {
		return 0, fmt.Errorf("earliest-offset: unexpected response length %d", len(resp.Payload))
	}
	return binary.BigEndian.Uint64(resp.Payload), nil
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) roundTrip(req *protocol.Request) (*protocol.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := protocol.WriteRequest(c.conn, req); err != nil {
		return nil, err
	}
	resp, err := protocol.ReadResponse(c.conn)
	if err != nil {
		return nil, err
	}
	if resp.Status != protocol.StatusOK {
		return nil, fmt.Errorf("server error: %s", string(resp.Payload))
	}
	return resp, nil
}
