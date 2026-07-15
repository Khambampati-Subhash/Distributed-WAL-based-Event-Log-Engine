package network

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
)

type Client struct {
	mu   sync.Mutex
	conn net.Conn
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("network: dial %s: %w", addr, err)
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Produce(data []byte) (uint64, error) {
	resp, err := c.roundTrip(&Request{Op: OpProduce, Payload: data})
	if err != nil {
		return 0, err
	}
	if len(resp.Payload) != 8 {
		return 0, fmt.Errorf("produce: unexpected response length %d", len(resp.Payload))
	}
	return binary.BigEndian.Uint64(resp.Payload), nil
}

func (c *Client) Read(offset uint64) ([]byte, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], offset)
	resp, err := c.roundTrip(&Request{Op: OpRead, Payload: buf[:]})
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
	if err := WriteRequest(c.conn, &Request{Op: OpStreamRead, Payload: buf[:]}); err != nil {
		return startOffset, err
	}

	next := startOffset
	for {
		resp, err := ReadResponse(c.conn)
		if err != nil {
			return next, err
		}
		switch resp.Status {
		case StatusStreamEnd:
			return next, nil
		case StatusError:
			return next, fmt.Errorf("server error: %s", string(resp.Payload))
		case StatusOK:
			if err := fn(next, resp.Payload); err != nil {
				return next, err
			}
			next++
		default:
			return next, fmt.Errorf("stream: unknown status %d", resp.Status)
		}
	}
}

func (c *Client) NextOffset() (uint64, error) {
	resp, err := c.roundTrip(&Request{Op: OpNextOffset})
	if err != nil {
		return 0, err
	}
	if len(resp.Payload) != 8 {
		return 0, fmt.Errorf("next-offset: unexpected response length %d", len(resp.Payload))
	}
	return binary.BigEndian.Uint64(resp.Payload), nil
}

func (c *Client) EarliestOffset() (uint64, error) {
	resp, err := c.roundTrip(&Request{Op: OpEarliestOffset})
	if err != nil {
		return 0, err
	}
	if len(resp.Payload) != 8 {
		return 0, fmt.Errorf("earliest-offset: unexpected response length %d", len(resp.Payload))
	}
	return binary.BigEndian.Uint64(resp.Payload), nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) roundTrip(req *Request) (*Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := WriteRequest(c.conn, req); err != nil {
		return nil, err
	}
	resp, err := ReadResponse(c.conn)
	if err != nil {
		return nil, err
	}
	if resp.Status != StatusOK {
		return nil, fmt.Errorf("server error: %s", string(resp.Payload))
	}
	return resp, nil
}
