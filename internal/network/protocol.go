package network

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	OpProduce        byte = 1
	OpRead           byte = 2
	OpNextOffset     byte = 3
	OpEarliestOffset byte = 4
	OpStreamRead     byte = 5 // stream every record from a start offset up to the head

	StatusOK        byte = 0
	StatusError     byte = 1
	StatusStreamEnd byte = 2 // terminal frame of a stream: caller is caught up to the head

	MaxPayloadSize = 64 * 1024 * 1024 // 64 MB
)

// Request is the wire format sent by clients.
//
//	[opcode:1][length:4][payload:N]
//
// For OpProduce, payload is the record data.
// For OpRead, payload is an 8-byte big-endian offset.
// For OpNextOffset and OpEarliestOffset, payload is empty.
type Request struct {
	Op      byte
	Payload []byte
}

// Response is the wire format sent by the server.
//
//	[status:1][length:4][payload:N]
//
// For StatusOK on OpProduce, payload is an 8-byte big-endian offset.
// For StatusOK on OpRead, payload is the record data.
// For StatusOK on OpNextOffset/OpEarliestOffset, payload is 8-byte offset.
// For StatusError, payload is a UTF-8 error message.
type Response struct {
	Status  byte
	Payload []byte
}

func WriteRequest(w io.Writer, req *Request) error {
	header := make([]byte, 5)
	header[0] = req.Op
	binary.BigEndian.PutUint32(header[1:], uint32(len(req.Payload)))
	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("write request header: %w", err)
	}
	if len(req.Payload) > 0 {
		if _, err := w.Write(req.Payload); err != nil {
			return fmt.Errorf("write request payload: %w", err)
		}
	}
	return nil
}

func ReadRequest(r io.Reader) (*Request, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[1:])
	if length > MaxPayloadSize {
		return nil, fmt.Errorf("request payload too large: %d", length)
	}
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("read request payload: %w", err)
		}
	}
	return &Request{Op: header[0], Payload: payload}, nil
}

func WriteResponse(w io.Writer, resp *Response) error {
	header := make([]byte, 5)
	header[0] = resp.Status
	binary.BigEndian.PutUint32(header[1:], uint32(len(resp.Payload)))
	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("write response header: %w", err)
	}
	if len(resp.Payload) > 0 {
		if _, err := w.Write(resp.Payload); err != nil {
			return fmt.Errorf("write response payload: %w", err)
		}
	}
	return nil
}

func ReadResponse(r io.Reader) (*Response, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[1:])
	if length > MaxPayloadSize {
		return nil, fmt.Errorf("response payload too large: %d", length)
	}
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("read response payload: %w", err)
		}
	}
	return &Response{Status: header[0], Payload: payload}, nil
}
