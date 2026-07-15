package network

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/protocol"
	"github.com/Khambampati-Subhash/Distributed-WAL-based-Event-Log-Engine/internal/segment"
)

// ServerConfig tunes connection lifecycle. Zero values on a timeout mean "no
// limit", but NewServer applies DefaultServerConfig unless options override it.
type ServerConfig struct {
	// IdleTimeout bounds how long a connection may stay open while waiting for
	// and reading the next request. It reaps dead/slow clients so a hung peer
	// never holds a goroutine forever. It does NOT limit how long a stream may
	// take to send — only request reads. 0 == unlimited.
	IdleTimeout time.Duration
	// WriteTimeout bounds a single response (or one stream frame) write. If a
	// consumer stops reading, the write times out and the connection is closed
	// instead of blocking a goroutine. 0 == unlimited.
	WriteTimeout time.Duration
}

func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		IdleTimeout:  10 * time.Minute,
		WriteTimeout: 30 * time.Second,
	}
}

// ServerOption mutates a ServerConfig; passed variadically to NewServer.
type ServerOption func(*ServerConfig)

func WithIdleTimeout(d time.Duration) ServerOption {
	return func(c *ServerConfig) { c.IdleTimeout = d }
}

func WithWriteTimeout(d time.Duration) ServerOption {
	return func(c *ServerConfig) { c.WriteTimeout = d }
}

type Server struct {
	manager  *segment.Manager
	listener net.Listener
	cfg      ServerConfig
	wg       sync.WaitGroup
	quit     chan struct{}
	quitOnce sync.Once
}

func NewServer(manager *segment.Manager, addr string, opts ...ServerOption) (*Server, error) {
	cfg := DefaultServerConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("network: listen %s: %w", addr, err)
	}
	return &Server{
		manager:  manager,
		listener: ln,
		cfg:      cfg,
		quit:     make(chan struct{}),
	}, nil
}

func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

func (s *Server) Serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Printf("network: accept: %v", err)
				continue
			}
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

func (s *Server) Close() error {
	s.quitOnce.Do(func() { close(s.quit) })
	err := s.listener.Close()
	s.wg.Wait()
	return err
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	for {
		// Bound the wait-for-and-read of each request so a dead peer is reaped.
		if s.cfg.IdleTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout)); err != nil {
				return
			}
		}

		req, err := protocol.ReadRequest(conn)
		if err != nil {
			// EOF/closed/idle-timeout are normal ways a connection ends: stay quiet.
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
				errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrDeadlineExceeded) {
				return
			}
			log.Printf("network: read request: %v", err)
			return
		}

		// Streaming writes many frames for one request, so it owns the conn directly.
		if req.Op == protocol.OpStreamRead {
			if err := s.handleStreamRead(conn, req.Payload); err != nil {
				return
			}
			continue
		}

		if err := s.writeResponse(conn, s.dispatch(req)); err != nil {
			return
		}
	}
}

func (s *Server) writeResponse(conn net.Conn, resp *protocol.Response) error {
	if s.cfg.WriteTimeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout)); err != nil {
			return err
		}
	}
	if err := protocol.WriteResponse(conn, resp); err != nil {
		log.Printf("network: write response: %v", err)
		return err
	}
	return nil
}

func (s *Server) dispatch(req *protocol.Request) *protocol.Response {
	switch req.Op {
	case protocol.OpProduce:
		return s.handleProduce(req.Payload)
	case protocol.OpRead:
		return s.handleRead(req.Payload)
	case protocol.OpNextOffset:
		return s.handleNextOffset()
	case protocol.OpEarliestOffset:
		return s.handleEarliestOffset()
	default:
		return errorResponse(fmt.Sprintf("unknown opcode: %d", req.Op))
	}
}

func (s *Server) handleProduce(data []byte) *protocol.Response {
	if len(data) == 0 {
		return errorResponse("empty payload")
	}
	offset, err := s.manager.Append(data)
	if err != nil {
		return errorResponse(err.Error())
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], offset)
	return &protocol.Response{Status: protocol.StatusOK, Payload: buf[:]}
}

func (s *Server) handleRead(payload []byte) *protocol.Response {
	if len(payload) != 8 {
		return errorResponse("read requires 8-byte offset")
	}
	offset := binary.BigEndian.Uint64(payload)
	data, err := s.manager.ReadAt(offset)
	if err != nil {
		return errorResponse(err.Error())
	}
	return &protocol.Response{Status: protocol.StatusOK, Payload: data}
}

func (s *Server) handleNextOffset() *protocol.Response {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], s.manager.NextOffset())
	return &protocol.Response{Status: protocol.StatusOK, Payload: buf[:]}
}

func (s *Server) handleEarliestOffset() *protocol.Response {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], s.manager.EarliestOffset())
	return &protocol.Response{Status: protocol.StatusOK, Payload: buf[:]}
}

// handleStreamRead sends every record from the requested start offset up to the
// current head as a sequence of StatusOK frames, then a StatusStreamEnd frame.
// One request, N frames — the consumer avoids a round-trip per record. Records
// are streamed in order, so the client tracks offsets itself (start + index).
//
// A real read error (corruption, out-of-retention) ends the stream with an
// error frame instead of StatusStreamEnd, so the client knows it did not simply
// catch up. io.EOF means "caught up to the head" and ends the stream cleanly.
func (s *Server) handleStreamRead(conn net.Conn, payload []byte) error {
	if len(payload) != 8 {
		return s.writeResponse(conn, errorResponse("stream-read requires 8-byte offset"))
	}
	offset := binary.BigEndian.Uint64(payload)
	for {
		data, err := s.manager.ReadAt(offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // caught up to the head
			}
			return s.writeResponse(conn, errorResponse(err.Error()))
		}
		if err := s.writeResponse(conn, &protocol.Response{Status: protocol.StatusOK, Payload: data}); err != nil {
			return err
		}
		offset++
	}
	return s.writeResponse(conn, &protocol.Response{Status: protocol.StatusStreamEnd})
}

func errorResponse(msg string) *protocol.Response {
	return &protocol.Response{Status: protocol.StatusError, Payload: []byte(msg)}
}
