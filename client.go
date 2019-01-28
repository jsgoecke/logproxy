package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type ConnectionStatus int64

// Connection Status
const (
	// initializing, not active yet
	Active = ConnectionStatus(iota + 1)
	CloseNormal
	CloseError
	ServerShutdown
)

func (s ConnectionStatus) String() string {
	switch s {
	case Active:
		return "Active"
	case CloseNormal:
		return "CloseNormal"
	case CloseError:
		return "CloseError"
	case ServerShutdown:
		return "ServerShutdown"
	default:
		return "Unknown"
	}
}

// Client is created for each incoming TCP connection, and manages all
// communication with the client for the duration of the socket connection.
// This class is used by all TCP protocols. (see proto.go)
//
// Message handling is performed asynchronously. Whenever bytes are received
// on a connection, the protocol handler parses the buffer to determine
// if there is at least one complete message received.
// Each received message is passed asynchronously to an output handler.
// As currently implemented, none of the protocols provide any response or
// acknowledgement to the TCP client, other than TCP ACK.
type Client struct {
	conn       net.Conn
	srv        *Server
	startTime  time.Time
	lastActive time.Time
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	status     ConnectionStatus
	pserver    *ProtocolServer
	reason     error
	host       string
	port       int
	ident      string
	cid        uint32
}

// NewTCPClient - create handler for new connection.
func NewTCPClient(ctx context.Context, cid uint32, pserver *ProtocolServer, conn *net.TCPConn) *Client {

	now := time.Now()

	c := &Client{
		cid:        cid,
		srv:        pserver.s,
		startTime:  now,
		lastActive: now,
		conn:       conn,
		status:     Active,
		pserver:    pserver,
	}
	c.ctx, c.cancel = context.WithCancel(ctx)

	// snapshot the string version of the connection
	addr := conn.RemoteAddr().(*net.TCPAddr)
	c.host = addr.IP.String()
	c.port = addr.Port
	c.ident = fmt.Sprintf("[%d] %s (%s) %s:%d",
		c.cid, pserver.cfg.Name, pserver.cfg.Type, addr.IP, addr.Port)

	c.srv.log.Debugf("[%d] New connection: %s\n", c.cid, c.ident)
	return c
}

func (c *Client) String() string {
	return "Client: " + c.ident
}

func (c *Client) isActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status == Active
}

func (c *Client) close(code ConnectionStatus, reason error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status == Active {
		c.status = code
		c.reason = reason
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		if code != CloseNormal {
			c.srv.log.Debugf("Closing client connection %s; code=%v; %v\n", c.ident, code, reason)
		}
	} else {
		c.srv.log.Printf("Warning: closing already closed connection %s, code=%v", c.ident, c.status)
	}
}

func (c *Client) readLoop() {

	debugf := c.srv.log.Debugf

	defer func() {
		c.mu.Lock()
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		c.mu.Unlock()

		if c.reason != nil && c.reason != io.EOF {
			c.pserver.inErrors.Inc()
		}
		if c.status != CloseNormal {
			debugf("[%d] connection closed, code=%v, reason %v\n", c.cid, c.status, c.reason)
		}

		c.pserver.mu.Lock()
		delete(c.pserver.clients, c.cid)
		c.pserver.mu.Unlock()

		c.cancel()
	}()

	if !c.srv.isRunning() || !c.isActive() {
		return
	}
	//debugf("[%d] Entering read loop\n", c.cid)

	c.conn.SetDeadline(time.Now().Add(clientTimeout))

	scanner := bufio.NewScanner(c.conn)
	// increase buf max size to 128k to support syslog spec
	scanBuf := make([]byte, 0, initialBufferSize)
	scanner.Buffer(scanBuf, maxBufferSize)
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		cr, err := c.pserver.handler.ProcessChunk(data, atEOF)
		if err == nil && cr != nil && cr.bytesRead > 0 {
			c.pserver.inBytes.Add(float64(cr.bytesRead))
			c.pserver.inMsgs.Add(float64(cr.msgsRead))

			c.lastActive = time.Now()
			// the second paramer is unused but must be non-null byte array
			return int(cr.bytesRead), data, nil
		}
		return 0, nil, err
	})

	// read until eof or error (io error, protocol error, system shutdown)
	for scanner.Scan() && c.srv.isRunning() {
		select {
		case <-c.ctx.Done():
			break
		default:
		}
	}
	scannerErr := scanner.Err()
	if scannerErr != nil {
		c.close(CloseError, scannerErr)
	}
	// scanner exited normally - probably EOF
	if c.srv.isRunning() {
		c.close(CloseNormal, nil)
	} else {
		c.close(ServerShutdown, nil)
	}
}
