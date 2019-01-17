package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

//var ErrProtocolError = errors.New("Protocol Error")

type ProtocolType string

type ChunkResult struct {
	bytesRead uint32
	msgsRead  uint32
}

// ProtocolHandler provides protocol-specific processing,
// to turn an incoming byte stream into
// distinct messages,for forwarding to the output channel.
//
// The interface is designed to be independendent of the
// transport protocol, and can be invoked
// for bytes received on a TCP connection
// (a Client instance) or to process the body
// of an HTTP post (by httpsever.go)
type ProtocolHandler interface {

	// ProcessChunk handles a chunk of bytes received via the TCP
	// connection. On each call, the handler first determines
	// whether there is a complete message. If a complete message
	// is available, process the message, forwarding to the output
	// channel, and return ChunkResult indicating how many messages
	// were processed, and how many bytes were consumed from the
	// input buffer.
	// If there are no complete messages yet, the handler signals
	// to the caller to wait for more data by returning a nil error
	// and either a nil (*ChunkResult) or ChunkResult with bytesRead=0
	//
	// If atEOF is true, the buffer should be interpreted as
	// a complete message, even if it does not include a delimiter
	ProcessChunk(data []byte, atEOF bool) (*ChunkResult, error)
}

// protocolConstructor defines the interface for creating a ProtocolHandler
type protocolConstructor func(log Logr, out OutputChannel, pcfg *ProtocolConfig) ProtocolHandler

// protocolTypes is the list of input protocol names and constructor functions.
// The string name is used in the "Type" field of config.yaml.
var protocolTypes = map[string]protocolConstructor{
	"log":        NewLogHandler,
	"json":       NewJsonHandler,
	"syslog":     NewSyslogRelpHandler,
	"fluent":     NewFluentForwardReader,
	"prometheus": NewPromRemoteHandler,
}

// isProtocolType returns true if the parameter is the name of a supported protocol
func isProtocolType(s string) bool {
	_, ok := protocolTypes[s]
	return ok
}

// ProtocolServer manages the TCP listener
// all client connections, and traffic metrics for a supported prortocol.
// This implementation
// is shared by all TCP protocols and abstracts the network
// so that ProtocolHandler can decode the bytes.
//
// The ProtocolServer launches when the Server process starts, and creates
// a ProtocolHandler instance. For each incoming TCP connection, a Client
// is created handle communication with the sending client.
type ProtocolServer struct {
	cfg       *ProtocolConfig
	s         *Server
	listener  *net.TCPListener
	handler   ProtocolHandler
	inBytes   prometheus.Counter
	inMsgs    prometheus.Counter
	inErrors  prometheus.Counter
	clients   map[uint32]*Client
	mu        sync.Mutex
	cid       uint32
	connCount uint32
}

var (
	// inBytes is a prometheus Counter metric that counts the number of bytes received for each proto
	inBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricPrefix + "received_bytes_total",
			Help: "Number of bytes received",
		},
		[]string{"proto"},
	)
	// inMsgs is a prometheus Counter metric that counts the number of messages received for each proto
	inMsgs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricPrefix + "received_messages_total",
			Help: "Number of messages received",
		},
		[]string{"proto"},
	)
	// inErrors is a prometheus Counter metric that counts the number of errors encountered processing proto streams
	inErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricPrefix + "errors",
			Help: "Number of connection or protocol errors",
		},
		[]string{"proto"},
	)
)

func init() {
	prometheus.MustRegister(inBytes)
	prometheus.MustRegister(inMsgs)
	prometheus.MustRegister(inErrors)
}

// isTCP - returns true if this protocol server has a TCP listener
func (pserver *ProtocolServer) isTCP() bool {
	return pserver.cfg.Host != "" && pserver.cfg.Port != 0
}

// isHTTP - returns true if this protocol server has an http handler
func (pserver *ProtocolServer) isHTTP() bool {
	return pserver.cfg.HttpPath != ""
}

func (pserver *ProtocolServer) numConnections() int {
	if pserver.isTCP() {
		pserver.mu.Lock()
		defer pserver.mu.Unlock()
		return len(pserver.clients)
	}
	return 0
}

// listClients - helper function to get list of clients while minimizing
// potential for lock conflicts
func (pserver *ProtocolServer) listClients() []*Client {

	if pserver.isTCP() {
		pserver.mu.Lock()
		defer pserver.mu.Unlock()

		clients := make([]*Client, 0, len(pserver.clients))
		for _, c := range pserver.clients {
			clients = append(clients, c)
		}
		return clients
	} else {
		return make([]*Client, 0, 0)
	}
}

// serve is a goroutine for handling all connections. Returns when server shuts down.
func (pserver *ProtocolServer) serve(ctx context.Context) {

	if !pserver.isTCP() {
		return
	}
	cfg := pserver.cfg
	s := pserver.s
	protoCtx, cancel := context.WithCancel(ctx)
	defer func() {
		if pserver.listener != nil {
			s.log.Debugf("Proto listener (%v, port %d) exiting\n", cfg.Type, cfg.Port)
			pserver.listener.Close()
			pserver.listener = nil
		}

		// get list of clients first, then iterate without lock,
		// since each call to close() needs to lock pserver to remove itself
		for _, client := range pserver.listClients() {
			client.close(ServerShutdown, nil)
		}
		cancel()
	}()

	s.log.Printf("Starting protocol handler %v\n", cfg.Type)

	for s.isRunning() {
		select {
		case <-protoCtx.Done():
			break
		default:
			/*  pass */
		}

		// listen, but check for server shutdown every 250 ms
		pserver.listener.SetDeadline(time.Now().Add(250 * time.Millisecond))
		conn, err := pserver.listener.AcceptTCP()
		if err == nil {
			s.log.Debug("Got new connection")
			conn.SetNoDelay(true)
			conn.SetKeepAlive(true)
			if s.cfg.Server.KeepAlivePeriod > 0 {
				conn.SetKeepAlivePeriod(s.cfg.Server.KeepAlivePeriod * time.Second)
			}

			pserver.mu.Lock()
			pserver.connCount++
			cid := pserver.connCount
			pserver.mu.Unlock()

			client := NewTCPClient(protoCtx, cid, pserver, conn)

			//pserver.mu.Lock()
			pserver.clients[cid] = client
			//pserver.mu.Unlock()
			s.log.Debugf("entering client read loop for %s\n", pserver.cfg.Type)

			go client.readLoop()
		}
	}
}

// NewProtocolServer - create protocol server and, if it's listening on tcp port, open listener port
func NewProtocolServer(s *Server, out OutputChannel, cfg *ProtocolConfig) (pserver *ProtocolServer, err error) {

	newHandlerFunc, ok := protocolTypes[cfg.Type]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Invalid protocol type %s\n", cfg.Type))
	}

	var listener *net.TCPListener

	if cfg.Host != "" && cfg.Port != 0 {
		ip := net.ParseIP(cfg.Host)
		if ip == nil {
			msg := fmt.Sprintf("Invalid IP host %s for service %s", cfg.Host, cfg.Type)
			return nil, errors.New(msg)
		}
		tcpAddr := net.TCPAddr{
			IP:   ip,
			Port: cfg.Port,
		}
		listener, err = net.ListenTCP("tcp", &tcpAddr)
		if err != nil {
			return nil, err
		}
	}

	pserver = &ProtocolServer{
		s:        s,
		cfg:      cfg,
		listener: listener,
		handler:  newHandlerFunc(s.log, out, cfg),
		clients:  make(map[uint32]*Client),
		inBytes:  inBytes.WithLabelValues(cfg.Type),
		inMsgs:   inMsgs.WithLabelValues(cfg.Type),
		inErrors: inErrors.WithLabelValues(cfg.Type),
	}

	return pserver, nil
}
