package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	smallBufferSize = 256

	// maxBufferSize is the maximum size of a protocol input buffer in bytes
	maxBufferSize = 128 * 1024

	// initial size of protocol input buffer in bytes
	initialBufferSize = 4 * 1024

	// is this used?
	//messageVerbose          = true

	// httpShutdownGracePeriod is the time allowed for the http server
	// to respond to existing requests and close client connections.
	// this value should be less than shutdownGracePeriod.
	httpShutdownGracePeriod = 5 * time.Second

	// shutdownGracePeriod is the time allowed for the entire server shutdown:
	// protocol servers and http servers stop listeners, process and respond to existing clients,
	// push received messages to the queues, close all client connections,
	// and flush outgoing message queues.
	// This value should be longer than httpShutdownGracePeriod
	shutdownGracePeriod = 20 * time.Second

	// clientTimeout is how long a read or write will block before
	// the socket is closed
	clientTimeout = 3 * time.Minute

	// metricPrefix is the prefix on all prometheus exporter metrics
	metricPrefix = "logproxy_"

	// defaultQueueSize is the queue size to use if none is specified. The queue size
	// is the maximum number of messages that can be queued before senders (input protocols) are blocked
	defaultQueueSize = 1000

	// defaultQueue is a token indicating the default queue (the first output queue configured)
	defaultQueue = "@DEFAULT@"
)

// ServerStatus is the current server status
type ServerStatus int64

const (
	// Running - server is running and listening for new connections
	Running = ServerStatus(iota + 1)
	// ShuttingDown - no new connections will be accepted
	ShuttingDown
)

type MessageQueue struct {
	Name  string
	Queue chan PubMessage
}

type Server struct {
	goVersion  string
	cfg        *ServerConfig
	startTime  time.Time
	log        Logr
	httpServer *http.Server
	status     ServerStatus
	//ctx        context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	pservers []*ProtocolServer
	queues   []*MessageQueue
	//cancelers []func()
}

func (s *Server) enabledProtocols() []string {
	names := make([]string, 0, len(s.cfg.Protocols))
	for i := range s.cfg.Protocols {
		cfg := &s.cfg.Protocols[i]
		if cfg.Enable {
			names = append(names, cfg.Type)
		}
	}
	return names
}

// NewServer constructs the Server instance (but does not start it)
func NewServer(cfg *ServerConfig) *Server {

	now := time.Now()
	s := &Server{
		startTime: now,
		goVersion: runtime.Version(),
		cfg:       cfg,
		pservers:  make([]*ProtocolServer, 0, 0),
		queues:    make([]*MessageQueue, 0, 0),
		//cancelers: make([]func(), 0, 0),
		log: NewLogr(os.Stdout, &LogrOptions{Debug: cfg.Server.Debug}),
	}
	return s
}

func (s *Server) setupSinks(ctx context.Context) error {

	for _, cfg := range s.cfg.Sinks {

		startSinkFn, ok := sinkTypes[cfg.Type]
		if !ok {
			return fmt.Errorf("Unsupported sink type %s", cfg.Type)
		}
		if cfg.Name == "" || cfg.Size == 0 {
			return errors.New("Sink configuration requires Type, Name, and Size")
		}
		mq := &MessageQueue{
			Name:  cfg.Name,
			Queue: make(chan PubMessage, cfg.Size),
		}
		if _ /*cancelFn*/, err := startSinkFn(ctx, s.log, mq.Queue, &cfg); err != nil {
			close(mq.Queue)
			return err
		}
		s.queues = append(s.queues, mq)
	}

	if len(s.queues) == 0 {
		return errors.New("Invalid configuration: must be at least one output sink")
	}
	return nil
}

func (s *Server) Push(name string, msg *PubMessage) error {

	var mq *MessageQueue

	if name == defaultQueue {
		mq = s.queues[0]
	} else {
		// no locks because the queues never change after server start
		for _, pm := range s.queues {
			if pm.Name == name {
				mq = pm
				break
			}
		}
	}
	if mq == nil {
		// this should probably be a panic ...
		return fmt.Errorf("Invalid queue: %s", name)
	}
	mq.Queue <- *msg
	return nil
}

func (s *Server) start(parentCtx context.Context) error {

	var httpHandlers int

	s.log.Println("Starting server")
	s.status = Running
	s.log.Debug("Server is in debug logging")

	serverHttpEnabled := s.cfg.Http.Enable && s.cfg.Http.Listen != ""
	ctx, cancel := context.WithCancel(parentCtx)
	s.cancel = cancel

	// start output sinks
	if err := s.setupSinks(ctx); err != nil {
		return err
	}

	// start listeners
	for i := range s.cfg.Protocols {
		cfg := &s.cfg.Protocols[i]
		if cfg.Enable {
			// check that it's a valid protocol and create chunk handler
			// the server is also the output channel (second param)
			pserver, err := NewProtocolServer(s, s, cfg)
			if err != nil {
				// failure to start any protocol server is fatal
				return err
			}
			s.mu.Lock()
			s.pservers = append(s.pservers, pserver)
			s.mu.Unlock()

			if pserver.isHTTP() {
				if !serverHttpEnabled {
					msg := fmt.Sprintf(`Config Error: HTTPPath listed for %s but Http server is not enabled.\n`+
						`Http.Enabled should be true and Http.Listen contains "host:port"\n`, cfg.Type)
					return errors.New(msg)
				}
				http.HandleFunc(cfg.HTTP_path, protoHandler(pserver))
				s.log.Printf("HTTP listener %s at %s\n",
					cfg.Type, cfg.HTTP_path)
				httpHandlers++
			}
			if pserver.isTCP() {
				go pserver.serve(ctx)
			}
		}
	}

	if serverHttpEnabled {
		if s.cfg.Http.Telemetry_path != "" {
			var opts promhttp.HandlerOpts
			reg := prometheus.DefaultGatherer
			opts.ErrorLog = s.log
			http.Handle(s.cfg.Http.Telemetry_path, promhttp.HandlerFor(reg, opts))
			s.log.Printf("Http listener telemetry at %s %s\n",
				s.cfg.Http.Listen, s.cfg.Http.Telemetry_path)
			httpHandlers++
		}
		if httpHandlers > 0 {
			// startHttpServer launches a goroutine to serve http requests and returns immediately.
			// If the http server fails to start (for example, the http port is unavailable),
			// Server.Shutdown is invoked.
			httpServer, err := startHttpServer(s, &s.cfg.Http)
			if err != nil {
				return err
			}
			s.httpServer = httpServer
		} else {
			s.log.Printf("Warning: Http server was enabled, but no listenes were configured. Http not started\n")
		}
	}
	return nil
}

func (s *Server) queuedMessages() int {
	count := 0
	for _, mq := range s.queues {
		count += len(mq.Queue)
	}
	return count
}

func (s *Server) numClients() (int, int) {
	pCount := 0
	cCount := 0
	for _, ph := range s.pservers {
		pCount++
		cCount += ph.numConnections()
	}
	return pCount, cCount
}

// Shutdown - stop server, close all listeners and all client connections
func (s *Server) Shutdown() {

	// change status to shutting down so protocol handlers will stop receiving messages and close their sockets
	wasShuttingDown := false
	s.mu.Lock()
	if s.status == ShuttingDown {
		wasShuttingDown = true
	} else {
		s.status = ShuttingDown
	}
	s.mu.Unlock()
	if wasShuttingDown {
		s.log.Printf("Warning: Shutdown called more than once")
		return
	}

	var httpUp int32
	// close http servers gracefully - process existing connections and send responses
	if s.httpServer != nil {
		httpUp = 1
		go func() {
			httpShutdownCtx, cancel := context.WithTimeout(context.Background(), httpShutdownGracePeriod)
			s.httpServer.Shutdown(httpShutdownCtx)
			atomic.StoreInt32(&httpUp, int32(0))
			cancel()
		}()
	}

	// wait for input connections to close and output queues to flush
	shutdownTimer := time.NewTimer(shutdownGracePeriod)

	for {
		pCount, cCount := s.numClients()
		qLen := s.queuedMessages()
		ht := atomic.LoadInt32(&httpUp)

		if cCount == 0 && qLen == 0 && ht == 0 {
			// done, leave early
			shutdownTimer.Stop()
			break
		}

		select {
		case <-shutdownTimer.C:
			if cCount > 0 {
				s.log.Printf("Warning: %d handlers still running with %d connections\n", pCount, cCount)
			}
			if ht == 1 {
				s.log.Printf("Warning: HTTP server is still up\n")
			}
			if qLen > 0 {
				s.log.Printf("Warning: quitting with %d messages queued\n", qLen)
			}
			break
		default:
			s.log.Printf("Waiting: HT=%d  proto=%d  conn=%d  msgs=%d\n", ht, pCount, cCount, qLen)
			time.Sleep(1 * time.Second)
		}
	}
	// close queues
	for _, mq := range s.queues {
		close(mq.Queue)
	}
	// terminate context and all child contexts (ProtocolServers)
	s.cancel()
}

// Protected check on running state
func (s *Server) isRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status == Running
}

func (s *Server) activeProtocols() []*ProtocolServer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pservers[:]
}

func (s *Server) catchSignals(ctx context.Context) <-chan bool {

	done := make(chan bool, 1)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

	go func() {
		for {
			sig := <-sigs
			s.log.Printf("Caught signal %v\n", sig)
			if sig == syscall.SIGUSR1 {
				//s.dumpStats()
				continue
			} else {
				break
			}
		}
		done <- true
	}()
	return done
}

func main() {

	ctx := context.Background()

	cfg, err := LoadConfig(ctx)
	if err != nil {
		panic(fmt.Sprintln("Config error:", err))
	}

	s := NewServer(cfg)

	err = s.start(ctx)
	sigwait := s.catchSignals(ctx)

	if err != nil {
		s.log.Printf("Server start failed with error: %v\n", err)
		s.Shutdown()
	}

	for s.isRunning() {
		select {
		//case <-ctx.Done():
		//	break
		case <-sigwait:
			s.Shutdown()
			break
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
	s.log.Println("Exiting")
}
