package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type IEvent interface{}
type IEvents []IEvent

// LogWriter wraps a writable stream handle for logging
type LogWriter struct {
	out       io.Writer
	outBytes  prometheus.Counter
	outMsgs   prometheus.Counter
	outErrors prometheus.Counter
}

func NewLogWriter(path string) (lw *LogWriter, err error) {

	fo, err := OpenOutputFile(path)
	if err != nil {
		return nil, err
	}
	lw = &LogWriter{
		out:       fo,
		outBytes:  outBytes.WithLabelValues("file", "default"),
		outMsgs:   outMsgs.WithLabelValues("file", "default"),
		outErrors: outErrors.WithLabelValues("file", "default"),
	}
	return lw, nil
}

// OpenOutputFile opens the destination log file
func OpenOutputFile(path string) (io.Writer, error) {

	if path == "" {
		return nil, errors.New("Missing output file path")
	}
	flags := os.O_APPEND | os.O_CREATE | os.O_WRONLY
	out, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		msg := fmt.Sprintf("Failed opening output file %s: %s\n", path, err.Error())
		return nil, errors.New(msg)
	}
	return out, nil
}

// WriteEvent writes a single event to the output file
func (lw *LogWriter) WriteEvent(e interface{}) error {
	// first try binary
	eb, ok := e.([]byte)
	if ok {
		// already binary
		nb, err := lw.out.Write(eb)
		if err != nil {
			return NewConnectionError("Write failure", err, true)
		}
		lw.outBytes.Add(float64(nb))
		lw.outMsgs.Inc()
		return nil
	}

	// then try string (and then to bytes)
	es, ok := e.(fmt.Stringer)
	if ok {
		nb, err := lw.out.Write([]byte(es.String()))
		if err != nil {
			return NewConnectionError("Write failure", err, true)
		}
		lw.outBytes.Add(float64(nb))
		lw.outMsgs.Inc()
		return nil
	}

	// otherwise give up
	errMsg := fmt.Sprintf("Can't stringify event %v", reflect.TypeOf(e))
	lw.outErrors.Inc()
	return NewConnectionError("", errors.New(errMsg), false)
}

// WriteEvents sends an array of events to the output file
func (lw *LogWriter) WriteEvents(events []interface{}) error {

	for _, e := range events {
		if err := lw.WriteEvent(e); err != nil {
			return err
		}
	}
	return nil
}

// Close closes the output stream if it has a Close method
// This method does not return errors - any error during closing is ignored
// so that we may proceed to cleanly close other connections
func (lw *LogWriter) Close() {
	if lw.out != nil {
		if cl, ok := lw.out.(io.Closer); ok {
			cl.Close()
		}
	}
}

func startFileSink(parentCtx context.Context, log Logr, recv <-chan PubMessage, cfg *SinkConfig) (shutdown func(), err error) {

	if err = ensureConfig([]string{"path"}, cfg); err != nil {
		return nil, err
	}

	path := getConfigString("path", cfg.Config)
	logWriter, err := NewLogWriter(path)
	if err != nil {
		return nil, err
	}
	logWriter.outMsgs.Inc()
	stopChan := make(chan bool)
	go func() {
		defer logWriter.Close()

		for {
			select {

			case pubMsg, ok := <-recv:
				if ok {
					logWriter.WriteEvent(pubMsg.Data)
				} else {
					// message channel closd, terminate loop
					break
				}
			case <-stopChan:
				break

			default:
				/* if no messages are available, keep waiting */
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()

	stopFunc := func() {
		stopChan <- true
	}
	return stopFunc, nil
}
