package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"
)

type IEvent interface{}
type IEvents []IEvent

type LogWriter struct {
	out      io.Writer
	outBytes int64
}

func NewLogWriter(path string) (lw *LogWriter, err error) {

	if fo, err := OpenOutputFile(path); err != nil {
		return nil, err
	} else {
		lw := &LogWriter{
			out: fo,
		}
		return lw, err
	}
}

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

func (lw *LogWriter) WriteEvent(e interface{}) error {
	// first try binary
	eb, ok := e.([]byte)
	if ok {
		// already binary
		nb, err := lw.out.Write(eb)
		if err != nil {
			return NewConnectionError("Write failure", err, true)
		}
		lw.outBytes += int64(nb)
		return nil
	}

	// then try string (and then to bytes)
	es, ok := e.(fmt.Stringer)
	if ok {
		nb, err := lw.out.Write([]byte(es.String()))
		if err != nil {
			return NewConnectionError("Write failure", err, true)
		}
		lw.outBytes += int64(nb)
		return nil
	}

	// otherwise give up
	errMsg := fmt.Sprintf("Can't stringify event %v", reflect.TypeOf(e))
	return NewConnectionError("", errors.New(errMsg), false)
}

func (lw *LogWriter) WriteEvents(events []interface{}) error {

	for _, e := range events {
		if err := lw.WriteEvent(e); err != nil {
			return err
		}
	}
	return nil
}

func (lw *LogWriter) Close() {
	if lw.out != nil {
		if cl, ok := lw.out.(io.Closer); ok {
			cl.Close()
		}
	}
}

func startFileSink(parentCtx context.Context, log Logr, recv <-chan PubMessage, cfg *SinkConfig) (shutdown func(), err error) {

	if err := ensureConfig([]string{"path"}, cfg); err != nil {
		return nil, err
	}

	path := getConfigString("path", cfg.Config)
	logWriter, err := NewLogWriter(path)
	if err != nil {
		return nil, err
	}
	stopChan := make(chan bool)
	go func() {
		defer logWriter.Close()

		for {
			select {

			case pubMsg, ok := <-recv:
				if ok {
					logWriter.WriteEvent(pubMsg.Data)
					// TODO: update send metrics
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
