/*
Logr package implements a simple logging api that has
exactly two levels: normal and debug.
Inspired by https://dave.cheney.net/2015/11/05/lets-talk-about-logging
*/
package main

import (
	"fmt"
	"io"
	"os"
)

type Logr interface {

	// Printf - log printf-formatted message to output log
	Printf(format string, v ...interface{})
	// Printf - log values to output log, with newline
	Println(v ...interface{})
	// Debugf - log printf-formatted message to output log, if debug is enabled
	Debugf(format string, v ...interface{})
	// Debugf - log values to output log, with newline, if debug is enabled
	Debug(v ...interface{})
	// Turn on debug logging
	EnableDebug(debug bool)
	// IsDebugEnabled returns true of debug is enabled
	IsDebugEnabled() bool

	// SetPrefix sets a prefix to begin all log messages. The prefix is applied
	// to logs written with Printf, Println, Debugf, and Debug; but not Write().
	SetPrefix(prefix string)

	Write(b []byte) (n int, err error)
}

// LogrOptions are initial settings for Logr
type LogrOptions struct {

	// Debug is the initial debug flag (that enables Debugf). Debug state can be changed with EnableDebug
	Debug bool

	// Prefix will be prepended to all debug lines (sent with Print* and Debug* but not Write()
	Prefix string

	// OnFail is callback to be invoked if there is a failure writing logs.
	// If a callback is not provided (OnFail is nil), the built-in panic() will be used,
	// and the logproxy will shut down. This was chosen for default behavior, because
	// if you didn't read the documentation, and logging is incorrectly configured, it's
	// better to fail fast and alert the developer, than to fail silently.
	// A more gentle callback is provided to put notifications on stderr (logr.OnFailStderr):
	//
	//     func OnFailStderr(err error) {
	//	     m := fmt.Sprintf("LOG FAIL: %s\n", err.Error())
	//	     os.Stderr.Write([]byte(m))
	//     }
	//
	// Note that the OnFail callback doesn't provide the failed log message, it just
	// reports that logs did not get written.
	OnFail func(err error)
}

// NewLogr wraps a Writer with the Logr interface
func NewLogr(Writer io.Writer, opts *LogrOptions) Logr {

	l := &logr{writer: Writer}
	if opts != nil {
		l.debug = opts.Debug
		l.prefix = opts.Prefix
		l.onFail = opts.OnFail
	}
	if l.onFail == nil {
		l.onFail = func(err error) {
			panic(err)
		}
	}
	return l
}

// logr is our internal implementation of Logr
type logr struct {
	writer io.Writer
	debug  bool
	prefix string
	onFail func(error)
}

func (l *logr) Write(b []byte) (n int, err error) {
	return l.writer.Write(b)
}

func (l *logr) Printf(format string, v ...interface{}) {
	if l.prefix != "" {
		_, err := fmt.Fprint(l.writer, l.prefix)
		if err != nil {
			l.onFail(err)
		}
	}
	_, err := fmt.Fprintf(l.writer, format, v...)
	if err != nil {
		l.onFail(err)
	}
}

func (l *logr) Println(v ...interface{}) {
	if l.prefix != "" {
		_, err := fmt.Fprint(l.writer, l.prefix)
		if err != nil {
			l.onFail(err)
		}
	}
	fmt.Fprint(l.writer, v...)
	_, err := fmt.Fprint(l.writer, "\n")
	if err != nil {
		l.onFail(err)
	}
}

func (l *logr) Debugf(format string, v ...interface{}) {
	if l.debug {
		if l.prefix != "" {
			_, err := fmt.Fprint(l.writer, l.prefix)
			if err != nil {
				l.onFail(err)
			}
		}
		_, err := fmt.Fprintf(l.writer, format, v...)
		if err != nil {
			l.onFail(err)
		}
	}
}

func (l *logr) Debug(v ...interface{}) {
	if l.debug {
		if l.prefix != "" {
			_, err := fmt.Fprint(l.writer, l.prefix)
			if err != nil {
				l.onFail(err)
			}
		}
		fmt.Fprint(l.writer, v...)
		_, err := l.writer.Write([]byte("\n"))
		if err != nil {
			l.onFail(err)
		}
	}
}

// EnableDebug turns on or off debug logging
func (l *logr) EnableDebug(debug bool) {
	l.debug = debug
}

// IsDebugEnabled returns true if debug logging is enabled
func (l *logr) IsDebugEnabled() bool {
	return l.debug
}

// SetPrefix sets a prefix that will be inserted at the start of each log line.
// The prefix is only used for Printf,Println,Debugf, and Debug, but not Write()
func (l *logr) SetPrefix(prefix string) {
	l.prefix = prefix
}

// OnFailStderr is a failure handler to report on stderr if there were failures writing logs
func OnFailStderr(err error) {
	m := fmt.Sprintf("LOG FAIL: %s\n", err.Error())
	os.Stderr.Write([]byte(m))
}
