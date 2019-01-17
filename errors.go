package main

import "os"

// ConnectionError is an error that occurred on the connection.
// if Close is true, the error is unrecoverable and the handler
// should close the connection
type ConnectionError struct {
	Detail string
	Err    error
	Close  bool
}

func (e ConnectionError) Error() string {
	return "ConnectionError(" + e.Detail + "; " + e.Err.Error() + ")"
}

func NewConnectionError(detail string, err error, close bool) error {
	if err == nil {
		return nil
	}
	return &ConnectionError{detail, err, close}
}

func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(ConnectionError)
	return ok
}

func ShouldClose(err error) bool {
	if err == nil {
		return false
	}
	if IsConnectionError(err) {
		cerr, ok := err.(ConnectionError)
		return ok && cerr.Close
	}
	if os.IsTimeout(err) {
		return false
	}
	return true
}
