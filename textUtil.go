package main

import (
	"strings"
)

/*
 * textUtil - utilities for processing text procols (line & json)
 */

type trimmable func(b byte) bool

func lineTrim(b byte) bool {
	return b == 0 || b == '\r' || b == '\n'
}

func nullTrim(b byte) bool {
	return b == 0
}

// trim - remove leading & trailing nulls and trailing \r from byte array.
// This handles lines that ehd in \r\n, and allows client to send
// strings with trailing null.
// Returns slice of original byte array, which may have length zero
// if nothing is left after trimming.
// This is used for line-oriented protocols including json
func trim(b []byte, trimmable trimmable) []byte {
	start := 0
	end := len(b)
	for start < end && trimmable(b[start]) {
		start++
	}
	for start < end && trimmable(b[end-1]) {
		end--
	}
	return b[start:end]
}

// defaultSplitter splits incoming messages with either a newline or null byte.
// return @advance number of bytes input parser should advance
//        @token the next full message
// We can scan for \n or 0 in the a byte array, without converting to string,
// because in UTF8, \n and 0 do not occur as part of any multi-byte characters
// other than exactly \n and 0.
func defaultSplitter(data []byte, atEOF bool) (advance int, token []byte, err error) {

	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	for i := 0; i < len(data); i++ {
		ch := data[i]
		if ch == 0 || ch == '\n' {
			return i + 1, data[:i], nil
		}
	}

	// If we're at EOF (connection was closed), and we have non-terminated data,
	// it may be complete or incomplete. Return it as a final message,
	// and let protocol handler decide if it's valid
	if atEOF {
		return len(data), data, nil
	}

	// didn't find delimiter, wait for more data
	return 0, nil, nil
}

// getConfigString returns string from untyped config structure using dotted path
// example:
//    Config:
//      foo:
//        baz: "yeah!"
//    getConfigString("foo.baz") returns "yeah!"
//    any non-existent value or non-string type returns an empty string
func getConfigString(path string, cfg map[string]interface{}) string {
	val := getConfigVal(path, cfg)
	if val != nil {
		if sval, ok := val.(string); ok {
			return sval
		}
	}
	return ""
}

func getConfigInt(path string, cfg map[string]interface{}) (ival int, ok bool) {
	val := getConfigVal(path, cfg)
	if val != nil {
		if ival, ok := val.(int); ok {
			return ival, true
		}
	}
	return 0, false
}

func getConfigVal(path string, cfg map[string]interface{}) interface{} {
	node := cfg
	parts := strings.Split(path, ".")
	plen := len(parts)

	for _, step := range parts[:plen-1] {
		if next, ok := node[step]; ok {
			if nextNode, ok := next.(map[string]interface{}); ok {
				node = nextNode
			} else {
				return nil
			}
		} else {
			return nil
		}
	}
	if val, ok := node[parts[plen-1]]; ok {
		return val
	}
	return nil
}
