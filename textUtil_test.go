package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLineTrim(t *testing.T) {
	b := []byte{0, '\r', '\n', 'a'}
	assert.Equal(t, lineTrim(b[0]), true, "trim at null byte")
	assert.Equal(t, lineTrim(b[1]), true, "trim at cr")
	assert.Equal(t, lineTrim(b[2]), true, "trim at newline")
	assert.Equal(t, lineTrim(b[3]), false, "no trim at char")
}

func TestTrim(t *testing.T) {

	buf_n := []byte{0, 'h', 'e', 'l', 'l', 'o', 0}
	buf_nt := trim(buf_n, nullTrim)
	assert.Equal(t, len(buf_nt), 5, "null trimmer")
	assert.Equal(t, buf_nt[0], byte('h'), "null trimmer")

	buf_nl := []byte{'\n', 'h', 'e', 'l', 'l', 'o', '\r', '\n'}
	buf_nlt := trim(buf_nl, lineTrim)
	assert.Equal(t, len(buf_nlt), 5, "null trimmer")
	assert.Equal(t, buf_nlt[0], byte('h'), "null trimmer")

}

func TestSplitter(t *testing.T) {

	buf := []byte{'h', 'e', 'l', 'l', 'o', '\r', '\n', 0, 'x', 'y', 'z'}

	var adv int
	var token []byte
	var err error

	// read a line, not EOF
	adv, token, err = defaultSplitter(buf, false)
	assert.Equal(t, adv, 7, "splitter advance bytes")
	assert.Equal(t, len(token), 6, "splitter on newline")
	assert.Equal(t, err, nil)

	// data with line ending, EOF
	adv, token, err = defaultSplitter(buf, true)
	assert.Equal(t, adv, 7, "splitter with newline and EOF ignores EOF")
	assert.Equal(t, err, nil)

	// no data, EOF
	adv, token, err = defaultSplitter(make([]byte, 0, 0), true)
	assert.Equal(t, adv, 0, "splitter atEOF with empty buf - no bytes")
	assert.Equal(t, err, nil, "splitter atEOF with empty buf - no err")

	// data with no line ending, EOF
	adv, token, err = defaultSplitter(buf[8:], true)
	assert.Equal(t, adv, 3, "splitter at EOF with bytes - len")
	assert.Equal(t, err, nil, "splitter atEOF with empty buf - no err")

	// data with no line ending, not eof
	adv, token, err = defaultSplitter(buf[8:], false)
	assert.Equal(t, adv, 0, "splitter no term, not eof")
	assert.Equal(t, err, nil, "splitter no term, not eof")
}

func TestConfigPathString(t *testing.T) {

	level0 := make(map[string]interface{})
	level1 := make(map[string]interface{})
	level2 := make(map[string]interface{})

	level0["zero"] = "valzero"
	level0["int"] = 100
	level0["one"] = level1

	level1["bar"] = "baz"
	level1["float"] = 3.14
	level1["two"] = level2

	level2["bar"] = "bazbaz"
	level2["list"] = []string{"a", "b", "c"}
	level2["three"] = make(map[string]interface{})

	assert.Equal(t, "valzero", getConfigString("zero", level0), "first level")
	assert.Equal(t, "", getConfigString("missing", level0), "missing entry returns empty string")
	assert.Equal(t, "", getConfigString("int", level0), "integer returns empty string")

	assert.Equal(t, "baz", getConfigString("one.bar", level0), "second level")
	assert.Equal(t, "", getConfigString("one.float", level0), "float returns empty string")

	assert.Equal(t, "bazbaz", getConfigString("one.two.bar", level0), "third level")
	assert.Equal(t, "", getConfigString("one.two.list", level0), "string array returns empty string")
	assert.Equal(t, "", getConfigString("one.two.missing", level0), "missing entry level three")
	assert.Equal(t, "", getConfigString("one.two.three", level0), "map returns empty string")

	ival, ok := getConfigInt("int", level0)
	assert.True(t, ok, "int val found")
	assert.Equal(t, 100, ival, "int value")

	ival, ok = getConfigInt("float", level0)
	assert.False(t, ok, "implicit conversion float to int")
	assert.Equal(t, ival, 0, "int val returns zero on no match")

}
