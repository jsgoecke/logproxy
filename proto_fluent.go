package main

import (
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/davecgh/go-spew/spew"
	"github.com/ugorji/go/codec"
)

const pushBinary = true

type MTypeCount struct {
	fm  int64
	e   int64
	unk int64
}

type FluentForwardReader struct {
	log      Logr
	out      OutputChannel
	counters MTypeCount
	mh       codec.MsgpackHandle
}

func NewFluentForwardReader(log Logr, out OutputChannel, _ *ProtocolConfig) ProtocolHandler {
	ffr := &FluentForwardReader{log: log, out: out}
	ffr.mh.RawToString = false
	return ffr
}

type Entry struct {
	Tag       string
	Timestamp interface{}
	Record    map[string]interface{}
}

func (e Entry) String() string {
	s := fmt.Sprintf(" T:%v Record: {\n", e.Timestamp)
	for k, v := range e.Record {
		s += fmt.Sprintf("      %s: \t%s", k, spew.Sdump(v))
	}
	s += "}\n"
	return s
}

type FluentMessage struct {
	Tag     string
	Entries []Entry
	Option  map[string]interface{}
}

func (fm FluentMessage) String() string {
	s := fmt.Sprintf("Msg tag:'%s' (%d) Entries\n", fm.Tag, len(fm.Entries))
	for i, v := range fm.Entries {
		s += fmt.Sprintf("    Entry[%d]: %v", i, v)
	}
	for k, v := range fm.Option {
		s += fmt.Sprintf("    option: %s: %v, \n", k, v)
	}
	return s
}

func imin(a int, b int) int {
	if a <= b {
		return a
	}
	return b
}

func asString(x interface{}) (tag string) {
	if xa, ok := x.([]uint8); ok {
		return string(xa)
	}
	return fmt.Sprintf("%v (type %v)", x, reflect.TypeOf(x))
}

func asTimestamp(x interface{}) uint64 {
	if xv, ok := x.(uint64); ok {
		return xv
	}
	return 0
}

// buildRecord - create record Map from an array of values that represents
// alternating keys and values.
func buildRecord(x interface{}) map[string]interface{} {
	//u8arr := reflect.TypeOf(make([]uint8, 0, 0))
	result := make(map[string]interface{})
	if xa, ok := x.([]interface{}); ok {
		for i := 0; i < (len(xa) - 1); i += 2 {
			k := asString(xa[i])
			v := xa[i+1]
			if vs, ok := v.([]uint8); ok {
				result[k] = string(vs)
			} else {
				result[k] = v
			}
		}
	}
	return result
}

func asEntry(xa []interface{}) *Entry {
	numElements := len(xa)
	if numElements >= 3 {
		tag := asString(xa[0])
		timestamp := asTimestamp(xa[1])
		record := buildRecord(xa[2])

		if tag != "" && record != nil {
			entry := &Entry{
				Tag:       tag,
				Timestamp: timestamp,
				Record:    record,
			}
			return entry
		}
	}

	// Entry might be missing tag
	if numElements >= 2 {
		timestamp := asTimestamp(xa[0])
		record := buildRecord(xa[1])
		if record != nil {
			entry := &Entry{
				Tag:       "",
				Timestamp: timestamp,
				Record:    record,
			}
			return entry
		}
	}
	return nil
}

func asIEntry(x interface{}) *Entry {
	if xa, ok := x.([]interface{}); ok {
		return asEntry(xa)
	}
	return nil
}

func asFluentMessage(xa []interface{}) *FluentMessage {
	numElements := len(xa)
	if numElements >= 2 {
		tag := asString(xa[0])
		if arr, ok := xa[1].([]interface{}); ok {
			if len(arr) > 0 {
				if e0 := asIEntry(arr[0]); e0 != nil {
					entries := make([]Entry, 0, len(arr))
					for _, v := range arr {
						e := asIEntry(v)
						if e != nil {
							entries = append(entries, *e)
						}
					}
					return &FluentMessage{
						Tag:     tag,
						Entries: entries,
						Option:  make(map[string]interface{}),
					}
				}
			}
		}
	}
	return nil
}

func handleEntry(e *Entry) {
	fmt.Printf("Entry: tag %s, tstamp %d, pairs(%d):\n", e.Tag, e.Timestamp, len(e.Record))
	for k, v := range e.Record {
		fmt.Printf("   %s=%v (type %v)\n", k, v, reflect.TypeOf(v))
	}
}

func handleFluentMessage(fm *FluentMessage) (numMsgs uint32) {
	count := uint32(len(fm.Entries))
	return count
}

func (ffr *FluentForwardReader) ProcessChunk(data []byte, atEOF bool) (*ChunkResult, error) {

	var (
		msgX   []interface{}
		pm     interface{}
		result *ChunkResult
	)

	if len(data) == 0 && atEOF {
		return nil, nil
	}

	pm = &msgX
	ffr.mh.MapType = reflect.TypeOf(msgX)
	dec := codec.NewDecoderBytes(data, &ffr.mh)
	err := dec.Decode(pm)
	if err != nil && err != io.EOF {
		return nil, NewConnectionError("MsgPack", err, true)
	}
	nb := dec.NumBytesRead()
	if pushBinary {
		pubmsg := &PubMessage{Data: data[:nb]}
		ffr.out.Push(defaultQueue, pubmsg)
		result = &ChunkResult{
			bytesRead: uint32(nb),
			msgsRead:  uint32(1),
		}
	} else {
		if entry := asEntry(msgX); entry != nil {
			handleEntry(entry)
			result = &ChunkResult{
				bytesRead: uint32(nb),
				msgsRead:  uint32(1),
			}
		} else if fmsg := asFluentMessage(msgX); fmsg != nil {
			count := handleFluentMessage(fmsg)
			result = &ChunkResult{
				bytesRead: uint32(nb),
				msgsRead:  count,
			}
		}
	}
	if result != nil {
		// success
		return result, nil
	}

	ffr.log.Printf(" msgPack error. len(data)=%d, ateof=%v\n", len(data), atEOF)
	return nil, errors.New("Unrecognized msgpack format")
}
