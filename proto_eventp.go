package main

import (
	"errors"
)

// Events in msgpack format

type eventHandler struct {
	log Logr
	out OutputChannel
}

// verifyHeader checks magic number at header of packed message
func verifyHeader(data []byte) bool {
	return data[0] == 3 && data[1] == 23
}

func newEventHandler(log Logr, out OutputChannel, _ *ProtocolConfig) ProtocolHandler {
	return &eventHandler{log: log, out: out}
}

func (h *eventHandler) ProcessChunk(data []byte, atEOF bool) (*ChunkResult, error) {

	result := &ChunkResult{}

	// Message format
	//    bytes 0,1:  magic number (3, 23)
	//    bytes 2,3:  length of serialized event (uint16, big-endian)
	//    serialized event (msgpack)
	// If a valid message is received, we don't decode it, just forward along

	if atEOF && len(data) == 0 {
		return nil, nil
	}

	for {
		if len(data) < 4 {
			return result, nil
		}
		if !verifyHeader(data) {
			return nil, errors.New("Invalid Event message header")
		}
		eventSize := int(uint(data[2])*256 + uint(data[3]))
		if len(data) < (eventSize + 4) {
			// have apparently valid header but not enough data
			return result, nil
		}
		// have valid header and complete message
		// send directly to output queue without decoding
		result.bytesRead += uint32(eventSize + 4)
		result.msgsRead++
		body := data[:eventSize+4]
		pm := &PubMessage{
			Data:       body,
			Attributes: map[string]string{"content": "Event", "fmt": "msgpack"},
		}
		h.out.Push(defaultQueue, pm)
		data = data[eventSize+4:]
	}
}
