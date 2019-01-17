package main

import (
	"bytes"
	//"fmt"
	//"github.com/davecgh/go-spew/spew"
)

type JsonHandler struct {
	log Logr
	out OutputChannel
}

func NewJsonHandler(log Logr, out OutputChannel, _ *ProtocolConfig) ProtocolHandler {
	return &JsonHandler{log: log, out: out}
}

func (h *JsonHandler) ProcessChunk(data []byte, atEOF bool) (*ChunkResult, error) {

	result := &ChunkResult{}

	if atEOF && len(data) == 0 {
		return nil, nil
	}

	msgLen := bytes.IndexByte(data, 0)
	if msgLen > 0 {
		result.bytesRead = uint32(msgLen + 1)
	} else if atEOF {
		msgLen = len(data)
		result.bytesRead = uint32(msgLen)
	} else if msgLen <= 0 {
		return nil, nil
	}

	data = trim(data[:msgLen], lineTrim)
	if len(data) > 0 {

		// forward as binary, without json validation
		pm := &PubMessage{Data: data}
		h.out.Push(defaultQueue, pm)

		// text hanlding
		//var msg map[string]interface{}
		//if err := json.Unmarshal(data, &msg); err != nil {
		//	return nil, NewConnectionError("JSON", err, false)
		//}
		result.msgsRead = 1
	}

	return result, nil
}
