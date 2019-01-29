package main

//"fmt"
//"github.com/davecgh/go-spew/spew"

type jsonHandler struct {
	log Logr
	out OutputChannel
}

func newJsonHandler(log Logr, out OutputChannel, _ *ProtocolConfig) ProtocolHandler {
	return &jsonHandler{log: log, out: out}
}

func (h *jsonHandler) ProcessChunk(data []byte, atEOF bool) (*ChunkResult, error) {

	result := &ChunkResult{}

	if atEOF && len(data) == 0 {
		return nil, nil
	}

	msgLen, data, _ := defaultSplitter(data, atEOF)
	if msgLen == 0 {
		return nil, nil
	}
	result.bytesRead = uint32(msgLen)

	if len(data) > 0 {
		// forward as binary, without json validation
		pm := &PubMessage{
			Data:       data,
			Attributes: map[string]string{"fmt": "json"},
		}
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
