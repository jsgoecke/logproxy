package main

// send all messages received to output terminated by newline
type LogHandler struct {
	log Logr
	out OutputChannel
}

func NewLogHandler(log Logr, out OutputChannel, pc *ProtocolConfig) ProtocolHandler {
	return &LogHandler{log: log, out: out}
}

func (h *LogHandler) ProcessChunk(data []byte, atEOF bool) (*ChunkResult, error) {

	result := &ChunkResult{}
	bytesRead, msg, err := defaultSplitter(data, atEOF)
	if err != nil {
		return nil, err
	}

	if bytesRead > 0 {
		result.bytesRead = uint32(bytesRead)
		msg = trim(msg, lineTrim)
		if len(msg) > 0 {
			pm := &PubMessage{Data: msg}
			h.out.Push(defaultQueue, pm)
			result.msgsRead = 1
		}
	}

	return result, nil
}
