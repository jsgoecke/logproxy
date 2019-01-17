package main

import (
	"bytes"
)

const (
	maxHeaderLen = 53
	maxDataLen   = maxBufferSize - maxHeaderLen - 1
)

type SyslogRelpHandler struct {
	log Logr
	out OutputChannel
}

func NewSyslogRelpHandler(log Logr, out OutputChannel, _ *ProtocolConfig) ProtocolHandler {
	return &SyslogRelpHandler{log: log, out: out}
}

// Parse int from byte slice. returns value and ok=true if all bytes are digits.
// syslog RELP uses latin digits only: https://www.rsyslog.com/doc/relp.html
func atoi(b []byte) (val int, ok bool) {
	n := 0
	for _, ch := range b {
		if '0' <= ch && ch <= '9' {
			n = n*10 + (int(ch) - int('0'))
		} else {
			return 0, false
		}
	}
	return n, true
}

// split byte array by delimiter, with up to 'count' tokens
// and up to maxLen bytes processed. Returns array of indexes of delimiters.
// Only works with ascii, not utf8 or other encodings
func tokenAscii(data []byte, delimiter byte, count int, maxParse int) []int {
	if len(data) > maxParse {
		data = data[:maxParse]
	}
	marks := make([]int, 0, count)
	pos := 0
	for i := 0; i < count; i++ {
		if ix := bytes.IndexByte(data[pos:], delimiter); ix != -1 {
			marks = append(marks, ix)
			pos = ix + 1
		} else {
			break
		}
	}
	return marks
}

func (h *SyslogRelpHandler) ProcessChunk(data []byte, atEOF bool) (*ChunkResult, error) {

	result := &ChunkResult{}

	marks := tokenAscii(data, ' ', 3, 53)
	if len(marks) >= 3 {
		txNum, ok1 := atoi(data[:marks[0]])
		//command := string(data[marks[0]+1 : marks[1]])
		dataLen, ok2 := atoi(data[marks[1]+1 : marks[2]])
		if ok1 && ok2 {
			trailerLen := 1           /* final \n */
			headerLen := marks[2] + 1 /* three tokens plus final space */
			messageLen := headerLen + dataLen + trailerLen

			if len(data) >= messageLen {
				// yes, we have a full message
				// when returning, exclude trailing newline
				if dataLen > maxDataLen {
					h.log.Printf("Warning: Message #%d exceeds maximum data length (len=%d); may be truncated\n", txNum, dataLen)
					dataLen = maxDataLen
				} else {
					dataLen -= trailerLen /* remove trailing \n */
				}

				result.bytesRead = uint32(messageLen)
				result.msgsRead = uint32(1)
				body := data[headerLen : headerLen+dataLen]

				pm := &PubMessage{Data: body}
				h.out.Push(defaultQueue, pm)

				// this won't look great if the syslog message is not a simple string,
				// but it's ok for debugging
				//fmt.Printf("SYSLOG: [%d] %s %s\n", txNum, command, string(body))
			}
		}
	}
	return result, nil
}
