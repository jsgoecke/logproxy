package main

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

/*
PromRemoteHandler handles Prometheus remote write protocol
*/
type promRemoteHandler struct {
	log Logr
	out OutputChannel
}

func newPromRemoteHandler(log Logr, out OutputChannel, _ *ProtocolConfig) ProtocolHandler {
	return &promRemoteHandler{log: log, out: out}
}

func (h *promRemoteHandler) ProcessChunk(data []byte, atEOF bool) (*ChunkResult, error) {

	reqBuf, err := snappy.Decode(nil, data)
	if err != nil {
		h.log.Printf("prometheus proto decompress error: %s\n", err.Error())
		return nil, err
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		h.log.Printf("prometheus proto unmarshall error: %s\n", err.Error())
		return nil, err
	}

	events := protoToSamples(&req)
	h.log.Debugf("\n\nVERBOSE Samples\n%s\n", spew.Sdump(events))
	result := &ChunkResult{
		bytesRead: uint32(len(data)),
		msgsRead:  uint32(len(events)),
	}
	h.log.Debugf("Prom-proto %d Bcomp, %d Bunc, %d samples\n", len(data), len(reqBuf), len(events))

	return result, nil
}

func protoToSamples(req *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.Timestamp),
			})
		}
	}
	return samples
}
