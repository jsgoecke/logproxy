package main

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

// PubMessage defines an outgoing message
type PubMessage struct {

	// Data is the data of the message
	Data []byte

	// Attributes are additional labels as string key-value pairs
	Attributes map[string]string
}

// OutputChannel represents an outgoing message queue
type OutputChannel interface {
	// Push adds message to output queue
	Push(channel string, msg *PubMessage) error
}

// sinkRunner defines the function that launches a queue handler/output processor
type sinkRunner func(parentCtx context.Context, log Logr, recv <-chan PubMessage, cfg *SinkConfig) (shutdown func(), err error)

// supported types of output
var sinkTypes = map[string]sinkRunner{
	"pubsub": startPubsubSink,
	"redis":  startRedisSink,
	"file":   startFileSink,
	"stdout": startStdoutSink,
}

var (
	// outBytes is a prometheus Counter metric that counts the number of bytes sent
	outBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricPrefix + "out_bytes_total",
			Help: "Number of bytes sent",
		},
		[]string{"sink", "topic"},
	)

	// outMsgs is a prometheus Counter metric that counts the number of messages sent
	outMsgs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricPrefix + "out_messages_total",
			Help: "Number of messages sent",
		},
		[]string{"sink", "topic"},
	)

	// outErrors is a prometheus Counter metric that counts the number of errors
	// encountered - either from network io (e.g., socket timeouts) or
	// protocol errors (such as invalid json)
	outErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricPrefix + "out_errors_total",
			Help: "Number of errors sending",
		},
		[]string{"sink", "topic"},
	)
)

func init() {
	prometheus.MustRegister(outBytes)
	prometheus.MustRegister(outMsgs)
	prometheus.MustRegister(outErrors)
}
