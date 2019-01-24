package main

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

// OutputChannel represents an outgoing message queue
type OutputChannel interface {
	// Push adds message to output queue
	Push(channel string, msg *PubMessage) error
}

// sinkRunner defines the function that launches a queue handler/output processor
type sinkRunner func(parentCtx context.Context, log Logr, recv <-chan PubMessage, cfg *SinkConfig) (shutdown func(), err error)

// supported types of output
var sinkTypes = map[string]sinkRunner{
	"pubsub": startPublisherSink,
	"file":   startFileSink,
	"stdout": startStdoutSink,
}

var (
	// outBytes is a prometheus Counter metric that counts the number of bytes sent via pubsub
	outBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricPrefix + "out_bytes_total",
			Help: "Number of bytes sent",
		},
		[]string{"sink", "topic"},
	)

	// outBytes is a prometheus Counter metric that counts the number of bytes sent via pubsub
	outMsgs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricPrefix + "out_messages_total",
			Help: "Number of messages sent",
		},
		[]string{"sink", "topic"},
	)
	// outBytes is a prometheus Counter metric that counts the number of bytes sent via pubsub
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
