package main

import "context"

type OutputChannel interface {
	// pushes message to output queue
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
