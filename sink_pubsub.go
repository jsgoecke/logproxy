package main

import (
	"context"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/prometheus/client_golang/prometheus"
)

// PublishOptions provide options for the Google PubSub publisher
type PublishOptions struct {

	// topicId is the PubSub topicId. Must be a valid Google PubSub topic name
	topicId string

	// projectId is the google cloud project
	projectId string

	// logging interface
	log Logr

	// msgQ is a channel that publisher will use to receive messages.
	// When the channel is closed, the publisher exits
	msgQ <-chan PubMessage
}

type gpubsubPublisher struct {
	topicId   string
	log       Logr
	client    *pubsub.Client
	topic     *pubsub.Topic
	outBytes  prometheus.Counter
	outMsgs   prometheus.Counter
	outErrors prometheus.Counter
	inChan    <-chan PubMessage
}

// initTopicPublisher creates authenticated pubsub client and ensures topic exists.
// returns error if there were problems or nil if client and topic are ready for publishing
func initTopicPublisher(ctx context.Context, pub *gpubsubPublisher, projectId string) error {

	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		return err
	}
	pub.client = client
	pub.topic = client.Topic(pub.topicId)
	pub.outBytes = outBytes.WithLabelValues("pubsub", pub.topicId)
	pub.outMsgs = outMsgs.WithLabelValues("pubsub", pub.topicId)
	pub.outErrors = outErrors.WithLabelValues("pubsub", pub.topicId)

	if exists, err := pub.topic.Exists(ctx); exists {
		// topic exists, return
		return nil
	} else if err != nil {
		pub.log.Printf("Failed to check for PubSub topic: %v\n", err)
		return err
	}
	if _, err := pub.client.CreateTopic(ctx, pub.topicId); err != nil {
		pub.log.Printf("Failed to create PubSub topic: %v\n", err)
		return err
	}
	// all ok
	return nil
}

// startPublisher creates the connection to Google Pubsub
// and launches the loop to process incoming messages.
// If successful, returns a shutdown function that can be called to
// shut down the publisher
func startPubsubSink(parentCtx context.Context, log Logr, recv <-chan PubMessage, cfg *SinkConfig) (shutdown func(), err error) {

	if err = ensureConfig([]string{"topicId", "auth.projectId"}, cfg); err != nil {
		return nil, err
	}

	pub := &gpubsubPublisher{
		topicId: getConfigString("topicId", cfg.Config),
		log:     NewLogr(os.Stdout, nil),
	}
	//TODO: make the terminology between messages and events more consistent in this package

	ctx, cancel := context.WithCancel(parentCtx)
	if err = initTopicPublisher(ctx, pub, getConfigString("auth.projectId", cfg.Config)); err != nil {
		cancel()
		return nil, err
	}

	stopChan := make(chan bool)
	log.Println("PUBSUB sink started!!")
	go func() {
		cleanup := func() {
			// cleanup the context and stop topic goroutines
			pub.topic.Stop()
			cancel()
		}
		defer cleanup()

		for {
			select {

			case pubMsg, ok := <-recv:
				if ok {
					pub.send(ctx, pubMsg)
				} else {
					// message channel closd, terminate loop
					break
				}

			case <-ctx.Done():
				break

			case <-stopChan:
				break

			default:
				/* if no messages are available, keep waiting */
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()

	stopFunc := func() {
		stopChan <- true
	}
	return stopFunc, nil
}

func (pub *gpubsubPublisher) send(ctx context.Context, pubMsg PubMessage) error {

	m := &pubsub.Message{
		Data:       pubMsg.Data,
		Attributes: pubMsg.Attributes,
	}

	_, err := pub.topic.Publish(ctx, m).Get(ctx)
	if err != nil {
		pub.outErrors.Inc()
		// todo: what to do about errors?
		// (a) disconnect, wait, and retry?
		// (b) abort ?
		// (c) ignore for up to n errors?
		return err
	}

	// Increment stats counters.
	// Bytes sent is length of Data byte array, i.e., serialized message
	// Protocol overhead isn't measured here - that would be on network interface
	pub.outMsgs.Inc()
	pub.outBytes.Add(float64(len(pubMsg.Data)))
	return nil

}
