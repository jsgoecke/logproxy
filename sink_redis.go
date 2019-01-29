package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/mgo.v2/bson"
)

// redisPubOptions provide options for the Google PubSub publisher
type redisPubOptions struct {

	// topicId is the PubSub topicId.
	topicId string

	// logging interface
	log Logr

	// msgQ is a channel that publisher will use to receive messages.
	// When the channel is closed, the publisher exits
	msgQ <-chan PubMessage
}

type topicMap map[string]string

// specific publish topics by content

type redisPublisher struct {
	topicId   string
	log       Logr
	client    *redis.Client
	outBytes  prometheus.Counter
	outMsgs   prometheus.Counter
	outErrors prometheus.Counter
	inChan    <-chan PubMessage
	channels  topicMap
}

type topicConfig []map[string]string

func getChannelTopics(cfg *SinkConfig, log Logr) (topicMap, error) {

	cfgError := false
	topics := make(topicMap)

	if topicCfg := getConfigVal("topics", cfg.Config); topicCfg != nil {
		if tlist, ok2 := topicCfg.([]interface{}); ok2 {
			for _, topicDef := range tlist {
				tm, _ := topicDef.(map[interface{}]interface{})
				content := tm["content"]
				topic := tm["topic"]
				if content != nil && topic != nil {
					topics[content.(string)] = topic.(string)
					log.Printf("Sink %s will route content '%s' to topic '%s'\n",
						cfg.Name, content, topic)
				} else {
					cfgError = true
					break
				}
			}
		} else {
			cfgError = true
		}
	}
	if cfgError {
		errmsg := fmt.Sprintf("invalid config value for 'topics' in sink %s. "+
			"Should be list of maps each containing (content:, topic:)", cfg.Name)
		return nil, errors.New(errmsg)
	}
	return topics, nil
}

// startRedisPubSink creates the connection to Google Pubsub
// and launches the loop to process incoming messages.
// If successful, returns a shutdown function that can be called to
// shut down the publisher
func startRedisSink(parentCtx context.Context, log Logr, recv <-chan PubMessage, cfg *SinkConfig) (shutdown func(), err error) {

	if err = ensureConfig([]string{"topicId"}, cfg); err != nil {
		log.Printf("Redis sink config error - missing topicId. %v\n", err)
		return nil, err
	}

	topicId := getConfigString("topicId", cfg.Config)
	// for example: redis://:qwerty@localhost:6379/1
	urlConfig := getConfigString("url", cfg.Config)
	opts, err := redis.ParseURL(urlConfig)
	if err != nil {
		log.Printf("Redis config error: missing or invalid 'url': %v\n", err)
		return nil, err
	}

	topics, err := getChannelTopics(cfg, log)
	if err != nil {
		return nil, err
	}

	pub := &redisPublisher{
		client:    redis.NewClient(opts),
		topicId:   topicId,
		log:       NewLogr(os.Stdout, nil),
		outBytes:  outBytes.WithLabelValues("redis", topicId),
		outMsgs:   outMsgs.WithLabelValues("redis", topicId),
		outErrors: outErrors.WithLabelValues("redis", topicId),
		channels:  topics,
	}

	stopChan := make(chan bool)
	ctx, cancel := context.WithCancel(parentCtx)
	go func() {
		cleanup := func() {
			// cleanup the context and stop topic goroutines
			pub.client.FlushAll()
			time.AfterFunc(time.Second, func() {
				_ = pub.client.Close()
				cancel()
			})
		}
		defer cleanup()
		log.Println("Redis publisher sink started!!")

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

func (pub *redisPublisher) send(ctx context.Context, pubMsg PubMessage) error {

	data, err := bson.Marshal(&pubMsg)
	if err != nil {
		pub.log.Printf("Redis publish error - can't encode message: %v\n", err)
		return err
	}

	// attempt to send to pubsub.
	// TODO: ensure ability to configure backoff & retries.
	// Ideally, the backoff/retry parameters are parsed in ParseURL
	// check here: https://github.com/go-redis/redis/blob/master/options.go#L163
	// but if not we'll have to parse them from cfg.Config
	// and set in Options before NewClient constructor.
	err = pub.client.Publish(pub.topicId, data).Err()
	if err != nil {
		pub.log.Printf("Redis sink publish error: %v\n", err)
		pub.outErrors.Inc()
		return err
	}

	// Increment stats counters.
	// Bytes sent is length of Data byte array, i.e., serialized message
	// Protocol overhead isn't measured here - that would be on network interface
	pub.outMsgs.Inc()
	pub.outBytes.Add(float64(len(pubMsg.Data)))
	return nil
}
