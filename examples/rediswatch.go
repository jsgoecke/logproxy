package main

import (
	"flag"
	"os"

	"github.com/go-redis/redis"
	"gopkg.in/mgo.v2/bson"
)

func main() {

	url := flag.String("url", "redis://127.0.0.1:6379/1", "redis url")
	topic := flag.String("topic", "mytopic", "topic name")
	flag.Parse()

	opts, err := redis.ParseURL(*url)
	if err != nil {
		panic(err)
	}

	client := redis.NewClient(opts)
	pubsub := client.Subscribe(*topic)
	_, err = pubsub.Receive()
	if err != nil {
		panic(err)
	}
	// channel for receiving messages.
	ch := pubsub.Channel()
	defer func() {
		pubsub.Close()
	}()

	// PubMessage defines an outgoing message
	type InMessage struct {
		// Data is the data of the message
		Data []byte
		// Attributes are additional labels as string key-value pairs
		Attributes map[string]string
	}
	var inMessage InMessage

	// Consume messages.
	for msg := range ch {
		bson.Unmarshal([]byte(msg.Payload), &inMessage)
		os.Stderr.Write(inMessage.Data)
		os.Stderr.Write([]byte("\n"))
	}

}
