package main

import (
	"context"
	"os"
	"time"
)

func startStdoutSink(parentCtx context.Context, log Logr, recv <-chan PubMessage, cfg *SinkConfig) (shutdown func(), err error) {

	rawFmt := false

	raw := getConfigVal("raw", cfg.Config)
	if raw != nil {
		if asBool, ok := raw.(bool); ok {
			rawFmt = asBool
		}
	}

	outBytes := outBytes.WithLabelValues("stdout", "default")
	outMsgs := outMsgs.WithLabelValues("stdout", "default")
	//outErrors := outErrors.WithLabelValues("stdout", "default")

	stopChan := make(chan bool)
	go func() {
		for {
			select {
			case pubMsg, _ := <-recv:
				if rawFmt {
					os.Stdout.Write(pubMsg.Data)
					outMsgs.Add(float64(len(pubMsg.Data)))
				} else {
					bdata := []byte(string(pubMsg.Data))
					os.Stdout.Write(bdata)
					outMsgs.Add(float64(len(bdata)))
				}
				outBytes.Inc()
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
