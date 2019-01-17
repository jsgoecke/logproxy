package main

import (
	"io/ioutil"
	"net/http"
	"time"
	//"github.com/go-kit/kit/log"
	//"github.com/go-kit/kit/log/level"
)

// startHttpServer launches the internal http server.
// Prior to using this function, http handlers should be set up with http.HandleFunc
func startHttpServer(s *Server, cfg *HttpConfig) (hs *http.Server, err error) {

	logger := s.log // promlog.New(&cfg.promlogConfig)

	httpServer := &http.Server{
		Addr:           cfg.Listen,
		Handler:        nil,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		MaxHeaderBytes: 1 << 13,
	}
	httpServer.RegisterOnShutdown(func() {
		logger.Debugf("Http Server shutdown at %v\n", time.Now())
	})

	go func() {
		err := httpServer.ListenAndServe()
		if err != http.ErrServerClosed {
			logger.Printf("Http Server fatal error: %v\n", err)
			s.Shutdown()
		}
	}()
	return httpServer, nil
}

// protoHandler creates an http handler for forwarding
// HTTP post data to a ProtocolHandler
func protoHandler(pserver *ProtocolServer) http.HandlerFunc {

	hf := func(w http.ResponseWriter, r *http.Request) {

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			pserver.s.log.Printf("http read error: %s\n", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		cr, err := pserver.handler.ProcessChunk(data, true)
		if err != nil {
			pserver.s.log.Printf("http bad content: %s\n", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		pserver.inBytes.Add(float64(cr.bytesRead))
		pserver.inMsgs.Add(float64(cr.msgsRead))
	}

	return hf
}
