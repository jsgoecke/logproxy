package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/fluent/fluent-logger-golang/fluent"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

type PortNum int

const (
	startPortRange = PortNum(iota + 13310)
	testJson1Port
	testJson2Port
	testLogPort
	testFluentPort
	testSyslogPort
	testHttpPort
)

// metricInt - returns value of collector as int
func metricInt(c prometheus.Collector) int {
	return int(testutil.ToFloat64(c))
}

func dumpMetrics(t *testing.T, cfg *ServerConfig, args string) string {

	url := fmt.Sprintf("http://%s/%s%s", cfg.Http.Listen, cfg.Http.Telemetry_path, args)
	client := &http.Client{Timeout: 100 * time.Millisecond}
	response, err := client.Get(url)
	if err != nil {
		t.Errorf("Connecting to metrics url failed: %v\n", err)
		return ""
	}
	buf, err := ioutil.ReadAll(response.Body)
	if err != nil {
		t.Errorf("Reading telemetry response failed: %v\n", err)
		return ""
	}
	return string(buf)
}

func testMetrics(t *testing.T, cfg *ServerConfig, args string) {

	buf := dumpMetrics(t, cfg, args)
	assert.True(t, len(buf) > 0, "metrics pull is not-empty")

	hasProcMetrics := strings.Index(buf, "process_open_fds") != -1
	assert.True(t, hasProcMetrics, "export process metrics enabled")

	hasGoMetrics := strings.Index(buf, "go_memstats_alloc_bytes") != -1
	assert.True(t, hasGoMetrics, "export go metrics enabled")

	hasLogProxyMetrics := strings.Index(buf, "logproxy_in_bytes_total") != -1
	assert.True(t, hasLogProxyMetrics, "export logproxy metrics enabled")
}

func writeLines(t *testing.T, pserver *ProtocolServer) {

	tcpConn, err := net.DialTCP("tcp", nil,
		&net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: int(testLogPort)})
	if err != nil {
		t.FailNow()
	}
	tcpConn.SetNoDelay(true)

	// try both \n and \r\n line endings
	buf := "abc\ndef\r\nghi\nxyz"
	data := []byte(buf)
	tcpConn.Write(data)
	tcpConn.Close()
	fmt.Printf("Wrote %d bytes, 4 log messages\n", len(data))

	// give reader a chance to see it
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, len(data), metricInt(pserver.inBytes), "log bytes written")
	assert.Equal(t, 4, metricInt(pserver.inMsgs), "log msgs written")
	assert.Equal(t, 1, int(pserver.connCount), "log connections")
	assert.Equal(t, 0, metricInt(pserver.inErrors), "log errors")
}

func writeJson(t *testing.T, delim string, pserver *ProtocolServer) {

	var formatStr string
	var testPort int
	if delim == "null" {
		formatStr = "%s\x00%s\x00%s"
		testPort = int(testJson1Port)
	} else if delim == "newline" {
		formatStr = "%s\n%s\n%s"
		testPort = int(testJson2Port)
	} else {
		t.Errorf("Invalid parameter for delim")
	}

	tcpConn, err := net.DialTCP("tcp", nil,
		&net.TCPAddr{IP: []byte{127, 0, 0, 1}, Port: testPort})
	if err != nil {
		t.FailNow()
	}
	tcpConn.SetNoDelay(true)

	dtype := "(" + delim + "-terminated)"

	// three json messages separated by null bytes
	buf := fmt.Sprintf(formatStr,
		`{"a":1, "b":2, "c": [3.14159,1.61803,0.0],"d":"banana"}`,
		`{"x":1,"y":2,"z":["one","two"]}`,
		`{"w":"lime"}`,
	)
	data := []byte(buf)
	tcpConn.Write(data)
	tcpConn.Close()
	fmt.Printf("Wrote %d bytes %s, 3 json messages\n", len(data), dtype)

	// give reader a chance to see it
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, len(data), metricInt(pserver.inBytes), "json bytes written")
	assert.Equal(t, 3, metricInt(pserver.inMsgs), "json msgs written")
	assert.Equal(t, 1, int(pserver.connCount), "json connections")
	assert.Equal(t, 0, metricInt(pserver.inErrors), "json errors")
}

func testFluent(t *testing.T, pserver *ProtocolServer) {

	cfg := pserver.cfg

	logger, err := fluent.New(fluent.Config{
		FluentPort:   cfg.Port,
		FluentHost:   cfg.Host,
		Timeout:      100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		Async:        false, // send right away, no bg thread
	})
	if err != nil {
		t.Errorf("failed creating fluentd client: %v\n", err)
		return
	}
	defer logger.Close()

	tag1 := "myapp.foo"
	var data1 = map[string]interface{}{
		"color":  "blue",
		"width":  300,
		"height": 150,
	}
	err = logger.Post(tag1, data1)
	assert.NoError(t, err, "Post 1")

	now := time.Now()
	tag2 := "myapp.timer"
	var data2 = map[string]string{
		"host":  "example.com",
		"start": "Monday",
	}
	err = logger.PostWithTime(tag2, now, data2)
	assert.NoError(t, err, "Post 2 with time")

	// give reader a chance to catch up
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 2, metricInt(pserver.inMsgs), "fluent msgs written")
	// go client can send more than one entry in a single connection
	//assert.Equal(t, 2, int(pserver.connCount), "fluent connections")
	assert.Equal(t, 0, metricInt(pserver.inErrors), "fluent errors")

}

func contains(list []string, val string) bool {
	for _, v := range list {
		if v == val {
			return true
		}
	}
	return false
}

func testProtocolList(t *testing.T, server *Server, expectedLen int) {

	plist := server.enabledProtocols()

	assert.NotNil(t, plist, "enabledProtocols result")
	assert.Equal(t, expectedLen, len(plist), "number of protocols enabled")

	assert.True(t, contains(plist, "json"), "protocol list includes json")
}

func TestProtocols(t *testing.T) {

	ctx := context.Background()

	cfg := &ServerConfig{
		Http: HttpConfig{
			Enable:         true,
			Listen:         fmt.Sprintf("127.0.0.1:%d", int(testHttpPort)),
			Telemetry_path: "/metrics",
		},
		Sinks: []SinkConfig{
			SinkConfig{
				Type:   "file",
				Name:   "myfile",
				Size:   50,
				Config: map[string]interface{}{"path": "./test.out"},
			},
			/*
				SinkConfig{
					Type: "pubsub",
					Name: "mypub",
					Size: 200,
					Config: map[string]interface{}{
						"topicId": "mytopic",
						"auth": map[string]interface{}{
							"projectId": "myproject",
						},
					},
				},
			*/
		},
		Protocols: []ProtocolConfig{
			{Type: "json", Name: "json1", Enable: true, Host: "127.0.0.1", Port: int(testJson1Port)},
			{Type: "json", Name: "json2", Enable: true, Host: "127.0.0.1", Port: int(testJson2Port)},
			{Type: "log", Enable: true, Host: "127.0.0.1", Port: int(testLogPort)},
			{Type: "fluent", Enable: true, Host: "127.0.0.1", Port: int(testFluentPort)},
			{Type: "syslog", Enable: true, Host: "127.0.0.1", Port: int(testSyslogPort)},
		},
	}
	cfg.Server.Debug = true
	server := NewServer(cfg)
	assert.Equal(t, server.log.IsDebugEnabled(), true, "server debug")
	err := server.start(ctx)
	if err != nil {
		fmt.Printf("Server start generated error: %v\n", err)
		return
	}

	testProtocolList(t, server, len(cfg.Protocols))

	// wait for server to finish starting
	time.Sleep(100 * time.Millisecond)

	t.Run("proto-json-1", func(t *testing.T) { writeJson(t, "null", server.pservers[0]) })
	t.Run("proto-json-2", func(t *testing.T) { writeJson(t, "newline", server.pservers[1]) })

	t.Run("proto-log", func(t *testing.T) { writeLines(t, server.pservers[2]) })
	t.Run("proto-fluent", func(t *testing.T) { testFluent(t, server.pservers[3]) })
	t.Run("promhttp", func(t *testing.T) { testMetrics(t, cfg, "") })

	server.Shutdown()
}
