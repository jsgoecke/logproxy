/*
Package logproxy is a server that receives streaming
log records,
metrics, and events, from multiple sources,
and forwards them to Google PubSub.

Input Protocols supported:
  - syslog (repl)
  - fluent forward (from fluentd, fluent-bit)
  - any line-oriented records (delimited witn \n)
  - json
  - prometheus remote write

Logproxy includes a prometheus exporter that reports
its own metrics (for each protocol: nuber of bytes transferred,
number of messages, etc.)
*/
package main
