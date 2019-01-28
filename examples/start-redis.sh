#!/bin/sh

REDIS_PORT="127.0.0.1:6379"

# start redis listening on localhost
docker run --rm -d --name logproxy_redis \
  -p "${REDIS_PORT}:6379" \
  redis:5-alpine

# to pull from the redis subscription queue, use
# ./rediswatch --topic myqueue --url redis://127.0.0.1:6379/1
