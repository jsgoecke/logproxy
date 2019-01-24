#!/bin/sh

# example script showing how to send json data to TCP socket with netcat


# Send a json event from command line with netcat
# (the -N parameter closes the connection immediately,
# which forces the server to process the data)
# The trailing newline is optional, i.e., you can use 'echo' or 'echo -n'

echo -n '{"id":1234,"msg":"Hello World",value=3.14159}' | nc -N localhost 6801
