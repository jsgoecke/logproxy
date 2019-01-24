#!/bin/sh

# example script to show sending HTTP json data from command-line with curl


# (the -N parameter closes the connection immediately,
# which forces the server to process the data)
# The trailing newline is optional, i.e., you can use 'echo' or 'echo -n'

echo '{"id":9876,"msg":"Hello World HTTP",value=3.14159}' | curl http://localhost:4999/json --data-binary @-
