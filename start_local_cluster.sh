#!/bin/bash
set -e
echo "Starting cluster"

# start the node and not wait for it to finish
RUST_LOG=info cargo run cluster -s3 -r2 -p9 -w3001 &
PID1=$!
RUST_LOG=info cargo run join -w3002 -n4110 -a127.0.0.1:4109 &
PID2=$!
RUST_LOG=info cargo run join -w3003 -n4111 -a127.0.0.1:4109 &
PID3=$!

trap "kill $PID1 $PID2 $PID3" EXIT

wait