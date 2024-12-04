#!/usr/bin/env bash

PID_FILE=server.pid

./server -id 1.1 -algorithm paxos -fz=0 &
./server -id 1.2 -algorithm paxos -fz=0 &
./server -id 1.3 -algorithm paxos -fz=0 &
echo $! >> ${PID_FILE}
