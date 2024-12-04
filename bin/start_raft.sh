#!/usr/bin/env bash

PID_FILE=server.pid

./server -id 1.1 -algorithm raft -fz=0 &
./server -id 1.2 -algorithm raft -fz=0 &
./server -id 1.3 -algorithm raft -fz=0 &
echo $! >> ${PID_FILE}
