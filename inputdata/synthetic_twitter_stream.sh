#!/bin/bash
set -eu

#every 2 seconds, pull 1 tweet from one company in the historical archives and send it as a kafka message
while true; do
python synthetic_twitter_stream.py
sleep 2
done
