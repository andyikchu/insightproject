#!/bin/bash
set -eu

. ~/address.sh
#TODO: read hostname from address.sh
ssh -i ~/.ssh/insight-andy.pem ubuntu@$spark1_pubdns -t 'tmux new-session -d -s trade_stream ". ~/address.sh; . ~/.profile ; spark-submit --master spark://ip-172-31-11-143:7077 --driver-memory 2G --executor-memory 2G --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.1 ~/insightproject/realtime_processing/trades_stream.py"'
