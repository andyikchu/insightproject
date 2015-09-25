#!/bin/bash
set -eu

. ~/address.sh
#TODO: read hostname from address.sh
ssh -i ~/.ssh/insight-andy.pem ubuntu@spark1 -t 'tmux new-session -d -s trade_stream ". ~/address.sh; . ~/.profile ; spark-submit --master spark://ip-172-31-11-143:7077 --driver-memory 1G --executor-memory 1G ~/insightproject/realtime_processing/trades_stream.py"'
