#!/bin/bash
set -eu

. ~/address.sh
ssh -i .ssh/insight-andy.pem ubuntu@spark1 <<- 'ENDSSH'
tmux kill-session -t trade_stream
ENDSSH
