#!/bin/bash
set -eu

#write text file to webserver to signal which database to read from

stream=$1
. ~/address.sh
if [ $stream == "rts1" ]; then
ssh -i ~/.ssh/insight-andy.pem ubuntu@$webapp_pubdns <<- 'ENDSSH'
echo "rts1" > ~/.insightproject/cassandra.txt
ENDSSH
fi
if [ $stream == "rts2" ]; then
ssh -i ~/.ssh/insight-andy.pem ubuntu@$webapp_pubdns <<- 'ENDSSH'
echo "rts2" > ~/.insightproject/cassandra.txt
ENDSSH
fi
