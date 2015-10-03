#!/bin/bash
set -eu

#write text file to webserver to signal which database to read from

. ~/address.sh
ssh -i ~/.ssh/insight-andy.pem ubuntu@$webapp_pubdns <<- 'ENDSSH'
echo "rts2" > ~/.insightproject/cassandra.txt
ENDSSH
