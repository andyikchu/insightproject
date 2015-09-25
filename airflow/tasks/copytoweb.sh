#!/bin/bash
set -eu

stream=$1
. ~/address.sh
if [ $stream == "rts1" ]; then
ssh -i ~/.ssh/insight-andy.pem ubuntu@$db1_pubdns <<- 'ENDSSH'
cqlsh -f ~/insightproject/airflow/copytoweb_rts1.cql
ENDSSH
fi
if [ $stream == "rts2" ]; then
ssh -i ~/.ssh/insight-andy.pem ubuntu@$db1_pubdns <<- 'ENDSSH'
cqlsh -f ~/insightproject/airflow/copytoweb_rts2.cql
ENDSSH
fi
