#!/bin/bash
set -eu

. ~/address.sh
ssh -i ~/.ssh/insight-andy.pem ubuntu@$db1_pubdns <<- 'ENDSSH'
cqlsh -f ~/insightproject/airflow/tasks/truncate_rts2.cql
ENDSSH
