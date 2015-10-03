#!/bin/bash
set -eu

. ~/address.sh 
#TODO: get hostip from address
. ~/address.sh
ssh -i ~/.ssh/insight-andy.pem ubuntu@$spark1_pubdns <<- 'ENDSSH'
spark-submit --master spark://ip-172-31-11-143:7077 --driver-memory 10G --executor-memory 10G ~/insightproject/airflow/tasks/sum_batch_rts.py rts2
ENDSSH
