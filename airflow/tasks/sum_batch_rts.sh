#!/bin/bash
set -eux

. ~/address.sh 
#TODO: get hostip from address
stream=$1
. ~/address.sh
if [ $stream == "rts1" ]; then
ssh -i ~/.ssh/insight-andy.pem ubuntu@$spark1_pubdns <<- 'ENDSSH'
spark-submit --master spark://ip-172-31-11-143:7077 --driver-memory 5G --executor-memory 5G ~/insightproject/airflow/tasks/sum_batch_rts.py rts1
ENDSSH
fi
if [ $stream == "rts2" ]; then
ssh -i ~/.ssh/insight-andy.pem ubuntu@$spark1_pubdns <<- 'ENDSSH'
spark-submit --master spark://ip-172-31-11-143:7077 --driver-memory 5G --executor-memory 5G ~/insightproject/airflow/tasks/sum_batch_rts.py rts2
ENDSSH
fi
