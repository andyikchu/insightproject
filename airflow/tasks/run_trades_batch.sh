#!/bin/bash
set -eux

. ~/address.sh 
#TODO: get hostip from address
ssh -i ~/.ssh/insight-andy.pem ubuntu@$spark1_pubdns <<- 'ENDSSH'
spark-submit --master spark://ip-172-31-11-143:7077 --driver-memory 25G --executor-memory 25G ~/insightproject/batch_processing/trades.py
ENDSSH
