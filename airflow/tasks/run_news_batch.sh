#!/bin/bash
set -eux

. ~/address.sh 
#TODO: get hostip from address
ssh -i ~/.ssh/insight-andy.pem ubuntu@$spark1_pubdns <<- 'ENDSSH'
spark-submit  --master spark://ip-172-31-11-143:7077 --driver-memory 2G --executor-memory 2G ~/insightproject/batch_processing/twitter.py
ENDSSH
