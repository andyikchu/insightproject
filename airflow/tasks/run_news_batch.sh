#!/bin/bash
set -eux

. ~/address.sh 
#TODO: get hostip from address
ssh -i ~/.ssh/insight-andy.pem ubuntu@$spark1_pubdns <<- 'ENDSSH'
spark-submit  --master spark://ip-172-31-11-143:7077 --driver-memory 5G --executor-memory 5G ~/insightproject/batch_processing/twitter.py
ENDSSH
