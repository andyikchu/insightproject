#!/bin/bash
set -eu

. ~/address.sh 
dir=`dirname $0`
for file in `bash $dir/hdfs-ls.sh 2>&1 | grep camus`; do
ssh -i ~/.ssh/insight-andy.pem ubuntu@$spark1_pubdns "/usr/local/spark/bin/spark-submit --master spark://ip-172-31-11-143:7077 --driver-memory 25G --executor-memory 25G ~/insightproject/batch_processing/trades.py $file"
done
