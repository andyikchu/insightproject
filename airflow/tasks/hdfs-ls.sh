#!/bin/bash
set -eu
#pull trades read by Camus onto HDFS as individual files, so they can be fed to batch processing
#this microbatching method is a temporary workaround until the issue with running the full job at once can be resolved

. ~/address.sh 
ssh -i ~/.ssh/insight-andy.pem ubuntu@$master_pubdns <<- 'ENDSSH'
for year in `hdfs dfs -ls /camus/topics/trades/hourly/ | awk '{print $8}' | sed "s#/camus/topics/trades/hourly/##"`; do
	for month in `hdfs dfs -ls /camus/topics/trades/hourly/$year | awk '{print $8}' | sed "s#/camus/topics/trades/hourly/$year/##"`; do
		for day in `hdfs dfs -ls /camus/topics/trades/hourly/$year/$month | awk '{print $8}' | sed "s#/camus/topics/trades/hourly/$year/$month/##"`; do
			for hour in `hdfs dfs -ls /camus/topics/trades/hourly/$year/$month/$day | awk '{print $8}' | sed "s#/camus/topics/trades/hourly/$year/$month/$day/##"`; do
				for file in `hdfs dfs -ls /camus/topics/trades/hourly/$year/$month/$day/$hour | awk '{print $8}'`; do
					echo $file
				done
			done
		done
	done
done
ENDSSH
