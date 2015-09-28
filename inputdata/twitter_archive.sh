#!/bin/bash
set -eu

#Poll twitter to generate a historical set of tweets for later synthetic tweet generation
dir=`dirname $0`
while true; do
python $dir/twitter_search.py
for file in /home/ubuntu/synthetic_twitter/*.txt ; do
	sort -u $file >> $file.archive
done
sleep 10m
done
