#!/bin/bash
set -eu

#Poll twitter to generate a historical set of tweets for later synthetic tweet generation

python twitter_search.py
for file in /home/ubuntu/synthetic_twitter/* ; do
	sort -u $file > $file.tmp && mv $file.tmp $file
done
sleep 10m
