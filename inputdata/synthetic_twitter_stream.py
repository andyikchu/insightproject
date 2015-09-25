import glob, os
import random
import time
import hashlib
from kafka.client import KafkaClient
from kafka.producer import KeyedProducer

#select a random piece of news from collected set of tweets and send as a Kafka message

DATADIR="/home/ubuntu/synthetic_twitter/"
KAFKA_NODE="ec2-54-215-247-116.us-west-1.compute.amazonaws.com"
KAFKA_TOPIC="twitter"
os.chdir(DATADIR)
files = glob.glob("*.archive")

#select a random company
datafile = random.choice(files)
datafile = DATADIR + datafile
#select a random line in data file for news
#R(3.4.2) (Waterman's "Reservoir Algorithm")
data = open(datafile, "r")
line = next(data)
for num, nextline in enumerate(data):
    if random.randrange(num + 2): continue
    line = nextline

data.close()

#add "Synthetic Twitter" as news outlet
line = line.rstrip().replace('}', ', "newsoutlet":"Synthetic Twitter"}')
#add timestamp
line = line.replace('}', ',"newstime":"' + time.strftime("%c") + '"}')

#Create producer and send message
client = KafkaClient(KAFKA_NODE)
producer = KeyedProducer(client)
producer.send_messages('twitter', str(hash(line) % 2), line)
