from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cqlengine import connection
from cqlengine import columns
from cqlengine.models import Model
from cqlengine.management import sync_table

from datetime import datetime
import json

conf = SparkConf().setAppName("Finance News, Stream Twitter").set("spark.cores.max", "2")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 1)

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

kafkaStream = KafkaUtils.createStream(ssc, "ec2-54-215-247-116.us-west-1.compute.amazonaws.com:2181", "twitter_stream", {"twitter": 1})
lines = kafkaStream.map(lambda x: x[1])

# connect to cassandra
cluster = Cluster(['ec2-54-215-237-86.us-west-1.compute.amazonaws.com'])
session = cluster.connect("finance_news") 
st_news = session.prepare("INSERT INTO news (company, summary, newstime, author, newsoutlet, source) VALUES (?,?,?,?,?,?) USING TTL 7776000") #let news live for 90 days in the database

def process(rdd):
    sqlContext = getSqlContextInstance(rdd.context)
    rowRdd = rdd.map(lambda w: Row(summary=json.loads(w)["summary"],
        source=json.loads(w)["source"],
        newsoutlet=json.loads(w)["newsoutlet"],
        author=json.loads(w)["author"],
        company=json.loads(w)["company"].replace('$', ''),
        newstime=datetime.strptime(json.loads(w)["newstime"].encode('utf-8'), "%a %b %d %H:%M:%S %Y")))
    df_news = sqlContext.createDataFrame(rowRdd)
    for row in df_news.collect():
        session.execute(st_news, (row.company, row.summary, row.newstime, row.author, row.newsoutlet, row.source, ))

lines.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
