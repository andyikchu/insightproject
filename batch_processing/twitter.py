from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Window
from cqlengine import connection
from cqlengine.connection import get_session
from datetime import date, timedelta, datetime

sc = SparkContext(appName="Finance News") 
sqlContext = SQLContext(sc) 

df = sqlContext.read.json("hdfs://ec2-54-215-247-116.us-west-1.compute.amazonaws.com:9000/camus/topics/twitter/*/*/*/*/*/*")

df.registerTempTable("news_history")
df_latest_news = sqlContext.sql("SELECT * FROM news_history WHERE newstime > '" + (date.today() - timedelta(days=30)).strftime("%Y-%m-%d") + "'")

rdd_cassandra = df_latest_news.map(lambda r: {"company": r.company.replace('$', ''),
    "summary": r.summary,
    "newstime": datetime.strptime(r.newstime, "%a %b %d %H:%M:%S %Y").strftime("%Y-%m-%d %H:%M:%S"),
    "author": r.author,
    "source": r.source,
    "newsoutlet": r.newsoutlet})

#save to Cassandra
def AddToCassandra_news_bypartition(d_iter):
    from cqlengine import columns
    from cqlengine.models import Model
    from cqlengine import connection
    from cqlengine.management import sync_table
    
    class news(Model):
        company = columns.Text(primary_key=True)
        summary = columns.Text(primary_key=True)
        newstime = columns.Text(primary_key=True)
        author = columns.Text()
        newsoutlet = columns.Text()
        source = columns.Text()
        
    host="ec2-54-215-237-86.us-west-1.compute.amazonaws.com" #cassandra seed node, TODO: do not hard code this
    connection.setup([host], "finance_news")
    sync_table(news)
    for d in d_iter:
        news.create(**d)

AddToCassandra_news_bypartition([])
rdd_cassandra.foreachPartition(AddToCassandra_news_bypartition)
