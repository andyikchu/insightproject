from pyspark import SparkContext
from pyspark import StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from cqlengine import connection
from cqlengine.connection import get_session

from cassandra.cluster import Cluster

sc = SparkContext(appName="Finance News, Batch Trades") 
sqlContext = SQLContext(sc) 

json_format = [StructField("user", StringType(), True),
        StructField("company", StringType(), True),
        StructField("numstock", IntegerType(), True),
        StructField("timestamp", StringType(), True)]
df = sqlContext.read.json("hdfs://ec2-54-215-247-116.us-west-1.compute.amazonaws.com:9000/camus/topics/trades/*/*/*/*/*/*", StructType(json_format))

#calculate current stock count holdings for each user and company
df.registerTempTable("trade_history")
df_stockcount = sqlContext.sql("SELECT user AS stockcount_user, company, SUM(numstock) AS stock_total FROM trade_history WHERE timestamp IS NOT NULL GROUP BY user, company")

#sum total portfolio stock by user
df_stockcount.registerTempTable("stockcount")
df_totalportfolio = sqlContext.sql("SELECT stockcount_user AS totalportfolio_user, SUM(ABS(stock_total)) AS portfolio_total FROM stockcount GROUP BY stockcount_user")

df_totalportfolio.registerTempTable("totalportfolio")
df_cassandra = sqlContext.sql("SELECT s.stockcount_user, s.company, s.stock_total, p.portfolio_total FROM stockcount s JOIN totalportfolio p ON s.stockcount_user = p.totalportfolio_user")

#for each row in df_stockcount, calculate a ratio of stock count of user and company by total portfolio by user
#hard code contact limit to 10% as default
rdd_stockcounts = df_cassandra.map(lambda r: {"user": str(r.stockcount_user), 
    "company": r.company,
    "stock_total": r.stock_total,
    "portfolio_ratio": 0 if r.stock_total == 0 and r.portfolio_total == 0 else abs(r.stock_total) / float(r.portfolio_total),
    "contact_limit": 0.10})
rdd_totals = df_cassandra.map(lambda r: {"user": str(r.stockcount_user), 
    "portfolio_total": r.portfolio_total})

# connect to cassandra
cluster = Cluster(['ec2-54-215-237-86.us-west-1.compute.amazonaws.com'])
session = cluster.connect("finance_news") 

insert_stock_count = session.prepare("INSERT INTO stock_counts_batch (user, company, stock_total, portfolio_ratio, contact_limit) VALUES (?,?,?,?,?)")
insert_portfolio_total = session.prepare("INSERT INTO stock_totals_batch (user, portfolio_total) VALUES (?,?)")

for row in rdd_stockcounts:
    session.execute(insert_stock_count,(row.user,row.company,row.stock_total,row.portfolio_ratio,row.contact_limit,))

for row in rdd_totals:
    session.execute(insert_portfolio_total,(row.user,row.portfolio_total,))
