from pyspark import SparkContext
from pyspark import StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from cqlengine import connection
from cqlengine.connection import get_session

sc = SparkContext(appName="Finance News, Batch Trades") 
sqlContext = SQLContext(sc) 

json_format = [StructField("user", StringType(), True),
        StructField("company", StringType(), True),
        StructField("numstock", IntegerType(), True),
        StructField("timestamp", StringType(), True)]
#df = sqlContext.read.json("hdfs://ec2-54-215-247-116.us-west-1.compute.amazonaws.com:9000/camus/topics/trades/*/*/*/*/*/*", StructType(json_format))
#use smaller dataset for test
df = sqlContext.read.json("hdfs://ec2-54-215-247-116.us-west-1.compute.amazonaws.com:9000/camus/topics/trades/hourly/2015/09/19/20/trades.0.1.421565.421565.1442718000000.gz", StructType(json_format))

#calculate current stock count holdings for each user and company
df.registerTempTable("trade_history")
df_stockcount = sqlContext.sql("SELECT user AS stockcount_user, company, SUM(numstock) AS stock_total FROM trade_history WHERE timestamp IS NOT NULL GROUP BY user, company")

#sum total portfolio stock by user
df_stockcount.registerTempTable("stockcount")
df_totalportfolio = sqlContext.sql("SELECT stockcount_user AS totalportfolio_user, SUM(ABS(stock_total)) AS portfolio_total FROM stockcount GROUP BY stockcount_user")

df_cassandra = df_stockcount.join(df_totalportfolio, df_stockcount.stockcount_user == df_totalportfolio.totalportfolio_user)

#for each row in df_stockcount, calculate a ratio of stock count of user and company by total portfolio by user
#hard code contact limit to 10% as default
rdd_stockcounts = df_cassandra.map(lambda r: {"user": str(r.stockcount_user), 
    "company": r.company,
    "stock_total": r.stock_total,
    "portfolio_ratio": abs(r.stock_total) / float(r.portfolio_total),
    "contact_limit": 0.25})
rdd_stockcounts = rdd_stockcounts.repartition(20)
rdd_stockcounts.persist(StorageLevel.MEMORY_AND_DISK_SER)
rdd_totals = df_cassandra.map(lambda r: lambda r: {"user": str(r.stockcount_user), 
    "portfolio_total": r.portfolio_total})
rdd_totals = rdd_totals.repartition(20)
rdd_totals.persist(StorageLevel.MEMORY_AND_DISK_SER)

print rdd_stockcounts.getNumPartitions()
print rdd_totals.getNumPartitions()

#save to Cassandra
def AddToCassandra_stockcountsbatch_bypartition(d_iter):
    from cqlengine import columns
    from cqlengine.models import Model
    from cqlengine import connection
    from cqlengine.management import sync_table
    
    class stock_counts_batch(Model):
        user = columns.Text(primary_key=True)
        company = columns.Text(primary_key=True)
        stock_total = columns.Integer()
        portfolio_ratio = columns.Float()
        contact_limit = columns.Float()
        
    host="ec2-54-215-237-86.us-west-1.compute.amazonaws.com" #cassandra seed node, TODO: do not hard code this
    connection.setup([host], "finance_news")
    sync_table(stock_counts_batch)
    for d in d_iter:
        stock_counts_batch.create(**d)

AddToCassandra_stockcountsbatch_bypartition([])
rdd_stockcounts.foreachPartition(AddToCassandra_stockcountsbatch_bypartition)

def AddToCassandra_stocktotalsbatch_bypartition(d_iter):
    from cqlengine import columns
    from cqlengine.models import Model
    from cqlengine import connection
    from cqlengine.management import sync_table
    
    class stock_totals_batch(Model):
        user = columns.Text(primary_key=True)
        portfolio_total = columns.Integer()
        
    host="ec2-54-215-237-86.us-west-1.compute.amazonaws.com" #cassandra seed node, TODO: do not hard code this
    connection.setup([host], "finance_news")
    sync_table(stock_totals_batch)
    for d in d_iter:
        stock_totals_batch.create(**d)

AddToCassandra_stocktotalsbatch_bypartition([])
rdd_totals.foreachPartition(AddToCassandra_stocktotalsbatch_bypartition)
