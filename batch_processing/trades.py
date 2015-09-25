from pyspark import SparkContext
from pyspark.sql import SQLContext
from cqlengine import connection
from cqlengine.connection import get_session

sc = SparkContext(appName="Finance News, Batch Trades") 
sqlContext = SQLContext(sc) 

df = sqlContext.read.json("hdfs://ec2-54-215-247-116.us-west-1.compute.amazonaws.com:9000/camus/topics/trades/*/*/*/*/*/*")

#calculate current stock count holdings for each user and company
df.registerTempTable("trade_history")
df_stockcount = sqlContext.sql("SELECT user AS stockcount_user, company, SUM(numstock) AS stock_total FROM trade_history WHERE timestamp IS NOT NULL GROUP BY user, company")

#sum total portfolio stock by user
df_stockcount.registerTempTable("stockcount")
df_totalportfolio = sqlContext.sql("SELECT stockcount_user AS totalportfolio_user, SUM(ABS(stock_total)) AS portfolio_total FROM stockcount GROUP BY stockcount_user")

df_cassandra = df_stockcount.join(df_totalportfolio, df_stockcount.stockcount_user == df_totalportfolio.totalportfolio_user)

#for each row in df_stockcount, calculate a ratio of stock count of user and company by total portfolio by user
#hard code contact limit to 10% as default
rdd_cassandra = df_cassandra.map(lambda r: {"user": str(r.stockcount_user), 
    "company": r.company,
    "stock_total": r.stock_total,
    "portfolio_total": r.portfolio_total,
    "portfolio_ratio": abs(r.stock_total) / float(r.portfolio_total),
    "contact_limit": 0.25})

#save to Cassandra
def AddToCassandra_stockcountsbatch_bypartition(d_iter):
    from cqlengine import columns
    from cqlengine.models import Model
    from cqlengine import connection
    from cqlengine.management import sync_table
    
    class stock_counts_batch(Model):
        user = columns.Text(primary_key=True)
        company = columns.Text()
        stock_total = columns.Integer()
        portfolio_total = columns.Integer()
        portfolio_ratio = columns.Float(primary_key=True)
        contact_limit = columns.Float()
        
    host="ec2-54-215-237-86.us-west-1.compute.amazonaws.com" #cassandra seed node, TODO: do not hard code this
    connection.setup([host], "finance_news")
    sync_table(stock_counts_batch)
    for d in d_iter:
        stock_counts_batch.create(**d)

AddToCassandra_stockcountsbatch_bypartition([])
rdd_cassandra.foreachPartition(AddToCassandra_stockcountsbatch_bypartition)
