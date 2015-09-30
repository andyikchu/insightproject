import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import play.api.libs.json._

import com.datastax.spark.connector._ 

val sparkConf = new SparkConf().setAppName("Finance News, Batch Trades").set("spark.cassandra.connection.host", "ec2-54-215-237-86.us-west-1.compute.amazonaws.com")
val sc = new SparkContext(,sparkConf)
val sqlContext = SQLContextSingleton.getInstance(sc)
val tradehistory = sqlContext.jsonFile("hdfs://ec2-54-215-247-116.us-west-1.compute.amazonaws.com:9000/camus/topics/trades/*/*/*/*/*/*")

tradehistory.registerTempTable("tradehistory")
val stockcount = sqlContext.sql("SELECT user AS stockcount_user, company, SUM(numstock) AS stock_total FROM tradehistory WHERE timestamp IS NOT NULL GROUP BY user, company")
stockcount.registerTempTable("stockcount")
val totalportfolio = sqlContext.sql("SELECT stockcount_user AS totalportfolio_user, SUM(ABS(stock_total)) AS portfolio_total FROM stockcount GROUP BY stockcount_user")
totalportfolio.registerTempTable("totalportfolio")
val output = sqlContext.sql("SELECT s.stockcount_user AS user, s.company, 0.10 AS contact_limit, s.stock_total, CASE s.stock_total WHEN 0 THEN 0 ELSE s.stock_total/p.portfolio_total END AS portfolio_ratio, p.portfolio_total FROM stockcount s JOIN totalportfolio p ON s.stockcount_user = p.totalportfolio_user")

output.saveToCassandra("finance_news", "stock_counts_batch", SomeColumns("user", "company", "stock_total", "portfolio_ratio", "contact_limit"))
output.saveToCassandra("finance_news", "stock_totals_batch", SomeColumns("user", "portfolio_total"))
