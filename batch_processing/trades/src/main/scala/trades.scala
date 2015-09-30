import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame

import play.api.libs.json._

import com.datastax.spark.connector._ 
import com.datastax.driver.core.utils._

object trades_batch {
	def main(args: Array[String]) {
		val sparkConf = new SparkConf().setAppName("Finance News, Batch Trades").set("spark.cassandra.connection.host", "ec2-54-215-237-86.us-west-1.compute.amazonaws.com")
		val sc = new SparkContext(sparkConf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		val tradehistory = sqlContext.jsonFile("hdfs://ec2-54-215-247-116.us-west-1.compute.amazonaws.com:9000/camus/topics/trades/*/*/*/*/*/*")
		
		//Sum stock and portfolio counts per user and company
		tradehistory.registerTempTable("tradehistory")
		val stockcount = sqlContext.sql("SELECT user AS stockcount_user, company, SUM(numstock) AS stock_total FROM tradehistory WHERE timestamp IS NOT NULL GROUP BY user, company")
		stockcount.registerTempTable("stockcount")
		val totalportfolio = sqlContext.sql("SELECT stockcount_user AS user, SUM(ABS(stock_total)) AS portfolio_total FROM stockcount GROUP BY stockcount_user")
		totalportfolio.registerTempTable("totalportfolio")

		val stock_counts_batch = sqlContext.sql("SELECT s.stockcount_user AS user, s.company, 0.10 AS contact_limit, s.stock_total, CASE s.stock_total WHEN 0 THEN 0 ELSE s.stock_total/p.portfolio_total END AS portfolio_ratio FROM stockcount s JOIN totalportfolio p ON s.stockcount_user = p.user")
		
		stock_counts_batch.write
			.format("org.apache.spark.sql.cassandra")
			.options(Map( "table" -> "stock_counts_batch", "keyspace" -> "finance_news"))
			.save()

		totalportfolio.write
			.format("org.apache.spark.sql.cassandra")
			.options(Map( "table" -> "stock_counts_batch", "keyspace" -> "finance_news"))
			.save()
	}
}
