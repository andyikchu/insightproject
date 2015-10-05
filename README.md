# AutoNews Financial
#### Real-time news summaries customized to your portfolio

http://m.autonews.code0.org

http://autonews.code0.org

[About](http://autonews.code0.org/slides)

AutoNews Financial is a news service that provides news summaries to users for companies that make up a significant proportion of their portfolios. User trades are passed along to this service to customize the news to their real-time portfolios. AutoNews Financial is built upon the following technologies:

- Apache Kafka
- Camus by LinkedIn
- Apache HDFS
- Spark
- Spark Streaming
- Apache Cassandra
- Airflow by AirBnB
- Flask

## AutoNews Financial Website

AutoNews Financial reads in news from various sources and pushes them to users along with a link to the original source. A dashboard shows the latest trades made by the user, as well as their up-to-date portfolios. These are used to determine stocks of interest, so users do not have to manually adjust their news feed settings when they invest into or divest from particular stocks.

![Mobile Screenshot] (images/screenshot.png)

## How It Works

AutoNews Financial reads in user trades and news as JSON messages using Kafka. These are processed in real-time using Spark Streaming, and updated share counts and news are updated in a Cassandra database. On the batch layer, Camus periodly loads Kafka messages into HDFS. The complete set of historical trades (half billion records as of Oct 1, 2015) are then processed in bulk using Spark and the share counts are loaded into Cassandra. Lambda architecture is implemented using Airflow, which maintains real-time counts from Spark Streaming while ensuring eventual consistency using the results of the batch calculations (see Lambda Architecture [below](#lambda-architecture)).

![Pipeline] (images/pipeline.png)

## Data Synthesis

User trades are simulated using a set of 1,000,000 users and the 30 companies that make up the Dow Jones Industrial Average. Every second, a random sample of 500,000 users select 1 of the 30 companies to trade. Trades are generated from a normal distribution and sent to Kafka as a JSON message using the rkafka library, v1.0. In addition, users 1 to 10 are selected as "super-traders" for the purposes of the project demonstration. Every second, 6 of these 10 users each select between 1 to 3 companies and makes a trade using the same method.

News for the demo are synthesized using a historical set of Twitter tweets (henceforth referred to as "Synthetic Twitter") extracted from the API which matches the 30 companies. Every second, 1 tweet is selected from 1 company's news and sent with the current timestamp. A live stream of Twitter messages are also being pulled from the API and sent as real tweets. In addition, daily editions of the Financial Times from 2010 to 2015 have been downloaded onto Amazon S3, and converted to text using Xpdf. The ability to process and send summaries of these articles have yet to be implemented.

## Batch Processing

A batch process reads all trades on HDFS and sums a count of shares owned for each stock by each user. The total value of their portfolio is summed to calculate for calculating the contribution to the total portfolio by each stock. If a user owns a negative share count for a company, it is considered a short sell, and makes a positive contribution to the size of their portfolio for the purposes of user interest in news updates. Sums are written to a batch processing copy of database tables in Cassandra, to be coordinated with real time counts by Airflow. Airflow runs a batch process every 2 days.

## Real-Time Processing

A real time stream is set up to read in trades, and another to read in news from Twitter and Synthetic Twitter. Relevant fields from the Twitter feed are extracted and written to Cassandra. For trade messages, the number of shares bought or sold are incremented/decremented in 2 real time trade count databases in Cassandra (see Lambda Architecture [below](#lambda-architecture)).

## Lambda Architecture

As hundreds of millions of trades are read, a system that relies only on updating the current count of shares will be incorrect when an eventual system failure drops a message. The batch processing based on all historical data will guarantee a correct result, but since it takes several hours to run, it can not be used to serve real time results. A real time database manages the real-time trades using updates for the gap between batch processing runs, which is overwritten with guaranteed correct data after each batch process. The results are combined by using three sets of identical tables to track the share counts. One that stores the results of a batch run, and two that updates based on real-time trades. After a batch run finishes, one real-time database is emptied so that it captures only trades that occur since the last batch run, while the other real-time database, which only captures the previous delta, is summed with the results of the batch database results to arrive at the "eventual consistency" of the real-time results guaranteed by the lambda architecture. As each batch process runs, one real-time database tracks the current results, which the other database tracks the delta from the previous run.

## API Calls

Trade, portfolio and news data used by the web interface are available through an API call to http://autonews.code0.org/retrieve_user_data/<userid>
