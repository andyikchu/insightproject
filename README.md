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

AutoNews Financial reads in user trades and news as JSON messages using Kafka. These are processed in real-time using Spark Streaming, and updated share counts and news are updated in a Cassandra database. On the batch layer, Camus periodly loads Kafka messages into HDFS. The complete set of historical trades (half billion records as of Oct 1, 2015) are then processed in bulk using Spark and the share counts are loaded into Cassandra. Lambda architecture is implemented using Airflow, which maintains real-time counts from Spark Streaming while ensuring eventual consistency using the results of the batch calculations (see Lambda Architecture below).

![Pipeline] (images/pipeline.png)

## Data Synthesis

User trades are simulated using a set of 1,000,000 users and the 30 companies that make up the Dow Jones Industrial Average. Every second, a random sample of 500,000 users select 1 of the 30 companies to trade. Trades are generated from a normal distribution and sent to Kafka as a JSON message using the rkafka library, v1.0. In addition, users 1 to 10 are selected as "super-traders" for the purposes of the project demonstration. Every second, 6 of these 10 users each select between 1 to 3 companies and makes a trade using the same method.

News for the demo are synthesized using a historical set of Twitter tweets extracted from the API which matches the 30 companies. Every second, 1 tweet is selected from 1 company's news and sent with the current timestamp. A live stream of Twitter messages are also being pulled from the API and sent as real tweets. In addition, daily editions of the Financial Times from 2010 to 2015 have been downloaded onto Amazon S3, and converted to text using Xpdf. The ability to process and send summaries of these articles have yet to be implemented.

## Batch Processing

## Real-Time Processing

## Lambda Architecture

## API Calls
