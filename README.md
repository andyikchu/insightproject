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

## Batch Processing

## Real-Time Processing

## Lambda Architecture

## API Calls
