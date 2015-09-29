from kafka import KafkaConsumer
from cqlengine import connection
from cqlengine import columns
from cqlengine.models import Model
from cqlengine.management import sync_table

import json
from datetime import datetime

class news(Model):
  company = columns.Text(primary_key=True)
  summary = columns.Text(primary_key=True)
  newstime = columns.Text(primary_key=True)
  author = columns.Text()
  newsoutlet = columns.Text()
  source = columns.Text()
  def __repr__(self):
    return '%s %s %s' % (self.company, self.summary, self.newstime)

connection.setup(["ec2-54-215-237-86.us-west-1.compute.amazonaws.com"], "finance_news") #TODO: do not hardcode this, use second seed node
sync_table(news)

consumer = KafkaConsumer('twitter',
                        "twitter_stream",
                        bootstrap_servers=['ec2-54-215-247-116.us-west-1.compute.amazonaws.com']) #TODO: do not hardcode this, use multiple addresses

news.ttl(7776000) #let news live for 90 days in the database
for message in consumer:
    j = json.loads(message.value)
    news.create(company = j["company"].replace('$', ''), summary = j["summary"], newstime = datetime.strptime(j["newstime"], "%a %b %d %H:%M:%S %Y").strftime("%Y-%m-%d %H:%M:%S"), author = j["author"], newsoutlet = j["newsoutlet"], source = j["source"])
