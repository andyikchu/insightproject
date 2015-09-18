from kafka import KafkaConsumer
from cqlengine import connection
from cqlengine import columns
from cqlengine.models import Model
from cqlengine.management import sync_table

import json
from datetime import datetime

class trade_history(Model):
  user = columns.Text(primary_key=True)
  company = columns.Text(primary_key=True)
  numstock = columns.Integer()
  tradetime = columns.Text()
  def __repr__(self):
    return '%s %s' % (self.user, self.company)

connection.setup(["ec2-52-8-238-175.us-west-1.compute.amazonaws.com"], "finance_news")
sync_table(trade_history)

consumer = KafkaConsumer('trades',
                        "trade_hist_stream",
                        bootstrap_servers=['ec2-52-8-238-175.us-west-1.compute.amazonaws.com:9092'])

for message in consumer:
    j = json.loads(message.value)
    trade_history.create(user = str(j["user"]), company = j["company"], numstock = j["numstock"], tradetime = j["timestamp"])
