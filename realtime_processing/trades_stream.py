from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cqlengine import connection
from cqlengine import columns
from cqlengine.models import Model
from cqlengine.management import sync_table
from datetime import datetime
import json

sc = SparkContext(appName="Finance News, Stream Trades") 
ssc = StreamingContext(sc, 1)

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

#create Prepared Statements
def makest_getcount(table):
    #read current stock counts for value to update
    getcount = "SELECT stock_total, contact_limit FROM " + table + " WHERE user = ? AND company = ?"
    st_getcount = session.prepare(getcount)
    st_getcount.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return st_getcount

def makest_gettotal(table):
    #read portfolio total to update ratio
    gettotal = "SELECT portfolio_total FROM " + table + " WHERE user = ?"
    st_gettotal = session.prepare(gettotal)
    st_gettotal.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return st_gettotal

def makest_setcount(table):
    #write new calculated stock counts to database
    setcount = "INSERT INTO " + table + " (user, company, stock_total, portfolio_ratio, contact_limit) VALUES (?,?,?,?,?)"
    st_setcount = session.prepare(setcount)
    st_setcount.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return st_setcount

def makest_settotal(table):
    #write new portfolio total
    settotal = "INSERT INTO " + table + " (user, portfolio_total) VALUES (?,?)"
    st_settotal = session.prepare(settotal)
    st_settotal.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return st_settotal

def calculate_portfolio_total(company_stock, total_stock, tradeamount):
    if company_stock > 0:
        if tradeamount >= 0:
            #if holding a postive amount and buying more, just increase total portfolio
            return total_stock + tradeamount
        else:
            #if selling, first see if the amount being held is still postive or negative
            new_stock = company_stock + tradeamount
            if new_stock >= 0:
                #if it is still positive, subtract the amount from the total portfolio (+ because negative number)
                return total_stock + tradeamount
            else:
                #if it becomes negative, subtract all holdings, then add the remaining negative shares as a short
                sell = -tradeamount
                return total_stock - company_stock + (sell - company_stock)
    if company_stock < 0:
        #treat a negative count as a short, which makes a positive count towards portfolio size for news relevance
        if tradeamount < 0:
            #selling more on a short counts as increasing portfolio
            return total_stock + abs(tradeamount)
        else:
            #see if the amount being held becomes positive or negative
            new_stock = company_stock + tradeamount
            if new_stock <= 0:
                #still negative, which means less shares are shorted, decrease the amount from total portfolio
                return total_stock - abs(tradeamount)
            else:
                #if it becomes positive, subtract all holdings, then add the remaining positive shares as a buy
                return total_stock - abs(company_stock) + (company_stock + tradeamount)
    else:
        #previously held 0 shares, add the amount being traded to portfolio total
        return total_stock + abs(tradeamount)

kafkaStream = KafkaUtils.createStream(ssc, "ec2-54-215-247-116.us-west-1.compute.amazonaws.com:2181", "trades_stream", {"trades": 1})
lines = kafkaStream.map(lambda x: x[1])

# connect to cassandra
cluster = Cluster(['ec2-54-215-237-86.us-west-1.compute.amazonaws.com'])
session = cluster.connect("finance_news") 
st_getcount_rts1 = makest_getcount("stock_counts_rts1")
st_getcount_rts2 = makest_getcount("stock_counts_rts2")
st_gettotal_rts1 = makest_gettotal("stock_totals_rts1")
st_gettotal_rts2 = makest_gettotal("stock_totals_rts2")
st_setcount_rts1 = makest_setcount("stock_counts_rts1")
st_setcount_rts2 = makest_setcount("stock_counts_rts2")
st_settotal_rts1 = makest_settotal("stock_totals_rts1")
st_settotal_rts2 = makest_settotal("stock_totals_rts2")
st_tradehistory = session.prepare("INSERT INTO trade_history (user, company, tradetime, num_stock) VALUES (?,?,?,?) USING TTL 6000") #let trade history live for 100 minutes in the database

def process(rdd):
    sqlContext = getSqlContextInstance(rdd.context)
    rowRdd = rdd.map(lambda w: Row(user=str(json.loads(w)["user"]), company=json.loads(w)["company"], numstock=json.loads(w)["numstock"] ,timestamp=json.loads(w)["timestamp"] ))
    df_trades = sqlContext.createDataFrame(rowRdd)
    for row in df_trades.collect():
        tradetime = row.timestamp.encode('utf-8')
        if len(tradetime) == 0:
            continue
        tradetime = datetime.strptime(tradetime, "%Y-%m-%d %H:%M:%S%z")
        user = row.user
        company = row.company
        tradeamount = row.numstock
        session.execute(st_tradehistory, (user, company, tradetime, tradeamount, ))
        #get current value from each stream DB
        rts1_stocks = session.execute(st_getcount_rts1, (user, company, ))
        rts2_stocks = session.execute(st_getcount_rts2, (user, company, ))
        rts1_totals = session.execute(st_gettotal_rts1, (user, ))
        rts2_totals = session.execute(st_gettotal_rts2, (user, ))
        #set new value for stock_total, portfolio_total, and portfolio_ratio by summing retrieved values
        #if this is a new user/company combination, set current counts as 0, contact_limit as default
        rts1_stock = 0 if len(rts1_stocks) == 0 else rts1_stocks[0].stock_total
        rts2_stock = 0 if len(rts2_stocks) == 0 else rts2_stocks[0].stock_total
        rts1_total = 0 if len(rts1_totals) == 0 else rts1_totals[0].portfolio_total
        rts2_total = 0 if len(rts2_totals) == 0 else rts2_totals[0].portfolio_total
        rts1_contact = .25 if len(rts1_stocks) == 0 else rts1_stocks[0].contact_limit
        rts2_contact = .25 if len(rts2_stocks) == 0 else rts2_stocks[0].contact_limit
        rts1_total = calculate_portfolio_total(rts1_stock, rts1_total, tradeamount)
        rts2_total = calculate_portfolio_total(rts2_stock, rts2_total, tradeamount)
        rts1_stock += tradeamount
        rts2_stock += tradeamount
        #account for division by 0 if buys and sells balance out portfolio
        if rts1_stock == 0 and rts1_total == 0:
            rts1_ratio = 0
        else:
            rts1_ratio = abs(rts1_stock)/float(rts1_total)
        if rts2_stock == 0 and rts2_total == 0:
            rts2_ratio = 0
        else:
            rts2_ratio = abs(rts2_stock)/float(rts2_total)
        session.execute(st_setcount_rts1,(user, company, rts1_stock, rts1_ratio, rts1_contact,))
        session.execute(st_setcount_rts2,(user, company, rts2_stock, rts2_ratio, rts2_contact,))
        session.execute(st_settotal_rts1,(user, rts1_total,))
        session.execute(st_settotal_rts2,(user, rts2_total,))


lines.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
