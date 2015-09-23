from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
import json

#connect to Kafka
consumer = KafkaConsumer('trades',
                        "trade_stream",
                        bootstrap_servers=['ec2-54-215-247-116.us-west-1.compute.amazonaws.com:9092']) #TODO: do not hardcode, use additional addresses


# connect to cassandra
cluster = Cluster(['ec2-54-215-237-86.us-west-1.compute.amazonaws.com'])
session = cluster.connect("finance_news") 

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

getcount_rts1 = makest_getcount("stock_counts_rts1")
getcount_rts2 = makest_getcount("stock_counts_rts2")
gettotal_rts1 = makest_gettotal("stock_totals_rts1")
gettotal_rts2 = makest_gettotal("stock_totals_rts2")
setcount_rts1 = makest_setcount("stock_counts_rts1")
setcount_rts2 = makest_setcount("stock_counts_rts2")
settotal_rts1 = makest_settotal("stock_totals_rts1")
settotal_rts2 = makest_settotal("stock_totals_rts2")

for message in consumer:
    j = json.loads(message.value)
    user = str(j["user"])
    company = j["company"]
    tradeamount = j["numstock"]
    #get current value from each stream DB
    rts1_stocks = session.execute(st_getcount_rts1, (user, company))
    rts2_stocks = session.execute(st_getcount_rts2, (user, company))
    rts1_totals = session.execute(st_gettotal_rts1, (user))
    rts2_totals = session.execute(st_gettotal_rts2, (user))
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

    session.execute(st_setcount_rts1,(user, company, rts1_stock, abs(rts1_stock)/float(rts1_total), rts1_contact))
    session.execute(st_setcount_rts2,(user, company, rts2_stock, abs(rts2_stock)/float(rts2_total), rts2_contact))
    session.execute(st_settotal_rts1,(user, rts1_total))
    session.execute(st_settotal_rts2,(user, rts2_total))
