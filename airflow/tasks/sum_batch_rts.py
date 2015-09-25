from cassandra.query import named_tuple_factory
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

import sys

#TODO: improve argument parsing with getopt and handling for bad input
rts = sys.argv[1]
if rts == "rts1":
    stock_counts_table = stock_counts_rts1
    stock_totals_table = stock_counts_rts1
if rts == "rts2":
    stock_counts_table = stock_counts_rts2
    stock_totals_table = stock_counts_rts2

cluster = Cluster(['ec2-54-215-237-86.us-west-1.compute.amazonaws.com'])
session = cluster.connect("finance_news") 

#TODO: break these functions out into a module
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

st_getcount = makest_getcount(stock_counts_table)
st_gettotal = makest_gettotal(stock_totals_table)
st_setcount = makest_setcount(stock_counts_table)
st_settotal = makest_settotal(stock_totals_table)

#get the portfolio total for each user
session.row_factory = named_tuple_factory
for users in session.execute("SELECT user, portfolio_total FROM stock_totals_batch"):
    user = users.user
    batch_portfolio_total = users.portfolio_total
    for row in session.execute("SELECT user, company, stock_total, portfolio_ratio FROM stock_counts_batch WHERE user = '" + user + "'"):
        #get the stock count and total from the real time table
        rts_stocks = session.execute(st_getcount, (user, company, ))
        rts_totals = session.execute(st_gettotal, (user, ))
        rts_stock = 0 if len(rts_stocks) == 0 else rts_stocks[0].stock_total
        rts_total = 0 if len(rts_totals) == 0 else rts_totals[0].portfolio_total
        rts_contact = .25 if len(rts_stocks) == 0 else rts_stocks[0].contact_limit
        #write the sum of the batch and real time stream back to the real time stream table
        rts_stock = rts_stock + row.stock_total
        rts_total = rts_total + batch_portfolio_total
        if rts_stock == 0 and rts_total == 0:
            rts_ratio = 0
        else:
            rts_ratio = abs(rts_stock)/float(rts_total)

        session.execute(st_setcount,(user, company, rts_stock, rts_ratio, rts_contact,))
        session.execute(st_settotal,(user, rts_total,))
