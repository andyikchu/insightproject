from flask import jsonify 
from app import app
from cassandra.cluster import Cluster

#connect to cassandra
cluster = Cluster(["ec2-54-67-33-180.us-west-1.compute.amazonaws.com"])
session = cluster.connect("finance_news")

@app.route('/')
@app.route('/index')

def index():
    return "Financial Updates"

@app.route('/tradesummary/<user>')

def get_trade_summary(user):
    portfolio = {}
    portfolio_total = 0
    contact_limit = 0

    #TODO: fix logic in calculating total counts
    stmt = "SELECT company, stock_total, contact_limit FROM stock_counts WHERE user=%s"
    stock_counts = session.execute(stmt, parameters=[user])
    for c in stock_counts:
        portfolio[c.company] = c.stock_total
        contact_limit = c.contact_limit

    stmt = "SELECT company, numstock FROM trade_history WHERE user=%s"
    new_trades = session.execute(stmt, parameters=[user])
    for n in new_trades:
        portfolio[n.company] += n.stock_total
    
    for company in portfolio:
        portfolio_total += portfolio[company]
     
    jsonresponse = [{"user": user, "company": company, "stock_total": portfolio[company], "portfolio_total": portfolio_total, "contact_limit": contact_limit, "portfolio_ratio": portfolio[company]/portfolio_total} for company in portfolio]
    return jsonify(tradesummary=jsonresponse)
