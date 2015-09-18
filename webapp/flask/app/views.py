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
    stmt = "SELECT * FROM stock_counts WHERE user=%s"
    response = session.execute(stmt, parameters=[user])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"user": x.user, "company": x.company, "stock_total": x.stock_total, "portfolio_total": x.portfolio_total, "contact_limit": x.contact_limit, "portfolio_ratio": x.portfolio_ratio} for x in response_list]
    return jsonify(tradesummary=jsonresponse)
