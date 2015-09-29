from flask import render_template
from flask import jsonify 
from app import app
from cassandra.cluster import Cluster

#connect to cassandra
cluster = Cluster(["ec2-54-215-237-86.us-west-1.compute.amazonaws.com"])
session = cluster.connect("finance_news")

@app.route('/')
@app.route('/index')

def index():
    return "Financial Updates"

@app.route('/tradesummary/<user>')

def get_trade_summary(user):
    #check which database to query
    dbfile = open("/home/ubuntu/.insightproject/cassandra.txt")
    db = dbfile.readline().rstrip()
    dbfile.close()

    stmt = "SELECT user, company, stock_total, portfolio_ratio, contact_limit FROM stock_counts_" + db + " WHERE user=%s"
    stock_counts = session.execute(stmt, parameters=[user]
     
    jsonresponse = [{"user": user, "company": company, "stock_total": stock_total, "portfolio_ratio": portfolio_ratio, "contact_limit": contact_limit} for row in stock_counts]
    return jsonify(tradesummary=jsonresponse)

def index():
    return render_template("index.html", user = "1")
