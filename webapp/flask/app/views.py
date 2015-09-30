from flask import render_template, request
from flask import jsonify 
from app import app
from cassandra.cluster import Cluster

#connect to cassandra
cluster = Cluster(["ec2-54-215-237-86.us-west-1.compute.amazonaws.com"])
session = cluster.connect("finance_news")

@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")

@app.route('/user')
def get_user():
    #temporary hacky method to query for all companies; TODO: redesign cassandra schema or use Presto to do this 
    COMPANIES = ["MMM", "AXP", "AAPL", "BA", "CAT", "CVX", "CSCO", "KO", "DD", "XOM", "GE", "GS", "HD", "INTC", "IBM", "JNJ", "JPM", "MCD", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV", "UNH", "UTX", "VZ", "V", "WMT", "DIS"]
    user=request.args.get("user")

    #check which database to query
    dbfile = open("/home/ubuntu/.insightproject/cassandra.txt")
    db = dbfile.readline().rstrip()
    dbfile.close()

    #TODO, feed in ','.join(COMPANIES) as parameter instead of messy string
    latest_trades = session.execute("SELECT company, num_stock, tradetime FROM trade_history WHERE user=%s AND company IN ('" + "','".join(COMPANIES) + "') ORDER BY tradetime LIMIT 5", parameters=[user])

    return render_template("user.html", user=user, latest_trades = latest_trades)

@app.route('/tradesummary/<user>')
def get_trade_summary(user):
    #check which database to query
    dbfile = open("/home/ubuntu/.insightproject/cassandra.txt")
    db = dbfile.readline().rstrip()
    dbfile.close()

    stmt = "SELECT user, company, stock_total, portfolio_ratio, contact_limit FROM stock_counts_" + db + " WHERE user=%s"
    stock_counts = session.execute(stmt, parameters=[user])
     
    jsonresponse = [{"user": user, "company": company, "stock_total": stock_total, "portfolio_ratio": portfolio_ratio, "contact_limit": contact_limit} for row in stock_counts]
    return jsonify(tradesummary=jsonresponse)
