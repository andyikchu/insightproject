from flask import render_template, request
from flask import jsonify 
from app import app
from cassandra.cluster import Cluster

#connect to cassandra
cluster = Cluster(["ec2-54-215-237-86.us-west-1.compute.amazonaws.com"])
session = cluster.connect("finance_news")
session.default_fetch_size = None #turn off paging to allow IN () ORDER BY queries, since only a few records are SELECTed anyway

@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")

@app.route('/slides')
def slides():
    return render_template("slides.html")

@app.route('/user')
def get_user():
    #temporary hacky method to query for all companies; TODO: redesign cassandra schema or use Presto to do this 
    COMPANIES = ["MMM", "AXP", "AAPL", "BA", "CAT", "CVX", "CSCO", "KO", "DD", "XOM", "GE", "GS", "HD", "INTC", "IBM", "JNJ", "JPM", "MCD", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV", "UNH", "UTX", "VZ", "V", "WMT", "DIS"]
    user=request.args.get("user")

    #check which database to query
    dbfile = open("/home/ubuntu/.insightproject/cassandra.txt")
    db = dbfile.readline().rstrip()
    dbfile.close()

    #get data from DB for the user
    latest_trades = session.execute("SELECT company, num_stock, tradetime FROM trade_history WHERE user=%s AND company IN ('" + "','".join(COMPANIES) + "') ORDER BY tradetime LIMIT 5", parameters=[user])
    #more efficient to calculate portfolio_ratios here than to update all portfolio_ratios for all companies owned by a user during stream calculation
    #TODO: remove portfolio_ratios from cassandra and batch/stream calculations
    user_companies = session.execute("SELECT company, stock_total, contact_limit FROM stock_counts_" + db + " WHERE user=%s", parameters=[user])
    user_companies = map(lambda row: dict(zip(["company","stock_total","contact_limit"], row)), user_companies) #map to list of dicts to manage more easily
    #process companies to calculate total, collect companies above contact ratio
    portfolio_total = session.execute("SELECT portfolio_total FROM stock_totals_" + db + " WHERE user=%s", parameters=[user])
    if len(portfolio_total) != 1:
        portfolio_total = 1
    else:
        portfolio_total = portfolio_total[0].portfolio_total

    #calculate portfolio ratios and generate list of companies above contact limit
    for row in user_companies:
        row["portfolio_ratio"] = 100*float(row["stock_total"])/portfolio_total
        if row["portfolio_ratio"] > row["contact_limit"]:
            user_companies_list.append(row["company"])

    user_companies.sort(key = lambda row: row["portfolio_ratio"])
    user_companies_list = []

    latest_news = session.execute("SELECT company, summary, newsoutlet, source, author, newstime FROM news WHERE company IN ('" + "','".join(user_companies_list) + "') ORDER BY newstime DESC LIMIT 10")

    return render_template("user.html", user=user, latest_trades = latest_trades, portfolio = user_companies, latest_news = latest_news)

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
