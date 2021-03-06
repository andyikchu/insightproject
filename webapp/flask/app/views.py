from flask import render_template, request
from flask import jsonify 
from app import app
from cassandra.cluster import Cluster

#connect to cassandra
cluster = Cluster(["ec2-54-215-237-86.us-west-1.compute.amazonaws.com"])
session = cluster.connect("finance_news")
session.default_fetch_size = None #turn off paging to allow IN () ORDER BY queries, since only a few records are SELECTed anyway

def _get_user_data(user):
    #pull latest trades, latest news, and portfolio from database for the user
    #check which database to query
    dbfile = open("/home/ubuntu/.insightproject/cassandra.txt")
    db = dbfile.readline().rstrip()
    dbfile.close()
    #get a few of the latest trades for the user
    latest_trades = session.execute("SELECT company, num_stock, tradetime FROM trade_history WHERE user=%s ORDER BY tradetime DESC LIMIT 5", parameters=[user])
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

    user_companies_list = []
    #calculate portfolio ratios and generate list of companies above contact limit
    for row in user_companies:
        #custom limit for super-traders used in the demo
        if int(user) <= 10:
            row["contact_limit"] = .05
        row["portfolio_ratio"] = 100*float(row["stock_total"])/portfolio_total
        if abs(row["portfolio_ratio"]) > 100 * row["contact_limit"]:
            user_companies_list.append(row["company"])
        row["graphic"] = '|' * abs(int(round(row["portfolio_ratio"])))
        row["portfolio_ratio"] = "%.2f" % row["portfolio_ratio"]

    user_companies.sort(key = lambda row: abs(float(row["portfolio_ratio"])), reverse=True)
    if len(user_companies) > 0:
        latest_news = session.execute("SELECT company, summary, newsoutlet, source, author, newstime FROM news WHERE company IN ('" + "','".join(user_companies_list) + "') ORDER BY newstime DESC LIMIT 10")
    else:
        latest_news = []

    return [latest_trades, user_companies, latest_news]

def _get_json_user_data(user):
    latest_trades, portfolio, latest_news = _get_user_data(user)
    trades_json = [{"company": row.company, "num_stock": row.num_stock, "tradetime": row.tradetime} for row in latest_trades]
    portfolio_json = [{"company": row["company"], "stock_total": row["stock_total"], "contact_limit": row["contact_limit"], "portfolio_ratio": row["portfolio_ratio"], "graphic": row["graphic"]} for row in portfolio]
    news_json = [{"company": row.company, "summary": row.summary, "newsoutlet": row.newsoutlet, "source": row.source, "author": row.author, "newstime": row.newstime} for row in latest_news]
    return jsonify(latest_trades = trades_json, portfolio = portfolio_json, news = news_json)

@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html")

@app.route('/slides')
def slides():
    return render_template("slides.html")

@app.route('/user')
def get_user():
    user=request.args.get("user")
    latest_trades, portfolio, latest_news = _get_user_data(request.args.get("user"))
    return render_template("user.html", user=user, latest_trades = latest_trades, portfolio = portfolio, latest_news = latest_news)

@app.route('/retrieve_user_data')
def retrieve_user_data():
    return _get_json_user_data(request.args.get("user"))

@app.route('/retrieve_user_data/<user>')
def retrieve_user_data_api(user):
    return _get_json_user_data(user)

@app.route('/tradesummary/<user>')
def get_trade_summary(user):
    #check which database to query
    dbfile = open("/home/ubuntu/.insightproject/cassandra.txt")
    db = dbfile.readline().rstrip()
    dbfile.close()

    stmt = "SELECT user, company, stock_total, portfolio_ratio, contact_limit FROM stock_counts_" + db + " WHERE user=%s"
    stock_counts = session.execute(stmt, parameters=[user])
     
    jsonresponse = [{"user": row.user, "company": row.company, "stock_total": row.stock_total, "portfolio_ratio": row.portfolio_ratio, "contact_limit": row.contact_limit} for row in stock_counts]
    return jsonify(tradesummary=jsonresponse)
