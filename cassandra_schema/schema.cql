CREATE KEYSPACE finance_news WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
USE finance_news;
CREATE TABLE trade_history (user text, company text, numstock int, tradetime timestamp, PRIMARY KEY (user, company));
CREATE TABLE stock_counts (user text, company text, stock_total int, portfolio_total int, portfolio_ratio float, contact_limit float, PRIMARY KEY ((user), portfolio_ratio), ) WITH CLUSTERING ORDER BY (portfolio_ratio DESC);
