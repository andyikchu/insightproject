CREATE KEYSPACE finance_news WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
USE finance_news;

--stock count table
--_batch is generated from batch processing, after each batch run, the real time database is switched from _rts1 to _rts2 and vice versa
--_rts1 is a real time stream that continuously updates after being reset to 0 counts immediately after every switchover
--_web is a copy of the real time stream that gets generated immediately before every switchover
CREATE TABLE stock_counts_batch (user text, company text, stock_total int, portfolio_ratio float, contact_limit float, PRIMARY KEY ((user, company), portfolio_ratio), ) WITH CLUSTERING ORDER BY (portfolio_ratio DESC);
CREATE TABLE stock_counts_rts1 (user text, company text, stock_total int, portfolio_ratio float, contact_limit float, PRIMARY KEY ((user, company), portfolio_ratio), ) WITH CLUSTERING ORDER BY (portfolio_ratio DESC);
CREATE TABLE stock_counts_rts2 (user text, company text, stock_total int, portfolio_ratio float, contact_limit float, PRIMARY KEY ((user, company), portfolio_ratio), ) WITH CLUSTERING ORDER BY (portfolio_ratio DESC);
CREATE TABLE stock_counts_web (user text, company text, stock_total int, portfolio_ratio float, contact_limit float, PRIMARY KEY ((user, company), portfolio_ratio), ) WITH CLUSTERING ORDER BY (portfolio_ratio DESC);

CREATE TABLE stock_totals_batch (user text, portfolio_total int, PRIMARY KEY (user));
CREATE TABLE stock_totals_rts1 (user text, portfolio_total int, PRIMARY KEY (user));
CREATE TABLE stock_totals_rts2 (user text, portfolio_total int, PRIMARY KEY (user));
CREATE TABLE stock_totals_web (user text, portfolio_total int, PRIMARY KEY (user));

--news
CREATE TABLE news (company text, summary text, author text, source text, newstime timestamp, newsoutlet text, PRIMARY KEY ((company, summary), newstime), ) WITH CLUSTERING ORDER BY (newstime DESC);
