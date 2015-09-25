COPY finance_news.stock_counts_rts1 (user, company, stock_total, portfolio_ratio, contact_limit) TO '/tmp/temp.csv';
COPY finance_news.stock_counts_web (user, company, stock_total, portfolio_ratio, contact_limit) FROM '/tmp/temp.csv';
