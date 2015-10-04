#every second, 6 of the super-traders trade between 1 to 3 stocks
traderIDs = seq(1,10)
tickerIDs = c("MMM", "AXP", "AAPL", "BA", "CAT", "CVX", "CSCO", "KO", "DD", "XOM", "GE", "GS", "HD", "INTC", "IBM", "JNJ", "JPM", "MCD", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV", "UNH", "UTX", "VZ", "V", "WMT", "DIS")

library(rkafka)

kafkanode="ec2-54-215-247-116.us-west-1.compute.amazonaws.com:9092"
producer=rkafka.createProducer(kafkanode)

generate_trades = function() {
	traders = sample(traderIDs, 6)
	numtrades_per_trader = sample(seq(1,3),length(traders), replace=T)
	trades = data.frame(trader=sort(rep(traders, numtrades_per_trader)))
	#associate a ticker name to trade from every trader
	trades["ticker"] = sample(tickerIDs, nrow(trades), replace=T)
	#associate a quantity of stock to buy or sell for each trade
	trades["numstocks"] = floor(rnorm(nrow(trades), mean=10, sd=750))

	for(i in 1:nrow(trades)) {
		message = paste0('{"user":', trades[i,"trader"], ', "company":"', trades[i,"ticker"],'", "numstock":', trades[i,"numstocks"], ', "timestamp":"', Sys.time(), '"}')
		rkafka.send(producer, "trades", kafkanode, message)
	}
}

while(1) {
	generate_trades()
	Sys.sleep(1)
}
rkafka.closeProducer(producer)
