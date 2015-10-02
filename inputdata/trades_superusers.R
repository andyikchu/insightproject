#10 users trade 2 companies every second
traderIDs = seq(1,10)
tickerIDs = c("MMM", "AXP", "AAPL", "BA", "CAT", "CVX", "CSCO", "KO", "DD", "XOM", "GE", "GS", "HD", "INTC", "IBM", "JNJ", "JPM", "MCD", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV", "UNH", "UTX", "VZ", "V", "WMT", "DIS")

library(rkafka)

generate_trades = function() {
	trades = data.frame(trader=sort(rep(traderIDs, 2)))
	#associate a ticker name to trade from every trader
	trades["ticker"] = sample(tickerIDs, nrow(trades), replace=T)
	#associate a quantity of stock to buy or sell for each trade
	trades["numstocks"] = floor(rnorm(nrow(trades), mean=10, sd=750))
	
	kafkanode="ec2-54-215-247-116.us-west-1.compute.amazonaws.com:9092"
	producer=rkafka.createProducer(kafkanode)

	for(i in 1:nrow(trades)) {
		message = paste0('{"user":', trades[i,"trader"], ', "company":"', trades[i,"ticker"],'", "numstock":', trades[i,"numstocks"], ', "timestamp":"', Sys.time(), '"}')
		rkafka.send(producer, "trades", kafkanode, message)
	}
	rkafka.closeProducer(producer)
}

while(1) {
	generate_trades()
	sleep(1)
}
