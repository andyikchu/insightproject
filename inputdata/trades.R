set.seed(1)

traderIDs = seq(1,1000000)
tickerIDs = c("MMM", "AXP", "AAPL", "BA", "CAT", "CVX", "CSCO", "KO", "DD", "XOM", "GE", "GS", "HD", "INTC", "IBM", "JNJ", "JPM", "MCD", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV" "UNH", "UTX", "VZ", "V", "WMT", "DIS")
numtraders_persec=500000

library(rkafka)
kafkanode="ec2-54-215-247-116.us-west-1.compute.amazonaws.com:9092"
producer=rkafka.createProducer(kafkanode)

generate_trades = function() {
	#select numtraders_persec from traderIDs
	trades = data.frame(trader=sample(traderIDs, numtraders_persec))
	#associate a ticker name to trade from every trader
	trades["ticker"] = sample(tickerIDs, nrow(trades), replace=T)
	#associate a quantity of stock to buy or sell for each trade
	trades["numstocks"] = floor(rnorm(nrow(trades), mean=0, sd=500))
	
	for(i in 1:nrow(trades)) {
		message = paste0('{"user":', trades[i,"trader"], ', "company":"', trades[i,"ticker"],'", "numstock":', trades[i,"numstocks"], ', "timestamp":"', Sys.time(), '"}')
		rkafka.send(producer, "trades", kafkanode, message)
	}
}

while(1) {
	generate_trades()
}

rkafka.closeProducer(producer)
