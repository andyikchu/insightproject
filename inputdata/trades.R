args=commandArgs=TRUE
traderIDfile=args[1] #file of user IDs
tickerIDfile=args[2] #file of ticker names
numtraders_persec=args[3] #number of traders making trades every second

#traderIDs = read.csv()
traderIDs = seq(1,5)
#tickerIDs = read.csv()
tickerIDs = paste0("ti", seq(1,2))
numtraders_persec=2

library(rkafka)
kafkanode="ec2-54-67-10-231.us-west-1.compute.amazonaws.com:9092"
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
	Sys.sleep(30)
}

rkafka.closeProducer(producer)
