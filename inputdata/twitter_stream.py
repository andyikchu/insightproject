#from twython import Twython
from twython import TwythonStreamer
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()

#TODO: load from external file for all producers
COMPANIES = ["MMM", "AXP", "AAPL", "BA", "CAT", "CVX", "CSCO", "KO", "DD", "XOM", "GE", "GS", "HD", "INTC", "IBM", "JNJ", "JPM", "MCD", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV", "UNH", "UTX", "VZ", "V", "WMT", "DIS"]

COMPANIES = list('$' + company for company in COMPANIES)

keyfile = open("/home/ubuntu/.twitterkeys/keys.txt", "r")
TWITTERKEYS = dict(line.rstrip().split(": ") for line in keyfile)
keyfile.close()

class NewsStreamer(TwythonStreamer):
    def on_success(self, data):
        if 'text' in data:
            print data['text'].encode('utf-8') + data['timestamp_ms'] + str(data['id'])
    def on_error(self, status_code, data):
        print "error" + str(status_code)

stream = NewsStreamer(TWITTERKEYS['consumer key'], TWITTERKEYS['consumer secret'], TWITTERKEYS['access token'], TWITTERKEYS['access token secret'])

stream.statuses.filter(filter_level = 'medium', language='en', track=','.join(COMPANIES))
