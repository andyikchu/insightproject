from twython import Twython
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()

#TODO: load from external file for all producers
COMPANIES = ["MMM", "AXP", "AAPL", "BA", "CAT", "CVX", "CSCO", "KO", "DD", "XOM", "GE", "GS", "HD", "INTC", "IBM", "JNJ", "JPM", "MCD", "MRK", "MSFT", "NKE", "PFE", "PG", "TRV", "UNH", "UTX", "VZ", "V", "WMT", "DIS"]

COMPANIES = list('$' + company for company in COMPANIES)

keyfile = open("/home/ubuntu/.twitterkeys/keys.txt", "r")
TWITTERKEYS = dict(line.rstrip().split(": ") for line in keyfile)
keyfile.close()

t = Twython(TWITTERKEYS['consumer key'], TWITTERKEYS['consumer secret'], oauth_version=2)
ACCESS_TOKEN = t.obtain_access_token()
t = Twython(TWITTERKEYS['consumer key'], access_token=ACCESS_TOKEN)

for co in COMPANIES:
    dump = open("/home/ubuntu/synthetic_twitter/" + co + ".txt", "a")

    results = t.search(q=co, count=100)
    for res in results['statuses']:
        text = res['text']
        source = 'https://twitter.com/' + res['user']['name'] + '/status/' + str(res['id'])

        json = '{"company":"' + co + '", "summary":"' + text +'", "source":"' + source + '"}'
        dump.write(json.encode('utf-8').strip())
    dump.close
