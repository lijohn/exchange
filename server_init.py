import pandas as pd
import pickle

def backup(bids, asks, fills, payouts, exchanges, users, mrp, heads, tails, passwords, topics):

	pickle_out = open("bids.pickle","wb")
	pickle.dump(bids, pickle_out)
	pickle_out.close()
	
	pickle_out = open("asks.pickle","wb")
	pickle.dump(asks, pickle_out)
	pickle_out.close()
	
	pickle_out = open("fills.pickle","wb")
	pickle.dump(fills, pickle_out)
	pickle_out.close()
	
	pickle_out = open("payouts.pickle","wb")
	pickle.dump(payouts, pickle_out)
	pickle_out.close()
	
	pickle_out = open("exchanges.pickle","wb")
	pickle.dump(exchanges, pickle_out)
	pickle_out.close()
	
	pickle_out = open("users.pickle","wb")
	pickle.dump(users, pickle_out)
	pickle_out.close()

	pickle_out = open("mrp.pickle","wb")
	pickle.dump(mrp, pickle_out)
	pickle_out.close()

	pickle_out = open("heads.pickle","wb")
	pickle.dump(heads, pickle_out)
	pickle_out.close()

	pickle_out = open("tails.pickle","wb")
	pickle.dump(tails, pickle_out)
	pickle_out.close()

	pickle_out = open("passwords.pickle","wb")
	pickle.dump(passwords, pickle_out)
	pickle_out.close()
	
	pickle_out = open("topics.pickle","wb")
	pickle.dump(topics, pickle_out)
	pickle_out.close()

bids = []
asks = []
fills = []
payouts = {}
exchanges = []
users = {}
most_recent_prices = {}
heads = []
tails = []
passwords = {}
topics = []

backup(bids, asks, fills, payouts, exchanges, users, most_recent_prices, heads, tails, passwords, topics)