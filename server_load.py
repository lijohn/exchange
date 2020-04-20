# import socket programming library 
import socket 

# import thread module 
from _thread import *
import threading 

#import others
import datetime
import time as t
import pandas as pd
import math
import matplotlib.pyplot as plt
from datetime import date, timedelta
import pickle
import datetime
from decimal import *

#set decimal to 2 digits
getcontext().prec = 2

def backup(bids, asks, fills, payouts, exchanges, users, mrp, passwords, topics):

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
	
	pickle_out = open("passwords.pickle","wb")
	pickle.dump(passwords, pickle_out)
	pickle_out.close()
	
	pickle_out = open("topics.pickle","wb")
	pickle.dump(topics, pickle_out)
	pickle_out.close()


def create_bid_ask(security,user_id, volume, price):
	temp = {}
	temp['security_id'] = security
	temp['user_id'] = user_id
	temp['volume'] = volume
	temp['price'] = price
	temp['time'] = datetime.datetime.now()
	return temp

def create_filled(bid, ask):
	global users
	price = 0
	if bid['time'] < ask['time']:
		price = min(bid['price'],ask['price'])
	else:
		price = max(ask['price'],bid['price'])
		
	temp = {}
	temp['bid_id'] = bid['user_id']
	temp['ask_id'] = ask['user_id']
	temp['volume'] = min(bid['volume'],ask['volume'])
	temp['price'] = price
	temp['time'] = datetime.datetime.now()
	temp['bid_time'] = bid['time']
	temp['ask_time'] = ask['time']
	temp['security_id'] = bid['security_id']
	
	users[bid['user_id']] -= price * min(bid['volume'],ask['volume'])
	users[ask['user_id']] += price * min(bid['volume'],ask['volume'])
	
	return temp

def order_flow(bids, asks):
	
	temp = []
	
	for bid in bids:
		bid_id = bid['user_id']
		bid_volume = bid['volume']
		bid_price = bid['price']
		bid_time = bid['time']
		for ask in asks:
			ask_id = ask['user_id']
			ask_volume = ask['volume']
			ask_price = ask['price']
			ask_time = ask['time']
			
			if bid_id != ask_id and bid['security_id'] == ask['security_id']:
				if ask_price <= bid_price:
					fills.append(create_filled(bid,ask))
					sub = min(bid['volume'],ask['volume'])
					bid['volume'] -= sub
					ask['volume'] -= sub
					
					bids = list(filter(lambda i: i['volume'] != 0, bids)) 
					asks = list(filter(lambda i: i['volume'] != 0, asks)) 
					
	bids = list(filter(lambda i: i['volume'] != 0, bids)) 
	asks = list(filter(lambda i: i['volume'] != 0, asks)) 
	
	temp.append(bids)
	temp.append(asks)
	
	return temp

bids = pickle.load(open("bids.pickle","rb"))
asks = pickle.load(open("asks.pickle","rb"))
fills = pickle.load(open("fills.pickle","rb"))
payouts = pickle.load(open("payouts.pickle","rb"))
exchanges = pickle.load(open("exchanges.pickle","rb"))
users = pickle.load(open("users.pickle","rb"))
most_recent_prices = pickle.load(open("mrp.pickle","rb"))
heads = pickle.load(open("heads.pickle","rb"))
tails = pickle.load(open("tails.pickle","rb"))
passwords = pickle.load(open("passwords.pickle","rb"))
topics = pickle.load(open("topics.pickle","rb"))

print_lock = threading.Lock() 

# thread fuction 
def threaded(c): 
	global users, bids, asks, fills, most_recent_prices, payouts, exchanges, heads, tails, passwords, topics
	while True: 
	
		#save backups every 10 minutes
		now = datetime.datetime.now()
		minute = now.minute
		second = now.second
		if minute % 10 == 0 and second % 10 == 0:
			backup(bids, asks, fills, payouts, exchanges, users, most_recent_prices, passwords, topics)

		# data received from client 
		data = c.recv(2048) 
		if not data: 
			print('Bye') 

			# lock released on exit 
			print_lock.release() 
			break
		
		data_type = str(data).split('_')[0]
		
		#handles the incoming strings from the client
		if data_type[2:] == 'user':
			username = str(data).split('_')[1][:-1]
			if username in users:
				c.send(('\n' + 'Logged in as: ' + username).encode('ascii'))
			else:
				users[username] = 10000
				c.send((('\n' + 'Created new user: ' + username + '\n').encode('ascii')))
		elif data_type[2:] == 'bid':
			username = str(data).split('_')[-1][:-1]
			volume = int(str(data).split('_')[1])
			price = float(str(data).split('_')[2])
			security = str(data).split('_')[3]
			bids.append(create_bid_ask(security,username,volume,price))
			c.send(('Order received\n').encode('ascii'))
		elif data_type[2:] == 'ask':
			username = str(data).split('_')[-1][:-1]
			volume = int(str(data).split('_')[1])
			price = float(str(data).split('_')[2])
			security = str(data).split('_')[3]
			asks.append(create_bid_ask(security,username,volume,price))
			c.send(('Order received\n').encode('ascii'))
		elif data_type[2:] == 'cancel':
			username = str(data).split('_')[-1][:-1]
			sendstr = '\n' + 'Invalid cancellation\n'
			order_type = str(data).split('_')[1]
			vol = str(data).split('_')[2]
			price = str(data).split('_')[3]
			security = str(data).split('_')[4]
			
			if order_type == 'bid':
				for bid in bids:
					if bid['user_id'] == username and int(bid['volume']) == int(vol) and float(bid['price']) == float(price) and bid['security_id'] == security:
						bid['volume'] = 0
						sendstr = '\n' + 'Order successfully cancelled\n'
			elif order_type == 'ask':
				for ask in asks:
					if ask['user_id'] == username and int(ask['volume']) == int(vol) and float(ask['price']) == float(price) and ask['security_id'] == security:
						ask['volume'] = 0
						sendstr = '\n' + 'Order successfully cancelled\n'
			else:
				sendstr = '\n' + 'Invalid cancellation\n'
			
			c.send((sendstr).encode('ascii'))
		
		elif data_type[2:] == 'frontmenu':
			sendstr = '\n'
			counter = 0
			for topic in exchanges:
				counter += 1
				sendstr += str(counter) + ': ' + str(topic) + ', Currently trading at: ' + str(most_recent_prices[topic]) + '\n'
			c.send((sendstr).encode('ascii'))
			
		elif data_type[2:] == 'fronttopic':
			sendstr = '\n'
			
			temp_list = []
			i = 0
			while i < len(exchanges):
				temp_list.append(i)
				i += 1
				
			topic = int(str(data).split('_')[1]) - 1
			
			if topic not in temp_list:
				sendstr += 'Not a valid topic\n' + '_invalid'
			else:
				sendstr += '_' + str(exchanges[topic])
			
			c.send((sendstr).encode('ascii'))
				
		elif data_type[2:] == 'refresh':
			username = str(data).split('_')[-1][:-1]
			sendstr = '\n'
			
			#balance section
			sendbal = str(users[username])
			sendstr += 'Cash balance: ' + sendbal + '\n'
			
			bal = users[username]
			
			for topic in exchanges:
				#print(fills)
				#print(most_recent_prices)
				#print(topic)
				sendstr += 'Exposure to ' + topic + ':\n'
				temp_fills = list(filter(lambda i: i['security_id'] == topic, fills))
				#print(temp_fills)
				#print(sorted(fills, key = lambda i: i['time'],reverse=True)[0]['price'])
				num = 0
				for fill in temp_fills:
					if fill['bid_id'] == username:
						num += fill['volume']
					elif fill['ask_id'] == username:
						num -= fill['volume']
				if num < 0:
					sendstr += 'Short ' + str(-num) + ' with value ' + str(num*most_recent_prices[topic]) + '\n'
					bal += num * most_recent_prices[topic]
				elif num > 0:
					sendstr += 'Long ' + str(num) + ' with value ' + str(num*most_recent_prices[topic]) + '\n'
					bal += num * most_recent_prices[topic]
				else:
					sendstr += 'no net positions\n'
			
			sendstr += '\n' + 'Net balance: ' + str(bal) + '\n' + '\n'

			#Open orders section
			user_bids = [d for d in bids if d['user_id'] == username]
			user_asks = [d for d in asks if d['user_id'] == username]
			
			user_bids = sorted(user_bids, key = lambda i: i['security_id'],reverse=False)
			user_asks = sorted(user_asks, key = lambda i: i['security_id'],reverse=False)
			
			sendstr += 'Open Orders\n' + 'Bids' + '\n'
			for bid in user_bids:
				sendstr += 'Price: ' + str(bid['price']) + ' Volume: ' + str(bid['volume']) + ' Security: ' + str(bid['security_id']) + '\n'
			sendstr += 'Asks' + '\n'
			for ask in user_asks:
				sendstr += 'Price: ' + str(ask['price']) + ' Volume: ' + str(ask['volume']) + ' Security: ' + str(ask['security_id']) + '\n'
			sendstr += '\n'
			
			#leaderbook
			sendstr += 'Leaderboard\n'
			total_balance = {}
			for user in users:
				cash_balance = users[user]
				ask_fills = list(filter(lambda i: i['ask_id'] == user, fills))
				bid_fills = list(filter(lambda i: i['bid_id'] == user, fills))
				for fill in bid_fills:
					cash_balance += fill['volume'] * most_recent_prices[fill['security_id']]
				for fill in ask_fills:
					cash_balance -= fill['volume'] * most_recent_prices[fill['security_id']]
				total_balance[user] = cash_balance
			
			for user in total_balance:
				sendstr += str(user) + ': ' + str(total_balance[user]) + '\n'
			
			c.send((sendstr).encode('ascii'))
			
		elif data_type[2:] == 'refreshsec':
			username = str(data).split('_')[-1][:-1]
			security = str(data).split('_')[1]
			sendstr = '\n'
			
			#balance section
			sendbal = str(users[username])
			sendstr += 'Cash balance: ' + sendbal + '\n'
			
			num = 0
			for fill in fills:
				if fill['bid_id'] == username and fill['security_id'] == security:
					num += fill['volume']
				elif fill['ask_id'] == username and fill['security_id'] == security:
					num -= fill['volume']
			if num < 0:
				sendstr += 'Short ' + str(-num) + ' with value ' + str(num*most_recent_prices[security])
			elif num > 0:
				sendstr += 'Long ' + str(num) + ' with value ' + str(num*most_recent_prices[security])
			else:
				sendstr += 'No net positions'

			#Open orders section
			user_bids = [d for d in bids if d['user_id'] == username]
			user_asks = [d for d in asks if d['user_id'] == username]
			sendstr += '\n' + 'Open Orders\n' + 'Bids' + '\n'
			for bid in user_bids:
				if bid['security_id'] == security:
					sendstr += 'Price: ' + str(bid['price']) + ' Volume: ' + str(bid['volume']) + '\n'
			sendstr += 'Asks' + '\n'
			for ask in user_asks:
				if ask['security_id'] == security:
					sendstr += 'Price: ' + str(ask['price']) + ' Volume: ' + str(ask['volume']) + '\n'
			sendstr += '\n'
				
			#Orderbook section	  
			asks_string = ''
			bids_string = ''
			
			temp_bids = list(filter(lambda i: i['security_id'] == security, bids))
			temp_asks = list(filter(lambda i: i['security_id'] == security, asks))

			if temp_asks == []:
				asks_string = 'No asks'
			else:
				lowest_ask = sorted(temp_asks, key = lambda i: i['price'],reverse=False)[0]['price']
				lowest_ask_volume = sorted(temp_asks, key = lambda i: i['price'],reverse=True)[0]['volume']
				asks_string = 'Lowest Ask: ' + str(lowest_ask) + ' Volume: ' + str(lowest_ask_volume)
			if temp_bids == []:
				bids_string = 'No bids' + '\n'
			else:
				highest_bid = sorted(temp_bids, key = lambda i: i['price'],reverse=True)[0]['price']
				highest_bid_volume = sorted(temp_bids, key = lambda i: i['price'],reverse=True)[0]['volume']
				bids_string = 'Highest Bid: ' + str(highest_bid) + ' Volume: ' + str(highest_bid_volume) + '\n'
			sendstr += 'Orderbook (Highest Bid and Lowest Ask only)\n' + bids_string + asks_string + '\n' + '\n'
			
			c.send((sendstr).encode('ascii'))
		
		elif data_type[2:] == 'addtopic':
		
			sendstr = '\n'	
			
			username = str(data).split('_')[-1][:-1]
			password = str(data).split('_')[1]
			topic = str(data).split('_')[2]
			
			if password == 'asdf':
				exchanges.append(topic)
				sendstr += 'Topic added\n'
			else:
				sendstr += 'Invalid password\n'
			
			c.send((sendstr).encode('ascii'))
		
		#DO NOT DELETE USERS WHILE THEY ARE EXPOSED TO ANY TOPIC
		elif data_type[2:] == 'deluser':
		
			sendstr = '\n'	
			
			username = str(data).split('_')[-1][:-1]
			password = str(data).split('_')[1]
			user = str(data).split('_')[2]
			
			if password == 'asdf':
				if user in users:
					users.pop(user,None)
					bids = list(filter(lambda i: i['user_id'] != user, bids)) 
					asks = list(filter(lambda i: i['user_id'] != user, asks))  
					sendstr += 'User Deleted\n'
				else:
					sendstr += 'User does not exist\n'
			else:
				sendstr += 'Invalid password\n'
			
			c.send((sendstr).encode('ascii'))
			
		#backup
		elif data_type[2:] == 'backup':
		
			sendstr = '\n'	
			
			username = str(data).split('_')[-1][:-1]
			password = str(data).split('_')[1]
			
			if password == 'asdf':
				backup(bids, asks, fills, payouts, exchanges, users, most_recent_prices, passwords, topics)
				sendstr += 'Successful Backup\n'
			else:
				sendstr += 'Invalid password\n'
			
			c.send((sendstr).encode('ascii'))
		
		#Shows info for the coin flip minigame
		elif data_type[2:] == 'coininfo':
			
			sendstr = 'Coins are flipped immediately after matching an open heads/tails with the opposite side\n'
			
			#coin_flip is list of dictionaries with structure:
			#[{user_id: x,bet: heads/tails, volume: y}...]
			
			if heads == [] and tails == []:
				sendstr += 'No current bets\n'
			elif heads == []:
				sendstr += 'No heads\n'
				sendstr += 'Tails\n'
				for tail in tails:
					sendstr += 'User: ' + tail['user_id'] + 'Volume: ' + tail['volume'] + '\n'
			elif tails == []:
				sendstr += 'Heads\n'
				for head in heads:
					sendstr += 'User: ' + head['user_id'] + 'Volume: ' + head['volume'] + '\n'
				sendstr += 'No tails\n'
			else:
				sendstr += 'Heads\n'
				for head in heads:
					sendstr += 'User: ' + head['user_id'] + 'Volume: ' + head['volume'] + '\n'
				sendstr += 'Tails\n'
				for tail in tails:
					sendstr += 'User: ' + tail['user_id'] + 'Volume: ' + tail['volume'] + '\n'
					
			c.send((sendstr).encode('ascii'))
		
		#Add topics to suggested topics list
		elif data_type[2:] == 'suggest':
		
			sendstr = '\n'	
			
			#decode the string sent from the client
			username = str(data).split('_')[-1][:-1]
			suggestion = str(data).split('_')[1]
			
			if suggestion not in topics:
				topics.append(suggestion)
				sendstr += 'Suggestion added'
			else:
				sendstr += 'Suggestion already added'
			
			sendstr += '\n'
			
			c.send((sendstr).encode('ascii'))
			
				
		#Add topics to suggested topics list
		elif data_type[2:] == 'changevalue':
		
			sendstr = '\n'	
			
			#decode the string sent from the client
			username = str(data).split('_')[-1][:-1]
			user_to_change = str(data).split('_')[1]
			amount = int(str(data).split('_')[2])
			
			if user_to_change not in users:
				sendstr += 'User not found'
			else:
				users[user_to_change] = amount
				sendstr += 'Values successfully changed'
			
			sendstr += '\n'
			
			c.send((sendstr).encode('ascii'))
			
		#View suggested topics
		elif data_type[2:] == 'viewtopics':
		
			sendstr = '\n'	
			
			#decode the string sent from the client
			username = str(data).split('_')[-1][:-1]
			
			if topics == []:
				sendstr += 'No suggestions'
			else:
				sendstr += 'List of suggestions\n'
				i = 0
				while i < len(topics):
					sendstr += str(i+1) + ': ' + topics[i] + '\n'
					i += 1
			
			c.send((sendstr).encode('ascii'))
		
		#Do payouts of the topics
		elif data_type[2:] == 'payout':
		
			sendstr = '\n'	
			
			#decode the string sent from the client
			username = str(data).split('_')[-1][:-1]
			password = str(data).split('_')[1]
			payout = float(str(data).split('_')[2])
			security = int(str(data).split('_')[3]) - 1
			
			temp_list = []
			i = 0
			while i < len(exchanges):
				temp_list.append(i)
				i += 1
				
			topic = exchanges[security]
			
			#validates that the topic exists
			if security not in temp_list:
				sendstr += 'Not a valid topic\n'
			#buyers get credited volume * payout value, where sellers get debited volume * payout value
			else:
				if password == 'asdf':
					
					#deletes any outstanding bids and asks on that topic
					bids = list(filter(lambda i: i['security_id'] != topic, bids)) 
					asks = list(filter(lambda i: i['security_id'] != topic, asks)) 
					
					#records the results
					payouts[security] = payout
					
					if payout == 0:
						fills = list(filter(lambda i: i['security_id'] != topic, fills))
						
					else:
						temp_fills = list(filter(lambda i: i['security_id'] == topic, fills))
						for fill in temp_fills:
							users[fill['bid_id']] += fill['volume'] * payout
							users[fill['ask_id']] -= fill['volume'] * payout
							
						fills = list(filter(lambda i: i['security_id'] != topic, fills))
			
					sendstr += 'Payout complete\n'
					exchanges.remove(topic)
					most_recent_prices.pop(topic)
				else:
					sendstr += 'Invalid password\n'
	
			c.send((sendstr).encode('ascii'))
		else:
			pass
		
		bids, asks = order_flow(bids,asks)
		bids = sorted(bids, key = lambda i: i['price'],reverse=True)
		asks = sorted(asks, key = lambda i: i['price'],reverse=False)
		
		print(bids)
		print(asks)
		
		#update the most recent prices for each exchange/ticker
		for exchange in exchanges:
			temp_fills = list(filter(lambda i: i['security_id'] == exchange, fills))
			if temp_fills == []:
				most_recent_prices[exchange] = 0
			else:
				most_recent_prices[exchange] = sorted(fills, key = lambda i: i['time'],reverse=True)[0]['price']

	# connection closed 
	c.close() 

def Main(): 
	host = "" 

	# reverse a port on your computer 
	# in our case it is 12345 but it 
	# can be anything 
	port = 12346
	
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	s.bind((host, port)) 
	print("socket binded to post", port) 

	# put the socket into listening mode 
	s.listen(5) 
	print("socket is listening") 
   

	# a forever loop until client wants to exit 
	while True: 

		# establish connection with client 
		c, addr = s.accept() 

		# lock acquired by client 
		print_lock.acquire() 
		print('Connected to :', addr[0], ':', addr[1]) 

		# Start a new thread and return its identifier 
		start_new_thread(threaded, (c,)) 
	s.close() 


if __name__ == '__main__': 
	Main() 