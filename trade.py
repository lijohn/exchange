#!/usr/bin/env/python3

# Import socket module 
import socket 

# local host IP '127.0.0.1' 
#host = '52.90.72.19'
host = '127.0.0.1'

# Define the port on which you want to connect 
port = 12346

s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 

def check_username():
	save_user = input("Enter Username: ")
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	user = 'user_' + save_user
	s.send(user.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return save_user, printstr

def send_bid_ask(bidask,vol,price,security):
	global username
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = bidask + '_' + str(vol) + '_' + str(price) + '_' + str(security) + '_' + str(username)
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr

def refresh():
	global username
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'refresh_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr

def refresh_sec(security):
	global username
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'refreshsec_' + security + '_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr
	
def cancel_order(security):
	global username
	bidask = input("bid or ask? (type out fully no caps) ")
	vol = input('Volume? (must match previous order exactly) ')
	price = input('Price? (must match previous order exactly) ')
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'cancel_' + str(bidask) + '_' + str(vol) + '_' + str(price) + '_' + str(security) + '_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr
	
def payout(security):
	global username
	password = input('Password: ')
	payout = input('Payout: ')
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'payout_' + str(password) + '_' + str(payout) + '_' + str(security) + '_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr

def security_menu():
	global username
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'refresh_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr

def front_menu():
	global username
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'frontmenu_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr

def front_menu_topic():
	global username
	topic = input('Topic: ')
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'fronttopic_' + topic + '_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	send_topic = ''
	send_topic = printstr.split('_')[-1]
	printstr = printstr.split('_')[0]
	
	return printstr, send_topic

def add_topic():
	global username
	password = input('Password: ')
	topic = input('Topic: ')
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'addtopic_' + password + '_' + topic + '_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr
	
def delete_user():
	global username
	password = input('Password: ')
	user = input('User: ')
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'deluser_' + password + '_' + user + '_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr
	
def manual_backup():
	global username
	password = input('Password: ')
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'backup_' + password + '_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr
	
def get_coin_info():
	global username
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'coininfo_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr

def suggest_topic():
	global username
	topic = input('Suggest a topic: ')
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'suggest_' + topic + '_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr

def view_topics():
	global username
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'viewtopics_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr
	
# to do
def change_values():
	global username
	user = input('Which user to change: ')
	amount = input('Amount: ')
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'changevalue_' + user + '_' + amount + '_' + username
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	return printstr

# to do
def sign_in():
	user = input('Username: ')
	password = input('Password: ')
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'signin_'
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	
	if printstr == 'Success': 
		return user, printstr
	else:
		return user, 'Failed log in'
		
# to do
def register():
	user = input('Username: ')
	password = input('Password: ')
	s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
	s.connect((host,port))
	message = 'register_'
	s.send(message.encode('ascii')) 
	data = s.recv(1024) 
	printstr = str(data.decode('ascii'))
	s.close()
	
	if printstr == 'Success': 
		return user, printstr
	else:
		return user, 'Failed log in'

	
print('Welcome to the trading game')
username, prnt = check_username()
print(prnt)

'''
sign = input('1. Sign in\n2. Register')

if int(sign) == 1:
	username, prnt = sign_in()
	if prnt == 'Failed log in':
		return 'Failure'
elif int(sign) == 2:
	username, prnt = sign_in()
	if prnt == 'Failed log in':
		return 'Failure'
else:
	return 'Failure'
'''

security = ''

cont = 0
menu = 0
while cont == 0:
	while menu == 0:
		print('Current topics being traded on:')
		prnt = front_menu()
		print(prnt)
		print('1. Trade')
		print('2. View Account Info (Balance, Open Orders, Etc.)')
		print('3. Suggest a topic')
		print('4. View suggested topics')
		print('5. Exit')
		
		if username == 'michael':
			print('99. Admin Tools')
		
		option = int(input('Choose Option: '))
		
		if option == 1:
			print('Pick topic to trade')
			prnt, topic = front_menu_topic()
			print(prnt)
			print(topic)
			
			if topic == 'invalid':
				menu = 0
			else:
				security = topic
				menu = 1
		elif option == 2:
			prnt = refresh()
			print(prnt)	 
		elif option == 3:
			prnt = suggest_topic()
			print(prnt)
		elif option == 4:
			prnt = view_topics()
			print(prnt)
		elif option == 99 and username == 'michael':
			select = int(input('1: Do payouts\n2: Add topic\n3: Delete User\n4: Change Coin Values\n5.Manual Backup\n'))
			if select == 1:
				security = int(input('\nSecurity index'))
				prnt = payout(security)
				print(prnt)
			elif select == 2:
				prnt = add_topic()
				print(prnt)
			elif select == 3:
				prnt = delete_user()
				print(prnt)
			elif select == 4:
				prnt = change_values()
				print(prnt)
			elif select == 5:
				prnt = manual_backup()
				print(prnt)
			else:
				print('Invalid option')
		else:
			menu = 2
			cont = 1
		
	while menu == 1:
		print(security + ' Options')
		print('1: Bid')
		print('2: Ask')
		print('3. Refresh Info (orderbook, open orders, fills)')
		print('4. Cancel Order')
		print('5. Back to main menu')
		print('6. Exit')
	
		option = int(input('Choose Option: '))
	
		if option == 1:
			volume = int(input('volume: '))
			limit_price = float(input('limit price: '))
			prnt = send_bid_ask('bid', volume,limit_price,security)
			print(prnt)
		elif option == 2:
			volume = int(input('volume: '))
			limit_price = float(input('limit price: '))
			prnt = send_bid_ask('ask', volume,limit_price,security)
			print(prnt)
		elif option == 3:
			prnt = refresh_sec(security)
			print(prnt)
		elif option == 4:
			prnt = cancel_order(security)
			print(prnt)			 
		elif option == 5:
			menu = 0
		else:
			menu = 0
			cont = 1
			
			