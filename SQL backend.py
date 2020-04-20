#!/usr/bin/env python
# coding: utf-8

# # Exchange example with SQL backend

import sqlite3
import datetime
import pandas as pd
from collections import OrderedDict

from flask import Flask, request
from flask_restful import Api, Resource, reqparse
from flask_jsonpify import jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
api = Api(app)

conn = sqlite3.connect('test.db', check_same_thread=False)
c = conn.cursor()

def create_filled(bid, ask):
    
    users = list(OrderedDict(sorted(pd.read_sql_query('''SELECT * FROM users''', conn).to_dict(orient='index').items())).values())
    
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

    for user in users:
        if user['user_id'] == bid['user_id']:
            user['cash'] -= price * min(bid['volume'],ask['volume'])
        elif user['user_id'] == ask['user_id']:
            user['cash'] += price * min(bid['volume'],ask['volume'])
        else:
            pass
    
    users_df = pd.DataFrame(users)
    users_df.to_sql(name='users', con=conn, if_exists='replace', index = False)

    return temp

c.execute('DROP TABLE bids')
c.execute('DROP TABLE asks')
c.execute('DROP TABLE fills')
c.execute('DROP TABLE users')
c.execute('DROP TABLE markets')
c.execute('DROP TABLE positions')
c.execute('DROP TABLE ref_prices')

c.execute('''CREATE TABLE bids
             (security_id, user_id, volume, price, time)''')

c.execute('''CREATE TABLE asks
             (security_id, user_id, volume, price, time)''')

c.execute('''CREATE TABLE fills
             (bid_id, ask_id, volume, price, time, bid_time, ask_time, security_id)''')

c.execute('''CREATE TABLE users
             (user_id, username, cash)''')

c.execute('''CREATE TABLE markets
             (security_id, market_name, market_descriptor, create_time, end_time)''')

c.execute('''CREATE TABLE positions
             (security_id, user_id, position)''')

c.execute('''CREATE TABLE ref_prices
             (security_id, ref_price)''')


# # Create new users

def create_user(username):
    users = pd.read_sql_query('''SELECT * FROM users''', conn)
    if len(users) > 0:
        user_id = int(users.user_id.max()) + 1
    else:
        user_id = 0
        
    exec_string = 'INSERT INTO users (user_id, username, cash) values (?, ?, ?)'
    c.execute(exec_string,
        (user_id, username, 0))

create_user('Michael')
create_user('John')

pd.read_sql_query('''SELECT * FROM users''', conn)

class Users(Resource):
  def get(self):
    return jsonify(pd.read_sql_query('''SELECT * FROM users''', conn).to_dict("records"))
  def post(self):
    parser = reqparse.RequestParser()
    parser.add_argument("username")
    args = parser.parse_args()
    create_user(args["username"])
    return jsonify(pd.read_sql_query('''SELECT * FROM users''', conn).to_dict("records"))


# # Creating new markets

def create_market(market_name,market_descriptor, end_time):
    markets = users = pd.read_sql_query('''SELECT * FROM markets''', conn)
    if len(markets) > 0:
        security_id = int(markets.security_id.max()) + 1
    else:
        security_id = 0
        
    exec_string = 'INSERT INTO markets (security_id, market_name, market_descriptor, create_time, end_time) values (?, ?, ?, ?, ?)'
    create_time = datetime.datetime.now()
    c.execute(exec_string,
        (security_id, market_name, market_descriptor, create_time, end_time))

end_time = datetime.datetime(2019, 12, 31, 23, 59) #Dec 31st, 11:59 pm
create_market('Browns win the super bowl','Binary payout - contract size $1', end_time)

pd.read_sql_query('''SELECT * FROM markets''', conn).sort_values('create_time')

class Markets(Resource):
  def get(self):
    return jsonify(pd.read_sql_query('''SELECT * FROM markets''', conn).to_dict("records"))
  def post(self):
    parser = reqparse.RequestParser()
    parser.add_argument("name")
    parser.add_argument("description")
    parser.add_argument("end")
    args = parser.parse_args()
    create_market(args["name"], args["description"], args["end"])
    return jsonify(pd.read_sql_query('''SELECT * FROM markets''', conn).to_dict("records"))

class Market(Resource):
  def get(self, security_id):
    return jsonify(pd.read_sql_query('''SELECT * FROM markets WHERE security_id = {}'''
      .format(security_id), conn).to_dict("records")[0])

# # Create bid

def create_bid(security, user_id, volume, price):
    
    if user_id not in pd.read_sql_query('''SELECT * FROM users''', conn).user_id:
        return 'User does not exist'
    
    if security not in pd.read_sql_query('''SELECT * FROM markets''', conn).security_id:
        return 'Security does not exist'
    
    if user_id in pd.read_sql_query('''SELECT * FROM asks''', conn).user_id:
        asks = pd.read_sql_query('''SELECT * FROM asks''', conn)
        if price >= asks.loc[asks.user_id == user_id].price.max():
            return 'Invalid Order - crossing own ask'
    
    exec_string = 'INSERT INTO bids (security_id, user_id, volume, price, time) values (?, ?, ?, ?, ?)'
    time = datetime.datetime.now()
    c.execute(exec_string,
        (security, user_id, volume, price, time))

create_bid(0,0,10,0.5)

pd.read_sql_query('''SELECT * FROM bids''', conn)

class Bids(Resource):
  def get(self):
    return jsonify(pd.read_sql_query('''SELECT * FROM bids''', conn).to_dict("records"))
  def post(self):
    parser = reqparse.RequestParser()
    parser.add_argument("security_id", type=int)
    parser.add_argument("user_id", type=int)
    parser.add_argument("volume", type=int)
    parser.add_argument("price", type=float)
    args = parser.parse_args()
    create_bid(args["security_id"], args["user_id"], args["volume"], args["price"])
    order_flow()
    return jsonify(pd.read_sql_query('''SELECT * FROM bids''', conn).to_dict("records"))


# # Create Ask

def create_ask(security,user_id, volume, price):
    
    if user_id not in pd.read_sql_query('''SELECT * FROM users''', conn).user_id:
        return 'User does not exist'
    
    if security not in pd.read_sql_query('''SELECT * FROM markets''', conn).security_id:
        return 'Security does not exist'
    
    if user_id in pd.read_sql_query('''SELECT * FROM bids''', conn).user_id:
        bids = pd.read_sql_query('''SELECT * FROM bids''', conn)
        if price <= bids.loc[bids.user_id == user_id].price.max():
            return 'Invalid Order - crossing own bid'

    exec_string = 'INSERT INTO asks (security_id, user_id, volume, price, time) values (?, ?, ?, ?, ?)'
    time = datetime.datetime.now()
    c.execute(exec_string,
        (security, user_id, volume, price, time))

create_ask(0,1,10,0.5)

pd.read_sql_query('''SELECT * FROM asks''', conn)

class Asks(Resource):
  def get(self):
    return jsonify(pd.read_sql_query('''SELECT * FROM bids''', conn).to_dict("records"))
  def post(self):
    parser = reqparse.RequestParser()
    parser.add_argument("security_id", type=int)
    parser.add_argument("user_id", type=int)
    parser.add_argument("volume", type=int)
    parser.add_argument("price", type=float)
    args = parser.parse_args()
    create_ask(args["security_id"], args["user_id"], args["volume"], args["price"])
    order_flow()
    return jsonify(pd.read_sql_query('''SELECT * FROM asks''', conn).to_dict("records"))


# # Handling crossing own market

create_ask(0,0,10,0.5)


# # Other faulty orders

create_bid(1,0,10,0.5)

create_bid(0,2,10,0.5)


# # Run orderflow

def order_flow():
    
    bids = list(OrderedDict(sorted(pd.read_sql_query('''SELECT * FROM bids''', conn).sort_values('time').to_dict(orient='index').items())).values())
    asks = list(OrderedDict(sorted(pd.read_sql_query('''SELECT * FROM asks''', conn).sort_values('time').to_dict(orient='index').items())).values())

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
                    exec_string = 'INSERT INTO fills (bid_id, ask_id, volume, price, time, bid_time, ask_time, security_id) values (?, ?, ?, ?, ?, ?, ?, ?)'
                    temp = create_filled(bid, ask)
                    c.execute(exec_string,
                        (temp['bid_id'], temp['ask_id'], temp['volume'], temp['price'], temp['time'], temp['bid_time'], temp['ask_time'], temp['security_id']))
                    sub = min(bid['volume'],ask['volume'])
                    bid['volume'] -= sub
                    ask['volume'] -= sub

                    bids = list(filter(lambda i: i['volume'] != 0, bids)) 
                    asks = list(filter(lambda i: i['volume'] != 0, asks)) 
                    
            # to do, prevent crossing bids/asks from same user

    bids = list(filter(lambda i: i['volume'] != 0, bids)) 
    asks = list(filter(lambda i: i['volume'] != 0, asks))
    
    if bids == []:
        c.execute('DELETE FROM bids')
    else:
        bids_df = pd.DataFrame(bids)
        bids_df.to_sql(name='bids', con=conn, if_exists='replace', index = False)
    
    if asks == []:
        c.execute('DELETE FROM asks')
    else:
        asks_df = pd.DataFrame(asks)
        asks_df.to_sql(name='asks', con=conn, if_exists='replace', index = False)        


order_flow() #this should actually be put at the end of both the bid and ask functions, but is here to illustrate use
# call every time a bid or ask is added

pd.read_sql_query('''SELECT * FROM bids''', conn)

pd.read_sql_query('''SELECT * FROM asks''', conn)

pd.read_sql_query('''SELECT * FROM fills''', conn)

pd.read_sql_query('''SELECT * FROM users''', conn)

class Fills(Resource):
  def get(self):
    return jsonify(pd.read_sql_query('''SELECT * FROM fills''', conn).to_dict("records"))


# # Get positions

def update_positions():
    fills = list(OrderedDict(sorted(pd.read_sql_query('''SELECT * FROM fills''', conn).to_dict(orient='index').items())).values())
    positions = []
    for fill in fills:
        temp_ask = {}
        temp_ask['security_id'] = fill['security_id']
        temp_ask['user_id'] = fill['ask_id']
        temp_ask['position'] = -fill['volume']

        temp_bid = {}
        temp_bid['security_id'] = fill['security_id']
        temp_bid['user_id'] = fill['bid_id']
        temp_bid['position'] = fill['volume']

        positions.append(temp_bid)
        positions.append(temp_ask)
    
    if positions == []:
        return
    else:
        positions_df = pd.DataFrame(positions)
        add_list = []
        for name, group in positions_df.groupby(['security_id','user_id']):
            temp = {}
            security_id = name[0]
            user_id = name[1]
            position = group.position.sum()
            temp['security_id'] = security_id
            temp['user_id'] = user_id
            temp['position'] = position
            add_list.append(temp)

        pd.DataFrame(add_list).to_sql(name='positions', con=conn, if_exists='replace', index = False)   

update_positions()

pd.read_sql_query('''SELECT * FROM positions''', conn)

class Positions(Resource):
  def get(self):
    return jsonify(pd.read_sql_query('''SELECT * FROM positions''', conn).to_dict("records"))


# # List of markets print out

def list_of_markets():
    
    list_of_marks = []
    
    fills_df = pd.read_sql_query('''SELECT * FROM fills''', conn)
    bids_df = pd.read_sql_query('''SELECT * FROM bids''', conn)
    asks_df = pd.read_sql_query('''SELECT * FROM asks''', conn)

    for index, row in pd.read_sql_query('''SELECT * FROM markets''', conn).iterrows():
        market = {
            'security_id': row.security_id,
            'create_time': row.create_time,
            'end_time': row.end_time,
            'market_descriptor': row.market_descriptor,
            'market_name': row.market_name,
            'price': None,
            'bid': None,
            'ask': None
        }
        
        if row.security_id in fills_df.security_id:
            if len(fills_df.loc[fills_df.security_id == row.security_id]) > 0:
                market['price'] = fills_df.loc[fills_df.security_id == row.security_id].sort_values('time', ascending = False).iloc[0].price

        if row.security_id in bids_df.security_id:
            if len(bids_df.loc[bids_df.security_id == row.security_id]) > 0:
                market['bid'] = bids_df.loc[bids_df.security_id == row.security_id].price.max()

        if row.security_id in asks_df.security_id:
            if len(asks_df.loc[asks_df.security_id == row.security_id]) > 0:
                market['ask'] = asks_df.loc[asks_df.security_id == row.security_id].price.min()
        
        list_of_marks.append(market)

    return list_of_marks

class MarketsList(Resource):
  def get(self):
    return list_of_markets()


# # User summary

def get_user_info(user_id):
    sendstr = 'Overall positions\n'
    user = pd.read_sql_query('''SELECT * FROM users WHERE user_id = {}'''.format(user_id), conn)
    sendstr += user.iloc[0].username + '\n'
    sendstr += 'Cash: ' + str(user.iloc[0].cash) + '\n'
    
    positions = pd.read_sql_query('''SELECT * 
                                     FROM positions 
                                     JOIN markets 
                                     ON positions.security_id = markets.security_id
                                     WHERE user_id = {}'''.format(user_id), conn)
    
    for index, row in positions.iterrows():
        sendstr += row.market_name + ': ' + str(row.position) + ' contracts\n\n'
        
    sendstr += 'Open orders\n'
    bids = pd.read_sql_query('''SELECT * 
                                 FROM bids 
                                 JOIN markets 
                                 ON bids.security_id = markets.security_id
                                 WHERE user_id = {}'''.format(user_id), conn)
        
    asks = pd.read_sql_query('''SELECT * 
                             FROM asks 
                             JOIN markets 
                             ON asks.security_id = markets.security_id
                             WHERE user_id = {}'''.format(user_id), conn)
    
    if len(bids) == 0:
        sendstr += 'No outstanding bids\n'
    else:
        sendstr += 'Outstanding bids\n'
        for index, row in bids.iterrows():
            sendstr += row.market_name + ' - ' + str(row.volume) + ' contracts for ' + str(row.price) + ' placed at: ' + str(row.time) + '\n'
    
    sendstr += '\n'
    
    if len(asks) == 0:
        sendstr += 'No outstanding asks\n'
    else:
        sendstr += 'Outstanding asks\n'
        for index, row in asks.iterrows():
            sendstr += row.market_name + ': ' + str(row.volume) + ' contracts for: ' + str(row.price) +'\n'
        
    return sendstr

class User(Resource):
  def get(self, user_id):
    return get_user_info(user_id)

# Flask routes
api.add_resource(Users, '/users')
api.add_resource(User, '/users/<user_id>')
api.add_resource(Markets, '/markets')
api.add_resource(MarketsList, '/markets/list')
api.add_resource(Market, '/markets/<security_id>')
api.add_resource(Bids, '/bids')
api.add_resource(Asks, '/asks')
api.add_resource(Positions, '/positions')
api.add_resource(Fills, '/fills')

app.run(port='3000')