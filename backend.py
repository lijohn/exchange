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

conn = sqlite3.connect('exchange.db', check_same_thread=False)
c = conn.cursor()

def create_filled(bid, ask):
    
    users = list(OrderedDict(sorted(pd.read_sql_query('''SELECT * FROM users''', conn).to_dict(orient='index').items())).values())
    
    price = 0
    if bid['time'] < ask['time']:
        price = max(bid['price'],ask['price'])
    else:
        price = min(ask['price'],bid['price'])

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
    
    execute = 'UPDATE ref_prices SET ref_price = {} WHERE security_id = {}'.format(price,temp['security_id'])
    c.execute(execute)

    return temp

# c.execute('DROP TABLE bids')
# c.execute('DROP TABLE asks')
# c.execute('DROP TABLE fills')
# c.execute('DROP TABLE users')
# c.execute('DROP TABLE markets')
# c.execute('DROP TABLE positions')
# c.execute('DROP TABLE ref_prices')
# c.execute('DROP TABLE settlement')

c.execute('''CREATE TABLE bids
             (security_id, user_id, volume, price, time)''')

c.execute('''CREATE TABLE asks
             (security_id, user_id, volume, price, time)''')

c.execute('''CREATE TABLE fills
             (bid_id, ask_id, volume, price, time, bid_time, ask_time, security_id)''')

c.execute('''CREATE TABLE users
             (user_id, username, pin, cash)''')

c.execute('''CREATE TABLE markets
            (security_id, market_name, market_descriptor, create_time, end_time,best_bid_volume,best_bid,best_ask,best_ask_volume,last_traded)''')

c.execute('''CREATE TABLE positions
             (security_id, user_id, position)''')

c.execute('''CREATE TABLE ref_prices
             (security_id, ref_price)''')

c.execute('''CREATE TABLE settlement
             (security_id, settle, in_settle)''')

def validate_pin(user, pin):
    
    users = pd.read_sql_query('''SELECT * FROM users''', conn)

    if user not in list(users.username):
        return False
    else:
        if users[users.username == user].pin.iloc[0] == pin:
            return True
        else:
            return False

# # Create new users

def create_user(user, pin):
    users = pd.read_sql_query('''SELECT * FROM users''', conn)
    if user in list(pd.read_sql_query('''SELECT * FROM users''', conn).username):
        return 'Username taken'
    else:
        if len(users) > 0:
            user_id = int(users.user_id.max()) + 1
        else:
            user_id = 0
        exec_string = 'INSERT INTO users (user_id, username, pin, cash) values (?, ?, ?, ?)'
        c.execute(exec_string,
            (user_id, user, pin, 0))

create_user('Michael', 'testing')
create_user('John', 'testing')

class Users(Resource):
  def post(self):
    parser = reqparse.RequestParser()
    parser.add_argument("user")
    parser.add_argument("pin")
    args = parser.parse_args()
    create_user(args["user"], args["pin"])
    return jsonify(pd.read_sql_query('''SELECT * FROM users WHERE username = "{}"'''.format(args["user"]), conn).to_dict("records"))
  def put(self):
    parser = reqparse.RequestParser()
    parser.add_argument("user")
    parser.add_argument("pin")
    args = parser.parse_args()
    return validate_pin(args["user"], args["pin"])

class User(Resource):
  def put(self, user):
    parser = reqparse.RequestParser()
    parser.add_argument("pin")
    args = parser.parse_args()
    return get_user_info(user, args["pin"])

# # Creating new markets

def create_market(market_name,market_descriptor, end_time):
    markets = pd.read_sql_query('''SELECT * FROM markets''', conn)
    if len(markets) > 0:
        security_id = int(markets.security_id.max()) + 1
    else:
        security_id = 0
    
    if len(markets.loc[markets.market_name == market_name]) > 0:
        return 'Market already exists'
    else: 
        exec_string = 'INSERT INTO markets (security_id, market_name, market_descriptor, create_time, end_time, best_bid_volume, best_bid, best_ask, best_ask_volume,last_traded) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        create_time = datetime.datetime.now()
        c.execute(exec_string,
            (security_id, market_name, market_descriptor, create_time, end_time, None, None, None, None, None))

        exec_string = 'INSERT INTO settlement (security_id,settle,in_settle) values ({},NULL,NULL)'.format(security_id)
        c.execute(exec_string)
        
        exec_string = 'INSERT INTO ref_prices (security_id,ref_price) values ({},NULL)'.format(security_id)
        c.execute(exec_string)

def update_bids_asks():
    for index, row in pd.read_sql_query('''SELECT * FROM markets''', conn).iterrows():
        
        bids = pd.read_sql_query('''SELECT * FROM bids''', conn)
        asks = pd.read_sql_query('''SELECT * FROM asks''', conn)
        bids = bids[bids.security_id == row.security_id]
        asks = asks[asks.security_id == row.security_id]
        
        if len(bids) > 0:
            
            bids = bids[bids.security_id == row.security_id]
            highest_bid = bids.sort_values('price', ascending = False).iloc[0].price
            highest_bid_volume = int(bids[bids.price == highest_bid].volume.sum())
            highest_bid = float(highest_bid)
            
        else:
            
            highest_bid = None
            highest_bid_volume = None
            
        if len(asks) > 0:
            
            asks = asks[asks.security_id == row.security_id]
            lowest_ask = asks.sort_values('price', ascending = True).iloc[0].price
            lowest_ask_volume = int(asks[asks.price == lowest_ask].volume.sum())
            lowest_ask = float(lowest_ask)
        
        else:
            
            lowest_ask = None
            lowest_ask_volume = None
            
        ref_prices = pd.read_sql_query('''SELECT * FROM ref_prices''', conn)
        ref_prices = ref_prices[ref_prices.security_id == row.security_id]
        
        if len(ref_prices) > 0 and ref_prices.iloc[0].ref_price is not None:
            price = float(ref_prices.iloc[0].ref_price)
        else:
            price = None
            
        exec_string = 'UPDATE markets SET best_bid_volume = ?, best_bid = ?, best_ask = ?, best_ask_volume = ?, last_traded = ? WHERE security_id = ?'
        c.execute(exec_string,(highest_bid_volume,highest_bid,lowest_ask,lowest_ask_volume,price,row.security_id))

class Markets(Resource):
  def get(self):
    df = pd.read_sql_query('''SELECT * FROM markets''', conn)
    df = df.where(pd.notnull(df), None)
    return jsonify(df.to_dict("records"))
  def post(self):
    parser = reqparse.RequestParser()
    parser.add_argument("name")
    parser.add_argument("description")
    parser.add_argument("end")
    args = parser.parse_args()
    create_market(args["name"], args["description"], args["end"])
    df = pd.read_sql_query('''SELECT * FROM markets''', conn)
    df = df.where(pd.notnull(df), None)
    return jsonify(df.to_dict("records"))

def close_markets():
    for index, row in pd.read_sql_query('''SELECT * FROM markets''', conn).iterrows():
        if datetime.datetime.now() >= datetime.datetime.strptime(row.end_time,'%Y-%m-%dT%H:%M'):
            execute = 'UPDATE settlement SET in_settle = 1 WHERE security_id = {}'.format(row.security_id)
            c.execute(execute)
            execute = 'DELETE FROM bids WHERE security_id = {}'.format(row.security_id)
            c.execute(execute)
            execute = 'DELETE FROM asks WHERE security_id = {}'.format(row.security_id)
            c.execute(execute)

def delete_exposure(security, user, pin):
    close_markets()
    users = pd.read_sql_query('''SELECT * FROM users''', conn)
    
    if user not in list(users.username):
        return 'User does not exist'
    else:
        user_id = int(users[users.username == user]['user_id'].iloc[0])

    if security in pd.read_sql_query('''SELECT * FROM settlement WHERE in_settle = 1''', conn).security_id:
        return 'Market is closed'
    
    if users[users.username == user].pin.iloc[0] == pin:

        execute = 'DELETE FROM bids WHERE security_id = {} AND user_id = {}'.format(security,user_id)
        c.execute(execute)

        execute = 'DELETE FROM asks WHERE security_id = {} AND user_id = {}'.format(security,user_id)
        c.execute(execute)
    
        order_flow()
        update_positions()
        update_bids_asks()
        return 'Deleted exposure'
    
    else:
        return 'Incorrect Pin'

class Market(Resource):
  def get(self, security_id):
    return jsonify(pd.read_sql_query('''SELECT * FROM markets WHERE security_id = {}'''
      .format(security_id), conn).to_dict("records")[0])
  def put(self, security_id):
    parser = reqparse.RequestParser()
    parser.add_argument("user", type=str)
    parser.add_argument("pin", type=str)
    args = parser.parse_args()
    return delete_exposure(security_id, args["user"], args["pin"])

# # Create bid

def create_bid(security, user, pin, volume, price):
    close_markets()
    users = pd.read_sql_query('''SELECT * FROM users''', conn)
    
    if user not in list(users.username):
        return 'User does not exist'
    else:
        user_id = users[users.username == user]['user_id'].iloc[0]
    
    if security not in pd.read_sql_query('''SELECT * FROM markets''', conn).security_id:
        return 'Security does not exist'
    
    if user_id in list(pd.read_sql_query('''SELECT * FROM asks''', conn)['user_id']):
        asks = pd.read_sql_query('''SELECT * FROM asks''', conn)
        if float(price) >= float(asks.loc[asks.user_id == user_id].price.min()):
            return 'Invalid Order - crossing own ask'
    
    if security in pd.read_sql_query('''SELECT * FROM settlement WHERE in_settle = 1''', conn).security_id:
        return 'Market is closed'
    
    if users[users.username == user].pin.iloc[0] == pin:
    
        exec_string = 'INSERT INTO bids (security_id, user_id, volume, price, time) values (?, ?, ?, ?, ?)'
        time = datetime.datetime.now()
        c.execute(exec_string,
            (security, int(user_id), volume, price, time))

        order_flow()
        update_positions()
        update_bids_asks()
        return jsonify(pd.read_sql_query('''SELECT * FROM bids WHERE security_id = {}'''
          .format(security), conn).to_dict("records"))

    else:
        return 'Incorrect Pin'

class Bids(Resource):
  def get(self):
    return jsonify(pd.read_sql_query('''SELECT * FROM bids''', conn).to_dict("records"))
  def post(self):
    parser = reqparse.RequestParser()
    parser.add_argument("security", type=int)
    parser.add_argument("user", type=str)
    parser.add_argument("volume", type=int)
    parser.add_argument("price", type=float)
    parser.add_argument("pin", type=str)
    args = parser.parse_args()
    return create_bid(args["security"], args["user"], args["pin"], args["volume"], args["price"])


# # Create Ask

def create_ask(security, user, pin, volume, price):
    close_markets()
    users = pd.read_sql_query('''SELECT * FROM users''', conn)
    
    if user not in list(users.username):
        return 'User does not exist'
    else:
        user_id = users[users.username == user]['user_id'].iloc[0]
    
    if security not in pd.read_sql_query('''SELECT * FROM markets''', conn).security_id:
        return 'Security does not exist'
    
    if user_id in list(pd.read_sql_query('''SELECT * FROM bids''', conn)["user_id"]):
        bids = pd.read_sql_query('''SELECT * FROM bids''', conn)
        if float(price) <= float(bids.loc[bids.user_id == user_id].price.max()):
            return 'Invalid Order - crossing own bid'
        
    if security in pd.read_sql_query('''SELECT * FROM settlement WHERE in_settle = 1''', conn).security_id:
        return 'Market is closed'
    
    if users[users.username == user].pin.iloc[0] == pin:
    
        exec_string = 'INSERT INTO asks (security_id, user_id, volume, price, time) values (?, ?, ?, ?, ?)'
        time = datetime.datetime.now()
        c.execute(exec_string,
            (security, int(user_id), volume, price, time))

        order_flow()
        update_positions()
        update_bids_asks()
        return jsonify(pd.read_sql_query('''SELECT * FROM asks WHERE security_id = {}'''
          .format(security), conn).to_dict("records"))
    
    else:
        return 'Incorrect Pin'

class Asks(Resource):
  def get(self):
    return jsonify(pd.read_sql_query('''SELECT * FROM bids''', conn).to_dict("records"))
  def post(self):
    parser = reqparse.RequestParser()
    parser.add_argument("security", type=int)
    parser.add_argument("user", type=str)
    parser.add_argument("volume", type=int)
    parser.add_argument("price", type=float)
    parser.add_argument("pin", type=str)
    args = parser.parse_args()
    return create_ask(args["security"], args["user"], args["pin"], args["volume"], args["price"])

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

def ret_leaderboard():
    positions_df = pd.read_sql_query('''SELECT * FROM positions''', conn)
    ref_prices_df = pd.read_sql_query('''SELECT * FROM ref_prices''', conn)
    merged = positions_df.merge(ref_prices_df, on = ['security_id'])
    merged['position_worth'] = merged.position * merged.ref_price

    temp_list = []

    for index, row in pd.read_sql_query('''SELECT * FROM users''', conn).iterrows():

        temp = {}
        temp['username'] = row.username
        temp['pnl'] = row.cash + merged[merged.user_id == row.user_id].position_worth.sum()
        temp_list.append(temp)

    return pd.DataFrame(temp_list)[['username','pnl']].sort_values('pnl',ascending = False)

def get_user_info(user, pin):
    
    ret_dict = {}
    
    fills_df = pd.read_sql_query('''SELECT * FROM fills''', conn)
    
    users = pd.read_sql_query('''SELECT * FROM users''', conn)

    if user not in list(users.username):
        return 'User does not exist'
    else:
        user_id = int(users[users.username == user]['user_id'].iloc[0])

    if users[users.username == user].pin.iloc[0] == pin:
    
        user_df = pd.read_sql_query('''SELECT * FROM users WHERE user_id = {}'''.format(user_id), conn)
        
        ret_dict['user'] = user_df.iloc[0].username
        ret_dict['cash'] = float(user_df.iloc[0].cash)
        

        positions = pd.read_sql_query('''SELECT * 
                                         FROM positions 
                                         JOIN markets 
                                         ON positions.security_id = markets.security_id
                                         WHERE user_id = {}'''.format(user_id), conn)

        positions = positions.loc[:,~positions.columns.duplicated()]    
        
        list_of_markets = []
        
        for index, row in positions.iterrows():
            list_of_markets.append(row.market_name + ': ' + str(row.position) + ' contracts. Last traded price: ' + str(fills_df[fills_df.security_id == row.security_id].sort_values('time', ascending = False).iloc[0].price))
        
        ret_dict['list_of_positions'] = list_of_markets
        
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
        
        list_of_bids = []
        list_of_asks = []

        if len(bids) == 0:
            list_of_bids.append('No outstanding bids')
        else:
            for index, row in bids.iterrows():
                list_of_bids.append(row.market_name + ': ' + str(row.volume) + ' contracts for $' + str(row.price) + ' placed at: ' + str(row.time))

        if len(asks) == 0:
            list_of_asks.append('No outstanding asks')
        else:
            for index, row in asks.iterrows():
                list_of_asks.append(row.market_name + ': ' + str(row.volume) + ' contracts for $' + str(row.price) + ' placed at: ' + str(row.time))

        leaderboard = ret_leaderboard()
                
        ret_dict['list_of_bids'] = list_of_bids 
        ret_dict['list_of_asks'] = list_of_asks
        ret_dict['pnl'] = float(leaderboard[leaderboard.username == user].iloc[0].pnl)
                
        return ret_dict
    
    else:
        return 'Incorrect Pin'

# Flask routes
api.add_resource(Users, '/users')
api.add_resource(User, '/users/<user>')
api.add_resource(Markets, '/markets')
api.add_resource(MarketsList, '/markets/list')
api.add_resource(Market, '/markets/<security_id>')
api.add_resource(Bids, '/bids')
api.add_resource(Asks, '/asks')
api.add_resource(Positions, '/positions')
api.add_resource(Fills, '/fills')

app.run(port='3000')