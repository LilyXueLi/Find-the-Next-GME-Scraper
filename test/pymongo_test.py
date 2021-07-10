from dotenv import load_dotenv
from pymongo import MongoClient
import os
from datetime import datetime

load_dotenv()

client = MongoClient(os.environ['DB_URL'])
collection = client['findMyGME']['stocks']

now = datetime.now()

prev_entry = collection.find({'timeStamp': {'$lte': now}}).sort([('timeStamp', -1)]).limit(1)

prev_time_stamp = prev_entry[0].get('timeStamp')
prev_stock_obj_list = collection.find({'timeStamp': prev_time_stamp})

print(prev_stock_obj_list)


# latest = collection.find().sort([('timeStamp', -1)]).limit(1)
# latest_time_stamp = latest[0].get('timeStamp')
#
# # print(latest_time_stamp)
#
# stockList = collection.find({'timeStamp': latest_time_stamp})
#
# # for stock in stockList:
# #     print(stock)
#
# delta = timedelta(days=1)
#
# print(latest_time_stamp - delta)
#
# prev = collection.find({'timeStamp': {'$lte': (latest_time_stamp - delta)}}).sort([('timeStamp', -1)]).limit(1)
#
# prev_time_stamp = prev[0].get('timeStamp')
#
# # print(prev_time_stamp)
#
# prev_stock_list = collection.find({'timeStamp': prev_time_stamp})
#
# my_list = []
#
# for stock in prev_stock_list:
#     my_list.append(stock['ticker'])
#
# print(my_list)
#
# def get_ranking_change(prev_stock_list, output_ticker, toDB):
#     is_new = False
#     if output_ticker not in prev_stock_list:
#         is_new: True
#
#     ranking_change = 0
#     if not is_new:
#         ranking_change = len(toDB) - prev_stock_list.index(output_ticker)
#
#     return is_new, ranking_change
#
# toDB = []
#
# result = get_ranking_change(my_list, 'GME', toDB)
#
# print(result[0], result[1])
#
#
#
# # print(my_list)
