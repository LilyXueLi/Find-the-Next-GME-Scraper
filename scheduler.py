from datetime import datetime
from dotenv import load_dotenv
import os
import praw
from pymongo import MongoClient
import yfinance as yf

# Number of the most discussed stocks
TOTAL_NUMBER_TO_PRESENT = 10

# Number of the most discussed stocks we save
# This should be greater than 10, in case Yahoo Finance is not able to provide information for certain stocks
TOTAL_NUMBER_OF_STOCKS = 20

# A list of abbreviations that generally do not represent tickers.
TICKER_BLACKLIST = ['A', 'ALL', 'ARE', 'AT', 'CEO', 'DD', 'EDIT', 'EV', 'FOR', 'ON', 'ONE', 'OR', 'RH', 'VERY']

# Information we look for in Yahoo Finance
YF_INFO_FIELDS = ['industry', 'previousClose', 'fiftyDayAverage', 'averageDailyVolume10Day']


# Reads, parses and converts the ticker files into dictionary
def build_dictionary(file_name, ticker_dict):
    with open(file_name, 'r') as file:
        for line in file:
            line_list = line.strip().split('\t')
            if len(line_list) < 2:
                continue
            symbol = line_list[0]
            name = line_list[1]
            if symbol not in ticker_dict:
                ticker_dict[symbol] = name


# Counts the tickers appeared in the titles and/or contents of the hot posts
# Returns a dictionary with top 20 counts in non-increasing order
def count_tickers(reddit, ticker_dict):
    ticker_count = {}
    hot_posts = reddit.subreddit('wallstreetbets').hot(limit=500)

    for post in hot_posts:
        word_in_title = post.title.split()
        add_to_dict(word_in_title, ticker_dict, ticker_count)
        word_in_content = post.selftext.split()
        add_to_dict(word_in_content, ticker_dict, ticker_count)

    sorted_ticker_count = dict(
        sorted(ticker_count.items(), key=lambda item: item[1], reverse=True)[:TOTAL_NUMBER_OF_STOCKS])

    return sorted_ticker_count


# Helper function to aggregate the count of each ticker in ticker_count
# Filters out tickers in the blacklist
def add_to_dict(word_in_post, ticker_dict, ticker_count):
    for word in word_in_post:
        clean_word = word.strip()
        if clean_word in TICKER_BLACKLIST:
            continue
        if clean_word in ticker_dict:
            ticker_count[clean_word] = ticker_count.get(clean_word, 0) + 1


# Helper function to determine if Yahoo Finance can provide the relevant information
# If not, we skip this stock
def found_all_fields(stock_info):
    for field in YF_INFO_FIELDS:
        if field not in stock_info.keys():
            return False

    return True


# Helper function to get the most recent Top 10 stock record from the database
# Returns a list of ticker symbols in non-decreasing order
def get_prev_stock_list(collection, now):
    prev_entry = collection.find({'timeStamp': {'$lte': now}}).sort([('timeStamp', -1)]).limit(1)

    prev_time_stamp = prev_entry[0].get('timeStamp')
    prev_stock_obj_list = collection.find({'timeStamp': prev_time_stamp})

    prev_stock_list = []
    for stock_obj in prev_stock_obj_list:
        prev_stock_list.append(stock_obj.get('ticker'))

    return prev_stock_list


# Helper function to calculate ranking changes for the current Top 10 stocks
# returns a tuple of whether this stock is new on the list and the ranking change (if applicable)
def get_ranking_change(prev_stock_list, output_ticker, to_DB):
    is_new = False
    if output_ticker not in prev_stock_list:
        is_new = True

    ranking_change = 0
    if not is_new:
        ranking_change = len(to_DB) - prev_stock_list.index(output_ticker)

    return is_new, ranking_change


# Packages the Top 10 tickers and related financial information into a list of dictionaries
# also inserts this information into the database with time stamps
def insert_to_db(sorted_ticker_dict, ticker_dict):
    client = MongoClient(os.environ['DB_URL'])
    collection = client['findMyGME']['stocks']

    now = datetime.now()

    to_DB = []

    sorted_ticker_list = list(sorted_ticker_dict.keys())

    prev_stock_list = get_prev_stock_list(collection, now)

    for i in range(TOTAL_NUMBER_OF_STOCKS):
        output_ticker = sorted_ticker_list[i]
        output_count = sorted_ticker_dict[output_ticker]
        stock_info = yf.Ticker(output_ticker).info

        if not found_all_fields(stock_info):
            continue

        ranking_change = get_ranking_change(prev_stock_list, output_ticker, to_DB)

        entry = {
            'rank': len(to_DB) + 1,
            'ticker': output_ticker,
            'name': ticker_dict[output_ticker],
            'industry': stock_info.get('industry', ''),
            'count': output_count,
            'previousClose': stock_info.get('previousClose', ''),
            'fiftyDayAverage': stock_info.get('fiftyDayAverage', ''),
            'averageDailyVolume10Day': stock_info.get('averageDailyVolume10Day', ''),
            'timeStamp': now,
            'newOccur': ranking_change[0],
            'rankingChange': ranking_change[1]
        }

        to_DB.append(entry)

        if len(to_DB) == TOTAL_NUMBER_TO_PRESENT:
            break

    # print(to_DB)
    collection.insert_many(to_DB)


def main():
    # Loads environment variables
    load_dotenv()

    # Creates an instance of the reddit object
    reddit = praw.Reddit(client_id=os.environ['client_id'], client_secret=os.environ['client_secret'],
                         user_agent=os.environ['user_agent'])

    # Creates a combined NASDAQ and NYSE stock ticker dictionary
    ticker_dict = {}
    build_dictionary('tickers/NASDAQ.txt', ticker_dict)
    build_dictionary('tickers/NYSE.txt', ticker_dict)

    # Counts the appearances of tickers in the top500 hot posts in WSB
    # Filters out the abbreviations that generally do not represent stock tickers
    # Also cuts down the size of the dictionary to 20
    sorted_ticker_dict = count_tickers(reddit, ticker_dict)

    # Inserts Top 10 tickers and the related financial information into the database
    insert_to_db(sorted_ticker_dict, ticker_dict)


if __name__ == "__main__":
    main()
