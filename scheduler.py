from datetime import datetime
from dotenv import load_dotenv
import os
import praw
from pymongo import MongoClient
import yfinance as yf

# Number of most discussed stocks
TOTAL_NUMBER_OF_STOCK = 10

# A list of abbreviations that generally do not represent tickers.
TICKER_BLACKLIST = ["A", "DD", "FOR", "CEO", "ALL", "EV", "OR", "AT", "RH", "ONE", "ARE", "VERY", "ON", "EDIT"]


# Reads, parses and converts the ticker files into dictionary
def build_dictionary(file_name, ticker_dict):
    with open(file_name, 'r') as file:
        for line in file:
            line_list = line.strip().split("\t")
            if len(line_list) < 2:
                continue
            symbol = line_list[0]
            name = line_list[1]
            if symbol not in ticker_dict:
                ticker_dict[symbol] = name


# Count the tickers appeared in the titles and/or contents of the hot posts
# Returns a dictionary with top 10 counts in non-increasing order
def count_tickers(reddit, ticker_dict):
    ticker_count = {}
    hot_posts = reddit.subreddit("wallstreetbets").hot(limit=500)

    for post in hot_posts:
        word_in_title = post.title.split()
        add_to_dict(word_in_title, ticker_dict, ticker_count)
        word_in_content = post.selftext.split()
        add_to_dict(word_in_content, ticker_dict, ticker_count)

    sorted_ticker_count = dict(
        sorted(ticker_count.items(), key=lambda item: item[1], reverse=True)[:TOTAL_NUMBER_OF_STOCK])

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


# Packages the top10 tickers and related financial information into dictionaries
# Inserts this information into the database with time stamps
def insert_to_db(sorted_ticker_dict, ticker_dict):
    toDB = []

    sorted_ticker_list = list(sorted_ticker_dict.keys())

    now = datetime.now()

    for i in range(TOTAL_NUMBER_OF_STOCK):
        output_ticker = sorted_ticker_list[i]
        output_count = sorted_ticker_dict[output_ticker]
        stock_info = yf.Ticker(output_ticker).info
        entry = {
            "rank": i + 1,
            "ticker": output_ticker,
            "name": ticker_dict[output_ticker],
            "industry": stock_info["industry"],
            "count": output_count,
            "previousClose": stock_info["previousClose"],
            "fiftyDayAverage": stock_info["fiftyDayAverage"],
            "averageDailyVolume10Day": stock_info["averageDailyVolume10Day"],
            "timeStamp": now
        }
        toDB.append(entry)
    print(toDB)
    client = MongoClient(os.environ['DB_URL'])
    collection = client["findMyGME"]["stocks"]
    collection.insert_many(toDB)


def main():
    # Loads environment variables
    load_dotenv()

    # Creates an instance of the reddit object
    reddit = praw.Reddit(client_id=os.environ['client_id'], client_secret=os.environ['client_secret'],
                         user_agent=os.environ['user_agent'])

    # Creates a combined NASDAQ and NYSE stock ticker dictionary
    ticker_dict = {}
    build_dictionary("tickers/NASDAQ.txt", ticker_dict)
    build_dictionary("tickers/NYSE.txt", ticker_dict)

    # Counts the appearances of tickers in the top500 hot posts in WSB
    # Filters out the abbreviations that generally do not represent stock tickers
    # Also cuts down the size of the dictionary to 10
    sorted_ticker_dict = count_tickers(reddit, ticker_dict)

    # Inserts top10 tickers and the related financial information into the database
    insert_to_db(sorted_ticker_dict, ticker_dict)


if __name__ == "__main__":
    main()
