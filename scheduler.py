import time
from datetime import datetime

import pymongo
from pymongo import MongoClient

import praw

import yfinance as yf
from dotenv import load_dotenv

import dotenv
import os


# Reads, parses and converts the ticker files into dictionary
def build_dictionary(file_name, ticker_dict):
    with open(file_name, 'r') as file:
        for line in file:
            line_list = line.strip().split("\t")
            if len(line_list) >= 2:
                symbol = line_list[0]
                name = line_list[1]
                if symbol not in ticker_dict:
                    ticker_dict[symbol] = name


# Count the tickers appeared in the titles and/or contents of the hot posts
# Returns a dictionary with counts in non-increasing order
def count_tickers(reddit, ticker_dict):
    ticker_count = {}
    hot_posts = reddit.subreddit("wallstreetbets").hot(limit=500)

    for post in hot_posts:
        word_in_title = post.title.split()
        add_to_dict(word_in_title, ticker_dict, ticker_count)
        word_in_content = post.selftext.split()
        add_to_dict(word_in_content, ticker_dict, ticker_count)

    sorted_ticker_count = dict(sorted(ticker_count.items(), key=lambda item: item[1], reverse=True))

    return sorted_ticker_count


# Helper function to aggregate the count of each ticker in ticker_count
def add_to_dict(word_in_post, ticker_dict, ticker_count):
    for word in word_in_post:
        clean_word = word.strip()
        if clean_word in ticker_dict:
            ticker_count[clean_word] = ticker_count.get(clean_word, 0) + 1


# Filters out the abbreviations that generally do not represent stock tickers
def filter_dictionary(sorted_ticker_count, black_list):
    clean_sorted_ticker_dict = {}

    for key, value in sorted_ticker_count.items():
        if key not in black_list:
            clean_sorted_ticker_dict[key] = value
        if len(clean_sorted_ticker_dict) == 10:
            break

    return clean_sorted_ticker_dict


# Packages the top10 tickers and related financial information into dictionaries
# and inserts them into the database with time stamps
def insert_to_db(sorted_ticker_list, filtered_sorted_ticker_dict, ticker_dict):
    toDB = []

    now = datetime.now()

    for i in range(10):
        output_ticker = sorted_ticker_list[i]
        output_count = filtered_sorted_ticker_dict[output_ticker]
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

    collection.insert_many(toDB)


def main():
    # Creates a combined NASDAQ and NYSE stock ticker dictionary
    ticker_dict = {}
    build_dictionary("Ticker Symbols/NASDAQ.txt", ticker_dict)
    build_dictionary("Ticker Symbols/NYSE.txt", ticker_dict)

    # Counts the appearances of tickers in the top500 hot posts in WSB
    sorted_ticker_count = count_tickers(reddit, ticker_dict)

    # Filters out the abbreviations that generally do not represent stock tickers
    # Also cuts down the size of the dictionary to 10
    filtered_sorted_ticker_dict = filter_dictionary(sorted_ticker_count, black_list)

    # Gets the list of top 10 tickers from the filtered dictionary
    sorted_ticker_list = list(filtered_sorted_ticker_dict.keys())

    # Inserts top10 tickers and the related financial information into the database
    insert_to_db(sorted_ticker_list, filtered_sorted_ticker_dict, ticker_dict)


if __name__ == "__main__":
    # Connects to database
    load_dotenv()
    client = MongoClient(os.environ['DB_URL'])
    collection = client["findMyGME"]["stocks"]

    # Creates an instance of the reddit object
    reddit = praw.Reddit(client_id=os.environ['client_id'], client_secret=os.environ['client_secret'],
                         user_agent=os.environ['user_agent'])

    # A list of abbreviations that generally do not represent tickers.
    black_list = ["A", "DD", "FOR", "CEO", "ALL", "EV", "OR", "AT", "RH", "ONE", "ARE", "VERY", "ON", "EDIT"]

    main()
