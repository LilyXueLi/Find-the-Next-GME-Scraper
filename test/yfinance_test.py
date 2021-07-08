import yfinance as yf

stock = yf.Ticker("GME")
print(stock)
print(stock.info)
print(f"industry: {stock.info['industry']}")
print(f"previous day closing price: {stock.info['previousClose']}")
print(f"50-day average price: {stock.info['fiftyDayAverage']}")
print(f"10-day average daily volume: {stock.info['averageDailyVolume10Day']}")


