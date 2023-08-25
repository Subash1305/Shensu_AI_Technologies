import yfinance as yf
from apscheduler.schedulers.blocking import BlockingScheduler
from pymongo import MongoClient
from datetime import datetime, time, timedelta

# MongoDB connection
client = MongoClient("mongodb://127.0.0.1:27017/")
db = client["stock_data"]
collection = db["icici_bank"]

# Function to fetch and store data
def fetch_and_store_data():
    now = datetime.now()
    if time(11, 15) <= now.time() <= time(14, 15):
        ticker = "ICICIBANK.NS"
        data = yf.download(ticker, interval="15m", end=datetime.now() - timedelta(minutes=15))
        if not data.empty:
            data = data.dropna()
            for index, row in data.iterrows():
                entry = {
                    "timestamp": index,
                    "open": row["Open"],
                    "high": row["High"],
                    "low": row["Low"],
                    "close": row["Close"],
                    "volume": row["Volume"]
                }
                collection.insert_one(entry)
            print(f"Data stored at {now}")
        else:
            print(f"No data available at {now}")

# Configure APScheduler
scheduler = BlockingScheduler()
scheduler.add_job(fetch_and_store_data, trigger="interval", minutes=15)
scheduler.start()
