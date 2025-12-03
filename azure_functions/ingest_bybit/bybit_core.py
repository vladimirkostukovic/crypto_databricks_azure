import logging
import requests
import json
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient

SYMBOLS = ["BTCUSDT", "ETHUSDT", "LDOUSDT", "LINKUSDT"]
BASE_URL = "https://api.bybit.com"
RETRY_AFTER = 60
SAVE_TO_CLOUD = True

def get_klines(symbol: str, interval: str = "15", limit: int = 20):
    url = f"{BASE_URL}/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def get_tickers(symbol: str):
    url = f"{BASE_URL}/v5/market/tickers"
    params = {
        "category": "linear",
        "symbol": symbol
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def get_orderbook(symbol: str, limit: int = 20):
    url = f"{BASE_URL}/v5/market/orderbook"
    params = {
        "category": "linear",
        "symbol": symbol,
        "limit": limit
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def get_funding_rate(symbol: str, limit: int = 10):
    url = f"{BASE_URL}/v5/market/funding/history"
    params = {
        "category": "linear",
        "symbol": symbol,
        "limit": limit
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def get_open_interest(symbol: str, interval: str = "15min", limit: int = 10):
    url = f"{BASE_URL}/v5/market/open-interest"
    params = {
        "category": "linear",
        "symbol": symbol,
        "intervalTime": interval,
        "limit": limit
    }
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def fetch_all_data():
    all_data = []

    for symbol in SYMBOLS:
        try:
            record = {
                "symbol": symbol,
                "timestamp": datetime.utcnow().isoformat(),
                "klines": get_klines(symbol),
                "ticker": get_tickers(symbol),
                "orderbook": get_orderbook(symbol),
                "funding_rate": get_funding_rate(symbol),
                "open_interest": get_open_interest(symbol)
            }
            all_data.append(record)
            logging.info(f"Fetched {symbol}")
        except Exception as e:
            logging.error(f"Error fetching {symbol}: {e}")
            all_data.append({
                "symbol": symbol,
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            })

    return all_data

def save_to_landing(data):
    cs = os.environ["STORAGE_CONNECTION_STRING"]
    blob_service = BlobServiceClient.from_connection_string(cs)
    container = blob_service.get_container_client("landing-dev")

    now = datetime.utcnow()
    date_folder = now.strftime('%Y-%m-%d')
    hour_folder = now.strftime('%H')
    filename = now.strftime('%Y-%m-%d_%H-%M-%S') + ".json"

    blob_name = f"bybit/{date_folder}/{hour_folder}/{filename}"
    container.get_blob_client(blob_name).upload_blob(json.dumps(data, indent=2), overwrite=True)

    logging.info(f"Saved to {blob_name}")
    return blob_name

def run():
    data = fetch_all_data()
    save_to_landing(data)