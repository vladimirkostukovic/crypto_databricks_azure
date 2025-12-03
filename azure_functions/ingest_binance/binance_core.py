import logging
import requests
import json
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient

SYMBOLS = ["BTCUSDT", "ETHUSDT", "LDOUSDT", "LINKUSDT"]
BASE_URL = "https://fapi.binance.com"
RETRY_AFTER = 51
SAVE_TO_CLOUD = True

def get_klines(symbol: str, interval: str = "15m", limit: int = 20):
    url = f"{BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def get_ticker_price(symbol: str):
    url = f"{BASE_URL}/fapi/v1/ticker/price"
    r = requests.get(url, params={"symbol": symbol}, timeout=10)
    r.raise_for_status()
    return r.json()

def get_depth(symbol: str, limit: int = 20):
    url = f"{BASE_URL}/fapi/v1/depth"
    r = requests.get(url, params={"symbol": symbol, "limit": limit}, timeout=10)
    r.raise_for_status()
    return r.json()

def get_funding_rate(symbol: str, limit: int = 10):
    url = f"{BASE_URL}/fapi/v1/fundingRate"
    r = requests.get(url, params={"symbol": symbol, "limit": limit}, timeout=10)
    r.raise_for_status()
    return r.json()

def get_open_interest(symbol: str):
    url = f"{BASE_URL}/fapi/v1/openInterest"
    r = requests.get(url, params={"symbol": symbol}, timeout=10)
    r.raise_for_status()
    return r.json()

def fetch_all_data():
    result = []
    for s in SYMBOLS:
        try:
            data = {
                "symbol": s,
                "timestamp": datetime.utcnow().isoformat(),
                "klines": get_klines(s),
                "ticker": get_ticker_price(s),
                "depth": get_depth(s),
                "funding_rate": get_funding_rate(s),
                "open_interest": get_open_interest(s)
            }
            result.append(data)
            logging.info(f"Fetched {s}")
        except Exception as e:
            logging.error(f"Failed {s}: {e}")
            result.append({"symbol": s, "error": str(e)})
    return result

def save_to_landing(data):
    cs = os.environ["STORAGE_CONNECTION_STRING"]
    svc = BlobServiceClient.from_connection_string(cs)
    container = svc.get_container_client("landing-dev")

    now = datetime.utcnow()
    d = now.strftime("%Y-%m-%d")
    h = now.strftime("%H")
    fname = now.strftime("%Y-%m-%d_%H-%M-%S") + ".json"

    blob = f"binance/{d}/{h}/{fname}"
    container.get_blob_client(blob).upload_blob(json.dumps(data, indent=2), overwrite=True)
    logging.info(f"Saved {blob}")
    return blob

def run():
    data = fetch_all_data()
    save_to_landing(data)