import azure.functions as func
import logging
import requests
import json
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# configuration
SYMBOLS = ["BTCUSDT", "ETHUSDT", "LDOUSDT", "LINKUSDT"]
BASE_URL = "https://fapi.binance.com"
RETRY_AFTER = 51
SAVE_TO_CLOUD = True  # True = to Azure, False =  JSON in browser

# Candles
def get_klines(symbol: str, interval: str = "15m", limit: int = 20) -> dict:
    url = f"{BASE_URL}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

# Prices
def get_ticker_price(symbol: str) -> dict:
    url = f"{BASE_URL}/fapi/v1/ticker/price"
    params = {"symbol": symbol}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

# Order book
def get_depth(symbol: str, limit: int = 20) -> dict:
    url = f"{BASE_URL}/fapi/v1/depth"
    params = {"symbol": symbol, "limit": limit}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

# Funding rate
def get_funding_rate(symbol: str, limit: int = 10) -> dict:
    url = f"{BASE_URL}/fapi/v1/fundingRate"
    params = {"symbol": symbol, "limit": limit}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

# open interest
def get_open_interest(symbol: str) -> dict:
    url = f"{BASE_URL}/fapi/v1/openInterest"
    params = {"symbol": symbol}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

# all data for all coins
def fetch_all_data() -> list:
    all_data = []

    for symbol in SYMBOLS:
        try:
            data = {
                "symbol": symbol,
                "timestamp": datetime.utcnow().isoformat(),
                "klines": get_klines(symbol),
                "ticker": get_ticker_price(symbol),
                "depth": get_depth(symbol),
                "funding_rate": get_funding_rate(symbol),
                "open_interest": get_open_interest(symbol)
            }
            all_data.append(data)
            logging.info(f"Fetched data for {symbol}")
        except Exception as e:
            logging.error(f"Error fetching {symbol}: {e}")
            all_data.append({
                "symbol": symbol,
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            })

    return all_data

# json to landing container
def save_to_landing(data: list) -> str:
    connection_string = os.environ["STORAGE_CONNECTION_STRING"]
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container = blob_service.get_container_client("landing-dev")

    now = datetime.utcnow()
    date_folder = now.strftime('%Y-%m-%d')
    hour_folder = now.strftime('%H')
    filename = now.strftime('%Y-%m-%d_%H-%M-%S')

    blob_name = f"binance/{date_folder}/{hour_folder}/{filename}.json"

    blob_client = container.get_blob_client(blob_name)
    blob_client.upload_blob(json.dumps(data, indent=2), overwrite=True)

    logging.info(f"Saved to {blob_name}")
    return blob_name


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Binance ingestion triggered")

    try:
        data = fetch_all_data()

        if SAVE_TO_CLOUD:
            blob_name = save_to_landing(data)
            result = {
                "status": "success",
                "mode": "cloud",
                "symbols": SYMBOLS,
                "blob": blob_name,
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            result = {
                "status": "success",
                "mode": "test",
                "symbols": SYMBOLS,
                "data": data,
                "timestamp": datetime.utcnow().isoformat()
            }

        return func.HttpResponse(
            json.dumps(result, indent=2),
            mimetype="application/json",
            status_code=200
        )

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            logging.warning(f"Rate limited, retry after {RETRY_AFTER}s")
            return func.HttpResponse(
                json.dumps({"error": "rate_limited", "retry_after": RETRY_AFTER}),
                mimetype="application/json",
                status_code=429,
                headers={"Retry-After": str(RETRY_AFTER)}
            )
        raise
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )