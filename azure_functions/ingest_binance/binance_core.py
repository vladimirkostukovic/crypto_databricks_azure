import logging
import requests
import json
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from azure.storage.blob import BlobServiceClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SYMBOLS = ["BTCUSDT", "ETHUSDT", "LDOUSDT", "LINKUSDT"]
BASE_URL = "https://fapi.binance.com"
CONTAINER_NAME = "landing-dev"
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10
SAVE_TO_CLOUD = True

# Interval limits configuration (for funding_rate)
INTERVAL_LIMITS = {
    "15m": 1500,
    "1h": 1000,
    "4h": 100,
    "1d": 100
}

# Interval duration in minutes (for endTime calculation)
INTERVAL_MINUTES = {
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440
}


# Create session with retry logic for HTTP requests
def get_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = get_session()


# Generic API call with retry logic, rate limit handling and timeout
def api_call(url: str, params: dict, retry_count: int = 0, max_retries: int = MAX_RETRIES) -> Optional[Any]:
    try:
        r = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)

        # Rate limit monitoring
        if 'X-MBX-USED-WEIGHT-1M' in r.headers:
            weight = int(r.headers['X-MBX-USED-WEIGHT-1M'])
            if weight > 1000:
                logging.warning(f"Rate limit close: {weight}/1200")
                time.sleep(2)

        # Handle 429 Rate Limit
        if r.status_code == 429:
            retry_after = int(r.headers.get('Retry-After', 60))
            logging.warning(f"Rate limited, waiting {retry_after}s")
            time.sleep(retry_after)
            if retry_count < max_retries:
                return api_call(url, params, retry_count + 1, max_retries)
            raise Exception("Max retries exceeded on 429")

        # Handle 418 IP Ban
        if r.status_code == 418:
            logging.error("IP banned by Binance")
            raise Exception("IP banned - please check your IP whitelist")

        r.raise_for_status()
        return r.json()

    except requests.exceptions.Timeout:
        logging.warning(f"Timeout, retry {retry_count + 1}/{max_retries}")
        if retry_count < max_retries:
            time.sleep(2 ** retry_count)
            return api_call(url, params, retry_count + 1, max_retries)
        raise

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        if retry_count < max_retries:
            time.sleep(2 ** retry_count)
            return api_call(url, params, retry_count + 1, max_retries)
        raise


# Fetch last closed kline only
def get_klines(symbol: str, interval: str = "15m") -> List:
    url = f"{BASE_URL}/fapi/v1/klines"

    # Calculate endTime to ensure closed candle only
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    interval_ms = INTERVAL_MINUTES.get(interval, 15) * 60 * 1000
    end_time = now_ms - interval_ms

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": 1,  # Only last closed candle
        "endTime": end_time
    }

    return api_call(url, params)


# Fetch current futures price
def get_ticker_price(symbol: str) -> Dict:
    url = f"{BASE_URL}/fapi/v1/ticker/price"
    return api_call(url, {"symbol": symbol})


# Fetch orderbook depth (limit: 5, 10, 20, 50, 100, 500, 1000)
def get_depth(symbol: str, limit: int = 20) -> Dict:
    url = f"{BASE_URL}/fapi/v1/depth"
    return api_call(url, {"symbol": symbol, "limit": min(limit, 1000)})


# Fetch funding rate history (max 1000)
def get_funding_rate(symbol: str, limit: int = 10) -> List:
    url = f"{BASE_URL}/fapi/v1/fundingRate"
    return api_call(url, {"symbol": symbol, "limit": min(limit, 1000)})


# Fetch current open interest
def get_open_interest(symbol: str) -> Dict:
    url = f"{BASE_URL}/fapi/v1/openInterest"
    return api_call(url, {"symbol": symbol})


# Validate fetched data for completeness
def validate_data(data: Dict, symbol: str) -> bool:
    required_fields = ["symbol", "timestamp", "intervals_included", "klines", "ticker"]

    for field in required_fields:
        if field not in data:
            logging.error(f"Missing field '{field}' for {symbol}")
            return False

    if not data["klines"]:
        logging.error(f"No klines data for {symbol}")
        return False

    return True


# Fetch data for one symbol with specified intervals
def fetch_symbol_data(symbol: str, intervals_to_fetch: List[str]) -> Optional[Dict]:
    try:
        now = datetime.utcnow()

        if not intervals_to_fetch:
            logging.debug(f"⊘ {symbol} - no intervals specified")
            return None

        # Fetch klines for specified intervals
        klines_data = {}
        for interval in intervals_to_fetch:
            klines_data[interval] = get_klines(symbol, interval)
            logging.debug(f"  Fetched {interval} for {symbol}")

        # Fetch market data
        data = {
            "symbol": symbol,
            "timestamp": now.isoformat(),
            "intervals_included": intervals_to_fetch,
            "klines": klines_data,
            "ticker": get_ticker_price(symbol),
            "depth": get_depth(symbol, 20),
            "funding_rate": get_funding_rate(symbol, 10),
            "open_interest": get_open_interest(symbol)
        }

        # Validate data
        if not validate_data(data, symbol):
            raise ValueError(f"Data validation failed for {symbol}")

        logging.info(f"✓ {symbol} - {len(intervals_to_fetch)} intervals")
        time.sleep(0.2)  # Rate limit protection
        return data

    except Exception as e:
        logging.error(f"✗ {symbol}: {e}", exc_info=True)
        return {
            "symbol": symbol,
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e),
            "error_type": type(e).__name__
        }


# Save combined data to single JSON file in Azure Storage
def save_combined_data(data: Dict) -> Optional[str]:
    if not SAVE_TO_CLOUD:
        return save_combined_local(data)

    storage_connection_string = os.environ.get("STORAGE_CONNECTION_STRING")

    if not storage_connection_string:
        logging.error("STORAGE_CONNECTION_STRING not set, saving locally")
        return save_combined_local(data)

    try:
        svc = BlobServiceClient.from_connection_string(storage_connection_string)
        container = svc.get_container_client(CONTAINER_NAME)

        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")

        blob_path = f"binance/date={date_str}/hour={hour_str}/{timestamp_str}.json"
        blob_client = container.get_blob_client(blob_path)

        for attempt in range(MAX_RETRIES):
            try:
                blob_client.upload_blob(
                    json.dumps(data, indent=2),
                    overwrite=True,
                    timeout=30
                )
                logging.info(f"✓ Saved {blob_path}")
                return blob_path
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    logging.warning(f"Upload retry {attempt + 1}/{MAX_RETRIES}: {e}")
                    time.sleep(2 ** attempt)
                else:
                    raise

    except Exception as e:
        logging.error(f"Storage failed: {e}")
        return save_combined_local(data)


# Fallback: save combined data locally
def save_combined_local(data: Dict) -> str:
    now = datetime.utcnow()
    local_dir = "/tmp/binance"
    os.makedirs(local_dir, exist_ok=True)
    local_path = f"{local_dir}/{now.strftime('%Y%m%d_%H%M%S')}.json"

    with open(local_path, 'w') as f:
        json.dump(data, f, indent=2)

    logging.info(f"Saved locally: {local_path}")
    return local_path


# Main entry point for ingestion with specified intervals
def run(intervals: List[str] = None) -> Dict:
    now = datetime.utcnow()
    logging.info(f"Starting Binance ingestion at {now.strftime('%Y-%m-%d %H:%M')} UTC")

    if not intervals:
        intervals = ["15m"]  # Default

    logging.info(f"Fetching intervals: {', '.join(intervals)}")
    start_time = time.time()

    try:
        all_symbols_data = []
        success_count = 0
        failed_count = 0
        errors = []

        for symbol in SYMBOLS:
            logging.info(f"Processing {symbol}...")
            symbol_data = fetch_symbol_data(symbol, intervals)

            if symbol_data is None:
                continue

            if "error" not in symbol_data:
                all_symbols_data.append(symbol_data)
                success_count += 1
            else:
                failed_count += 1
                errors.append({
                    "symbol": symbol,
                    "error": symbol_data.get("error"),
                    "error_type": symbol_data.get("error_type")
                })

        # Create one combined file with all symbols
        combined_data = {
            "timestamp": now.isoformat(),
            "intervals": intervals,
            "symbols": all_symbols_data
        }

        # Save single file
        saved_path = save_combined_data(combined_data)

        duration = time.time() - start_time
        logging.info(f"Completed in {duration:.2f}s")

        result = {
            "status": "success" if failed_count == 0 else "partial_success",
            "intervals_fetched": intervals,
            "symbols_processed": success_count,
            "symbols_failed": failed_count,
            "file_saved": saved_path,
            "duration_seconds": round(duration, 2),
            "timestamp": now.isoformat()
        }

        if errors:
            result["errors"] = errors

        return result

    except Exception as e:
        logging.error(f"Run failed: {e}", exc_info=True)
        return {
            "status": "failed",
            "error": str(e),
            "error_type": type(e).__name__,
            "timestamp": now.isoformat()
        }


# Health check endpoint
def health_check() -> Dict:
    try:
        # Test Binance API
        api_call(f"{BASE_URL}/fapi/v1/ping", {})

        # Test storage connection
        storage_connection_string = os.environ.get("STORAGE_CONNECTION_STRING")
        if storage_connection_string:
            svc = BlobServiceClient.from_connection_string(storage_connection_string)
            svc.get_container_client(CONTAINER_NAME).exists()

        return {
            "status": "healthy",
            "binance_api": "ok",
            "storage": "ok" if storage_connection_string else "not_configured",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }