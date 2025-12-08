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
BASE_URL = "https://api.bybit.com"
CONTAINER_NAME = "landing-dev"
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10
SAVE_TO_CLOUD = True

# Interval limits configuration (for funding_rate and open_interest)
INTERVAL_LIMITS = {
    "15": 1000,  # 15 minutes
    "60": 1000,  # 1 hour
    "240": 1000,  # 4 hours
    "D": 1000  # 1 day
}

# Bybit interval mapping
BYBIT_INTERVALS = {
    "15m": "15",
    "1h": "60",
    "4h": "240",
    "1d": "D"
}

# Interval duration in minutes (for endTime calculation)
INTERVAL_MINUTES = {
    "15": 15,
    "60": 60,
    "240": 240,
    "D": 1440
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

        # Bybit rate limit headers
        if 'X-Bapi-Limit-Status' in r.headers:
            limit_status = int(r.headers['X-Bapi-Limit-Status'])
            if limit_status < 10:
                logging.warning(f"Bybit rate limit low: {limit_status} remaining")
                time.sleep(1)

        # Handle 429 Rate Limit
        if r.status_code == 429:
            retry_after = int(r.headers.get('Retry-After', 60))
            logging.warning(f"Rate limited, waiting {retry_after}s")
            time.sleep(retry_after)
            if retry_count < max_retries:
                return api_call(url, params, retry_count + 1, max_retries)
            raise Exception("Max retries exceeded on 429")

        r.raise_for_status()

        # Bybit wraps response in retCode/retMsg
        response_data = r.json()
        if response_data.get("retCode") != 0:
            raise Exception(f"Bybit API error: {response_data.get('retMsg')}")

        return response_data

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
def get_klines(symbol: str, interval: str = "15") -> Dict:
    url = f"{BASE_URL}/v5/market/kline"

    # Calculate endTime to ensure closed candle only
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    interval_ms = INTERVAL_MINUTES.get(interval, 15) * 60 * 1000
    end_time = now_ms - interval_ms

    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": 1,  # Only last closed candle
        "end": end_time
    }

    return api_call(url, params)


# Fetch current futures ticker
def get_tickers(symbol: str) -> Dict:
    url = f"{BASE_URL}/v5/market/tickers"
    params = {
        "category": "linear",
        "symbol": symbol
    }
    return api_call(url, params)


# Fetch orderbook depth (limit: max 500)
def get_orderbook(symbol: str, limit: int = 20) -> Dict:
    url = f"{BASE_URL}/v5/market/orderbook"
    params = {
        "category": "linear",
        "symbol": symbol,
        "limit": min(limit, 500)
    }
    return api_call(url, params)


# Fetch funding rate history (max 200)
def get_funding_rate(symbol: str, limit: int = 10) -> Dict:
    url = f"{BASE_URL}/v5/market/funding/history"
    params = {
        "category": "linear",
        "symbol": symbol,
        "limit": min(limit, 200)
    }
    return api_call(url, params)


# Fetch open interest history (max 200)
def get_open_interest(symbol: str, interval: str = "15min", limit: int = 10) -> Dict:
    url = f"{BASE_URL}/v5/market/open-interest"
    params = {
        "category": "linear",
        "symbol": symbol,
        "intervalTime": interval,
        "limit": min(limit, 200)
    }
    return api_call(url, params)


# Validate fetched data for completeness
def validate_data(data: Dict, symbol: str) -> bool:
    required_fields = ["symbol", "timestamp", "interval", "klines", "ticker"]

    for field in required_fields:
        if field not in data:
            logging.error(f"Missing field '{field}' for {symbol}")
            return False

    if not data["klines"]:
        logging.error(f"No klines data for {symbol}")
        return False

    return True


# Fetch data for one symbol for ONE interval
def fetch_symbol_data(symbol: str, interval: str) -> Optional[Dict]:
    try:
        now = datetime.utcnow()
        bybit_interval = BYBIT_INTERVALS[interval]

        # Fetch klines for this interval
        klines_data = get_klines(symbol, bybit_interval)
        logging.debug(f"  Fetched {interval} for {symbol}")

        # Fetch market data
        data = {
            "symbol": symbol,
            "timestamp": now.isoformat(),
            "interval": interval,
            "klines": klines_data,
            "ticker": get_tickers(symbol),
            "orderbook": get_orderbook(symbol, 20),
            "funding_rate": get_funding_rate(symbol, 10),
            "open_interest": get_open_interest(symbol, "15min", 10)
        }

        # Validate data
        if not validate_data(data, symbol):
            raise ValueError(f"Data validation failed for {symbol}")

        logging.info(f"✓ {symbol} - {interval}")
        time.sleep(0.2)  # Rate limit protection
        return data

    except Exception as e:
        logging.error(f"✗ {symbol} [{interval}]: {e}", exc_info=True)
        return {
            "symbol": symbol,
            "timestamp": datetime.utcnow().isoformat(),
            "interval": interval,
            "error": str(e),
            "error_type": type(e).__name__
        }


# Save data for ONE interval to separate file
def save_interval_data(data: Dict, interval: str) -> Optional[str]:
    if not SAVE_TO_CLOUD:
        return save_interval_local(data, interval)

    cs = os.environ.get("STORAGE_CONNECTION_STRING")

    if not cs:
        logging.error("STORAGE_CONNECTION_STRING not set, saving locally")
        return save_interval_local(data, interval)

    try:
        svc = BlobServiceClient.from_connection_string(cs)
        container = svc.get_container_client(CONTAINER_NAME)

        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")

        # bybit/date=2025-12-08/hour=14/20251208_140315_15m.json
        blob_path = f"bybit/date={date_str}/hour={hour_str}/{timestamp_str}_{interval}.json"
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
        return save_interval_local(data, interval)


# Fallback: save interval data locally
def save_interval_local(data: Dict, interval: str) -> str:
    now = datetime.utcnow()
    local_dir = "/tmp/bybit"
    os.makedirs(local_dir, exist_ok=True)
    local_path = f"{local_dir}/{now.strftime('%Y%m%d_%H%M%S')}_{interval}.json"

    with open(local_path, 'w') as f:
        json.dump(data, f, indent=2)

    logging.info(f"Saved locally: {local_path}")
    return local_path


# Main entry point for ingestion with specified intervals
def run(intervals: List[str] = None) -> Dict:
    now = datetime.utcnow()
    logging.info(f"Starting Bybit ingestion at {now.strftime('%Y-%m-%d %H:%M')} UTC")

    if not intervals:
        intervals = ["15m"]  # Default

    logging.info(f"Fetching intervals: {', '.join(intervals)}")
    start_time = time.time()

    try:
        results_by_interval = {}
        total_success = 0
        total_failed = 0
        all_errors = []

        # Process each interval separately
        for interval in intervals:
            interval_symbols_data = []
            success_count = 0
            failed_count = 0

            for symbol in SYMBOLS:
                logging.info(f"Processing {symbol} [{interval}]...")
                symbol_data = fetch_symbol_data(symbol, interval)

                if symbol_data is None:
                    continue

                if "error" not in symbol_data:
                    interval_symbols_data.append(symbol_data)
                    success_count += 1
                else:
                    failed_count += 1
                    all_errors.append({
                        "symbol": symbol,
                        "interval": interval,
                        "error": symbol_data.get("error"),
                        "error_type": symbol_data.get("error_type")
                    })

            # Create separate file for this interval
            interval_data = {
                "timestamp": now.isoformat(),
                "interval": interval,
                "symbols": interval_symbols_data
            }

            # Save to separate file
            saved_path = save_interval_data(interval_data, interval)

            results_by_interval[interval] = {
                "symbols_processed": success_count,
                "symbols_failed": failed_count,
                "file_saved": saved_path
            }

            total_success += success_count
            total_failed += failed_count

        duration = time.time() - start_time
        logging.info(f"Completed in {duration:.2f}s")

        result = {
            "status": "success" if total_failed == 0 else "partial_success",
            "intervals_fetched": intervals,
            "total_symbols_processed": total_success,
            "total_symbols_failed": total_failed,
            "results_by_interval": results_by_interval,
            "duration_seconds": round(duration, 2),
            "timestamp": now.isoformat()
        }

        if all_errors:
            result["errors"] = all_errors

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
        # Test Bybit API
        api_call(f"{BASE_URL}/v5/market/time", {})

        # Test storage connection
        cs = os.environ.get("STORAGE_CONNECTION_STRING")
        if cs:
            svc = BlobServiceClient.from_connection_string(cs)
            svc.get_container_client(CONTAINER_NAME).exists()

        return {
            "status": "healthy",
            "bybit_api": "ok",
            "storage": "ok" if cs else "not_configured",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }