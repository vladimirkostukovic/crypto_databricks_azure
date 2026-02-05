import logging
import requests
import json
import os
import time
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient, ContainerClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SYMBOLS = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "LDO-USDT-SWAP", "LINK-USDT-SWAP"]
BASE_URL = "https://www.okx.com"
CONTAINER_NAME = "landing-dev"
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10
SAVE_TO_CLOUD = True
MAX_VALIDATION_RETRIES = 2

INTERVAL_TO_BAR = {
    "15m": "15m",
    "1h": "1H",
    "4h": "4H",
    "1d": "1D"
}

INTERVAL_LIMITS = {
    "15m": 300,
    "1h": 300,
    "4h": 300,
    "1d": 300
}

INTERVAL_MINUTES = {
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440
}

INTERVAL_CHECK_CONFIG = {
    "15m": {
        "daily_depth": 10,
        "weekly_depth": 672,
        "full_history": 300
    },
    "1h": {
        "daily_depth": 10,
        "weekly_depth": 168,
        "full_history": 300
    },
    "4h": {
        "daily_depth": 6,
        "weekly_depth": 42,
        "full_history": 300
    },
    "1d": {
        "daily_depth": 3,
        "weekly_depth": 30,
        "full_history": 300
    }
}

RETENTION_DAYS = {
    "15m": 30,
    "1h": 60,
    "4h": 90,
    "1d": 365
}


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


def api_call(url: str, params: dict, retry_count: int = 0, max_retries: int = MAX_RETRIES) -> Optional[Any]:
    try:
        r = SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT)

        if r.status_code == 429:
            retry_after = int(r.headers.get('Retry-After', 60))
            logging.warning(f"Rate limited, waiting {retry_after}s")
            time.sleep(retry_after)
            if retry_count < max_retries:
                return api_call(url, params, retry_count + 1, max_retries)
            raise Exception("Max retries exceeded on 429")

        r.raise_for_status()
        response = r.json()

        if isinstance(response, dict):
            code = response.get("code")
            if code != "0":
                msg = response.get("msg", "Unknown error")
                if code == "50011":
                    logging.warning(f"OKX rate limit reached, waiting 2s")
                    time.sleep(2)
                    if retry_count < max_retries:
                        return api_call(url, params, retry_count + 1, max_retries)
                raise Exception(f"OKX API error code={code}: {msg}")
            return response.get("data", [])

        return response

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


def get_klines(symbol: str, interval: str = "15m", limit: int = 1, include_current: bool = False) -> List:
    url = f"{BASE_URL}/api/v5/market/candles"
    bar = INTERVAL_TO_BAR.get(interval, "15m")

    request_limit = min(limit + 2, 300)

    params = {
        "instId": symbol,
        "bar": bar,
        "limit": str(request_limit)
    }

    data = api_call(url, params)

    if not data:
        return []

    if include_current:
        candles = [c for c in data if isinstance(c, list) and len(c) >= 9]
    else:
        # only confirmed candles
        candles = [c for c in data if isinstance(c, list) and len(c) >= 9 and c[8] == "1"]

    if limit and len(candles) > limit:
        candles = candles[:limit]

    return candles


def get_history_klines(symbol: str, interval: str = "15m", limit: int = 100) -> List:
    url = f"{BASE_URL}/api/v5/market/history-candles"
    bar = INTERVAL_TO_BAR.get(interval, "15m")

    all_candles = []
    after = None
    remaining = limit

    while remaining > 0:
        batch_limit = min(remaining, 100)

        params = {
            "instId": symbol,
            "bar": bar,
            "limit": str(batch_limit)
        }
        if after:
            params["after"] = str(after)

        data = api_call(url, params)

        if not data or len(data) == 0:
            break

        closed = [c for c in data if isinstance(c, list) and len(c) >= 9 and c[8] == "1"]
        all_candles.extend(closed)
        remaining -= len(closed)

        last_ts = data[-1][0] if data else None
        if last_ts:
            after = last_ts
        else:
            break

        time.sleep(0.2)

    return all_candles


def get_ticker(symbol: str) -> Optional[Dict]:
    url = f"{BASE_URL}/api/v5/market/ticker"
    data = api_call(url, {"instId": symbol})
    if data and isinstance(data, list) and len(data) > 0:
        return data[0]
    return None


def get_depth(symbol: str, limit: int = 20) -> Optional[Dict]:
    url = f"{BASE_URL}/api/v5/market/books"
    data = api_call(url, {"instId": symbol, "sz": str(min(limit, 400))})
    if data and isinstance(data, list) and len(data) > 0:
        return data[0]
    return None


def get_funding_rate(symbol: str) -> Optional[Dict]:
    url = f"{BASE_URL}/api/v5/public/funding-rate"
    data = api_call(url, {"instId": symbol})
    if data and isinstance(data, list) and len(data) > 0:
        return data[0]
    return None


def get_funding_rate_history(symbol: str, limit: int = 10) -> List:
    url = f"{BASE_URL}/api/v5/public/funding-rate-history"
    data = api_call(url, {"instId": symbol, "limit": str(min(limit, 100))})
    return data if data else []


def get_open_interest(symbol: str) -> Optional[Dict]:
    url = f"{BASE_URL}/api/v5/public/open-interest"
    data = api_call(url, {"instType": "SWAP", "instId": symbol})
    if data and isinstance(data, list) and len(data) > 0:
        return data[0]
    return None


def get_long_short_ratio(symbol: str, period: str = "15m") -> List:
    # OKX uses underlying asset (BTC, ETH) not full instId
    ccy = symbol.split("-")[0]
    url = f"{BASE_URL}/api/v5/rubik/stat/contracts/long-short-account-ratio"
    return api_call(url, {"ccy": ccy, "period": period}) or []


def get_taker_volume(symbol: str, period: str = "5m") -> List:
    # taker buy/sell volume - use CONTRACTS instType
    ccy = symbol.split("-")[0]
    url = f"{BASE_URL}/api/v5/rubik/stat/taker-volume"
    try:
        return api_call(url, {"ccy": ccy, "instType": "CONTRACTS", "period": period}) or []
    except Exception:
        return []




def get_realtime_kline(symbol: str, interval: str = "15m") -> Optional[Dict]:
    url = f"{BASE_URL}/api/v5/market/candles"
    bar = INTERVAL_TO_BAR.get(interval, "15m")

    params = {
        "instId": symbol,
        "bar": bar,
        "limit": "1"
    }

    data = api_call(url, params)

    if not data or not isinstance(data, list) or len(data) == 0:
        return None

    candle = data[0]
    if isinstance(candle, list) and len(candle) >= 9:
        return {
            "timestamp": int(candle[0]),
            "open": float(candle[1]),
            "high": float(candle[2]),
            "low": float(candle[3]),
            "close": float(candle[4]),
            "volume": float(candle[5]),
            "volume_ccy": float(candle[6]),
            "volume_quote": float(candle[7]),
            "is_closed": candle[8] == "1"
        }
    return None


def validate_data(data: Dict, symbol: str) -> bool:
    required_fields = ["symbol", "timestamp", "interval", "klines", "ticker"]

    for field in required_fields:
        if field not in data:
            logging.error(f"Missing field '{field}' for {symbol}")
            return False

    if not data["klines"]:
        logging.error(f"No klines data for {symbol}")
        return False

    if isinstance(data["klines"], list) and len(data["klines"]) == 0:
        logging.error(f"Empty klines data for {symbol}")
        return False

    return True


def fetch_symbol_data(symbol: str, interval: str, retry_attempt: int = 0) -> Optional[Dict]:
    try:
        now = datetime.utcnow()

        klines_data = get_klines(symbol, interval, limit=2)
        logging.debug(f"  Fetched {interval} for {symbol}: {len(klines_data)} candles")

        # map interval to OKX period format
        period_map = {"15m": "5m", "1h": "1H", "4h": "4H", "1d": "1D"}
        period = period_map.get(interval, "5m")

        data = {
            "symbol": symbol,
            "timestamp": now.isoformat(),
            "interval": interval,
            "klines": klines_data,
            "ticker": get_ticker(symbol),
            "depth": get_depth(symbol, 20),
            "funding_rate": get_funding_rate(symbol),
            "funding_rate_history": get_funding_rate_history(symbol, 10),
            "open_interest": get_open_interest(symbol),
            "long_short_ratio": get_long_short_ratio(symbol, period),
            "taker_volume": get_taker_volume(symbol, period)
        }

        if not validate_data(data, symbol):
            if retry_attempt < MAX_VALIDATION_RETRIES:
                logging.warning(
                    f"Validation failed for {symbol} [{interval}], retry {retry_attempt + 1}/{MAX_VALIDATION_RETRIES}")
                time.sleep(2 ** retry_attempt)
                return fetch_symbol_data(symbol, interval, retry_attempt + 1)
            else:
                raise ValueError(f"Data validation failed for {symbol} after {MAX_VALIDATION_RETRIES} retries")

        logging.info(f"✓ {symbol} - {interval}")
        time.sleep(0.2)
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


def append_to_logs(result: Dict, interval: str):
    try:
        cs = os.environ.get("STORAGE_CONNECTION_STRING")
        if not cs:
            logging.warning("No storage connection, skipping logs")
            return

        svc = BlobServiceClient.from_connection_string(cs)
        container = svc.get_container_client(CONTAINER_NAME)
        blob_client = container.get_blob_client("logs.json")

        existing_logs = []
        try:
            content = blob_client.download_blob().readall()
            existing_logs = json.loads(content)
        except:
            pass

        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "exchange": "okx",
            "interval": interval,
            "status": result.get("status"),
            "symbols_processed": result.get("symbols_processed", 0),
            "symbols_failed": result.get("symbols_failed", 0),
            "duration_seconds": result.get("duration_seconds", 0),
            "data_file": result.get("file_saved"),
            "errors": result.get("errors", [])
        }

        existing_logs.append(log_entry)

        blob_client.upload_blob(
            json.dumps(existing_logs, indent=2),
            overwrite=True,
            timeout=30
        )

        logging.info(f"✓ Logged to logs.json: {interval}")

    except Exception as e:
        logging.error(f"Failed to append to logs.json: {e}")


def save_interval_data(data: Dict, interval: str) -> Optional[str]:
    if not SAVE_TO_CLOUD:
        return save_interval_local(data, interval)

    storage_connection_string = os.environ.get("STORAGE_CONNECTION_STRING")

    if not storage_connection_string:
        logging.error("STORAGE_CONNECTION_STRING not set, saving locally")
        return save_interval_local(data, interval)

    try:
        svc = BlobServiceClient.from_connection_string(storage_connection_string)
        container = svc.get_container_client(CONTAINER_NAME)

        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")

        blob_path = f"okx/date={date_str}/hour={hour_str}/{timestamp_str}_{interval}.json"
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


def save_interval_local(data: Dict, interval: str) -> str:
    now = datetime.utcnow()
    local_dir = "/tmp/okx"
    os.makedirs(local_dir, exist_ok=True)
    local_path = f"{local_dir}/{now.strftime('%Y%m%d_%H%M%S')}_{interval}.json"

    with open(local_path, 'w') as f:
        json.dump(data, f, indent=2)

    logging.info(f"Saved locally: {local_path}")
    return local_path


def get_container_client() -> Optional[ContainerClient]:
    cs = os.environ.get("STORAGE_CONNECTION_STRING")
    if not cs:
        logging.error("STORAGE_CONNECTION_STRING not set")
        return None

    try:
        svc = BlobServiceClient.from_connection_string(cs)
        return svc.get_container_client(CONTAINER_NAME)
    except Exception as e:
        logging.error(f"Failed to connect to storage: {e}")
        return None


def backfill_missing_candles() -> Dict:
    logging.info("Starting OKX backfill check...")
    container = get_container_client()

    if not container:
        return {"status": "failed", "error": "No storage connection"}

    results = {}
    total_backfilled = 0

    for interval, config in INTERVAL_CHECK_CONFIG.items():
        required_depth = config["full_history"]

        logging.info(f"Checking {interval} history (required: {required_depth} candles)...")

        symbol_candle_counts = {}

        try:
            blobs = list(container.list_blobs(name_starts_with="okx/"))
            interval_blobs = [b for b in blobs if f"_{interval}.json" in b.name]

            for blob in interval_blobs:
                try:
                    blob_client = container.get_blob_client(blob.name)
                    content = blob_client.download_blob().readall()
                    data = json.loads(content)

                    for symbol_data in data.get("symbols", []):
                        symbol = symbol_data.get("symbol")
                        if not symbol:
                            continue

                        if symbol not in symbol_candle_counts:
                            symbol_candle_counts[symbol] = set()

                        klines = symbol_data.get("klines", [])
                        if isinstance(klines, list):
                            for candle in klines:
                                if isinstance(candle, list) and len(candle) > 0:
                                    symbol_candle_counts[symbol].add(candle[0])

                except Exception as e:
                    logging.error(f"Error parsing {blob.name}: {e}")

            backfilled_symbols = []

            for symbol in SYMBOLS:
                current_count = len(symbol_candle_counts.get(symbol, set()))
                missing_count = required_depth - current_count

                if missing_count > 0:
                    logging.info(
                        f"[{interval}] {symbol}: {current_count}/{required_depth} candles, backfilling {missing_count}...")

                    try:
                        backfill_limit = min(missing_count, INTERVAL_LIMITS.get(interval, 300))
                        backfill_data = get_history_klines(symbol, interval, limit=backfill_limit)

                        if backfill_data and isinstance(backfill_data, list) and len(backfill_data) > 0:
                            now = datetime.utcnow()
                            backfill_json = {
                                "timestamp": now.isoformat(),
                                "exchange": "okx",
                                "interval": interval,
                                "backfill": True,
                                "symbols": [{
                                    "symbol": symbol,
                                    "timestamp": now.isoformat(),
                                    "interval": interval,
                                    "klines": backfill_data,
                                    "ticker": get_ticker(symbol),
                                    "depth": get_depth(symbol, 20),
                                    "funding_rate": get_funding_rate(symbol),
                                    "funding_rate_history": get_funding_rate_history(symbol, 10),
                                    "open_interest": get_open_interest(symbol)
                                }],
                                "total_symbols": 1,
                                "fetched_symbols": 1,
                                "missing_symbols": 0
                            }

                            date_str = now.strftime("%Y-%m-%d")
                            hour_str = now.strftime("%H")
                            timestamp_str = now.strftime("%Y%m%d_%H%M%S")
                            blob_path = f"okx/date={date_str}/hour={hour_str}/{timestamp_str}_{interval}_backfill_{symbol.replace('-', '_')}.json"

                            blob_client = container.get_blob_client(blob_path)
                            blob_client.upload_blob(
                                json.dumps(backfill_json, indent=2),
                                overwrite=True,
                                timeout=30
                            )

                            backfilled_symbols.append({
                                "symbol": symbol,
                                "previous_count": current_count,
                                "backfilled_count": len(backfill_data),
                                "file": blob_path
                            })

                            total_backfilled += 1
                            logging.info(f"✓ Backfilled {symbol} [{interval}]: {len(backfill_data)} candles")
                            time.sleep(0.5)

                    except Exception as e:
                        logging.error(f"Failed to backfill {symbol} [{interval}]: {e}")
                else:
                    logging.info(f"[{interval}] {symbol}: {current_count}/{required_depth} candles - OK")

            results[interval] = {
                "required_depth": required_depth,
                "symbol_counts": {s: len(symbol_candle_counts.get(s, set())) for s in SYMBOLS},
                "backfilled_symbols": backfilled_symbols
            }

        except Exception as e:
            logging.error(f"Error processing {interval}: {e}")
            results[interval] = {"error": str(e)}

    logging.info(f"Backfill complete. Total symbols backfilled: {total_backfilled}")
    return {
        "status": "success",
        "total_backfilled": total_backfilled,
        "results": results,
        "timestamp": datetime.utcnow().isoformat()
    }


def daily_health_check() -> Dict:
    logging.info("Starting OKX daily health check...")
    container = get_container_client()

    if not container:
        return {"status": "failed", "error": "No storage connection"}

    results = {}
    issues_found = 0

    for interval, config in INTERVAL_CHECK_CONFIG.items():
        depth = config["daily_depth"]
        logging.info(f"Checking last {depth} files for {interval}...")

        blobs = list(container.list_blobs(name_starts_with="okx/"))
        interval_blobs = [b for b in blobs if f"_{interval}.json" in b.name]
        interval_blobs.sort(key=lambda x: x.name, reverse=True)

        recent_blobs = interval_blobs[:depth]
        incomplete_files = []

        for blob in recent_blobs:
            try:
                blob_client = container.get_blob_client(blob.name)
                content = blob_client.download_blob().readall()
                data = json.loads(content)

                if data.get("missing_symbols", 0) > 0:
                    incomplete_files.append({
                        "file": blob.name,
                        "fetched": data.get("fetched_symbols", 0),
                        "missing": data.get("missing_symbols", 0)
                    })
                    issues_found += 1

            except Exception as e:
                logging.error(f"Error checking {blob.name}: {e}")

        results[interval] = {
            "checked": len(recent_blobs),
            "incomplete": len(incomplete_files),
            "files": incomplete_files
        }

    logging.info(f"Daily health check complete. Issues found: {issues_found}")
    return {
        "status": "success",
        "issues_found": issues_found,
        "results": results,
        "timestamp": datetime.utcnow().isoformat()
    }


def weekly_full_audit() -> Dict:
    logging.info("Starting OKX weekly full audit...")
    container = get_container_client()

    if not container:
        return {"status": "failed", "error": "No storage connection"}

    results = {}
    total_issues = 0

    for interval, config in INTERVAL_CHECK_CONFIG.items():
        depth = config["weekly_depth"]
        logging.info(f"Auditing {depth} files for {interval}...")

        blobs = list(container.list_blobs(name_starts_with="okx/"))
        interval_blobs = [b for b in blobs if f"_{interval}.json" in b.name]
        interval_blobs.sort(key=lambda x: x.name, reverse=True)

        audit_blobs = interval_blobs[:depth]
        incomplete_files = []

        for blob in audit_blobs:
            try:
                blob_client = container.get_blob_client(blob.name)
                content = blob_client.download_blob().readall()
                data = json.loads(content)

                if data.get("missing_symbols", 0) > 0:
                    incomplete_files.append({
                        "file": blob.name,
                        "fetched": data.get("fetched_symbols", 0),
                        "missing": data.get("missing_symbols", 0)
                    })
                    total_issues += 1

            except Exception as e:
                logging.error(f"Error auditing {blob.name}: {e}")

        results[interval] = {
            "audited": len(audit_blobs),
            "incomplete": len(incomplete_files),
            "files": incomplete_files
        }

    logging.info(f"Weekly audit complete. Total issues: {total_issues}")
    return {
        "status": "success",
        "total_issues": total_issues,
        "results": results,
        "timestamp": datetime.utcnow().isoformat()
    }


def cleanup_old_files() -> Dict:
    logging.info("Starting cleanup of old OKX files...")
    container = get_container_client()

    if not container:
        return {"status": "failed", "error": "No storage connection"}

    now = datetime.utcnow()
    results = {}

    for interval, retention_days in RETENTION_DAYS.items():
        safety_margin = retention_days * 2
        cutoff_date = now - timedelta(days=safety_margin)

        logging.info(f"Cleaning {interval} files older than {cutoff_date.date()} (2x retention safety)")

        deleted_count = 0

        try:
            blobs = container.list_blobs(name_starts_with="okx/")

            for blob in blobs:
                if f"_{interval}.json" not in blob.name:
                    continue

                match = re.search(r'date=(\d{4}-\d{2}-\d{2})', blob.name)
                if match:
                    file_date = datetime.strptime(match.group(1), '%Y-%m-%d')

                    if file_date < cutoff_date:
                        container.delete_blob(blob.name)
                        deleted_count += 1
                        logging.debug(f"Deleted: {blob.name}")

            results[interval] = {
                "retention_days": retention_days,
                "safety_margin_days": safety_margin,
                "cutoff_date": cutoff_date.date().isoformat(),
                "deleted_files": deleted_count
            }

            logging.info(f"✓ Deleted {deleted_count} old {interval} files")

        except Exception as e:
            logging.error(f"Error cleaning {interval} files: {e}")
            results[interval] = {"error": str(e)}

    return {
        "status": "success",
        "note": "Using 2x retention safety (no Autoloader checkpoint yet)",
        "results": results,
        "timestamp": now.isoformat()
    }


def _fetch_symbol_parallel(symbol: str, interval: str) -> Dict:
    try:
        result = fetch_symbol_data(symbol, interval)
        return {"symbol": symbol, "data": result, "success": result is not None and "error" not in result}
    except Exception as e:
        logging.error(f"Parallel fetch failed for {symbol}: {e}")
        return {"symbol": symbol, "data": None, "success": False, "error": str(e)}


def run(intervals: List[str] = None, parallel: bool = True, max_workers: int = 4) -> Dict:
    now = datetime.utcnow()
    logging.info(f"Starting OKX ingestion at {now.strftime('%Y-%m-%d %H:%M')} UTC")

    if not intervals:
        intervals = ["15m"]

    logging.info(f"Fetching intervals: {', '.join(intervals)} (parallel={parallel})")
    start_time = time.time()

    try:
        results_by_interval = {}
        total_success = 0
        total_failed = 0
        all_errors = []

        for interval in intervals:
            interval_start_time = time.time()
            interval_symbols_data = []
            success_symbols = set()
            failed_symbols = []

            max_attempts = 3

            for attempt in range(max_attempts):
                if attempt == 0:
                    symbols_to_fetch = SYMBOLS
                else:
                    symbols_to_fetch = failed_symbols
                    logging.warning(
                        f"[{interval}] Attempt {attempt + 1}/{max_attempts} for symbols: {', '.join(symbols_to_fetch)}")

                failed_symbols = []

                if parallel and len(symbols_to_fetch) > 1:
                    with ThreadPoolExecutor(max_workers=min(max_workers, len(symbols_to_fetch))) as executor:
                        futures = {
                            executor.submit(_fetch_symbol_parallel, symbol, interval): symbol
                            for symbol in symbols_to_fetch
                        }

                        for future in as_completed(futures):
                            symbol = futures[future]
                            try:
                                result = future.result()
                                if result["success"]:
                                    interval_symbols_data.append(result["data"])
                                    success_symbols.add(symbol)
                                    logging.info(f"✓ {symbol} [{interval}]")
                                else:
                                    failed_symbols.append(symbol)
                                    error_info = result.get("data", {})
                                    all_errors.append({
                                        "symbol": symbol,
                                        "interval": interval,
                                        "attempt": attempt + 1,
                                        "error": error_info.get("error") if isinstance(error_info, dict) else str(result.get("error")),
                                        "error_type": error_info.get("error_type") if isinstance(error_info, dict) else "Unknown"
                                    })
                            except Exception as e:
                                failed_symbols.append(symbol)
                                logging.error(f"Future failed for {symbol}: {e}")
                else:
                    for symbol in symbols_to_fetch:
                        logging.info(f"Processing {symbol} [{interval}]...")
                        symbol_data = fetch_symbol_data(symbol, interval)

                        if symbol_data is None:
                            continue

                        if "error" not in symbol_data:
                            interval_symbols_data.append(symbol_data)
                            success_symbols.add(symbol)
                        else:
                            failed_symbols.append(symbol)
                            all_errors.append({
                                "symbol": symbol,
                                "interval": interval,
                                "attempt": attempt + 1,
                                "error": symbol_data.get("error"),
                                "error_type": symbol_data.get("error_type")
                            })

                if len(success_symbols) == len(SYMBOLS):
                    logging.info(f"✓ [{interval}] All {len(SYMBOLS)} symbols fetched successfully")
                    break

                if failed_symbols and attempt < max_attempts - 1:
                    wait_time = 2 ** attempt
                    logging.warning(
                        f"[{interval}] Missing {len(failed_symbols)} symbols, waiting {wait_time}s before retry...")
                    time.sleep(wait_time)

            success_count = len(success_symbols)
            failed_count = len(SYMBOLS) - success_count

            if failed_count > 0:
                logging.error(
                    f"✗ [{interval}] Failed to fetch {failed_count}/{len(SYMBOLS)} symbols after {max_attempts} attempts")

            interval_data = {
                "timestamp": now.isoformat(),
                "exchange": "okx",
                "interval": interval,
                "symbols": interval_symbols_data,
                "total_symbols": len(SYMBOLS),
                "fetched_symbols": success_count,
                "missing_symbols": failed_count
            }

            saved_path = save_interval_data(interval_data, interval)

            interval_duration = time.time() - interval_start_time

            results_by_interval[interval] = {
                "symbols_processed": success_count,
                "symbols_failed": failed_count,
                "file_saved": saved_path,
                "attempts_made": min(attempt + 1, max_attempts)
            }

            append_to_logs({
                "status": "success" if failed_count == 0 else "partial_success",
                "symbols_processed": success_count,
                "symbols_failed": failed_count,
                "duration_seconds": round(interval_duration, 2),
                "file_saved": saved_path,
                "attempts_made": min(attempt + 1, max_attempts),
                "errors": [e for e in all_errors if e.get("interval") == interval]
            }, interval)

            total_success += success_count
            total_failed += failed_count

        duration = time.time() - start_time
        logging.info(f"Completed in {duration:.2f}s")

        result = {
            "status": "success" if total_failed == 0 else "partial_success",
            "exchange": "okx",
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
            "exchange": "okx",
            "error": str(e),
            "error_type": type(e).__name__,
            "timestamp": now.isoformat()
        }


def health_check() -> Dict:
    try:
        data = api_call(f"{BASE_URL}/api/v5/public/time", {})

        storage_connection_string = os.environ.get("STORAGE_CONNECTION_STRING")
        if storage_connection_string:
            svc = BlobServiceClient.from_connection_string(storage_connection_string)
            svc.get_container_client(CONTAINER_NAME).exists()

        return {
            "status": "healthy",
            "exchange": "okx",
            "okx_api": "ok",
            "okx_server_time": data[0].get("ts") if data else None,
            "storage": "ok" if storage_connection_string else "not_configured",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "exchange": "okx",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
