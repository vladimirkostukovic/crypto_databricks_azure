import logging
import requests
import json
import os
import time
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from azure.storage.blob import BlobServiceClient, ContainerClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SYMBOLS = ["BTCUSDT", "ETHUSDT", "LDOUSDT", "LINKUSDT"]
BASE_URL = "https://api.bybit.com"
CONTAINER_NAME = "landing-dev"
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10
SAVE_TO_CLOUD = True
MAX_VALIDATION_RETRIES = 2

INTERVAL_LIMITS = {
    "15": 1000,
    "60": 1000,
    "240": 1000,
    "D": 1000
}

BYBIT_INTERVALS = {
    "15m": "15",
    "1h": "60",
    "4h": "240",
    "1d": "D"
}

INTERVAL_MINUTES = {
    "15": 15,
    "60": 60,
    "240": 240,
    "D": 1440
}

INTERVAL_CHECK_CONFIG = {
    "15m": {
        "daily_depth": 10,
        "weekly_depth": 672,
        "full_history": 1000
    },
    "1h": {
        "daily_depth": 10,
        "weekly_depth": 168,
        "full_history": 1000
    },
    "4h": {
        "daily_depth": 6,
        "weekly_depth": 42,
        "full_history": 500
    },
    "1d": {
        "daily_depth": 3,
        "weekly_depth": 30,
        "full_history": 365
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

        if 'X-Bapi-Limit-Status' in r.headers:
            limit_status = int(r.headers['X-Bapi-Limit-Status'])
            if limit_status < 10:
                logging.warning(f"Bybit rate limit low: {limit_status} remaining")
                time.sleep(1)

        if r.status_code == 429:
            retry_after = int(r.headers.get('Retry-After', 60))
            logging.warning(f"Rate limited, waiting {retry_after}s")
            time.sleep(retry_after)
            if retry_count < max_retries:
                return api_call(url, params, retry_count + 1, max_retries)
            raise Exception("Max retries exceeded on 429")

        r.raise_for_status()

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


def get_klines(symbol: str, interval: str = "15") -> Dict:
    url = f"{BASE_URL}/v5/market/kline"

    now_ms = int(datetime.utcnow().timestamp() * 1000)
    interval_ms = INTERVAL_MINUTES.get(interval, 15) * 60 * 1000
    end_time = now_ms - interval_ms

    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": 1,
        "end": end_time
    }

    return api_call(url, params)


def get_tickers(symbol: str) -> Dict:
    url = f"{BASE_URL}/v5/market/tickers"
    params = {
        "category": "linear",
        "symbol": symbol
    }
    return api_call(url, params)


def get_orderbook(symbol: str, limit: int = 20) -> Dict:
    url = f"{BASE_URL}/v5/market/orderbook"
    params = {
        "category": "linear",
        "symbol": symbol,
        "limit": min(limit, 500)
    }
    return api_call(url, params)


def get_funding_rate(symbol: str, limit: int = 10) -> Dict:
    url = f"{BASE_URL}/v5/market/funding/history"
    params = {
        "category": "linear",
        "symbol": symbol,
        "limit": min(limit, 200)
    }
    return api_call(url, params)


def get_open_interest(symbol: str, interval: str = "15min", limit: int = 10) -> Dict:
    url = f"{BASE_URL}/v5/market/open-interest"
    params = {
        "category": "linear",
        "symbol": symbol,
        "intervalTime": interval,
        "limit": min(limit, 200)
    }
    return api_call(url, params)


def get_long_short_ratio(symbol: str, period: str = "15min", limit: int = 10) -> Dict:
    url = f"{BASE_URL}/v5/market/account-ratio"
    params = {
        "category": "linear",
        "symbol": symbol,
        "period": period,
        "limit": min(limit, 500)
    }
    return api_call(url, params)


def get_open_interest_history(symbol: str, interval: str = "15min", limit: int = 50) -> Dict:
    url = f"{BASE_URL}/v5/market/open-interest"
    params = {
        "category": "linear",
        "symbol": symbol,
        "intervalTime": interval,
        "limit": min(limit, 200)
    }
    return api_call(url, params)


def validate_data(data: Dict, symbol: str) -> bool:
    required_fields = ["symbol", "timestamp", "interval", "klines", "ticker"]

    for field in required_fields:
        if field not in data:
            logging.error(f"Missing field '{field}' for {symbol}")
            return False

    if not data["klines"]:
        logging.error(f"No klines data for {symbol}")
        return False

    if isinstance(data["klines"], dict):
        result = data["klines"].get("result", {})
        klines_list = result.get("list", [])
        if not klines_list or len(klines_list) == 0:
            logging.error(f"Empty klines data for {symbol}")
            return False

    return True


def fetch_symbol_data(symbol: str, interval: str, retry_attempt: int = 0) -> Optional[Dict]:
    try:
        now = datetime.utcnow()
        bybit_interval = BYBIT_INTERVALS[interval]

        klines_data = get_klines(symbol, bybit_interval)
        logging.debug(f"  Fetched {interval} for {symbol}")

        # map interval to Bybit period format
        period_map = {"15m": "15min", "1h": "1h", "4h": "4h", "1d": "1d"}
        period = period_map.get(interval, "15min")

        data = {
            "symbol": symbol,
            "timestamp": now.isoformat(),
            "interval": interval,
            "klines": klines_data,
            "ticker": get_tickers(symbol),
            "orderbook": get_orderbook(symbol, 20),
            "funding_rate": get_funding_rate(symbol, 10),
            "open_interest": get_open_interest(symbol, "15min", 10),
            "long_short_ratio": get_long_short_ratio(symbol, period, 10)
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
            "exchange": "bybit",
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


def save_interval_local(data: Dict, interval: str) -> str:
    now = datetime.utcnow()
    local_dir = "/tmp/bybit"
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
    logging.info("Starting backfill check...")
    container = get_container_client()

    if not container:
        return {"status": "failed", "error": "No storage connection"}

    results = {}
    total_backfilled = 0

    for interval, config in INTERVAL_CHECK_CONFIG.items():
        required_depth = config["full_history"]
        bybit_interval = BYBIT_INTERVALS[interval]

        logging.info(f"Checking {interval} history (required: {required_depth} candles)...")

        symbol_candle_counts = {}

        try:
            blobs = list(container.list_blobs(name_starts_with="bybit/"))
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

                        klines = symbol_data.get("klines", {})
                        if isinstance(klines, dict):
                            result = klines.get("result", {})
                            klines_list = result.get("list", [])
                            for candle in klines_list:
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
                        backfill_data = get_klines(symbol, bybit_interval)

                        if backfill_data and isinstance(backfill_data, dict):
                            result = backfill_data.get("result", {})
                            klines_list = result.get("list", [])

                            if klines_list:
                                now = datetime.utcnow()
                                backfill_json = {
                                    "timestamp": now.isoformat(),
                                    "interval": interval,
                                    "backfill": True,
                                    "symbols": [{
                                        "symbol": symbol,
                                        "timestamp": now.isoformat(),
                                        "interval": interval,
                                        "klines": backfill_data,
                                        "ticker": get_tickers(symbol),
                                        "orderbook": get_orderbook(symbol, 20),
                                        "funding_rate": get_funding_rate(symbol, 10),
                                        "open_interest": get_open_interest(symbol, "15min", 10)
                                    }],
                                    "total_symbols": 1,
                                    "fetched_symbols": 1,
                                    "missing_symbols": 0
                                }

                                date_str = now.strftime("%Y-%m-%d")
                                hour_str = now.strftime("%H")
                                timestamp_str = now.strftime("%Y%m%d_%H%M%S")
                                blob_path = f"bybit/date={date_str}/hour={hour_str}/{timestamp_str}_{interval}_backfill_{symbol}.json"

                                blob_client = container.get_blob_client(blob_path)
                                blob_client.upload_blob(
                                    json.dumps(backfill_json, indent=2),
                                    overwrite=True,
                                    timeout=30
                                )

                                backfilled_symbols.append({
                                    "symbol": symbol,
                                    "previous_count": current_count,
                                    "backfilled_count": len(klines_list),
                                    "file": blob_path
                                })

                                total_backfilled += 1
                                logging.info(f"✓ Backfilled {symbol} [{interval}]: {len(klines_list)} candles")
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
    logging.info("Starting daily health check...")
    container = get_container_client()

    if not container:
        return {"status": "failed", "error": "No storage connection"}

    results = {}
    issues_found = 0

    for interval, config in INTERVAL_CHECK_CONFIG.items():
        depth = config["daily_depth"]
        logging.info(f"Checking last {depth} files for {interval}...")

        blobs = list(container.list_blobs(name_starts_with=f"bybit/"))
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
    logging.info("Starting weekly full audit...")
    container = get_container_client()

    if not container:
        return {"status": "failed", "error": "No storage connection"}

    results = {}
    total_issues = 0

    for interval, config in INTERVAL_CHECK_CONFIG.items():
        depth = config["weekly_depth"]
        logging.info(f"Auditing {depth} files for {interval}...")

        blobs = list(container.list_blobs(name_starts_with=f"bybit/"))
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
    logging.info("Starting cleanup of old files...")
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
            blobs = container.list_blobs(name_starts_with="bybit/")

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


def run(intervals: List[str] = None) -> Dict:
    now = datetime.utcnow()
    logging.info(f"Starting Bybit ingestion at {now.strftime('%Y-%m-%d %H:%M')} UTC")

    if not intervals:
        intervals = ["15m"]

    logging.info(f"Fetching intervals: {', '.join(intervals)}")
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


def health_check() -> Dict:
    try:
        api_call(f"{BASE_URL}/v5/market/time", {})

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
