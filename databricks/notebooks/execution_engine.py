# Databricks notebook source
# ---
# EXECUTION ENGINE
# Places and manages orders on exchange via API
# Handles: limit entry, TP1 partial close, SL to BE, TP2 full close
# ---

import requests
import hmac
import hashlib
import time
import json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

CATALOG = "crypto"
GOLD_SCHEMA = "gold"
TRADING_SCHEMA = "trading"

# ---
# CONFIGURATION
# ---

def get_config(param_name, default=None):
    try:
        result = spark.sql(f"""
            SELECT param_value, param_type
            FROM {CATALOG}.{GOLD_SCHEMA}.trading_config
            WHERE param_name = '{param_name}'
        """).first()
        if result:
            val, ptype = result["param_value"], result["param_type"]
            if ptype == "int": return int(val)
            elif ptype == "float": return float(val)
            elif ptype == "bool": return val.lower() == "true"
            elif ptype == "json": return json.loads(val)
            return val
        return default
    except:
        return default

# Load config
EXCHANGE = get_config("trading_exchange", "bybit")
USE_TESTNET = get_config("use_testnet", True)
RISK_PER_TRADE = get_config("risk_per_trade_pct", 1.0)
MAX_LEVERAGE = get_config("max_leverage", 10)
MAX_OPEN_POSITIONS = get_config("max_open_positions", 5)
TP1_CLOSE_PCT = get_config("tp1_close_pct", 50)

# API credentials (should be in Databricks secrets in production)
API_KEY = get_config("exchange_api_key", "")
API_SECRET = get_config("exchange_api_secret", "")

# Exchange URLs
EXCHANGE_URLS = {
    "bybit": {
        "mainnet": "https://api.bybit.com",
        "testnet": "https://api-testnet.bybit.com"
    },
    "binance": {
        "mainnet": "https://fapi.binance.com",
        "testnet": "https://testnet.binancefuture.com"
    }
}

BASE_URL = EXCHANGE_URLS.get(EXCHANGE, {}).get("testnet" if USE_TESTNET else "mainnet", "")

print("=" * 60)
print("EXECUTION ENGINE")
print("=" * 60)
print(f"Exchange: {EXCHANGE} ({'TESTNET' if USE_TESTNET else 'MAINNET'})")
print(f"Max positions: {MAX_OPEN_POSITIONS}")
print(f"Risk per trade: {RISK_PER_TRADE}%")
print(f"TP1 close: {TP1_CLOSE_PCT}%")
print("=" * 60)

if not API_KEY or not API_SECRET:
    print("\n⚠ WARNING: API credentials not configured!")
    print("Add to trading_config:")
    print("  - exchange_api_key")
    print("  - exchange_api_secret")


# ---
# EXCHANGE API FUNCTIONS
# ---

def sign_request(params: dict, secret: str) -> str:
    """Generate HMAC signature for request"""
    query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
    signature = hmac.new(
        secret.encode('utf-8'),
        query_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()
    return signature


def get_account_balance() -> float:
    """Get account USDT balance"""
    if not API_KEY:
        return 10000.0  # Mock for testing

    try:
        if EXCHANGE == "bybit":
            timestamp = str(int(time.time() * 1000))
            params = {
                "api_key": API_KEY,
                "timestamp": timestamp,
                "accountType": "UNIFIED"
            }
            params["sign"] = sign_request(params, API_SECRET)

            response = requests.get(
                f"{BASE_URL}/v5/account/wallet-balance",
                params=params,
                timeout=10
            )
            data = response.json()

            if data.get("retCode") == 0:
                for coin in data["result"]["list"][0]["coin"]:
                    if coin["coin"] == "USDT":
                        return float(coin["walletBalance"])

        elif EXCHANGE == "binance":
            timestamp = str(int(time.time() * 1000))
            params = {"timestamp": timestamp}
            params["signature"] = sign_request(params, API_SECRET)

            headers = {"X-MBX-APIKEY": API_KEY}
            response = requests.get(
                f"{BASE_URL}/fapi/v2/balance",
                params=params,
                headers=headers,
                timeout=10
            )
            data = response.json()

            for asset in data:
                if asset["asset"] == "USDT":
                    return float(asset["balance"])

    except Exception as e:
        print(f"Error getting balance: {e}")

    return 0.0


def get_current_price(symbol: str) -> float:
    """Get current market price"""
    try:
        if EXCHANGE == "bybit":
            response = requests.get(
                f"{BASE_URL}/v5/market/tickers",
                params={"category": "linear", "symbol": symbol},
                timeout=5
            )
            data = response.json()
            if data.get("retCode") == 0:
                return float(data["result"]["list"][0]["lastPrice"])

        elif EXCHANGE == "binance":
            response = requests.get(
                f"{BASE_URL}/fapi/v1/ticker/price",
                params={"symbol": symbol},
                timeout=5
            )
            return float(response.json()["price"])

    except Exception as e:
        print(f"Error getting price for {symbol}: {e}")

    return 0.0


def place_limit_order(symbol: str, side: str, quantity: float, price: float) -> dict:
    """Place a limit order"""
    if not API_KEY:
        # Mock response for testing
        return {
            "success": True,
            "order_id": f"MOCK_{int(time.time())}",
            "message": "Mock order (no API key)"
        }

    try:
        if EXCHANGE == "bybit":
            timestamp = str(int(time.time() * 1000))
            params = {
                "api_key": API_KEY,
                "timestamp": timestamp,
                "category": "linear",
                "symbol": symbol,
                "side": "Buy" if side == "long" else "Sell",
                "orderType": "Limit",
                "qty": str(quantity),
                "price": str(price),
                "timeInForce": "GTC",
                "positionIdx": 0
            }
            params["sign"] = sign_request(params, API_SECRET)

            response = requests.post(
                f"{BASE_URL}/v5/order/create",
                data=params,
                timeout=10
            )
            data = response.json()

            if data.get("retCode") == 0:
                return {
                    "success": True,
                    "order_id": data["result"]["orderId"],
                    "message": "Order placed"
                }
            else:
                return {
                    "success": False,
                    "order_id": None,
                    "message": data.get("retMsg", "Unknown error")
                }

        elif EXCHANGE == "binance":
            timestamp = str(int(time.time() * 1000))
            params = {
                "symbol": symbol,
                "side": "BUY" if side == "long" else "SELL",
                "type": "LIMIT",
                "quantity": quantity,
                "price": price,
                "timeInForce": "GTC",
                "timestamp": timestamp
            }
            params["signature"] = sign_request(params, API_SECRET)

            headers = {"X-MBX-APIKEY": API_KEY}
            response = requests.post(
                f"{BASE_URL}/fapi/v1/order",
                params=params,
                headers=headers,
                timeout=10
            )
            data = response.json()

            if "orderId" in data:
                return {
                    "success": True,
                    "order_id": str(data["orderId"]),
                    "message": "Order placed"
                }
            else:
                return {
                    "success": False,
                    "order_id": None,
                    "message": data.get("msg", "Unknown error")
                }

    except Exception as e:
        return {
            "success": False,
            "order_id": None,
            "message": str(e)
        }


def place_stop_loss(symbol: str, side: str, quantity: float, stop_price: float) -> dict:
    """Place a stop loss order"""
    if not API_KEY:
        return {"success": True, "order_id": f"MOCK_SL_{int(time.time())}", "message": "Mock SL"}

    try:
        if EXCHANGE == "bybit":
            timestamp = str(int(time.time() * 1000))
            params = {
                "api_key": API_KEY,
                "timestamp": timestamp,
                "category": "linear",
                "symbol": symbol,
                "side": "Sell" if side == "long" else "Buy",  # Opposite to close
                "orderType": "Market",
                "qty": str(quantity),
                "triggerPrice": str(stop_price),
                "triggerDirection": 2 if side == "long" else 1,  # 1=rise, 2=fall
                "timeInForce": "GTC",
                "positionIdx": 0,
                "reduceOnly": True
            }
            params["sign"] = sign_request(params, API_SECRET)

            response = requests.post(
                f"{BASE_URL}/v5/order/create",
                data=params,
                timeout=10
            )
            data = response.json()

            if data.get("retCode") == 0:
                return {"success": True, "order_id": data["result"]["orderId"], "message": "SL placed"}
            else:
                return {"success": False, "order_id": None, "message": data.get("retMsg")}

    except Exception as e:
        return {"success": False, "order_id": None, "message": str(e)}


def place_take_profit(symbol: str, side: str, quantity: float, tp_price: float) -> dict:
    """Place a take profit order"""
    if not API_KEY:
        return {"success": True, "order_id": f"MOCK_TP_{int(time.time())}", "message": "Mock TP"}

    try:
        if EXCHANGE == "bybit":
            timestamp = str(int(time.time() * 1000))
            params = {
                "api_key": API_KEY,
                "timestamp": timestamp,
                "category": "linear",
                "symbol": symbol,
                "side": "Sell" if side == "long" else "Buy",
                "orderType": "Limit",
                "qty": str(quantity),
                "price": str(tp_price),
                "timeInForce": "GTC",
                "positionIdx": 0,
                "reduceOnly": True
            }
            params["sign"] = sign_request(params, API_SECRET)

            response = requests.post(
                f"{BASE_URL}/v5/order/create",
                data=params,
                timeout=10
            )
            data = response.json()

            if data.get("retCode") == 0:
                return {"success": True, "order_id": data["result"]["orderId"], "message": "TP placed"}
            else:
                return {"success": False, "order_id": None, "message": data.get("retMsg")}

    except Exception as e:
        return {"success": False, "order_id": None, "message": str(e)}


def cancel_order(symbol: str, order_id: str) -> bool:
    """Cancel an order"""
    if not API_KEY:
        return True

    try:
        if EXCHANGE == "bybit":
            timestamp = str(int(time.time() * 1000))
            params = {
                "api_key": API_KEY,
                "timestamp": timestamp,
                "category": "linear",
                "symbol": symbol,
                "orderId": order_id
            }
            params["sign"] = sign_request(params, API_SECRET)

            response = requests.post(
                f"{BASE_URL}/v5/order/cancel",
                data=params,
                timeout=10
            )
            return response.json().get("retCode") == 0

    except:
        return False


# ---
# POSITION MANAGEMENT
# ---

def calculate_position_size(balance: float, risk_pct: float, entry: float, sl: float) -> float:
    """Calculate position size based on risk"""
    risk_amount = balance * (risk_pct / 100)
    risk_per_unit = abs(entry - sl)

    if risk_per_unit == 0:
        return 0

    # Position size in base currency
    position_size = risk_amount / risk_per_unit

    # Round to reasonable precision
    if entry > 1000:  # BTC-like
        position_size = round(position_size, 3)
    elif entry > 100:  # ETH-like
        position_size = round(position_size, 2)
    else:
        position_size = round(position_size, 1)

    return position_size


def get_open_positions_count() -> int:
    """Get count of currently open positions"""
    try:
        return spark.table(f"{CATALOG}.{TRADING_SCHEMA}.positions_open").count()
    except:
        return 0


def execute_signal(signal: dict, balance: float) -> dict:
    """Execute a trading signal"""

    symbol = signal["symbol"]
    side = signal["signal_type"]
    entry_price = signal["entry_price"]
    stop_loss = signal["stop_loss"]
    tp1 = signal["take_profit_1"]
    tp2 = signal["take_profit_2"]

    print(f"\n[EXECUTE] {symbol} {side.upper()}")
    print(f"  Entry: {entry_price}, SL: {stop_loss}, TP1: {tp1}, TP2: {tp2}")

    # Calculate position size
    qty = calculate_position_size(balance, RISK_PER_TRADE, entry_price, stop_loss)
    print(f"  Position size: {qty}")

    if qty <= 0:
        return {"success": False, "message": "Invalid position size"}

    # Place limit entry order at POC
    entry_result = place_limit_order(symbol, side, qty, entry_price)

    if not entry_result["success"]:
        return {"success": False, "message": f"Entry order failed: {entry_result['message']}"}

    entry_order_id = entry_result["order_id"]
    print(f"  Entry order: {entry_order_id}")

    # Place SL order
    sl_result = place_stop_loss(symbol, side, qty, stop_loss)
    sl_order_id = sl_result.get("order_id")

    # Place TP1 order (partial close)
    tp1_qty = qty * (TP1_CLOSE_PCT / 100)
    tp1_result = place_take_profit(symbol, side, tp1_qty, tp1)
    tp1_order_id = tp1_result.get("order_id")

    # Place TP2 order (remaining)
    tp2_qty = qty - tp1_qty
    tp2_result = place_take_profit(symbol, side, tp2_qty, tp2)
    tp2_order_id = tp2_result.get("order_id")

    # Create position record
    position = {
        "position_id": f"POS_{symbol}_{int(time.time())}",
        "signal_id": signal["signal_id"],
        "symbol": symbol,
        "side": side,
        "strategy": signal.get("strategy", "unknown"),
        "entry_price": entry_price,
        "entry_time": datetime.now(timezone.utc),
        "quantity": qty,
        "leverage": MAX_LEVERAGE,
        "stop_loss": stop_loss,
        "take_profit_1": tp1,
        "take_profit_2": tp2,
        "current_price": get_current_price(symbol),
        "unrealized_pnl": 0.0,
        "unrealized_pnl_pct": 0.0,
        "tp1_hit": False,
        "tp1_close_qty": tp1_qty,
        "remaining_qty": qty,
        "exchange": EXCHANGE,
        "order_ids": json.dumps({
            "entry": entry_order_id,
            "sl": sl_order_id,
            "tp1": tp1_order_id,
            "tp2": tp2_order_id
        }),
        "updated_at": datetime.now(timezone.utc)
    }

    return {
        "success": True,
        "position": position,
        "message": f"Position opened: {entry_order_id}"
    }


# ---
# MAIN EXECUTION LOOP
# ---

def process_ready_signals():
    """Process signals ready for execution"""

    signals_table = f"{CATALOG}.{GOLD_SCHEMA}.signals_all"
    positions_table = f"{CATALOG}.{TRADING_SCHEMA}.positions_open"

    # Check max positions
    open_count = get_open_positions_count()
    available_slots = MAX_OPEN_POSITIONS - open_count

    print(f"\nOpen positions: {open_count}/{MAX_OPEN_POSITIONS}")

    if available_slots <= 0:
        print("Max positions reached, skipping execution")
        return

    # Get balance
    balance = get_account_balance()
    print(f"Account balance: ${balance:,.2f}")

    if balance <= 0:
        print("No balance available")
        return

    # Get ready signals
    ready_signals = spark.table(signals_table).filter(
        (F.col("ai_status") == "confirmed") &
        (F.col("execution_status") == "ready")
    ).orderBy(F.col("ai_confidence").desc()).limit(available_slots).collect()

    print(f"Ready signals: {len(ready_signals)}")

    executed_positions = []
    executed_signal_ids = []

    for signal in ready_signals:
        signal_dict = signal.asDict()

        result = execute_signal(signal_dict, balance)

        if result["success"]:
            executed_positions.append(result["position"])
            executed_signal_ids.append(signal_dict["signal_id"])
            print(f"  ✓ {result['message']}")
        else:
            print(f"  ✗ {result['message']}")

    # Save positions
    if executed_positions:
        schema = StructType([
            StructField("position_id", StringType()),
            StructField("signal_id", StringType()),
            StructField("symbol", StringType()),
            StructField("side", StringType()),
            StructField("strategy", StringType()),
            StructField("entry_price", DoubleType()),
            StructField("entry_time", TimestampType()),
            StructField("quantity", DoubleType()),
            StructField("leverage", IntegerType()),
            StructField("stop_loss", DoubleType()),
            StructField("take_profit_1", DoubleType()),
            StructField("take_profit_2", DoubleType()),
            StructField("current_price", DoubleType()),
            StructField("unrealized_pnl", DoubleType()),
            StructField("unrealized_pnl_pct", DoubleType()),
            StructField("tp1_hit", BooleanType()),
            StructField("tp1_close_qty", DoubleType()),
            StructField("remaining_qty", DoubleType()),
            StructField("exchange", StringType()),
            StructField("order_ids", StringType()),
            StructField("updated_at", TimestampType())
        ])

        pos_df = spark.createDataFrame(executed_positions, schema)
        pos_df.write.format("delta").mode("append").saveAsTable(positions_table)

        print(f"\n✓ Saved {len(executed_positions)} positions")

    # Update signal status
    if executed_signal_ids:
        for sig_id in executed_signal_ids:
            spark.sql(f"""
                UPDATE {signals_table}
                SET execution_status = 'entered',
                    executed_at = current_timestamp()
                WHERE signal_id = '{sig_id}'
            """)

        print(f"✓ Updated {len(executed_signal_ids)} signals to 'entered'")


# ---
# RUN
# ---

process_ready_signals()

# Show current positions
print("\n" + "=" * 60)
print("CURRENT OPEN POSITIONS")
print("=" * 60)

try:
    spark.sql(f"""
        SELECT
            symbol,
            side,
            strategy,
            ROUND(entry_price, 2) as entry,
            ROUND(current_price, 2) as current,
            ROUND(stop_loss, 2) as sl,
            ROUND(take_profit_1, 2) as tp1,
            ROUND(unrealized_pnl_pct, 2) as pnl_pct,
            tp1_hit,
            entry_time
        FROM {CATALOG}.{TRADING_SCHEMA}.positions_open
        ORDER BY entry_time DESC
    """).show(truncate=False)
except:
    print("No open positions")

print("\n✓ Execution engine complete")
