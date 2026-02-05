# Databricks notebook source
# ---
# POSITION MANAGER
# Monitors open positions, handles:
# - Price updates
# - TP1 hit detection -> move SL to breakeven
# - TP2/SL hit detection -> close position
# - Update positions_open and positions_closed
# ---

import requests
import json
import time
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

CATALOG = "crypto"
SILVER_SCHEMA = "silver"
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
            return val
        return default
    except:
        return default

EXCHANGE = get_config("trading_exchange", "bybit")
USE_TESTNET = get_config("use_testnet", True)
TP1_CLOSE_PCT = get_config("tp1_close_pct", 50)

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
print("POSITION MANAGER")
print("=" * 60)
print(f"Exchange: {EXCHANGE}")
print("=" * 60)


# ---
# PRICE FUNCTIONS
# ---

def get_current_prices(symbols: list) -> dict:
    """Get current prices for multiple symbols"""
    prices = {}

    try:
        if EXCHANGE == "bybit":
            response = requests.get(
                f"{BASE_URL}/v5/market/tickers",
                params={"category": "linear"},
                timeout=5
            )
            data = response.json()
            if data.get("retCode") == 0:
                for ticker in data["result"]["list"]:
                    if ticker["symbol"] in symbols:
                        prices[ticker["symbol"]] = float(ticker["lastPrice"])

        elif EXCHANGE == "binance":
            response = requests.get(
                f"{BASE_URL}/fapi/v1/ticker/price",
                timeout=5
            )
            for ticker in response.json():
                if ticker["symbol"] in symbols:
                    prices[ticker["symbol"]] = float(ticker["price"])

    except Exception as e:
        print(f"Error getting prices: {e}")

        # Fallback to unified_klines
        for symbol in symbols:
            try:
                latest = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.unified_klines_15m") \
                    .filter(F.col("symbol") == symbol) \
                    .orderBy(F.col("timestamp").desc()) \
                    .select("close").first()
                if latest:
                    prices[symbol] = float(latest["close"])
            except:
                pass

    return prices


# ---
# POSITION MONITORING
# ---

def check_position_status(position: dict, current_price: float) -> dict:
    """Check if TP or SL was hit"""

    side = position["side"]
    entry = position["entry_price"]
    sl = position["stop_loss"]
    tp1 = position["take_profit_1"]
    tp2 = position["take_profit_2"]
    tp1_hit = position["tp1_hit"]
    remaining_qty = position["remaining_qty"]

    status = {
        "tp1_triggered": False,
        "tp2_triggered": False,
        "sl_triggered": False,
        "new_sl": sl,
        "exit_price": None,
        "exit_reason": None
    }

    if side == "long":
        # Check SL
        if current_price <= sl:
            status["sl_triggered"] = True
            status["exit_price"] = sl
            status["exit_reason"] = "stop_loss"
            return status

        # Check TP1
        if not tp1_hit and current_price >= tp1:
            status["tp1_triggered"] = True
            status["new_sl"] = entry  # Move to breakeven

        # Check TP2
        if current_price >= tp2:
            status["tp2_triggered"] = True
            status["exit_price"] = tp2
            status["exit_reason"] = "take_profit_2" if tp1_hit else "take_profit_full"
            return status

    else:  # short
        # Check SL
        if current_price >= sl:
            status["sl_triggered"] = True
            status["exit_price"] = sl
            status["exit_reason"] = "stop_loss"
            return status

        # Check TP1
        if not tp1_hit and current_price <= tp1:
            status["tp1_triggered"] = True
            status["new_sl"] = entry  # Move to breakeven

        # Check TP2
        if current_price <= tp2:
            status["tp2_triggered"] = True
            status["exit_price"] = tp2
            status["exit_reason"] = "take_profit_2" if tp1_hit else "take_profit_full"
            return status

    return status


def calculate_pnl(position: dict, current_price: float) -> tuple:
    """Calculate unrealized P&L"""
    side = position["side"]
    entry = position["entry_price"]
    qty = position["remaining_qty"]

    if side == "long":
        pnl = (current_price - entry) * qty
        pnl_pct = ((current_price - entry) / entry) * 100
    else:
        pnl = (entry - current_price) * qty
        pnl_pct = ((entry - current_price) / entry) * 100

    return round(pnl, 2), round(pnl_pct, 2)


def close_position(position: dict, exit_price: float, exit_reason: str):
    """Move position from open to closed"""

    positions_open = f"{CATALOG}.{TRADING_SCHEMA}.positions_open"
    positions_closed = f"{CATALOG}.{TRADING_SCHEMA}.positions_closed"
    signals_table = f"{CATALOG}.{GOLD_SCHEMA}.signals_all"

    # Calculate final P&L
    side = position["side"]
    entry = position["entry_price"]
    qty = position["quantity"]
    entry_time = position["entry_time"]

    if side == "long":
        realized_pnl = (exit_price - entry) * qty
        realized_pnl_pct = ((exit_price - entry) / entry) * 100
    else:
        realized_pnl = (entry - exit_price) * qty
        realized_pnl_pct = ((entry - exit_price) / entry) * 100

    # Fees (estimate 0.04% each way)
    fees = (entry * qty * 0.0004) + (exit_price * qty * 0.0004)
    net_pnl = realized_pnl - fees

    # Hold duration
    now = datetime.now(timezone.utc)
    if isinstance(entry_time, datetime):
        hold_minutes = int((now - entry_time).total_seconds() / 60)
    else:
        hold_minutes = 0

    # Determine if AI was correct
    ai_was_correct = net_pnl > 0

    # Create closed position record
    closed_record = {
        "position_id": position["position_id"],
        "signal_id": position["signal_id"],
        "symbol": position["symbol"],
        "side": side,
        "strategy": position["strategy"],
        "timeframe": None,
        "entry_price": entry,
        "entry_time": entry_time,
        "quantity": qty,
        "leverage": position["leverage"],
        "exit_price": exit_price,
        "exit_time": now,
        "exit_reason": exit_reason,
        "realized_pnl": round(realized_pnl, 2),
        "realized_pnl_pct": round(realized_pnl_pct, 2),
        "fees": round(fees, 2),
        "net_pnl": round(net_pnl, 2),
        "sl_price": position["stop_loss"],
        "tp1_price": position["take_profit_1"],
        "tp2_price": position["take_profit_2"],
        "tp1_hit": position["tp1_hit"],
        "tp2_hit": exit_reason in ["take_profit_2", "take_profit_full"],
        "sl_hit": exit_reason == "stop_loss",
        "hold_duration_minutes": hold_minutes,
        "ai_confidence": None,
        "ai_was_correct": ai_was_correct,
        "exchange": position["exchange"],
        "closed_at": now,
        "date": now.date()
    }

    # Write to positions_closed
    schema = StructType([
        StructField("position_id", StringType()),
        StructField("signal_id", StringType()),
        StructField("symbol", StringType()),
        StructField("side", StringType()),
        StructField("strategy", StringType()),
        StructField("timeframe", StringType()),
        StructField("entry_price", DoubleType()),
        StructField("entry_time", TimestampType()),
        StructField("quantity", DoubleType()),
        StructField("leverage", IntegerType()),
        StructField("exit_price", DoubleType()),
        StructField("exit_time", TimestampType()),
        StructField("exit_reason", StringType()),
        StructField("realized_pnl", DoubleType()),
        StructField("realized_pnl_pct", DoubleType()),
        StructField("fees", DoubleType()),
        StructField("net_pnl", DoubleType()),
        StructField("sl_price", DoubleType()),
        StructField("tp1_price", DoubleType()),
        StructField("tp2_price", DoubleType()),
        StructField("tp1_hit", BooleanType()),
        StructField("tp2_hit", BooleanType()),
        StructField("sl_hit", BooleanType()),
        StructField("hold_duration_minutes", IntegerType()),
        StructField("ai_confidence", DoubleType()),
        StructField("ai_was_correct", BooleanType()),
        StructField("exchange", StringType()),
        StructField("closed_at", TimestampType()),
        StructField("date", DateType())
    ])

    closed_df = spark.createDataFrame([closed_record], schema)
    closed_df.write.format("delta").mode("append").saveAsTable(positions_closed)

    # Delete from positions_open
    spark.sql(f"""
        DELETE FROM {positions_open}
        WHERE position_id = '{position["position_id"]}'
    """)

    # Update signal status
    spark.sql(f"""
        UPDATE {signals_table}
        SET execution_status = '{exit_reason}'
        WHERE signal_id = '{position["signal_id"]}'
    """)

    return closed_record


# ---
# MAIN MONITORING LOOP
# ---

def monitor_positions():
    """Monitor all open positions"""

    positions_table = f"{CATALOG}.{TRADING_SCHEMA}.positions_open"

    # Get open positions
    try:
        positions = spark.table(positions_table).collect()
    except:
        print("No positions table or empty")
        return

    if not positions:
        print("No open positions to monitor")
        return

    print(f"\nMonitoring {len(positions)} positions")

    # Get all symbols
    symbols = list(set(p["symbol"] for p in positions))
    current_prices = get_current_prices(symbols)

    print(f"Got prices for {len(current_prices)} symbols")

    updates = []
    closed = []

    for pos in positions:
        pos_dict = pos.asDict()
        symbol = pos_dict["symbol"]
        position_id = pos_dict["position_id"]

        if symbol not in current_prices:
            print(f"  [{position_id[:8]}] {symbol} - No price data")
            continue

        current_price = current_prices[symbol]

        # Check status
        status = check_position_status(pos_dict, current_price)

        # Calculate P&L
        pnl, pnl_pct = calculate_pnl(pos_dict, current_price)

        print(f"  [{position_id[:8]}] {symbol} {pos_dict['side']} @ {current_price:.2f} "
              f"| P&L: {pnl_pct:+.2f}%", end="")

        if status["sl_triggered"] or status["tp2_triggered"]:
            # Position closed
            reason = status["exit_reason"]
            exit_price = status["exit_price"]
            print(f" -> CLOSED ({reason})")

            closed_record = close_position(pos_dict, exit_price, reason)
            closed.append(closed_record)

        elif status["tp1_triggered"]:
            # TP1 hit - partial close and move SL
            print(f" -> TP1 HIT, SL -> BE")

            updates.append({
                "position_id": position_id,
                "tp1_hit": True,
                "stop_loss": status["new_sl"],
                "remaining_qty": pos_dict["quantity"] * (100 - TP1_CLOSE_PCT) / 100,
                "current_price": current_price,
                "unrealized_pnl": pnl,
                "unrealized_pnl_pct": pnl_pct,
                "updated_at": datetime.now(timezone.utc)
            })

        else:
            # Just update price
            print("")
            updates.append({
                "position_id": position_id,
                "tp1_hit": pos_dict["tp1_hit"],
                "stop_loss": pos_dict["stop_loss"],
                "remaining_qty": pos_dict["remaining_qty"],
                "current_price": current_price,
                "unrealized_pnl": pnl,
                "unrealized_pnl_pct": pnl_pct,
                "updated_at": datetime.now(timezone.utc)
            })

    # Apply updates
    if updates:
        schema = StructType([
            StructField("position_id", StringType()),
            StructField("tp1_hit", BooleanType()),
            StructField("stop_loss", DoubleType()),
            StructField("remaining_qty", DoubleType()),
            StructField("current_price", DoubleType()),
            StructField("unrealized_pnl", DoubleType()),
            StructField("unrealized_pnl_pct", DoubleType()),
            StructField("updated_at", TimestampType())
        ])

        updates_df = spark.createDataFrame(updates, schema)

        delta_table = DeltaTable.forName(spark, positions_table)
        delta_table.alias("t").merge(
            updates_df.alias("s"),
            "t.position_id = s.position_id"
        ).whenMatchedUpdate(set={
            "tp1_hit": "s.tp1_hit",
            "stop_loss": "s.stop_loss",
            "remaining_qty": "s.remaining_qty",
            "current_price": "s.current_price",
            "unrealized_pnl": "s.unrealized_pnl",
            "unrealized_pnl_pct": "s.unrealized_pnl_pct",
            "updated_at": "s.updated_at"
        }).execute()

        print(f"\n✓ Updated {len(updates)} positions")

    if closed:
        print(f"✓ Closed {len(closed)} positions")

        # Show closed positions
        for c in closed:
            emoji = "✓" if c["net_pnl"] > 0 else "✗"
            print(f"  {emoji} {c['symbol']}: {c['net_pnl']:+.2f} ({c['exit_reason']})")


# ---
# UPDATE DAILY P&L
# ---

def update_daily_pnl():
    """Update daily P&L table"""

    closed_table = f"{CATALOG}.{TRADING_SCHEMA}.positions_closed"
    daily_table = f"{CATALOG}.{TRADING_SCHEMA}.daily_pnl"

    today = datetime.now(timezone.utc).date()

    try:
        # Get today's stats
        today_stats = spark.sql(f"""
            SELECT
                COUNT(*) as trades_closed,
                SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) as trades_won,
                SUM(CASE WHEN net_pnl <= 0 THEN 1 ELSE 0 END) as trades_lost,
                SUM(CASE WHEN net_pnl > 0 THEN net_pnl ELSE 0 END) as gross_profit,
                SUM(CASE WHEN net_pnl <= 0 THEN ABS(net_pnl) ELSE 0 END) as gross_loss,
                SUM(net_pnl) as net_pnl,
                SUM(fees) as fees,
                AVG(CASE WHEN net_pnl > 0 THEN net_pnl END) as avg_win,
                AVG(CASE WHEN net_pnl <= 0 THEN net_pnl END) as avg_loss,
                SUM(CASE WHEN strategy = 'smc' THEN net_pnl ELSE 0 END) as smc_pnl,
                SUM(CASE WHEN strategy = 'smc' THEN 1 ELSE 0 END) as smc_trades,
                SUM(CASE WHEN strategy = 'liquidity' THEN net_pnl ELSE 0 END) as liq_pnl,
                SUM(CASE WHEN strategy = 'liquidity' THEN 1 ELSE 0 END) as liq_trades
            FROM {closed_table}
            WHERE date = '{today}'
        """).first()

        if today_stats and today_stats["trades_closed"] > 0:
            trades_closed = today_stats["trades_closed"]
            trades_won = today_stats["trades_won"] or 0
            trades_lost = today_stats["trades_lost"] or 0
            win_rate = trades_won / trades_closed if trades_closed > 0 else 0
            gross_profit = today_stats["gross_profit"] or 0
            gross_loss = today_stats["gross_loss"] or 0
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0

            # Get cumulative P&L
            cumulative = spark.sql(f"""
                SELECT SUM(net_pnl) as total
                FROM {closed_table}
            """).first()["total"] or 0

            # Get max drawdown (simplified - from cumulative)
            max_dd = spark.sql(f"""
                WITH running AS (
                    SELECT
                        date,
                        SUM(net_pnl) OVER (ORDER BY date) as cum_pnl
                    FROM {closed_table}
                ),
                peaks AS (
                    SELECT
                        date,
                        cum_pnl,
                        MAX(cum_pnl) OVER (ORDER BY date) as peak
                    FROM running
                )
                SELECT MAX(peak - cum_pnl) as max_dd FROM peaks
            """).first()["max_dd"] or 0

            # Calculate win rates by strategy
            smc_win_rate = 0
            liq_win_rate = 0

            smc_wins = spark.sql(f"""
                SELECT COUNT(*) as wins
                FROM {closed_table}
                WHERE date = '{today}' AND strategy = 'smc' AND net_pnl > 0
            """).first()["wins"] or 0
            if today_stats["smc_trades"] > 0:
                smc_win_rate = smc_wins / today_stats["smc_trades"]

            liq_wins = spark.sql(f"""
                SELECT COUNT(*) as wins
                FROM {closed_table}
                WHERE date = '{today}' AND strategy = 'liquidity' AND net_pnl > 0
            """).first()["wins"] or 0
            if today_stats["liq_trades"] > 0:
                liq_win_rate = liq_wins / today_stats["liq_trades"]

            # Merge into daily_pnl
            daily_record = {
                "date": today,
                "trades_opened": 0,  # Would need to track from signals
                "trades_closed": trades_closed,
                "trades_won": trades_won,
                "trades_lost": trades_lost,
                "gross_pnl": gross_profit - gross_loss,
                "fees": today_stats["fees"] or 0,
                "net_pnl": today_stats["net_pnl"] or 0,
                "cumulative_pnl": cumulative,
                "win_rate": win_rate,
                "avg_win": today_stats["avg_win"] or 0,
                "avg_loss": today_stats["avg_loss"] or 0,
                "profit_factor": profit_factor,
                "smc_pnl": today_stats["smc_pnl"] or 0,
                "smc_trades": today_stats["smc_trades"] or 0,
                "smc_win_rate": smc_win_rate,
                "liq_pnl": today_stats["liq_pnl"] or 0,
                "liq_trades": today_stats["liq_trades"] or 0,
                "liq_win_rate": liq_win_rate,
                "max_drawdown": max_dd,
                "sharpe_ratio": 0,  # Requires more data
                "updated_at": datetime.now(timezone.utc)
            }

            schema = StructType([
                StructField("date", DateType()),
                StructField("trades_opened", IntegerType()),
                StructField("trades_closed", IntegerType()),
                StructField("trades_won", IntegerType()),
                StructField("trades_lost", IntegerType()),
                StructField("gross_pnl", DoubleType()),
                StructField("fees", DoubleType()),
                StructField("net_pnl", DoubleType()),
                StructField("cumulative_pnl", DoubleType()),
                StructField("win_rate", DoubleType()),
                StructField("avg_win", DoubleType()),
                StructField("avg_loss", DoubleType()),
                StructField("profit_factor", DoubleType()),
                StructField("smc_pnl", DoubleType()),
                StructField("smc_trades", IntegerType()),
                StructField("smc_win_rate", DoubleType()),
                StructField("liq_pnl", DoubleType()),
                StructField("liq_trades", IntegerType()),
                StructField("liq_win_rate", DoubleType()),
                StructField("max_drawdown", DoubleType()),
                StructField("sharpe_ratio", DoubleType()),
                StructField("updated_at", TimestampType())
            ])

            daily_df = spark.createDataFrame([daily_record], schema)

            delta_table = DeltaTable.forName(spark, daily_table)
            delta_table.alias("t").merge(
                daily_df.alias("s"),
                "t.date = s.date"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            print(f"\n✓ Updated daily P&L for {today}")

    except Exception as e:
        print(f"Error updating daily P&L: {e}")


# ---
# RUN
# ---

monitor_positions()
update_daily_pnl()

# Summary
print("\n" + "=" * 60)
print("POSITION SUMMARY")
print("=" * 60)

try:
    spark.sql(f"""
        SELECT
            symbol,
            side,
            ROUND(entry_price, 2) as entry,
            ROUND(current_price, 2) as current,
            ROUND(stop_loss, 2) as sl,
            ROUND(take_profit_1, 2) as tp1,
            tp1_hit,
            ROUND(unrealized_pnl_pct, 2) as pnl_pct
        FROM {CATALOG}.{TRADING_SCHEMA}.positions_open
    """).show(truncate=False)
except:
    print("No open positions")

print("\n✓ Position manager complete")
