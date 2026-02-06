# Databricks notebook source
# ---
# BACKTESTING MODULE
# Runs strategies on historical data from silver
# Metrics: Sharpe, Max Drawdown, Win Rate, Profit Factor, Equity Curve
# Output: trading.backtest_history_results, trading.backtest_history_trades, trading.backtest_history_equity
# ---

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional
import math

# ---
# CONFIGURATION
# ---

CATALOG = "crypto"
SILVER_SCHEMA = "silver"
TRADING_SCHEMA = "trading"

# Dynamic start_date: earliest data available in unified_klines_15m
_min_ts_row = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.unified_klines_15m") \
    .select(F.min("timestamp").alias("min_ts")).first()
_dynamic_start = datetime.fromtimestamp(_min_ts_row["min_ts"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d") \
    if _min_ts_row and _min_ts_row["min_ts"] else "2024-01-01"

# Backtest parameters (can be overridden)
BACKTEST_CONFIG = {
    "start_date": _dynamic_start,
    "end_date": None,  # None = up to now
    "initial_capital": 10000,
    "risk_per_trade_pct": 1.0,
    "max_leverage": 10,
    "commission_pct": 0.04,  # 0.04% taker fee
    "slippage_pct": 0.02,    # 0.02% slippage
    "symbols": ["BTCUSDT", "ETHUSDT", "LDOUSDT", "LINKUSDT"],
    "timeframes": ["15m", "1h", "4h", "1d"],
    "strategies": ["smc", "liquidity"],

    # Strategy params
    "min_zone_strength": 70,
    "min_confluence_score": 4,
    "require_bos": True,
    "sl_atr_multiplier": 1.5,
    "tp1_rr_ratio": 1.5,
    "tp2_rr_ratio": 3.0,
    "tp1_close_pct": 50,
}

print("=" * 80)
print("BACKTESTING MODULE")
print("=" * 80)
print(f"Period: {BACKTEST_CONFIG['start_date']} to {BACKTEST_CONFIG['end_date'] or 'now'}")
print(f"Initial Capital: ${BACKTEST_CONFIG['initial_capital']:,}")
print(f"Symbols: {BACKTEST_CONFIG['symbols']}")
print(f"Timeframes: {BACKTEST_CONFIG['timeframes']}")
print(f"Strategies: {BACKTEST_CONFIG['strategies']}")
print("=" * 80)

# ---
# CREATE RESULTS TABLES
# ---

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TRADING_SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{TRADING_SCHEMA}.backtest_history_results (
    backtest_id STRING,
    run_timestamp TIMESTAMP,

    -- Config
    strategy STRING,
    symbol STRING,
    timeframe STRING,
    start_date DATE,
    end_date DATE,
    initial_capital DOUBLE,
    risk_per_trade_pct DOUBLE,

    -- Trade Stats
    total_trades INT,
    winning_trades INT,
    losing_trades INT,
    win_rate DOUBLE,

    -- P&L Metrics
    gross_profit DOUBLE,
    gross_loss DOUBLE,
    net_profit DOUBLE,
    net_profit_pct DOUBLE,
    profit_factor DOUBLE,

    -- Risk Metrics
    max_drawdown DOUBLE,
    max_drawdown_pct DOUBLE,
    avg_trade_pnl DOUBLE,
    avg_win DOUBLE,
    avg_loss DOUBLE,
    largest_win DOUBLE,
    largest_loss DOUBLE,

    -- Ratios
    sharpe_ratio DOUBLE,
    sortino_ratio DOUBLE,
    calmar_ratio DOUBLE,

    -- Time Metrics
    avg_hold_time_hours DOUBLE,
    max_consecutive_wins INT,
    max_consecutive_losses INT
)
USING DELTA
PARTITIONED BY (strategy)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{TRADING_SCHEMA}.backtest_history_trades (
    backtest_id STRING,
    strategy STRING,
    symbol STRING,
    timeframe STRING,

    trade_number INT,
    type STRING,

    entry_time BIGINT,
    entry_price DOUBLE,
    exit_time BIGINT,
    exit_price DOUBLE,

    stop_loss DOUBLE,
    take_profit_1 DOUBLE,
    take_profit_2 DOUBLE,

    position_size DOUBLE,
    exit_reason STRING,

    pnl DOUBLE,
    pnl_pct DOUBLE,
    commission DOUBLE,

    hold_time_hours DOUBLE,
    tp1_hit BOOLEAN,

    zone_strength DOUBLE,

    volume_at_entry DOUBLE,
    volume_at_exit DOUBLE,
    avg_volume_before DOUBLE,
    avg_volume_during DOUBLE,
    volume_ratio_entry DOUBLE,

    run_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (strategy)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{TRADING_SCHEMA}.backtest_history_equity (
    backtest_id STRING,
    strategy STRING,
    symbol STRING,
    timeframe STRING,

    timestamp BIGINT,
    equity DOUBLE,
    capital DOUBLE,
    drawdown_pct DOUBLE,

    run_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (strategy)
""")

print("✓ Tables trading.backtest_history_results, backtest_history_trades, backtest_history_equity ready")

# ---
# DATA LOADING
# ---

def load_historical_klines(symbol: str, timeframe: str, start_date: str, end_date: str = None):
    """Load historical klines data"""
    table = f"{CATALOG}.{SILVER_SCHEMA}.unified_klines_{timeframe}"

    start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)

    if end_date:
        end_ms = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)
    else:
        end_ms = int(datetime.now().timestamp() * 1000)

    df = spark.table(table).filter(
        (F.col("symbol") == symbol) &
        (F.col("timestamp") >= start_ms) &
        (F.col("timestamp") <= end_ms)
    ).groupBy("symbol", "timestamp").agg(
        F.avg("open").alias("open"),
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.avg("close").alias("close"),
        F.sum("volume").alias("volume")
    ).orderBy("timestamp")

    return df


def load_historical_zones(symbol: str, timeframe: str, start_date: str, end_date: str = None):
    """Load historical liquidity zones"""
    table = f"{CATALOG}.{SILVER_SCHEMA}.liq_{timeframe}"

    try:
        if not spark.catalog.tableExists(table):
            return None

        df = spark.table(table).filter(
            (F.col("symbol") == symbol) &
            (F.col("strength_score") >= BACKTEST_CONFIG["min_zone_strength"])
        )

        if BACKTEST_CONFIG["require_bos"]:
            df = df.filter(F.col("bos_confirmed") == True)

        return [row.asDict() for row in df.collect()]
    except:
        return None


def load_historical_bos(symbol: str, timeframe: str, start_date: str, end_date: str = None):
    """Load historical BOS signals"""
    table = f"{CATALOG}.{SILVER_SCHEMA}.bos_warnings"

    start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)

    if end_date:
        end_ms = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)
    else:
        end_ms = int(datetime.now().timestamp() * 1000)

    try:
        if not spark.catalog.tableExists(table):
            return []

        df = spark.table(table).filter(
            (F.col("symbol") == symbol) &
            (F.col("timeframe") == timeframe) &
            (F.col("timestamp") >= start_ms) &
            (F.col("timestamp") <= end_ms)
        ).orderBy("timestamp")

        return [row.asDict() for row in df.collect()]
    except:
        return []


# ---
# INDICATOR CALCULATIONS
# ---

def calculate_atr_series(klines_list: List[Dict], period: int = 14) -> Dict[int, float]:
    """Calculate ATR for each candle"""
    atr_map = {}
    tr_values = []

    for i, candle in enumerate(klines_list):
        if i == 0:
            tr = candle["high"] - candle["low"]
        else:
            prev_close = klines_list[i-1]["close"]
            tr = max(
                candle["high"] - candle["low"],
                abs(candle["high"] - prev_close),
                abs(candle["low"] - prev_close)
            )

        tr_values.append(tr)

        if len(tr_values) >= period:
            atr = sum(tr_values[-period:]) / period
            atr_map[candle["timestamp"]] = atr

    return atr_map


def identify_swing_points(klines_list: List[Dict], lookback: int = 3) -> Tuple[Dict, Dict]:
    """Identify swing highs and lows"""
    swing_highs = {}  # timestamp -> price
    swing_lows = {}

    for i in range(lookback, len(klines_list) - lookback):
        candle = klines_list[i]

        # Check swing high
        is_swing_high = True
        for j in range(1, lookback + 1):
            if (klines_list[i-j]["high"] >= candle["high"] or
                klines_list[i+j]["high"] >= candle["high"]):
                is_swing_high = False
                break

        if is_swing_high:
            swing_highs[candle["timestamp"]] = candle["high"]

        # Check swing low
        is_swing_low = True
        for j in range(1, lookback + 1):
            if (klines_list[i-j]["low"] <= candle["low"] or
                klines_list[i+j]["low"] <= candle["low"]):
                is_swing_low = False
                break

        if is_swing_low:
            swing_lows[candle["timestamp"]] = candle["low"]

    return swing_highs, swing_lows


# ---
# SIGNAL GENERATION (HISTORICAL)
# ---

def generate_smc_signals(klines_list: List[Dict], bos_list: List, zones: List,
                         atr_map: Dict, swing_highs: Dict, swing_lows: Dict) -> List[Dict]:
    """Generate SMC signals from historical data"""
    signals = []

    # Convert BOS to dict for faster lookup
    bos_by_ts = {b["timestamp"]: b for b in bos_list}

    # Track last swing levels
    last_swing_high = None
    last_swing_low = None
    last_swing_high_ts = None
    last_swing_low_ts = None

    for i, candle in enumerate(klines_list):
        ts = candle["timestamp"]
        close = candle["close"]

        # Update swing levels
        if ts in swing_highs:
            last_swing_high = swing_highs[ts]
            last_swing_high_ts = ts
        if ts in swing_lows:
            last_swing_low = swing_lows[ts]
            last_swing_low_ts = ts

        # Check for BOS at this candle
        if ts in bos_by_ts:
            bos = bos_by_ts[ts]
            bos_type = bos["bos_type"]

            # Find nearest zone
            nearest_zone = None
            min_distance = float('inf')

            for zone in zones or []:
                zone_mid = (zone["zone_low"] + zone["zone_high"]) / 2
                distance = abs(close - zone_mid) / close * 100

                if distance < min_distance and distance < 2.0:  # Within 2%
                    min_distance = distance
                    nearest_zone = zone

            if nearest_zone:
                atr = atr_map.get(ts, (candle["high"] - candle["low"]))

                # SMC signal logic
                if bos_type == "bullish" and last_swing_low:
                    # Look for long on retest of broken level
                    entry = nearest_zone["poc_price"]
                    sl = entry - (atr * BACKTEST_CONFIG["sl_atr_multiplier"])
                    risk = entry - sl
                    tp1 = entry + (risk * BACKTEST_CONFIG["tp1_rr_ratio"])
                    tp2 = entry + (risk * BACKTEST_CONFIG["tp2_rr_ratio"])

                    signals.append({
                        "timestamp": ts,
                        "type": "long",
                        "entry": entry,
                        "stop_loss": sl,
                        "take_profit_1": tp1,
                        "take_profit_2": tp2,
                        "zone_strength": nearest_zone.get("strength_score", 50),
                        "strategy": "smc"
                    })

                elif bos_type == "bearish" and last_swing_high:
                    # Look for short on retest of broken level
                    entry = nearest_zone["poc_price"]
                    sl = entry + (atr * BACKTEST_CONFIG["sl_atr_multiplier"])
                    risk = sl - entry
                    tp1 = entry - (risk * BACKTEST_CONFIG["tp1_rr_ratio"])
                    tp2 = entry - (risk * BACKTEST_CONFIG["tp2_rr_ratio"])

                    signals.append({
                        "timestamp": ts,
                        "type": "short",
                        "entry": entry,
                        "stop_loss": sl,
                        "take_profit_1": tp1,
                        "take_profit_2": tp2,
                        "zone_strength": nearest_zone.get("strength_score", 50),
                        "strategy": "smc"
                    })

    return signals


def generate_liquidity_signals(klines_list: List[Dict], zones: List, atr_map: Dict) -> List[Dict]:
    """Generate pure liquidity zone signals"""
    signals = []

    if not zones:
        return signals

    # Sort zones by strength
    sorted_zones = sorted(zones, key=lambda z: z.get("strength_score", 0), reverse=True)
    top_zones = sorted_zones[:10]  # Top 10 zones

    for i, candle in enumerate(klines_list):
        if i < 5:  # Need some history
            continue

        ts = candle["timestamp"]
        close = candle["close"]
        low = candle["low"]
        high = candle["high"]
        prev_close = klines_list[i-1]["close"]

        atr = atr_map.get(ts, (high - low))

        for zone in top_zones:
            zone_low = zone["zone_low"]
            zone_high = zone["zone_high"]
            zone_mid = (zone_low + zone_high) / 2
            zone_role = zone.get("zone_role", "")

            # Check if price touched zone
            touches_zone = (low <= zone_high) and (high >= zone_low)

            if not touches_zone:
                continue

            # Long signal: price touches support zone from above
            if zone_role in ["support", "mirror"] and prev_close > zone_high and low <= zone_high:
                entry = zone_mid
                sl = zone_low - (atr * 0.5)
                risk = entry - sl
                tp1 = entry + (risk * BACKTEST_CONFIG["tp1_rr_ratio"])
                tp2 = entry + (risk * BACKTEST_CONFIG["tp2_rr_ratio"])

                signals.append({
                    "timestamp": ts,
                    "type": "long",
                    "entry": entry,
                    "stop_loss": sl,
                    "take_profit_1": tp1,
                    "take_profit_2": tp2,
                    "zone_strength": zone.get("strength_score", 50),
                    "strategy": "liquidity"
                })

            # Short signal: price touches resistance zone from below
            elif zone_role in ["resistance", "mirror"] and prev_close < zone_low and high >= zone_low:
                entry = zone_mid
                sl = zone_high + (atr * 0.5)
                risk = sl - entry
                tp1 = entry - (risk * BACKTEST_CONFIG["tp1_rr_ratio"])
                tp2 = entry - (risk * BACKTEST_CONFIG["tp2_rr_ratio"])

                signals.append({
                    "timestamp": ts,
                    "type": "short",
                    "entry": entry,
                    "stop_loss": sl,
                    "take_profit_1": tp1,
                    "take_profit_2": tp2,
                    "zone_strength": zone.get("strength_score", 50),
                    "strategy": "liquidity"
                })

    return signals


# ---
# TRADE SIMULATION
# ---

class Trade:
    def __init__(self, signal: Dict, entry_price: float, position_size: float,
                 commission_pct: float, slippage_pct: float):
        self.signal = signal
        self.entry_time = signal["timestamp"]
        self.type = signal["type"]
        self.entry_price = entry_price * (1 + slippage_pct/100 if self.type == "long" else 1 - slippage_pct/100)
        self.stop_loss = signal["stop_loss"]
        self.take_profit_1 = signal["take_profit_1"]
        self.take_profit_2 = signal["take_profit_2"]
        self.position_size = position_size
        self.commission_pct = commission_pct
        self.remaining_size = position_size
        self.tp1_hit = False
        self.exit_price = None
        self.exit_time = None
        self.exit_reason = None
        self.pnl = 0
        self.commission = position_size * entry_price * commission_pct / 100

    def check_exit(self, candle: Dict) -> bool:
        """Check if trade should exit on this candle"""
        high = candle["high"]
        low = candle["low"]
        ts = candle["timestamp"]

        if self.type == "long":
            # Check stop loss
            if low <= self.stop_loss:
                self.exit_price = self.stop_loss
                self.exit_time = ts
                self.exit_reason = "stop_loss"
                self._calculate_pnl()
                return True

            # Check TP1
            if not self.tp1_hit and high >= self.take_profit_1:
                self.tp1_hit = True
                partial_pnl = (self.take_profit_1 - self.entry_price) * (self.position_size * BACKTEST_CONFIG["tp1_close_pct"] / 100)
                self.pnl += partial_pnl
                self.remaining_size = self.position_size * (100 - BACKTEST_CONFIG["tp1_close_pct"]) / 100
                # Move SL to breakeven
                self.stop_loss = self.entry_price

            # Check TP2
            if high >= self.take_profit_2:
                self.exit_price = self.take_profit_2
                self.exit_time = ts
                self.exit_reason = "take_profit_2" if self.tp1_hit else "take_profit_1"
                self._calculate_pnl()
                return True

        else:  # short
            # Check stop loss
            if high >= self.stop_loss:
                self.exit_price = self.stop_loss
                self.exit_time = ts
                self.exit_reason = "stop_loss"
                self._calculate_pnl()
                return True

            # Check TP1
            if not self.tp1_hit and low <= self.take_profit_1:
                self.tp1_hit = True
                partial_pnl = (self.entry_price - self.take_profit_1) * (self.position_size * BACKTEST_CONFIG["tp1_close_pct"] / 100)
                self.pnl += partial_pnl
                self.remaining_size = self.position_size * (100 - BACKTEST_CONFIG["tp1_close_pct"]) / 100
                self.stop_loss = self.entry_price

            # Check TP2
            if low <= self.take_profit_2:
                self.exit_price = self.take_profit_2
                self.exit_time = ts
                self.exit_reason = "take_profit_2" if self.tp1_hit else "take_profit_1"
                self._calculate_pnl()
                return True

        return False

    def _calculate_pnl(self):
        """Calculate final P&L"""
        if self.type == "long":
            remaining_pnl = (self.exit_price - self.entry_price) * self.remaining_size
        else:
            remaining_pnl = (self.entry_price - self.exit_price) * self.remaining_size

        self.pnl += remaining_pnl
        # Subtract commission for exit
        self.commission += self.remaining_size * self.exit_price * self.commission_pct / 100
        self.pnl -= self.commission


def simulate_trades(klines_list: List[Dict], signals: List[Dict],
                    initial_capital: float, risk_pct: float,
                    commission_pct: float, slippage_pct: float) -> Tuple[List[Trade], List[Dict]]:
    """Simulate trades and generate equity curve"""

    trades = []
    equity_curve = []
    capital = initial_capital
    open_trade = None

    # Create timestamp -> candle map
    candle_map = {c["timestamp"]: c for c in klines_list}

    # Sort signals by timestamp
    sorted_signals = sorted(signals, key=lambda s: s["timestamp"])
    signal_idx = 0

    for candle in klines_list:
        ts = candle["timestamp"]

        # Check open trade
        if open_trade:
            if open_trade.check_exit(candle):
                capital += open_trade.pnl
                trades.append(open_trade)
                open_trade = None

        # Check for new signals (only if no open trade)
        if not open_trade and signal_idx < len(sorted_signals):
            signal = sorted_signals[signal_idx]

            if signal["timestamp"] <= ts:
                signal_idx += 1

                # Calculate position size based on risk
                risk_amount = capital * risk_pct / 100
                entry = signal["entry"]
                sl = signal["stop_loss"]
                risk_per_unit = abs(entry - sl)

                if risk_per_unit > 0:
                    position_size = risk_amount / risk_per_unit

                    # Create trade
                    open_trade = Trade(signal, entry, position_size, commission_pct, slippage_pct)

        # Record equity
        unrealized_pnl = 0
        if open_trade:
            if open_trade.type == "long":
                unrealized_pnl = (candle["close"] - open_trade.entry_price) * open_trade.remaining_size
            else:
                unrealized_pnl = (open_trade.entry_price - candle["close"]) * open_trade.remaining_size

        equity_curve.append({
            "timestamp": ts,
            "equity": capital + unrealized_pnl,
            "capital": capital
        })

    # Close any remaining trade at last price
    if open_trade:
        last_candle = klines_list[-1]
        open_trade.exit_price = last_candle["close"]
        open_trade.exit_time = last_candle["timestamp"]
        open_trade.exit_reason = "end_of_backtest"
        open_trade._calculate_pnl()
        capital += open_trade.pnl
        trades.append(open_trade)

    return trades, equity_curve


# ---
# METRICS CALCULATION
# ---

def calculate_metrics(trades: List[Trade], equity_curve: List[Dict],
                      initial_capital: float, days: int) -> Dict:
    """Calculate all backtest metrics"""

    if not trades:
        return {
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "win_rate": 0,
            "gross_profit": 0,
            "gross_loss": 0,
            "net_profit": 0,
            "net_profit_pct": 0,
            "profit_factor": 0,
            "max_drawdown": 0,
            "max_drawdown_pct": 0,
            "avg_trade_pnl": 0,
            "avg_win": 0,
            "avg_loss": 0,
            "largest_win": 0,
            "largest_loss": 0,
            "sharpe_ratio": 0,
            "sortino_ratio": 0,
            "calmar_ratio": 0,
            "avg_hold_time_hours": 0,
            "max_consecutive_wins": 0,
            "max_consecutive_losses": 0
        }

    # Basic stats
    winning_trades = [t for t in trades if t.pnl > 0]
    losing_trades = [t for t in trades if t.pnl <= 0]

    gross_profit = sum(t.pnl for t in winning_trades)
    gross_loss = abs(sum(t.pnl for t in losing_trades))
    net_profit = gross_profit - gross_loss

    # Win rate
    win_rate = len(winning_trades) / len(trades) * 100 if trades else 0

    # Profit factor
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

    # Average P&L
    avg_trade_pnl = net_profit / len(trades) if trades else 0
    avg_win = gross_profit / len(winning_trades) if winning_trades else 0
    avg_loss = gross_loss / len(losing_trades) if losing_trades else 0

    # Largest win/loss
    largest_win = max(t.pnl for t in trades) if trades else 0
    largest_loss = min(t.pnl for t in trades) if trades else 0

    # Max drawdown
    peak = initial_capital
    max_dd = 0
    max_dd_pct = 0

    for point in equity_curve:
        equity = point["equity"]
        if equity > peak:
            peak = equity
        dd = peak - equity
        dd_pct = dd / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
            max_dd_pct = dd_pct

    # Returns for Sharpe/Sortino
    returns = []
    for i in range(1, len(equity_curve)):
        prev_eq = equity_curve[i-1]["equity"]
        curr_eq = equity_curve[i]["equity"]
        if prev_eq > 0:
            returns.append((curr_eq - prev_eq) / prev_eq)

    # Sharpe Ratio (annualized, assuming 365 trading days)
    if returns and len(returns) > 1:
        avg_return = sum(returns) / len(returns)
        std_return = math.sqrt(sum((r - avg_return)**2 for r in returns) / (len(returns) - 1))

        # Annualize
        periods_per_year = 365 * 24 * 4  # 15-min candles per year
        annualized_return = avg_return * periods_per_year
        annualized_std = std_return * math.sqrt(periods_per_year)

        sharpe_ratio = annualized_return / annualized_std if annualized_std > 0 else 0

        # Sortino (only downside deviation)
        negative_returns = [r for r in returns if r < 0]
        if negative_returns:
            downside_std = math.sqrt(sum(r**2 for r in negative_returns) / len(negative_returns))
            annualized_downside = downside_std * math.sqrt(periods_per_year)
            sortino_ratio = annualized_return / annualized_downside if annualized_downside > 0 else 0
        else:
            sortino_ratio = float('inf')
    else:
        sharpe_ratio = 0
        sortino_ratio = 0

    # Calmar Ratio
    annual_return_pct = (net_profit / initial_capital) * (365 / max(days, 1)) * 100
    calmar_ratio = annual_return_pct / max_dd_pct if max_dd_pct > 0 else 0

    # Average hold time
    hold_times = []
    for t in trades:
        if t.exit_time and t.entry_time:
            hold_hours = (t.exit_time - t.entry_time) / (1000 * 60 * 60)
            hold_times.append(hold_hours)
    avg_hold_time = sum(hold_times) / len(hold_times) if hold_times else 0

    # Consecutive wins/losses
    max_consec_wins = 0
    max_consec_losses = 0
    current_wins = 0
    current_losses = 0

    for t in trades:
        if t.pnl > 0:
            current_wins += 1
            current_losses = 0
            max_consec_wins = max(max_consec_wins, current_wins)
        else:
            current_losses += 1
            current_wins = 0
            max_consec_losses = max(max_consec_losses, current_losses)

    return {
        "total_trades": len(trades),
        "winning_trades": len(winning_trades),
        "losing_trades": len(losing_trades),
        "win_rate": round(win_rate, 2),
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(gross_loss, 2),
        "net_profit": round(net_profit, 2),
        "net_profit_pct": round(net_profit / initial_capital * 100, 2),
        "profit_factor": round(profit_factor, 2) if profit_factor != float('inf') else 999,
        "max_drawdown": round(max_dd, 2),
        "max_drawdown_pct": round(max_dd_pct, 2),
        "avg_trade_pnl": round(avg_trade_pnl, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "largest_win": round(largest_win, 2),
        "largest_loss": round(largest_loss, 2),
        "sharpe_ratio": round(sharpe_ratio, 2),
        "sortino_ratio": round(sortino_ratio, 2) if sortino_ratio != float('inf') else 999,
        "calmar_ratio": round(calmar_ratio, 2),
        "avg_hold_time_hours": round(avg_hold_time, 2),
        "max_consecutive_wins": max_consec_wins,
        "max_consecutive_losses": max_consec_losses
    }


# ---
# MAIN BACKTEST FUNCTION
# ---

def run_backtest(symbol: str, timeframe: str, strategy: str) -> Optional[Dict]:
    """Run backtest for a single symbol/timeframe/strategy combination.

    Returns dict with keys: result, trades_rows, equity_rows — or None.
    """

    print(f"\n[{symbol}] {timeframe} - {strategy.upper()}")

    start_date = BACKTEST_CONFIG["start_date"]
    end_date = BACKTEST_CONFIG["end_date"]

    # Load data
    print("  Loading data...", end=" ")
    klines_df = load_historical_klines(symbol, timeframe, start_date, end_date)
    klines_list = [row.asDict() for row in klines_df.collect()]

    if len(klines_list) < 100:
        print(f"Insufficient data ({len(klines_list)} candles)")
        return None

    print(f"{len(klines_list)} candles")

    # Load zones and BOS
    zones = load_historical_zones(symbol, timeframe, start_date, end_date)
    bos_list = load_historical_bos(symbol, timeframe, start_date, end_date)

    print(f"  Zones: {len(zones) if zones else 0}, BOS: {len(bos_list)}")

    # Calculate indicators
    atr_map = calculate_atr_series(klines_list)
    swing_highs, swing_lows = identify_swing_points(klines_list)

    # Generate signals
    if strategy == "smc":
        signals = generate_smc_signals(klines_list, bos_list, zones, atr_map, swing_highs, swing_lows)
    else:  # liquidity
        signals = generate_liquidity_signals(klines_list, zones, atr_map)

    print(f"  Signals generated: {len(signals)}")

    if not signals:
        print("  No signals - skipping")
        return None

    # Simulate trades
    trades, equity_curve = simulate_trades(
        klines_list, signals,
        BACKTEST_CONFIG["initial_capital"],
        BACKTEST_CONFIG["risk_per_trade_pct"],
        BACKTEST_CONFIG["commission_pct"],
        BACKTEST_CONFIG["slippage_pct"]
    )

    print(f"  Trades executed: {len(trades)}")

    # Calculate metrics
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
    days = (end_dt - start_dt).days

    metrics = calculate_metrics(trades, equity_curve, BACKTEST_CONFIG["initial_capital"], days)

    print(f"  Net P&L: ${metrics['net_profit']:,.2f} ({metrics['net_profit_pct']:.1f}%)")
    print(f"  Win Rate: {metrics['win_rate']:.1f}% | Sharpe: {metrics['sharpe_ratio']:.2f} | Max DD: {metrics['max_drawdown_pct']:.1f}%")

    # Prepare result
    backtest_id = f"{symbol}_{timeframe}_{strategy}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    run_ts = datetime.now(timezone.utc)

    # --- Build timestamp->index map and volume array for lookups ---
    ts_to_idx = {c["timestamp"]: i for i, c in enumerate(klines_list)}
    VOLUME_LOOKBACK = 10  # candles before entry for avg_volume_before

    # --- Serialize trades ---
    trades_rows = []
    for idx, t in enumerate(trades, start=1):
        hold_hours = (t.exit_time - t.entry_time) / (1000 * 60 * 60) if t.exit_time and t.entry_time else 0.0
        pnl_pct = (t.pnl / (t.position_size * t.entry_price) * 100) if (t.position_size * t.entry_price) > 0 else 0.0

        # --- Volume metrics ---
        entry_idx = ts_to_idx.get(t.entry_time)
        exit_idx = ts_to_idx.get(t.exit_time)

        vol_at_entry = float(klines_list[entry_idx]["volume"]) if entry_idx is not None else 0.0
        vol_at_exit = float(klines_list[exit_idx]["volume"]) if exit_idx is not None else 0.0

        # Avg volume N candles before entry
        if entry_idx is not None and entry_idx >= VOLUME_LOOKBACK:
            before_vols = [klines_list[j]["volume"] for j in range(entry_idx - VOLUME_LOOKBACK, entry_idx)]
            avg_vol_before = sum(before_vols) / len(before_vols)
        else:
            avg_vol_before = vol_at_entry  # fallback

        # Avg volume during hold
        if entry_idx is not None and exit_idx is not None and exit_idx > entry_idx:
            during_vols = [klines_list[j]["volume"] for j in range(entry_idx, exit_idx + 1)]
            avg_vol_during = sum(during_vols) / len(during_vols)
        else:
            avg_vol_during = vol_at_entry

        vol_ratio = vol_at_entry / avg_vol_before if avg_vol_before > 0 else 0.0

        trades_rows.append({
            "backtest_id": backtest_id,
            "strategy": strategy,
            "symbol": symbol,
            "timeframe": timeframe,
            "trade_number": idx,
            "type": t.type,
            "entry_time": t.entry_time,
            "entry_price": float(t.entry_price),
            "exit_time": t.exit_time,
            "exit_price": float(t.exit_price) if t.exit_price else None,
            "stop_loss": float(t.stop_loss),
            "take_profit_1": float(t.take_profit_1),
            "take_profit_2": float(t.take_profit_2),
            "position_size": float(t.position_size),
            "exit_reason": t.exit_reason,
            "pnl": float(round(t.pnl, 4)),
            "pnl_pct": float(round(pnl_pct, 4)),
            "commission": float(round(t.commission, 4)),
            "hold_time_hours": float(round(hold_hours, 2)),
            "tp1_hit": t.tp1_hit,
            "zone_strength": float(t.signal.get("zone_strength", 0)),
            "volume_at_entry": float(round(vol_at_entry, 2)),
            "volume_at_exit": float(round(vol_at_exit, 2)),
            "avg_volume_before": float(round(avg_vol_before, 2)),
            "avg_volume_during": float(round(avg_vol_during, 2)),
            "volume_ratio_entry": float(round(vol_ratio, 4)),
            "run_timestamp": run_ts,
        })

    # --- Serialize equity curve (sampled) ---
    # Sample rate: every 10th point for 15m, every point for 4h/1d
    sample_step = 10 if timeframe == "15m" else (4 if timeframe == "1h" else 1)
    sampled_equity = equity_curve[::sample_step]

    # Pre-compute peak for drawdown_pct
    peak = BACKTEST_CONFIG["initial_capital"]
    equity_rows = []
    for point in sampled_equity:
        eq = point["equity"]
        if eq > peak:
            peak = eq
        dd_pct = (peak - eq) / peak * 100 if peak > 0 else 0.0
        equity_rows.append({
            "backtest_id": backtest_id,
            "strategy": strategy,
            "symbol": symbol,
            "timeframe": timeframe,
            "timestamp": point["timestamp"],
            "equity": float(round(eq, 2)),
            "capital": float(round(point["capital"], 2)),
            "drawdown_pct": float(round(dd_pct, 2)),
            "run_timestamp": run_ts,
        })

    # --- Metrics result (no JSON columns) ---
    result = {
        "backtest_id": backtest_id,
        "run_timestamp": run_ts,
        "strategy": strategy,
        "symbol": symbol,
        "timeframe": timeframe,
        "start_date": start_dt.date(),
        "end_date": end_dt.date(),
        "initial_capital": float(BACKTEST_CONFIG["initial_capital"]),
        "risk_per_trade_pct": float(BACKTEST_CONFIG["risk_per_trade_pct"]),
        **metrics,
    }

    return {"result": result, "trades_rows": trades_rows, "equity_rows": equity_rows}


# ---
# MAIN EXECUTION
# ---

print("\n" + "=" * 80)
print("RUNNING BACKTESTS")
print("=" * 80)

all_results = []
all_trades_rows = []
all_equity_rows = []

for symbol in BACKTEST_CONFIG["symbols"]:
    for timeframe in BACKTEST_CONFIG["timeframes"]:
        for strategy in BACKTEST_CONFIG["strategies"]:
            try:
                output = run_backtest(symbol, timeframe, strategy)
                if output:
                    all_results.append(output["result"])
                    all_trades_rows.extend(output["trades_rows"])
                    all_equity_rows.extend(output["equity_rows"])
            except Exception as e:
                print(f"  ERROR: {str(e)[:100]}")

# ---
# SAVE RESULTS
# ---

print("\n" + "=" * 80)
print("SAVING RESULTS")
print("=" * 80)

if all_results:
    # --- 1. Save metrics to backtest_history_results ---
    results_schema = StructType([
        StructField("backtest_id", StringType()),
        StructField("run_timestamp", TimestampType()),
        StructField("strategy", StringType()),
        StructField("symbol", StringType()),
        StructField("timeframe", StringType()),
        StructField("start_date", DateType()),
        StructField("end_date", DateType()),
        StructField("initial_capital", DoubleType()),
        StructField("risk_per_trade_pct", DoubleType()),
        StructField("total_trades", IntegerType()),
        StructField("winning_trades", IntegerType()),
        StructField("losing_trades", IntegerType()),
        StructField("win_rate", DoubleType()),
        StructField("gross_profit", DoubleType()),
        StructField("gross_loss", DoubleType()),
        StructField("net_profit", DoubleType()),
        StructField("net_profit_pct", DoubleType()),
        StructField("profit_factor", DoubleType()),
        StructField("max_drawdown", DoubleType()),
        StructField("max_drawdown_pct", DoubleType()),
        StructField("avg_trade_pnl", DoubleType()),
        StructField("avg_win", DoubleType()),
        StructField("avg_loss", DoubleType()),
        StructField("largest_win", DoubleType()),
        StructField("largest_loss", DoubleType()),
        StructField("sharpe_ratio", DoubleType()),
        StructField("sortino_ratio", DoubleType()),
        StructField("calmar_ratio", DoubleType()),
        StructField("avg_hold_time_hours", DoubleType()),
        StructField("max_consecutive_wins", IntegerType()),
        StructField("max_consecutive_losses", IntegerType()),
    ])

    results_df = spark.createDataFrame(all_results, results_schema)
    results_table = f"{CATALOG}.{TRADING_SCHEMA}.backtest_history_results"
    results_df.write.format("delta").mode("append").saveAsTable(results_table)
    print(f"✓ Saved {len(all_results)} results to {results_table}")

    # --- 2. Save trades to backtest_history_trades ---
    if all_trades_rows:
        trades_schema = StructType([
            StructField("backtest_id", StringType()),
            StructField("strategy", StringType()),
            StructField("symbol", StringType()),
            StructField("timeframe", StringType()),
            StructField("trade_number", IntegerType()),
            StructField("type", StringType()),
            StructField("entry_time", LongType()),
            StructField("entry_price", DoubleType()),
            StructField("exit_time", LongType()),
            StructField("exit_price", DoubleType()),
            StructField("stop_loss", DoubleType()),
            StructField("take_profit_1", DoubleType()),
            StructField("take_profit_2", DoubleType()),
            StructField("position_size", DoubleType()),
            StructField("exit_reason", StringType()),
            StructField("pnl", DoubleType()),
            StructField("pnl_pct", DoubleType()),
            StructField("commission", DoubleType()),
            StructField("hold_time_hours", DoubleType()),
            StructField("tp1_hit", BooleanType()),
            StructField("zone_strength", DoubleType()),
            StructField("volume_at_entry", DoubleType()),
            StructField("volume_at_exit", DoubleType()),
            StructField("avg_volume_before", DoubleType()),
            StructField("avg_volume_during", DoubleType()),
            StructField("volume_ratio_entry", DoubleType()),
            StructField("run_timestamp", TimestampType()),
        ])

        trades_df = spark.createDataFrame(all_trades_rows, trades_schema)
        trades_table = f"{CATALOG}.{TRADING_SCHEMA}.backtest_history_trades"
        trades_df.write.format("delta").mode("append").saveAsTable(trades_table)
        print(f"✓ Saved {len(all_trades_rows)} trades to {trades_table}")

    # --- 3. Save equity curve to backtest_history_equity ---
    if all_equity_rows:
        equity_schema = StructType([
            StructField("backtest_id", StringType()),
            StructField("strategy", StringType()),
            StructField("symbol", StringType()),
            StructField("timeframe", StringType()),
            StructField("timestamp", LongType()),
            StructField("equity", DoubleType()),
            StructField("capital", DoubleType()),
            StructField("drawdown_pct", DoubleType()),
            StructField("run_timestamp", TimestampType()),
        ])

        equity_df = spark.createDataFrame(all_equity_rows, equity_schema)
        equity_table = f"{CATALOG}.{TRADING_SCHEMA}.backtest_history_equity"
        equity_df.write.format("delta").mode("append").saveAsTable(equity_table)
        print(f"✓ Saved {len(all_equity_rows)} equity points to {equity_table}")

else:
    print("No results to save")

# ---
# SUMMARY
# ---

print("\n" + "=" * 80)
print("BACKTEST SUMMARY")
print("=" * 80)

spark.sql(f"""
    SELECT
        strategy,
        symbol,
        timeframe,
        total_trades,
        ROUND(win_rate, 1) as win_rate,
        ROUND(net_profit, 2) as net_pnl,
        ROUND(net_profit_pct, 1) as pnl_pct,
        ROUND(profit_factor, 2) as pf,
        ROUND(sharpe_ratio, 2) as sharpe,
        ROUND(max_drawdown_pct, 1) as max_dd
    FROM {CATALOG}.{TRADING_SCHEMA}.backtest_history_results
    WHERE run_timestamp >= current_timestamp() - INTERVAL 1 HOUR
    ORDER BY net_profit_pct DESC
""").show(50, truncate=False)

# Best performing combinations
print("\n[TOP 5 PERFORMERS]")
spark.sql(f"""
    SELECT
        strategy,
        symbol,
        timeframe,
        ROUND(net_profit_pct, 1) as pnl_pct,
        ROUND(sharpe_ratio, 2) as sharpe,
        ROUND(win_rate, 1) as win_rate,
        ROUND(profit_factor, 2) as pf
    FROM {CATALOG}.{TRADING_SCHEMA}.backtest_history_results
    WHERE run_timestamp >= current_timestamp() - INTERVAL 1 HOUR
      AND total_trades >= 10
    ORDER BY sharpe_ratio DESC
    LIMIT 5
""").show(truncate=False)

print("\n✓ Backtesting complete")
