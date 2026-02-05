# Databricks notebook source
# ---
# INIT GOLD & TRADING SCHEMAS
# Creates all tables for signal generation and bot tracking
# ---

from pyspark.sql.types import *
from delta.tables import DeltaTable

CATALOG = "crypto"

# ---
# CREATE SCHEMAS
# ---

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.trading")

print("✓ Schemas created: gold, trading")

# ---
# GOLD.TRADING_CONFIG - Parameters table
# ---

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.trading_config (
    param_name STRING,
    param_value STRING,
    param_type STRING,
    category STRING,
    description STRING,
    updated_at TIMESTAMP
)
USING DELTA
""")

# Insert default config values
config_defaults = [
    # Risk Management
    ("risk_per_trade_pct", "1.0", "float", "risk", "% of deposit per trade"),
    ("max_leverage", "10", "int", "risk", "Maximum leverage allowed"),
    ("max_open_positions", "5", "int", "risk", "Max concurrent positions"),
    ("max_daily_trades", "10", "int", "risk", "Max trades per day"),
    ("max_drawdown_pct", "10", "float", "risk", "Stop trading at this drawdown %"),

    # AI Settings
    ("anthropic_api_key", "sk-ant-PLACEHOLDER", "string", "ai", "Anthropic API key"),
    ("ai_model", "claude-sonnet-4-20250514", "string", "ai", "Model for validation"),
    ("ai_temperature", "0.3", "float", "ai", "Low for consistency"),
    ("min_ai_confidence", "75", "int", "ai", "Min confidence to enter trade"),

    # Strategy Toggles
    ("enable_smc_strategy", "true", "bool", "strategy", "Enable Smart Money Concept"),
    ("enable_liq_strategy", "true", "bool", "strategy", "Enable Liquidity Zones"),

    # Timeframes
    ("scan_timeframes", '["15m", "1h", "4h"]', "json", "timeframes", "Timeframes to scan"),
    ("entry_timeframe", "15m", "string", "timeframes", "Timeframe for precise entry"),

    # Symbols
    ("trading_symbols", '["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]', "json", "symbols", "Symbols to trade"),

    # Signal Filters
    ("min_zone_strength", "70", "int", "filters", "Minimum zone strength score"),
    ("min_confluence_score", "4", "int", "filters", "Minimum TF confluence score"),
    ("min_confirmed_methods", "2", "int", "filters", "Min VP+OI+OB confirmations"),
    ("require_bos_confirmation", "true", "bool", "filters", "Require BOS for entry"),

    # Sentiment Filters
    ("crowding_threshold", "65", "int", "sentiment", "L/S ratio crowded threshold"),
    ("smart_money_div_threshold", "20", "int", "sentiment", "Top vs retail divergence %"),

    # Take Profit / Stop Loss
    ("sl_atr_multiplier", "1.5", "float", "levels", "SL = ATR * multiplier"),
    ("tp1_rr_ratio", "1.5", "float", "levels", "TP1 risk/reward ratio"),
    ("tp2_rr_ratio", "3.0", "float", "levels", "TP2 risk/reward ratio"),
    ("tp1_close_pct", "50", "int", "levels", "% to close at TP1"),

    # Exchange
    ("trading_exchange", "bybit", "string", "exchange", "Exchange for execution"),
    ("use_testnet", "true", "bool", "exchange", "Use testnet for safety"),
    ("exchange_api_key", "YOUR_API_KEY_HERE", "string", "exchange", "Exchange API key"),
    ("exchange_api_secret", "YOUR_API_SECRET_HERE", "string", "exchange", "Exchange API secret"),

    # Statistical Scorer (replaced AI validator)
    ("score_threshold_execute", "70", "int", "scorer", "Min score to auto-execute"),
    ("score_threshold_watch", "50", "int", "scorer", "Min score for watchlist"),
    ("min_rr_ratio", "2.0", "float", "scorer", "Minimum risk/reward ratio"),
]

from pyspark.sql import Row
from datetime import datetime

config_rows = [Row(
    param_name=c[0],
    param_value=c[1],
    param_type=c[2],
    category=c[3],
    description=c[4],
    updated_at=datetime.utcnow()
) for c in config_defaults]

config_df = spark.createDataFrame(config_rows)

# Merge to avoid duplicates
if spark.catalog.tableExists(f"{CATALOG}.gold.trading_config"):
    delta_config = DeltaTable.forName(spark, f"{CATALOG}.gold.trading_config")
    delta_config.alias("t").merge(
        config_df.alias("s"),
        "t.param_name = s.param_name"
    ).whenNotMatchedInsertAll().execute()
else:
    config_df.write.format("delta").saveAsTable(f"{CATALOG}.gold.trading_config")

print(f"✓ Config table: {CATALOG}.gold.trading_config ({len(config_defaults)} params)")

# ---
# GOLD.SENTIMENT_SCORES - Aggregated sentiment per symbol
# ---

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.sentiment_scores (
    symbol STRING,
    timeframe STRING,
    timestamp LONG,
    datetime TIMESTAMP,

    -- Long/Short Ratio
    long_short_ratio DOUBLE,
    long_account_pct DOUBLE,
    short_account_pct DOUBLE,
    ls_percentile INT,
    crowding_direction STRING,

    -- Top Traders (Binance)
    top_ls_ratio DOUBLE,
    top_long_pct DOUBLE,
    top_short_pct DOUBLE,

    -- Smart Money Divergence
    smart_money_divergence DOUBLE,
    divergence_direction STRING,

    -- Taker Activity
    taker_buy_sell_ratio DOUBLE,
    taker_buy_volume DOUBLE,
    taker_sell_volume DOUBLE,
    taker_percentile INT,
    aggression_direction STRING,

    -- Composite Score
    sentiment_score DOUBLE,
    sentiment_bias STRING,

    calculated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (timeframe)
""")

print(f"✓ Created: {CATALOG}.gold.sentiment_scores")

# ---
# GOLD.SIGNALS_ALL - Master signals table
# ---

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.gold.signals_all (
    signal_id STRING,
    created_at TIMESTAMP,

    -- Signal source
    symbol STRING,
    timeframe STRING,
    strategy STRING,
    signal_type STRING,

    -- Price context
    current_price DOUBLE,

    -- Zone/BOS data
    zone_id STRING,
    zone_low DOUBLE,
    zone_high DOUBLE,
    poc_price DOUBLE,
    zone_strength DOUBLE,
    bos_type STRING,
    bos_timestamp LONG,
    bos_price DOUBLE,

    -- Confluence
    confluence_score INT,
    confluence_tfs STRING,
    confirmed_methods INT,

    -- Sentiment
    sentiment_score DOUBLE,
    crowding_direction STRING,
    smart_money_divergence DOUBLE,
    taker_bias STRING,

    -- AI Decision
    ai_status STRING,
    ai_confidence DOUBLE,
    ai_reasoning STRING,
    ai_validated_at TIMESTAMP,

    -- Calculated levels (by AI)
    entry_price DOUBLE,
    stop_loss DOUBLE,
    take_profit_1 DOUBLE,
    take_profit_2 DOUBLE,
    risk_reward_ratio DOUBLE,
    position_size_pct DOUBLE,

    -- Execution status
    execution_status STRING,
    executed_at TIMESTAMP,

    -- Data snapshot for AI
    data_snapshot STRING,

    -- Partition
    date DATE
)
USING DELTA
PARTITIONED BY (date)
""")

print(f"✓ Created: {CATALOG}.gold.signals_all")

# ---
# TRADING.POSITIONS_OPEN - Current open positions
# ---

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.trading.positions_open (
    position_id STRING,
    signal_id STRING,

    -- Position info
    symbol STRING,
    side STRING,
    strategy STRING,

    -- Entry
    entry_price DOUBLE,
    entry_time TIMESTAMP,
    quantity DOUBLE,
    leverage INT,

    -- Levels
    stop_loss DOUBLE,
    take_profit_1 DOUBLE,
    take_profit_2 DOUBLE,

    -- Current state
    current_price DOUBLE,
    unrealized_pnl DOUBLE,
    unrealized_pnl_pct DOUBLE,

    -- Partial close tracking
    tp1_hit BOOLEAN,
    tp1_close_qty DOUBLE,
    remaining_qty DOUBLE,

    -- Metadata
    exchange STRING,
    order_ids STRING,
    updated_at TIMESTAMP
)
USING DELTA
""")

print(f"✓ Created: {CATALOG}.trading.positions_open")

# ---
# TRADING.POSITIONS_CLOSED - Trade history
# ---

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.trading.positions_closed (
    position_id STRING,
    signal_id STRING,

    -- Position info
    symbol STRING,
    side STRING,
    strategy STRING,
    timeframe STRING,

    -- Entry
    entry_price DOUBLE,
    entry_time TIMESTAMP,
    quantity DOUBLE,
    leverage INT,

    -- Exit
    exit_price DOUBLE,
    exit_time TIMESTAMP,
    exit_reason STRING,

    -- P&L
    realized_pnl DOUBLE,
    realized_pnl_pct DOUBLE,
    fees DOUBLE,
    net_pnl DOUBLE,

    -- Levels hit
    sl_price DOUBLE,
    tp1_price DOUBLE,
    tp2_price DOUBLE,
    tp1_hit BOOLEAN,
    tp2_hit BOOLEAN,
    sl_hit BOOLEAN,

    -- Duration
    hold_duration_minutes INT,

    -- AI accuracy
    ai_confidence DOUBLE,
    ai_was_correct BOOLEAN,

    -- Metadata
    exchange STRING,
    closed_at TIMESTAMP,
    date DATE
)
USING DELTA
PARTITIONED BY (date)
""")

print(f"✓ Created: {CATALOG}.trading.positions_closed")

# ---
# TRADING.DAILY_PNL - Daily performance
# ---

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.trading.daily_pnl (
    date DATE,

    -- Trade counts
    trades_opened INT,
    trades_closed INT,
    trades_won INT,
    trades_lost INT,

    -- P&L
    gross_pnl DOUBLE,
    fees DOUBLE,
    net_pnl DOUBLE,
    cumulative_pnl DOUBLE,

    -- Win rate
    win_rate DOUBLE,
    avg_win DOUBLE,
    avg_loss DOUBLE,
    profit_factor DOUBLE,

    -- By strategy
    smc_pnl DOUBLE,
    smc_trades INT,
    smc_win_rate DOUBLE,
    liq_pnl DOUBLE,
    liq_trades INT,
    liq_win_rate DOUBLE,

    -- Risk metrics
    max_drawdown DOUBLE,
    sharpe_ratio DOUBLE,

    updated_at TIMESTAMP
)
USING DELTA
""")

print(f"✓ Created: {CATALOG}.trading.daily_pnl")

# ---
# TRADING.BOT_SUMMARY - Overall bot stats
# ---

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.trading.bot_summary (
    metric_name STRING,
    metric_value DOUBLE,
    metric_type STRING,
    period STRING,
    updated_at TIMESTAMP
)
USING DELTA
""")

print(f"✓ Created: {CATALOG}.trading.bot_summary")

# ---
# SUMMARY
# ---

print("\n" + "=" * 60)
print("INITIALIZATION COMPLETE")
print("=" * 60)
print(f"""
Gold Schema:
  - {CATALOG}.gold.trading_config    (parameters)
  - {CATALOG}.gold.sentiment_scores  (sentiment analysis)
  - {CATALOG}.gold.signals_all       (master signals)

Trading Schema:
  - {CATALOG}.trading.positions_open    (current positions)
  - {CATALOG}.trading.positions_closed  (trade history)
  - {CATALOG}.trading.daily_pnl         (daily P&L)
  - {CATALOG}.trading.bot_summary       (overall stats)
""")

# Show config
print("Current Config:")
spark.sql(f"SELECT param_name, param_value, category FROM {CATALOG}.gold.trading_config ORDER BY category, param_name").show(50, truncate=False)
