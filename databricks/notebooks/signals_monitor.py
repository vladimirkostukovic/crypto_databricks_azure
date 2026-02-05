# Databricks notebook source
# ---
# SIGNALS MONITOR
# Dashboard for monitoring signals, positions, and performance
# ---

from pyspark.sql import functions as F
from datetime import datetime, timezone, timedelta

CATALOG = "crypto"
GOLD_SCHEMA = "gold"
TRADING_SCHEMA = "trading"

print("=" * 80)
print("SIGNALS & TRADING MONITOR")
print(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
print("=" * 80)

# COMMAND ----------

# ---
# 1. SIGNALS OVERVIEW
# ---

print("\n" + "=" * 80)
print("1. SIGNALS OVERVIEW")
print("=" * 80)

# Today's stats
spark.sql(f"""
    SELECT
        'Today' as period,
        COUNT(*) as total_signals,
        SUM(CASE WHEN ai_status = 'pending' THEN 1 ELSE 0 END) as pending,
        SUM(CASE WHEN ai_status = 'confirmed' THEN 1 ELSE 0 END) as confirmed,
        SUM(CASE WHEN ai_status = 'rejected' THEN 1 ELSE 0 END) as rejected,
        ROUND(AVG(CASE WHEN ai_status = 'confirmed' THEN ai_confidence END), 1) as avg_conf
    FROM {CATALOG}.{GOLD_SCHEMA}.signals_all
    WHERE date = current_date()
    UNION ALL
    SELECT
        'Last 7 days' as period,
        COUNT(*) as total_signals,
        SUM(CASE WHEN ai_status = 'pending' THEN 1 ELSE 0 END) as pending,
        SUM(CASE WHEN ai_status = 'confirmed' THEN 1 ELSE 0 END) as confirmed,
        SUM(CASE WHEN ai_status = 'rejected' THEN 1 ELSE 0 END) as rejected,
        ROUND(AVG(CASE WHEN ai_status = 'confirmed' THEN ai_confidence END), 1) as avg_conf
    FROM {CATALOG}.{GOLD_SCHEMA}.signals_all
    WHERE date >= current_date() - INTERVAL 7 DAYS
""").show()

# COMMAND ----------

# ---
# 2. CONFIRMED SIGNALS AWAITING EXECUTION
# ---

print("\n" + "=" * 80)
print("2. CONFIRMED SIGNALS - AWAITING EXECUTION")
print("=" * 80)

spark.sql(f"""
    SELECT
        signal_id,
        symbol,
        timeframe as tf,
        strategy,
        signal_type as type,
        ROUND(current_price, 2) as cur_price,
        ROUND(entry_price, 2) as entry,
        ROUND(stop_loss, 2) as sl,
        ROUND(take_profit_1, 2) as tp1,
        ROUND(take_profit_2, 2) as tp2,
        ROUND(risk_reward_ratio, 2) as rr,
        ROUND(ai_confidence, 0) as conf,
        ROUND(zone_strength, 0) as zone_str
    FROM {CATALOG}.{GOLD_SCHEMA}.signals_all
    WHERE ai_status = 'confirmed'
      AND (execution_status IS NULL OR execution_status = 'waiting')
    ORDER BY ai_confidence DESC, created_at DESC
    LIMIT 20
""").show(truncate=False)

# COMMAND ----------

# ---
# 3. SIGNALS BY STRATEGY
# ---

print("\n" + "=" * 80)
print("3. SIGNALS BY STRATEGY (Last 7 days)")
print("=" * 80)

spark.sql(f"""
    SELECT
        strategy,
        signal_type,
        COUNT(*) as total,
        SUM(CASE WHEN ai_status = 'confirmed' THEN 1 ELSE 0 END) as confirmed,
        SUM(CASE WHEN ai_status = 'rejected' THEN 1 ELSE 0 END) as rejected,
        ROUND(SUM(CASE WHEN ai_status = 'confirmed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as confirm_rate,
        ROUND(AVG(zone_strength), 1) as avg_zone_str,
        ROUND(AVG(sentiment_score), 1) as avg_sentiment
    FROM {CATALOG}.{GOLD_SCHEMA}.signals_all
    WHERE date >= current_date() - INTERVAL 7 DAYS
    GROUP BY strategy, signal_type
    ORDER BY strategy, signal_type
""").show()

# COMMAND ----------

# ---
# 4. SIGNALS BY SYMBOL
# ---

print("\n" + "=" * 80)
print("4. TOP SYMBOLS BY CONFIRMED SIGNALS (Last 7 days)")
print("=" * 80)

spark.sql(f"""
    SELECT
        symbol,
        COUNT(*) as total_signals,
        SUM(CASE WHEN ai_status = 'confirmed' THEN 1 ELSE 0 END) as confirmed,
        ROUND(AVG(CASE WHEN ai_status = 'confirmed' THEN ai_confidence END), 1) as avg_conf,
        ROUND(AVG(zone_strength), 1) as avg_zone_str,
        MAX(created_at) as last_signal
    FROM {CATALOG}.{GOLD_SCHEMA}.signals_all
    WHERE date >= current_date() - INTERVAL 7 DAYS
    GROUP BY symbol
    ORDER BY confirmed DESC
    LIMIT 15
""").show(truncate=False)

# COMMAND ----------

# ---
# 5. LATEST SENTIMENT
# ---

print("\n" + "=" * 80)
print("5. LATEST SENTIMENT SCORES")
print("=" * 80)

spark.sql(f"""
    WITH latest AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY symbol, timeframe ORDER BY timestamp DESC) as rn
        FROM {CATALOG}.{GOLD_SCHEMA}.sentiment_scores
    )
    SELECT
        symbol,
        timeframe as tf,
        ROUND(long_short_ratio, 2) as ls_ratio,
        crowding_direction as crowding,
        ROUND(sentiment_score, 1) as sent_score,
        sentiment_bias as bias,
        divergence_direction as smart_money,
        aggression_direction as taker
    FROM latest
    WHERE rn = 1
    ORDER BY symbol, timeframe
""").show(50, truncate=False)

# COMMAND ----------

# ---
# 6. OPEN POSITIONS
# ---

print("\n" + "=" * 80)
print("6. OPEN POSITIONS")
print("=" * 80)

try:
    positions = spark.table(f"{CATALOG}.{TRADING_SCHEMA}.positions_open")
    if positions.count() > 0:
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
    else:
        print("No open positions")
except:
    print("No positions table or empty")

# COMMAND ----------

# ---
# 7. RECENT CLOSED POSITIONS
# ---

print("\n" + "=" * 80)
print("7. RECENT CLOSED POSITIONS (Last 7 days)")
print("=" * 80)

try:
    spark.sql(f"""
        SELECT
            symbol,
            side,
            strategy,
            ROUND(entry_price, 2) as entry,
            ROUND(exit_price, 2) as exit,
            exit_reason,
            ROUND(realized_pnl_pct, 2) as pnl_pct,
            ROUND(net_pnl, 2) as net_pnl,
            hold_duration_minutes as hold_min,
            ai_was_correct
        FROM {CATALOG}.{TRADING_SCHEMA}.positions_closed
        WHERE date >= current_date() - INTERVAL 7 DAYS
        ORDER BY closed_at DESC
        LIMIT 20
    """).show(truncate=False)
except:
    print("No closed positions yet")

# COMMAND ----------

# ---
# 8. DAILY P&L
# ---

print("\n" + "=" * 80)
print("8. DAILY P&L (Last 14 days)")
print("=" * 80)

try:
    spark.sql(f"""
        SELECT
            date,
            trades_closed,
            trades_won,
            trades_lost,
            ROUND(win_rate * 100, 1) as win_rate_pct,
            ROUND(net_pnl, 2) as net_pnl,
            ROUND(cumulative_pnl, 2) as cumulative,
            ROUND(max_drawdown, 2) as max_dd
        FROM {CATALOG}.{TRADING_SCHEMA}.daily_pnl
        WHERE date >= current_date() - INTERVAL 14 DAYS
        ORDER BY date DESC
    """).show(truncate=False)
except:
    print("No daily P&L data yet")

# COMMAND ----------

# ---
# 9. STRATEGY PERFORMANCE
# ---

print("\n" + "=" * 80)
print("9. STRATEGY PERFORMANCE")
print("=" * 80)

try:
    spark.sql(f"""
        SELECT
            strategy,
            COUNT(*) as total_trades,
            SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN net_pnl <= 0 THEN 1 ELSE 0 END) as losses,
            ROUND(SUM(CASE WHEN net_pnl > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as win_rate,
            ROUND(SUM(net_pnl), 2) as total_pnl,
            ROUND(AVG(net_pnl), 2) as avg_pnl,
            ROUND(AVG(CASE WHEN net_pnl > 0 THEN net_pnl END), 2) as avg_win,
            ROUND(AVG(CASE WHEN net_pnl <= 0 THEN net_pnl END), 2) as avg_loss
        FROM {CATALOG}.{TRADING_SCHEMA}.positions_closed
        GROUP BY strategy
    """).show()
except:
    print("No strategy performance data yet")

# COMMAND ----------

# ---
# 10. AI ACCURACY
# ---

print("\n" + "=" * 80)
print("10. AI ACCURACY")
print("=" * 80)

try:
    spark.sql(f"""
        SELECT
            ROUND(ai_confidence / 10, 0) * 10 as confidence_bucket,
            COUNT(*) as total_trades,
            SUM(CASE WHEN ai_was_correct THEN 1 ELSE 0 END) as correct,
            ROUND(SUM(CASE WHEN ai_was_correct THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as accuracy_pct,
            ROUND(AVG(realized_pnl_pct), 2) as avg_pnl_pct
        FROM {CATALOG}.{TRADING_SCHEMA}.positions_closed
        WHERE ai_confidence IS NOT NULL
        GROUP BY ROUND(ai_confidence / 10, 0) * 10
        ORDER BY confidence_bucket
    """).show()
except:
    print("No AI accuracy data yet")

# COMMAND ----------

# ---
# 11. CONFIG CHECK
# ---

print("\n" + "=" * 80)
print("11. CURRENT TRADING CONFIG")
print("=" * 80)

spark.sql(f"""
    SELECT
        category,
        param_name,
        param_value,
        description
    FROM {CATALOG}.{GOLD_SCHEMA}.trading_config
    ORDER BY category, param_name
""").show(50, truncate=False)

# COMMAND ----------

# ---
# 12. ALERTS
# ---

print("\n" + "=" * 80)
print("12. ALERTS & WARNINGS")
print("=" * 80)

# Check for issues
alerts = []

# Check pending signals older than 1 hour
old_pending = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {CATALOG}.{GOLD_SCHEMA}.signals_all
    WHERE ai_status = 'pending'
      AND created_at < current_timestamp() - INTERVAL 1 HOUR
""").first()["cnt"]

if old_pending > 0:
    alerts.append(f"⚠ {old_pending} signals pending AI validation for >1 hour")

# Check API key
api_key = spark.sql(f"""
    SELECT param_value
    FROM {CATALOG}.{GOLD_SCHEMA}.trading_config
    WHERE param_name = 'anthropic_api_key'
""").first()

if api_key and "PLACEHOLDER" in api_key["param_value"]:
    alerts.append("⚠ Anthropic API key is placeholder - AI validation won't work")

# Check for high drawdown
try:
    latest_dd = spark.sql(f"""
        SELECT max_drawdown
        FROM {CATALOG}.{TRADING_SCHEMA}.daily_pnl
        ORDER BY date DESC
        LIMIT 1
    """).first()
    if latest_dd and latest_dd["max_drawdown"] and latest_dd["max_drawdown"] > 5:
        alerts.append(f"⚠ Current drawdown: {latest_dd['max_drawdown']:.1f}%")
except:
    pass

# Check sentiment extremes
try:
    extremes = spark.sql(f"""
        WITH latest AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
            FROM {CATALOG}.{GOLD_SCHEMA}.sentiment_scores
            WHERE timeframe = '1h'
        )
        SELECT symbol, sentiment_score, crowding_direction
        FROM latest
        WHERE rn = 1 AND ABS(sentiment_score) > 40
    """).collect()

    for row in extremes:
        alerts.append(f"📊 {row['symbol']}: Extreme sentiment ({row['sentiment_score']:.0f}), {row['crowding_direction']}")
except:
    pass

if alerts:
    for alert in alerts:
        print(alert)
else:
    print("✓ No alerts - all systems normal")

# COMMAND ----------

print("\n" + "=" * 80)
print("MONITOR COMPLETE")
print("=" * 80)
