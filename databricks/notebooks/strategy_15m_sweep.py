# Databricks notebook source
# ---
# 15M SWEEP STRATEGY
# Detects liquidity sweeps and volume-confirmed BOS on 15m timeframe
# Higher frequency strategy: 2-5 trades/day target
# ---

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timezone, timedelta
import hashlib

CATALOG = "crypto"
SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# ---
# STRATEGY CONFIG
# ---

TIMEFRAME = "15m"
SWING_LOOKBACK = 3
VOLUME_SPIKE_MULTIPLIER = 1.5   # BOS needs volume > 1.5x avg
SWEEP_RETURN_CANDLES = 3        # Sweep must return within N candles
LOOKBACK_HOURS = 24             # How far back to scan
MIN_SWEEP_WICK_PCT = 0.3        # Wick must be >30% of candle range

print("=" * 60)
print("15M SWEEP STRATEGY")
print("=" * 60)
print(f"Volume spike: >{VOLUME_SPIKE_MULTIPLIER}x avg")
print(f"Sweep return: {SWEEP_RETURN_CANDLES} candles")
print("=" * 60)

# ---
# HELPER FUNCTIONS
# ---

def get_symbols():
    """Get trading symbols from config"""
    try:
        result = spark.sql(f"""
            SELECT param_value FROM {CATALOG}.{GOLD_SCHEMA}.trading_config
            WHERE param_name = 'trading_symbols'
        """).first()
        if result:
            import json
            return json.loads(result["param_value"])
    except:
        pass
    return ["BTCUSDT", "ETHUSDT"]


def generate_signal_id(symbol, timestamp, signal_type):
    key = f"{symbol}_{timestamp}_{signal_type}_15m_sweep"
    return hashlib.md5(key.encode()).hexdigest()[:16]


# ---
# DATA LOADING
# ---

def load_klines(symbols):
    """Load 15m klines with volume"""
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).timestamp() * 1000)

    df = spark.table(f"{CATALOG}.{SCHEMA}.unified_klines_{TIMEFRAME}")
    df = df.filter(
        (F.col("symbol").isin(symbols)) &
        (F.col("timestamp") >= cutoff_ms)
    )

    # Aggregate across exchanges
    df = df.groupBy("symbol", "timestamp").agg(
        F.avg("open").alias("open"),
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.avg("close").alias("close"),
        F.sum("volume").alias("volume")
    )

    return df


# ---
# SWING DETECTION
# ---

def identify_swings(df):
    """Find swing highs and lows"""
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")

    # Add prev/next for swing detection
    for i in range(1, SWING_LOOKBACK + 1):
        df = df.withColumn(f"high_prev_{i}", F.lag("high", i).over(window_spec))
        df = df.withColumn(f"high_next_{i}", F.lead("high", i).over(window_spec))
        df = df.withColumn(f"low_prev_{i}", F.lag("low", i).over(window_spec))
        df = df.withColumn(f"low_next_{i}", F.lead("low", i).over(window_spec))

    # Swing conditions
    swing_high_cond = F.lit(True)
    swing_low_cond = F.lit(True)
    for i in range(1, SWING_LOOKBACK + 1):
        swing_high_cond = swing_high_cond & (F.col("high") > F.col(f"high_prev_{i}")) & (F.col("high") > F.col(f"high_next_{i}"))
        swing_low_cond = swing_low_cond & (F.col("low") < F.col(f"low_prev_{i}")) & (F.col("low") < F.col(f"low_next_{i}"))

    df = df.withColumn("is_swing_high", swing_high_cond)
    df = df.withColumn("is_swing_low", swing_low_cond)
    df = df.withColumn("swing_high_level", F.when(F.col("is_swing_high"), F.col("high")))
    df = df.withColumn("swing_low_level", F.when(F.col("is_swing_low"), F.col("low")))

    # Carry forward last swing levels
    carry_window = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)
    df = df.withColumn("last_swing_high", F.last("swing_high_level", ignorenulls=True).over(carry_window))
    df = df.withColumn("last_swing_low", F.last("swing_low_level", ignorenulls=True).over(carry_window))

    # Clean up
    cols_to_drop = []
    for i in range(1, SWING_LOOKBACK + 1):
        cols_to_drop += [f"high_prev_{i}", f"high_next_{i}", f"low_prev_{i}", f"low_next_{i}"]

    return df.drop(*cols_to_drop)


# ---
# VOLUME SPIKE BOS
# ---

def detect_volume_bos(df):
    """Detect BOS with volume confirmation"""
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")

    # Rolling average volume (20 periods)
    vol_window = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-20, -1)
    df = df.withColumn("avg_volume", F.avg("volume").over(vol_window))
    df = df.withColumn("volume_ratio",
        F.when(F.col("avg_volume") > 0, F.col("volume") / F.col("avg_volume")).otherwise(1.0)
    )

    # BOS conditions with volume spike
    df = df.withColumn("bos_bullish",
        (F.col("close") > F.col("last_swing_high")) &
        (F.col("last_swing_high").isNotNull()) &
        (F.col("volume_ratio") >= VOLUME_SPIKE_MULTIPLIER)
    )
    df = df.withColumn("bos_bearish",
        (F.col("close") < F.col("last_swing_low")) &
        (F.col("last_swing_low").isNotNull()) &
        (F.col("volume_ratio") >= VOLUME_SPIKE_MULTIPLIER)
    )

    return df


# ---
# SWEEP DETECTION
# ---

def detect_sweeps(df):
    """Detect liquidity sweeps (stop hunts)"""
    window_spec = Window.partitionBy("symbol").orderBy("timestamp")

    # Candle metrics
    df = df.withColumn("candle_range", F.col("high") - F.col("low"))
    df = df.withColumn("upper_wick", F.col("high") - F.greatest(F.col("open"), F.col("close")))
    df = df.withColumn("lower_wick", F.least(F.col("open"), F.col("close")) - F.col("low"))

    # Did price break swing level?
    df = df.withColumn("broke_high", F.col("high") > F.col("last_swing_high"))
    df = df.withColumn("broke_low", F.col("low") < F.col("last_swing_low"))

    # Look ahead for return
    for i in range(1, SWEEP_RETURN_CANDLES + 1):
        df = df.withColumn(f"close_{i}", F.lead("close", i).over(window_spec))

    # Sweep high: broke above then all closes back below (bearish)
    sweep_high_cond = (
        F.col("broke_high") &
        F.col("last_swing_high").isNotNull() &
        (F.col("upper_wick") / F.col("candle_range") >= MIN_SWEEP_WICK_PCT)  # Significant wick
    )
    for i in range(1, SWEEP_RETURN_CANDLES + 1):
        sweep_high_cond = sweep_high_cond & (F.col(f"close_{i}") < F.col("last_swing_high"))

    # Sweep low: broke below then all closes back above (bullish)
    sweep_low_cond = (
        F.col("broke_low") &
        F.col("last_swing_low").isNotNull() &
        (F.col("lower_wick") / F.col("candle_range") >= MIN_SWEEP_WICK_PCT)  # Significant wick
    )
    for i in range(1, SWEEP_RETURN_CANDLES + 1):
        sweep_low_cond = sweep_low_cond & (F.col(f"close_{i}") > F.col("last_swing_low"))

    df = df.withColumn("sweep_high", sweep_high_cond)  # Bearish: swept highs
    df = df.withColumn("sweep_low", sweep_low_cond)    # Bullish: swept lows

    # Clean up
    cols_to_drop = ["broke_high", "broke_low", "candle_range", "upper_wick", "lower_wick"]
    for i in range(1, SWEEP_RETURN_CANDLES + 1):
        cols_to_drop.append(f"close_{i}")

    return df.drop(*cols_to_drop)


# ---
# SIGNAL GENERATION
# ---

def extract_signals(df):
    """Extract actionable signals"""

    # Get latest candle per symbol for current price
    window_latest = Window.partitionBy("symbol").orderBy(F.col("timestamp").desc())
    df_latest = df.withColumn("rn", F.row_number().over(window_latest)).filter(F.col("rn") == 1)
    current_prices = {row["symbol"]: row["close"] for row in df_latest.select("symbol", "close").collect()}

    signals = []
    now = datetime.now(timezone.utc)

    # Volume BOS signals (last 4 hours only)
    cutoff_4h = int((now - timedelta(hours=4)).timestamp() * 1000)

    bos_df = df.filter(
        ((F.col("bos_bullish") == True) | (F.col("bos_bearish") == True)) &
        (F.col("timestamp") >= cutoff_4h)
    ).collect()

    for row in bos_df:
        symbol = row["symbol"]
        signal_type = "long" if row["bos_bullish"] else "short"

        signals.append({
            "signal_id": generate_signal_id(symbol, row["timestamp"], f"bos_{signal_type}"),
            "created_at": now,
            "symbol": symbol,
            "timeframe": TIMEFRAME,
            "strategy": "15m_volume_bos",
            "signal_type": signal_type,
            "current_price": current_prices.get(symbol),
            "trigger_price": row["close"],
            "swing_level": row["last_swing_high"] if signal_type == "long" else row["last_swing_low"],
            "volume_ratio": row["volume_ratio"],
            "confidence": min(95, 60 + (row["volume_ratio"] - 1.5) * 20),  # Higher volume = higher confidence
            "ai_status": "pending",
            "date": now.date()
        })

    # Sweep signals (last 2 hours only - more time sensitive)
    cutoff_2h = int((now - timedelta(hours=2)).timestamp() * 1000)

    sweep_df = df.filter(
        ((F.col("sweep_high") == True) | (F.col("sweep_low") == True)) &
        (F.col("timestamp") >= cutoff_2h)
    ).collect()

    for row in sweep_df:
        symbol = row["symbol"]
        signal_type = "long" if row["sweep_low"] else "short"  # Sweep low = bullish reversal

        signals.append({
            "signal_id": generate_signal_id(symbol, row["timestamp"], f"sweep_{signal_type}"),
            "created_at": now,
            "symbol": symbol,
            "timeframe": TIMEFRAME,
            "strategy": "15m_sweep",
            "signal_type": signal_type,
            "current_price": current_prices.get(symbol),
            "trigger_price": row["close"],
            "swing_level": row["last_swing_low"] if signal_type == "long" else row["last_swing_high"],
            "volume_ratio": row.get("volume_ratio", 1.0),
            "confidence": 75,  # Sweeps are high probability
            "ai_status": "pending",
            "date": now.date()
        })

    return signals


# ---
# MAIN EXECUTION
# ---

symbols = get_symbols()
print(f"\nSymbols: {symbols}")

# Load and process
df = load_klines(symbols)
print(f"Loaded {df.count()} candles")

df = identify_swings(df)
swing_highs = df.filter(F.col("is_swing_high")).count()
swing_lows = df.filter(F.col("is_swing_low")).count()
print(f"Swings: {swing_highs} highs, {swing_lows} lows")

df = detect_volume_bos(df)
bos_bull = df.filter(F.col("bos_bullish")).count()
bos_bear = df.filter(F.col("bos_bearish")).count()
print(f"Volume BOS: {bos_bull} bullish, {bos_bear} bearish")

df = detect_sweeps(df)
sweep_high = df.filter(F.col("sweep_high")).count()
sweep_low = df.filter(F.col("sweep_low")).count()
print(f"Sweeps: {sweep_high} high (bearish), {sweep_low} low (bullish)")

# Extract signals
signals = extract_signals(df)
print(f"\nSignals generated: {len(signals)}")

# ---
# WRITE TO SIGNALS TABLE
# ---

if signals:
    schema = StructType([
        StructField("signal_id", StringType()),
        StructField("created_at", TimestampType()),
        StructField("symbol", StringType()),
        StructField("timeframe", StringType()),
        StructField("strategy", StringType()),
        StructField("signal_type", StringType()),
        StructField("current_price", DoubleType()),
        StructField("trigger_price", DoubleType()),
        StructField("swing_level", DoubleType()),
        StructField("volume_ratio", DoubleType()),
        StructField("confidence", DoubleType()),
        StructField("ai_status", StringType()),
        StructField("date", DateType())
    ])

    signals_df = spark.createDataFrame(signals, schema)

    # Merge into sweep_signals table
    target_table = f"{CATALOG}.{SCHEMA}.sweep_signals_15m"

    if spark.catalog.tableExists(target_table):
        delta_table = DeltaTable.forName(spark, target_table)
        delta_table.alias("t").merge(
            signals_df.alias("s"),
            "t.signal_id = s.signal_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"✓ Merged {len(signals)} signals into {target_table}")
    else:
        signals_df.write.format("delta").partitionBy("date").saveAsTable(target_table)
        print(f"✓ Created {target_table} with {len(signals)} signals")

    # Show signals
    print("\n" + "=" * 60)
    print("SIGNALS")
    print("=" * 60)
    signals_df.select(
        "symbol", "strategy", "signal_type",
        F.round("trigger_price", 2).alias("price"),
        F.round("volume_ratio", 2).alias("vol_ratio"),
        F.round("confidence", 0).alias("conf")
    ).show(20, truncate=False)

else:
    print("\nNo signals found")

print("\n✓ 15m sweep strategy complete")
