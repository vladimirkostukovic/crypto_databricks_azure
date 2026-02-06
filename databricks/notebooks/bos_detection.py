# Databricks notebook source
# Break of Structure (BOS) Detection v4 - Optimized
# Features:
# - Single read per timeframe with caching
# - Minimal .count() calls
# - Gap detection for last 25 candles per timeframe
# - Checkpoint per timeframe
# - Processing log table

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType
from delta.tables import DeltaTable
from datetime import datetime, timezone
import time

# ---
# CONFIGURATION
# ---

CATALOG = "crypto"
SCHEMA = "silver"
TIMEFRAMES = ["15m", "1h", "4h", "1d"]
SWING_LOOKBACK = 3
DEBUG = False  # Set True for detailed counts (slower)

# Price range filter per timeframe (% from current price)
PRICE_RANGE_PCT = {
    "15m": 0.02,
    "1h": 0.05,
    "4h": 0.10,
    "1d": 0.15
}

# ---
# LOGGING FUNCTIONS
# ---

def ensure_log_table():
    log_table = f"{CATALOG}.{SCHEMA}.bos_processing_log"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {log_table} (
            run_timestamp TIMESTAMP,
            process_name STRING,
            timeframe STRING,
            status STRING,
            processed_rows INT,
            signals_found INT,
            checkpoint_used LONG,
            checkpoint_new LONG,
            message STRING
        )
    """)
    return log_table


def write_log(log_table, timeframe, status, processed, signals, checkpoint_used, checkpoint_new, message):
    log_df = spark.createDataFrame([(
        datetime.now(timezone.utc),
        "bos_detection",
        timeframe,
        status,
        processed,
        signals,
        checkpoint_used,
        checkpoint_new,
        message
    )], schema=StructType([
        StructField("run_timestamp", TimestampType(), False),
        StructField("process_name", StringType(), False),
        StructField("timeframe", StringType(), False),
        StructField("status", StringType(), False),
        StructField("processed_rows", IntegerType(), True),
        StructField("signals_found", IntegerType(), True),
        StructField("checkpoint_used", LongType(), True),
        StructField("checkpoint_new", LongType(), True),
        StructField("message", StringType(), True)
    ]))
    log_df.write.format("delta").mode("append").saveAsTable(log_table)

# ---
# CHECKPOINT FUNCTIONS
# ---

def get_checkpoint_for_timeframe(timeframe):
    bos_table = f"{CATALOG}.{SCHEMA}.bos_warnings"
    try:
        if spark.catalog.tableExists(bos_table):
            result = spark.sql(f"""
                SELECT MAX(timestamp) as last_ts
                FROM {bos_table}
                WHERE timeframe = '{timeframe}'
            """).first()
            if result and result["last_ts"]:
                return result["last_ts"]
    except Exception:
        pass
    return None

# ---
# DATA PROCESSING FUNCTIONS
# ---

def load_and_prepare_data(timeframe, checkpoint_ts=None):
    """Single read with aggregation and current prices"""
    df = spark.table(f"{CATALOG}.{SCHEMA}.unified_klines_{timeframe}")

    # Filter by checkpoint if incremental
    if checkpoint_ts:
        buffer_ms = TIMEFRAME_MS[timeframe] * (SWING_LOOKBACK + 1)
        df = df.filter(F.col("timestamp") >= (checkpoint_ts - buffer_ms))

    # Cache raw data for reuse
    df_cached = df.cache()

    # Get current prices (last close per symbol)
    window_latest = Window.partitionBy("symbol").orderBy(F.col("timestamp").desc())
    current_prices = (df_cached
        .withColumn("rn", F.row_number().over(window_latest))
        .filter(F.col("rn") == 1)
        .select("symbol", F.col("close").alias("current_price"))
    )

    # Aggregate OHLC across exchanges
    df_agg = (df_cached
        .groupBy("symbol", "timestamp")
        .agg(
            F.avg("open").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.avg("close").alias("close"),
            F.sum("volume").alias("volume"),
            F.max("datetime").alias("datetime"),
            F.max("date").alias("date")
        )
        .withColumn("timeframe", F.lit(timeframe))
    )

    return df_cached, df_agg, current_prices


def filter_by_price_range(df, current_prices_df, pct):
    df_with_price = df.join(F.broadcast(current_prices_df), "symbol", "left")
    df_filtered = (df_with_price
        .filter(
            (F.col("high") >= F.col("current_price") * (1 - pct)) &
            (F.col("low") <= F.col("current_price") * (1 + pct))
        )
        .drop("current_price")
    )
    return df_filtered


def identify_swings(df, lookback=3):
    window_spec = Window.partitionBy("symbol", "timeframe").orderBy("timestamp")

    df_with_context = df
    for i in range(1, lookback + 1):
        df_with_context = (df_with_context
            .withColumn(f"high_prev_{i}", F.lag("high", i).over(window_spec))
            .withColumn(f"high_next_{i}", F.lead("high", i).over(window_spec))
            .withColumn(f"low_prev_{i}", F.lag("low", i).over(window_spec))
            .withColumn(f"low_next_{i}", F.lead("low", i).over(window_spec))
        )

    swing_high_cond = F.lit(True)
    swing_low_cond = F.lit(True)
    for i in range(1, lookback + 1):
        swing_high_cond = swing_high_cond & (F.col("high") > F.col(f"high_prev_{i}")) & (F.col("high") > F.col(f"high_next_{i}"))
        swing_low_cond = swing_low_cond & (F.col("low") < F.col(f"low_prev_{i}")) & (F.col("low") < F.col(f"low_next_{i}"))

    df_swings = (df_with_context
        .withColumn("is_swing_high", swing_high_cond)
        .withColumn("is_swing_low", swing_low_cond)
        .withColumn("swing_high_level", F.when(F.col("is_swing_high"), F.col("high")))
        .withColumn("swing_low_level", F.when(F.col("is_swing_low"), F.col("low")))
        .withColumn("swing_high_ts", F.when(F.col("is_swing_high"), F.col("timestamp")))
        .withColumn("swing_low_ts", F.when(F.col("is_swing_low"), F.col("timestamp")))
    )

    cols_to_drop = []
    for i in range(1, lookback + 1):
        cols_to_drop += [f"high_prev_{i}", f"high_next_{i}", f"low_prev_{i}", f"low_next_{i}"]

    return df_swings.drop(*cols_to_drop)


def carry_forward_swings(df):
    window_spec = Window.partitionBy("symbol", "timeframe").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)
    df_carried = (df
        .withColumn("last_swing_high", F.last("swing_high_level", ignorenulls=True).over(window_spec))
        .withColumn("last_swing_high_ts", F.last("swing_high_ts", ignorenulls=True).over(window_spec))
        .withColumn("last_swing_low", F.last("swing_low_level", ignorenulls=True).over(window_spec))
        .withColumn("last_swing_low_ts", F.last("swing_low_ts", ignorenulls=True).over(window_spec))
    )
    return df_carried


def detect_bos(df):
    window_spec = Window.partitionBy("symbol", "timeframe").orderBy("timestamp")

    # Track when swing levels change
    df_with_swing_change = (df
        .withColumn("prev_swing_high", F.lag("last_swing_high").over(window_spec))
        .withColumn("prev_swing_low", F.lag("last_swing_low").over(window_spec))
        .withColumn("swing_high_changed",
            (F.col("last_swing_high") != F.col("prev_swing_high")) |
            F.col("prev_swing_high").isNull()
        )
        .withColumn("swing_low_changed",
            (F.col("last_swing_low") != F.col("prev_swing_low")) |
            F.col("prev_swing_low").isNull()
        )
    )

    # Create groups: new group when swing changes
    df_with_groups = (df_with_swing_change
        .withColumn("swing_high_group", F.sum(F.when(F.col("swing_high_changed"), 1).otherwise(0)).over(window_spec))
        .withColumn("swing_low_group", F.sum(F.when(F.col("swing_low_changed"), 1).otherwise(0)).over(window_spec))
    )

    # Check if close breaks the level
    df_with_breaks = (df_with_groups
        .withColumn("breaks_high",
            (F.col("close") > F.col("last_swing_high")) &
            (F.col("last_swing_high").isNotNull()) &
            (F.coalesce(F.col("is_swing_high"), F.lit(False)) == False)
        )
        .withColumn("breaks_low",
            (F.col("close") < F.col("last_swing_low")) &
            (F.col("last_swing_low").isNotNull()) &
            (F.coalesce(F.col("is_swing_low"), F.lit(False)) == False)
        )
    )

    # Find first break timestamp within each swing group
    window_high_group = Window.partitionBy("symbol", "timeframe", "swing_high_group")
    window_low_group = Window.partitionBy("symbol", "timeframe", "swing_low_group")

    df_with_first = (df_with_breaks
        .withColumn("first_break_high_ts",
            F.when(F.col("breaks_high"),
                F.min(F.when(F.col("breaks_high"), F.col("timestamp"))).over(window_high_group)
            )
        )
        .withColumn("first_break_low_ts",
            F.when(F.col("breaks_low"),
                F.min(F.when(F.col("breaks_low"), F.col("timestamp"))).over(window_low_group)
            )
        )
    )

    # BOS only on FIRST break
    df_bos = (df_with_first
        .withColumn("bos_bullish",
            (F.col("breaks_high") == True) & (F.col("timestamp") == F.col("first_break_high_ts"))
        )
        .withColumn("bos_bearish",
            (F.col("breaks_low") == True) & (F.col("timestamp") == F.col("first_break_low_ts"))
        )
        .drop("prev_swing_high", "prev_swing_low", "swing_high_changed", "swing_low_changed",
              "swing_high_group", "swing_low_group", "breaks_high", "breaks_low",
              "first_break_high_ts", "first_break_low_ts")
    )

    return df_bos


def process_timeframe(tf, checkpoint_ts=None):
    """Process single timeframe with optimized single read"""

    # Single read, get everything we need
    df_cached, df_agg, current_prices = load_and_prepare_data(tf, checkpoint_ts)

    # Filter by price range
    pct = PRICE_RANGE_PCT[tf]
    df_filtered = filter_by_price_range(df_agg, current_prices, pct)

    # Cache after filtering - this is reused multiple times
    df_filtered = df_filtered.cache()

    # Check if we have data (single count)
    if df_filtered.isEmpty():
        df_cached.unpersist()
        df_filtered.unpersist()
        return None, 0

    # Identify swings
    df_swings = identify_swings(df_filtered, SWING_LOOKBACK)

    # Carry forward swing levels
    df_carried = carry_forward_swings(df_swings)

    # Cache before BOS detection (heavy operation)
    df_carried = df_carried.cache()
    df_filtered.unpersist()

    # Detect BOS
    df_bos = detect_bos(df_carried)

    # Filter to only new data after checkpoint
    if checkpoint_ts:
        df_bos = df_bos.filter(F.col("timestamp") > checkpoint_ts)

    # Debug counts only if enabled
    if DEBUG:
        swing_high_cnt = df_carried.filter(F.col("is_swing_high")).count()
        swing_low_cnt = df_carried.filter(F.col("is_swing_low")).count()
        bos_bull = df_bos.filter(F.col("bos_bullish") == True).count()
        bos_bear = df_bos.filter(F.col("bos_bearish") == True).count()
        print(f"  Swings: {swing_high_cnt} high, {swing_low_cnt} low")
        print(f"  BOS: {bos_bull} bullish, {bos_bear} bearish")

    # Cleanup
    df_cached.unpersist()
    df_carried.unpersist()

    return df_bos, 1


def extract_bos_signals(df_processed):
    """Extract BOS signals from processed DataFrame"""
    df_bos = (df_processed
        .filter((F.col("bos_bullish") == True) | (F.col("bos_bearish") == True))
        .withColumn("bos_type",
            F.when(F.col("bos_bullish"), F.lit("bullish"))
             .when(F.col("bos_bearish"), F.lit("bearish"))
        )
        .withColumn("broken_level",
            F.when(F.col("bos_bullish"), F.col("last_swing_high"))
             .when(F.col("bos_bearish"), F.col("last_swing_low"))
        )
        .withColumn("swing_timestamp",
            F.when(F.col("bos_bullish"), F.col("last_swing_high_ts"))
             .when(F.col("bos_bearish"), F.col("last_swing_low_ts"))
        )
        .select(
            "symbol", "timeframe", "timestamp", "datetime", "bos_type",
            "broken_level", "close", "swing_timestamp",
            "last_swing_high", "last_swing_low",
            "open", "high", "low", "volume"
        )
        .withColumn("created_at", F.current_timestamp())
    )
    return df_bos

# ---
# MAIN EXECUTION
# ---

print("=" * 60)
print("BOS Detection v4 (Optimized)")
print("=" * 60)

log_table = ensure_log_table()
all_bos_signals = []

for tf in TIMEFRAMES:
    print(f"\n{'─' * 40}")
    print(f"Processing {tf}")
    print(f"{'─' * 40}")

    # Get checkpoint
    checkpoint_ts = get_checkpoint_for_timeframe(tf)
    print(f"  Checkpoint: {checkpoint_ts if checkpoint_ts else 'None (first run)'}")

    # Process
    df_processed, has_data = process_timeframe(tf, checkpoint_ts)

    if df_processed is None:
        write_log(log_table, tf, "skipped", 0, 0, checkpoint_ts, checkpoint_ts, "No data after filtering")
        print(f"  Skipped (no data)")
        continue

    # Extract signals
    df_bos = extract_bos_signals(df_processed)

    # Single count at the end
    signals_count = df_bos.count()

    if signals_count > 0:
        all_bos_signals.append(df_bos)
        new_checkpoint = df_processed.agg(F.max("timestamp")).first()[0]
    else:
        new_checkpoint = checkpoint_ts

    print(f"  Signals: {signals_count}")

    write_log(log_table, tf, "success", has_data, signals_count, checkpoint_ts, new_checkpoint, f"{signals_count} signals")
    print(f"  SUCCESS")

# ---
# WRITE RESULTS
# ---

print(f"\n{'=' * 60}")
print("Writing results")
print(f"{'=' * 60}")

if not all_bos_signals:
    print("No new BOS signals.")
else:
    df_all_signals = all_bos_signals[0]
    for df in all_bos_signals[1:]:
        df_all_signals = df_all_signals.unionByName(df)

    bos_table = f"{CATALOG}.{SCHEMA}.bos_warnings"

    if spark.catalog.tableExists(bos_table):
        delta_bos = DeltaTable.forName(spark, bos_table)
        delta_bos.alias("target").merge(
            df_all_signals.alias("source"),
            "target.symbol = source.symbol AND target.timestamp = source.timestamp AND target.timeframe = source.timeframe"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        # Get count from merge metrics
        history = spark.sql(f"DESCRIBE HISTORY {bos_table} LIMIT 1").first()
        metrics = history["operationMetrics"]
        inserted = metrics.get("numTargetRowsInserted", "?")
        print(f"Merged: +{inserted} new signals")
    else:
        df_all_signals.write.format("delta").mode("overwrite").saveAsTable(bos_table)
        print(f"Created {bos_table}")

print(f"\n{'=' * 60}")
print("Done!")
print(f"{'=' * 60}")
