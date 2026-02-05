# Databricks notebook source
# Databricks notebook source
# Break of Structure (BOS) Detection v3
# Features:
# - Gap detection for last 25 candles per timeframe
# - Sanity check on source data (not filtered)
# - Checkpoint per timeframe
# - Processing log table
# - Single BOS event per swing break

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, ArrayType, LongType
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
CANDLES_TO_CHECK = 25

# Timeframe intervals in milliseconds
TIMEFRAME_MS = {
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000
}

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

# Creates log table if not exists
def ensure_log_table():
    log_table = f"{CATALOG}.{SCHEMA}.bos_processing_log"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {log_table} (
            run_timestamp TIMESTAMP,
            process_name STRING,
            timeframe STRING,
            status STRING,
            expected_candles INT,
            found_candles INT,
            missing_timestamps ARRAY<LONG>,
            processed_rows INT,
            signals_found INT,
            checkpoint_used LONG,
            checkpoint_new LONG,
            message STRING
        )
    """)
    return log_table


# Writes log entry
def write_log(log_table, timeframe, status, expected, found, missing_ts, processed, signals, checkpoint_used, checkpoint_new, message):
    log_df = spark.createDataFrame([(
        datetime.now(timezone.utc),
        "bos_detection",
        timeframe,
        status,
        expected,
        found,
        missing_ts,
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
        StructField("expected_candles", IntegerType(), True),
        StructField("found_candles", IntegerType(), True),
        StructField("missing_timestamps", ArrayType(LongType()), True),
        StructField("processed_rows", IntegerType(), True),
        StructField("signals_found", IntegerType(), True),
        StructField("checkpoint_used", LongType(), True),
        StructField("checkpoint_new", LongType(), True),
        StructField("message", StringType(), True)
    ]))
    log_df.write.format("delta").mode("append").saveAsTable(log_table)

# ---
# DATA VALIDATION FUNCTIONS
# ---

# Calculates expected candle timestamps for last N candles from now
def get_expected_candles(timeframe, n=25):
    now_ms = int(time.time() * 1000)
    interval_ms = TIMEFRAME_MS[timeframe]
    last_closed = (now_ms // interval_ms) * interval_ms
    expected = [last_closed - (i * interval_ms) for i in range(n)]
    return expected


# Checks which candles exist in unified_klines for last N expected
def check_data_freshness(timeframe, expected_ts):
    df = spark.table(f"{CATALOG}.{SCHEMA}.unified_klines_{timeframe}")
    # Filter only to expected timestamps range to avoid full table scan
    min_ts = min(expected_ts)
    max_ts = max(expected_ts)
    existing_df = df.filter(
        (F.col("timestamp") >= min_ts) & (F.col("timestamp") <= max_ts)
    ).select("timestamp").distinct()
    existing = set([row["timestamp"] for row in existing_df.collect()])
    found = [ts for ts in expected_ts if ts in existing]
    missing = [ts for ts in expected_ts if ts not in existing]
    return found, missing

# ---
# CHECKPOINT FUNCTIONS
# ---

# Gets checkpoint for specific timeframe from bos_warnings
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

# Gets current price per symbol (last close from most recent candle)
def get_current_prices(timeframe):
    df = spark.table(f"{CATALOG}.{SCHEMA}.unified_klines_{timeframe}")
    window_spec = Window.partitionBy("symbol").orderBy(F.col("timestamp").desc())
    df_latest = (df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .select("symbol", F.col("close").alias("current_price"))
    )
    return df_latest


# Reads unified klines, aggregates OHLC across exchanges
def get_aggregated_klines(timeframe, checkpoint_ts=None):
    df = spark.table(f"{CATALOG}.{SCHEMA}.unified_klines_{timeframe}")
    
    # First run: process all history
    # Incremental: take data from checkpoint minus buffer for swing detection
    if checkpoint_ts:
        buffer_ms = TIMEFRAME_MS[timeframe] * (SWING_LOOKBACK + 1)
        df = df.filter(F.col("timestamp") >= (checkpoint_ts - buffer_ms))
    
    df_agg = (df
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
    return df_agg


# Filters data by price range from current price
def filter_by_price_range(df, current_prices_df, pct):
    df_with_price = df.join(current_prices_df, "symbol", "left")
    df_filtered = (df_with_price
        .filter(
            (F.col("high") >= F.col("current_price") * (1 - pct)) &
            (F.col("low") <= F.col("current_price") * (1 + pct))
        )
        .drop("current_price")
    )
    return df_filtered


# Identifies swing highs and swing lows
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


# Carries forward last known swing levels
def carry_forward_swings(df):
    window_spec = Window.partitionBy("symbol", "timeframe").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)
    df_carried = (df
        .withColumn("last_swing_high", F.last("swing_high_level", ignorenulls=True).over(window_spec))
        .withColumn("last_swing_high_ts", F.last("swing_high_ts", ignorenulls=True).over(window_spec))
        .withColumn("last_swing_low", F.last("swing_low_level", ignorenulls=True).over(window_spec))
        .withColumn("last_swing_low_ts", F.last("swing_low_ts", ignorenulls=True).over(window_spec))
    )
    return df_carried


# Detects BOS: only FIRST close beyond swing level after swing formed
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
    
    # Debug
    breaks_high_cnt = df_with_breaks.filter(F.col("breaks_high") == True).count()
    breaks_low_cnt = df_with_breaks.filter(F.col("breaks_low") == True).count()
    print(f"    Breaks (before rank): {breaks_high_cnt} high, {breaks_low_cnt} low")
    
    # Find first break timestamp within each swing group
    window_high_group = Window.partitionBy("symbol", "timeframe", "swing_high_group")
    window_low_group = Window.partitionBy("symbol", "timeframe", "swing_low_group")
    
    df_with_first = (df_with_breaks
        # Get min timestamp of break within each group
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
    
    # BOS only on FIRST break (timestamp matches first break in group)
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


# Process single timeframe
def process_timeframe(tf, checkpoint_ts=None):
    current_prices = get_current_prices(tf)
    pct = PRICE_RANGE_PCT[tf]
    
    df = get_aggregated_klines(tf, checkpoint_ts)
    total_before_filter = df.count()
    
    df = filter_by_price_range(df, current_prices, pct)
    row_count = df.count()
    
    print(f"    Total candles: {total_before_filter} | After price filter: {row_count}")
    
    if row_count == 0:
        return None, 0
    
    df = identify_swings(df, SWING_LOOKBACK)
    swing_high_cnt = df.filter(F.col("is_swing_high")).count()
    swing_low_cnt = df.filter(F.col("is_swing_low")).count()
    print(f"    Swings: {swing_high_cnt} high, {swing_low_cnt} low")
    
    df = carry_forward_swings(df)
    
    df = detect_bos(df)
    bos_bull = df.filter(F.col("bos_bullish") == True).count()
    bos_bear = df.filter(F.col("bos_bearish") == True).count()
    print(f"    BOS detected: {bos_bull} bullish, {bos_bear} bearish")
    
    # Filter to only new data (after checkpoint) for output
    if checkpoint_ts:
        df = df.filter(F.col("timestamp") > checkpoint_ts)
    
    return df, row_count

# ---
# MAIN EXECUTION
# ---

print("=" * 60)
print("BOS Detection v3")
print("=" * 60)

log_table = ensure_log_table()
print(f"Log table: {log_table}")

all_bos_signals = []

for tf in TIMEFRAMES:
    print(f"\n{'─' * 40}")
    print(f"Processing {tf}")
    print(f"{'─' * 40}")
    
    # Check data freshness
    expected_ts = get_expected_candles(tf, CANDLES_TO_CHECK)
    found_ts, missing_ts = check_data_freshness(tf, expected_ts)
    
    print(f"  Expected: {len(expected_ts)} | Found: {len(found_ts)} | Missing: {len(missing_ts)}")
    
    if missing_ts:
        print(f"  ⚠ Missing: {missing_ts[:3]}{'...' if len(missing_ts) > 3 else ''}")
    
    # Get checkpoint
    checkpoint_ts = get_checkpoint_for_timeframe(tf)
    print(f"  Checkpoint: {checkpoint_ts if checkpoint_ts else 'None (first run)'}")
    
    # Check if we have any data
    if not found_ts:
        write_log(log_table, tf, "error", len(expected_ts), 0, missing_ts, 0, 0, checkpoint_ts, None, "No candles found")
        print(f"  ✗ ERROR: No data")
        continue
    
    # Process
    df_processed, row_count = process_timeframe(tf, checkpoint_ts)
    
    if df_processed is None or df_processed.count() == 0:
        status = "warning" if missing_ts else "skipped"
        msg = "No new data" + (f", {len(missing_ts)} missing" if missing_ts else "")
        write_log(log_table, tf, status, len(expected_ts), len(found_ts), missing_ts, 0, 0, checkpoint_ts, checkpoint_ts, msg)
        print(f"  → Skipped (no new data)")
        continue
    
    # Extract BOS signals
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
    
    signals_count = df_bos.count()
    new_checkpoint = df_processed.agg(F.max("timestamp")).first()[0]
    
    print(f"  Processed: {row_count} | Signals: {signals_count}")
    
    if signals_count > 0:
        all_bos_signals.append(df_bos)
    
    # Log
    status = "warning" if missing_ts else "success"
    msg = f"{row_count} rows, {signals_count} signals" + (f", {len(missing_ts)} missing" if missing_ts else "")
    write_log(log_table, tf, status, len(expected_ts), len(found_ts), missing_ts, row_count, signals_count, checkpoint_ts, new_checkpoint, msg)
    print(f"  ✓ {status.upper()}")

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
    
    # Write bos_warnings
    bos_table = f"{CATALOG}.{SCHEMA}.bos_warnings"
    
    if spark.catalog.tableExists(bos_table):
        delta_bos = DeltaTable.forName(spark, bos_table)
        delta_bos.alias("target").merge(
            df_all_signals.alias("source"),
            "target.symbol = source.symbol AND target.timestamp = source.timestamp AND target.timeframe = source.timeframe"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"✓ Merged {df_all_signals.count()} into {bos_table}")
    else:
        df_all_signals.write.format("delta").mode("overwrite").saveAsTable(bos_table)
        print(f"✓ Created {bos_table}")

print(f"\n{'=' * 60}")
print("Done!")
print(f"{'=' * 60}")