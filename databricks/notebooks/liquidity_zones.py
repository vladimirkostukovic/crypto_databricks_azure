# Databricks notebook source
# CELL 0: CONFIGURATION
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, BooleanType
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Dict
from functools import reduce

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------

CATALOG = "crypto"
SCHEMA = "silver"

WARNINGS_TABLE = f"{CATALOG}.{SCHEMA}.warnings"
BOS_TABLE = f"{CATALOG}.{SCHEMA}.bos_warnings"
LIQ_DISTANCE_TABLE = f"{CATALOG}.{SCHEMA}.liq_distance"

FORCE_RECALC = True
STRICT_MODE = True
MAX_MISSING_PCT = 5
SCORE_THRESHOLD = 80
MIN_CONFIRMED_METHODS = 2
ZONE_MERGE_PCT = 0.5
BOS_ZONE_TOLERANCE_PCT = 1.0

TF_WEIGHTS = {"15m": 1, "1h": 2, "4h": 3, "1d": 4}
RECALC_INTERVALS = {"15m": 15, "1h": 60, "4h": 240, "1d": 1440}

TIMEFRAME_CONFIG = {
    "15m": {"klines_interval": "15m", "lookback_hours": 48, "bin_pct": 0.25, "output_table": f"{CATALOG}.{SCHEMA}.liq_15m"},
    "1h": {"klines_interval": "1h", "lookback_hours": 168, "bin_pct": 0.5, "output_table": f"{CATALOG}.{SCHEMA}.liq_1h"},
    "4h": {"klines_interval": "4h", "lookback_hours": 672, "bin_pct": 0.75, "output_table": f"{CATALOG}.{SCHEMA}.liq_4h"},
    "1d": {"klines_interval": "1d", "lookback_hours": 2160, "bin_pct": 1.0, "output_table": f"{CATALOG}.{SCHEMA}.liq_1d"}
}

KLINES_TABLE = f"{CATALOG}.{SCHEMA}.unified_klines"
ORDERBOOK_TABLE = f"{CATALOG}.{SCHEMA}.unified_orderbook"
OI_TABLE = f"{CATALOG}.{SCHEMA}.unified_oi"

def should_run(timeframe):
    now = datetime.now(timezone.utc)
    if timeframe == "15m":
        return True
    elif timeframe == "1h":
        return now.minute < 5
    elif timeframe == "4h":
        return now.hour % 4 == 0 and now.minute < 5
    elif timeframe == "1d":
        return now.hour == 0 and now.minute < 10
    return False

print("Config loaded")
print(f"  FORCE_RECALC: {FORCE_RECALC}")

# COMMAND ----------

# -----------------------------------------------------------------------------
# CELL 1: HELPER FUNCTIONS (OPTIMIZED)
# -----------------------------------------------------------------------------

def get_all_symbols_batch(spark):
    """Get symbols from all intervals in single pass"""
    dfs = []
    for interval in ["15m", "1h", "4h", "1d"]:
        try:
            df = spark.table(f"{KLINES_TABLE}_{interval}").select("symbol").distinct()
            dfs.append(df)
        except:
            continue
    if not dfs:
        return []
    all_symbols = reduce(lambda a, b: a.union(b), dfs).distinct()
    return sorted([row["symbol"] for row in all_symbols.collect()])


def get_current_prices_batch(spark, interval="15m"):
    """Get current prices for ALL symbols in single query"""
    try:
        klines = spark.table(f"{KLINES_TABLE}_{interval}")
        window = Window.partitionBy("symbol").orderBy(F.col("timestamp").desc())
        latest = klines.withColumn("rn", F.row_number().over(window)) \
            .filter(F.col("rn") == 1) \
            .select("symbol", F.col("close").alias("current_price"), F.col("timestamp").alias("price_ts"))
        return latest
    except:
        return None


def add_percentile_rank(df, value_col, partition_cols, output_col):
    window = Window.partitionBy(*partition_cols).orderBy(F.col(value_col))
    return df.withColumn(output_col, F.round(F.percent_rank().over(window) * 100, 0).cast(IntegerType()))


def should_recalculate(spark, timeframe):
    table = TIMEFRAME_CONFIG[timeframe]["output_table"]
    interval_minutes = RECALC_INTERVALS[timeframe]
    try:
        if not spark.catalog.tableExists(table):
            return True
        last_calc = spark.sql(f"SELECT MAX(calculated_at) as last_calc FROM {table}").first()["last_calc"]
        if last_calc is None:
            return True
        now = datetime.now(timezone.utc)
        last_calc_utc = last_calc.replace(tzinfo=timezone.utc) if last_calc.tzinfo is None else last_calc
        elapsed_minutes = (now - last_calc_utc).total_seconds() / 60
        return elapsed_minutes >= interval_minutes
    except:
        return True


def generate_zone_id(symbol, zone_low, zone_high, timeframe):
    import hashlib
    key = f"{symbol}_{timeframe}_{round(zone_low, 6)}_{round(zone_high, 6)}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


print("Helper functions loaded")

# COMMAND ----------

# -----------------------------------------------------------------------------
# CELL 2: CORE FUNCTIONS (OPTIMIZED - BATCH PROCESSING)
# -----------------------------------------------------------------------------

def load_bos_signals_batch(spark, symbols, timeframe, lookback_hours):
    """Load BOS signals for ALL symbols at once"""
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).timestamp() * 1000)
    try:
        if not spark.catalog.tableExists(BOS_TABLE):
            return None
        bos_df = spark.table(BOS_TABLE).filter(
            (F.col("symbol").isin(symbols)) &
            (F.col("timeframe") == timeframe) &
            (F.col("timestamp") >= cutoff_ms)
        )
        return bos_df
    except:
        return None


def load_klines_batch(spark, symbols, interval, lookback_hours):
    """Load klines for ALL symbols at once"""
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).timestamp() * 1000)
    klines = spark.table(f"{KLINES_TABLE}_{interval}").filter(
        (F.col("symbol").isin(symbols)) & (F.col("timestamp") >= cutoff_ms)
    )
    return klines.groupBy("symbol", "timestamp").agg(
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.avg("close").alias("close"),
        F.sum("volume").alias("volume")
    )


def calculate_volume_bins_batch(klines, bin_pct_map):
    """Calculate volume bins for ALL symbols with symbol-specific bin sizes"""
    # Join with bin_pct per symbol (use broadcast for small lookup)
    klines_with_typical = klines.withColumn(
        "typical_price", (F.col("high") + F.col("low") + F.col("close")) / 3
    )

    # For simplicity, use average bin_pct (can be refined per-symbol)
    avg_bin_pct = sum(bin_pct_map.values()) / len(bin_pct_map)

    # Get current price per symbol for bin calculation
    window_latest = Window.partitionBy("symbol").orderBy(F.col("timestamp").desc())
    latest_prices = klines.withColumn("rn", F.row_number().over(window_latest)) \
        .filter(F.col("rn") == 1) \
        .select("symbol", F.col("close").alias("ref_price"))

    df = klines_with_typical.join(F.broadcast(latest_prices), "symbol", "left")
    df = df.withColumn("bin_size", F.col("ref_price") * avg_bin_pct / 100)
    df = df.withColumn("bin_center", F.round(F.col("typical_price") / F.col("bin_size"), 0) * F.col("bin_size"))

    volume_bins = df.groupBy("symbol", "bin_center").agg(
        F.sum("volume").alias("volume"),
        F.count("*").alias("candle_count"),
        F.min("low").alias("bin_low"),
        F.max("high").alias("bin_high"),
        F.min("timestamp").alias("first_touch"),
        F.max("timestamp").alias("last_touch")
    )

    return add_percentile_rank(volume_bins, "volume", ["symbol"], "volume_score")


def calculate_oi_bins_batch(spark, symbols, interval, lookback_hours, klines):
    """Calculate OI bins for ALL symbols"""
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).timestamp() * 1000)

    try:
        oi_raw = spark.table(f"{OI_TABLE}_{interval}").filter(
            (F.col("symbol").isin(symbols)) & (F.col("timestamp") >= cutoff_ms)
        )
        if oi_raw.isEmpty():
            return None
    except:
        return None

    oi_data = oi_raw.groupBy("symbol", "timestamp").agg(
        F.sum("open_interest").alias("open_interest")
    )

    klines_price = klines.select("symbol", "timestamp", "close")

    # Proper alias usage to avoid ambiguous columns
    oi_with_price = oi_data.alias("oi").join(
        klines_price.alias("k"),
        (F.col("oi.symbol") == F.col("k.symbol")) & (F.col("oi.timestamp") == F.col("k.timestamp")),
        "inner"
    ).select(
        F.col("oi.symbol").alias("symbol"),
        F.col("oi.timestamp").alias("timestamp"),
        F.col("oi.open_interest").alias("open_interest"),
        F.col("k.close").alias("price")
    )

    window = Window.partitionBy("symbol").orderBy("timestamp")
    oi_with_delta = oi_with_price.withColumn("prev_oi", F.lag("open_interest", 1).over(window)) \
        .withColumn("oi_delta", F.col("open_interest") - F.col("prev_oi")) \
        .withColumn("oi_delta_abs", F.abs(F.col("oi_delta"))) \
        .filter(F.col("prev_oi").isNotNull())

    if oi_with_delta.isEmpty():
        return None

    # Get ref price per symbol
    window_latest = Window.partitionBy("symbol").orderBy(F.col("timestamp").desc())
    ref_prices = klines.withColumn("rn", F.row_number().over(window_latest)) \
        .filter(F.col("rn") == 1) \
        .select("symbol", F.col("close").alias("ref_price"))

    oi_with_ref = oi_with_delta.join(F.broadcast(ref_prices), "symbol", "left")
    oi_with_ref = oi_with_ref.withColumn("bin_size", F.col("ref_price") * 0.5 / 100)
    oi_with_ref = oi_with_ref.withColumn("bin_center", F.round(F.col("price") / F.col("bin_size"), 0) * F.col("bin_size"))

    oi_bins = oi_with_ref.groupBy("symbol", "bin_center").agg(
        F.sum("oi_delta_abs").alias("oi_delta_abs"),
        F.sum("oi_delta").alias("oi_delta_net"),
        F.count("*").alias("oi_records")
    )

    return add_percentile_rank(oi_bins, "oi_delta_abs", ["symbol"], "oi_score")


def calculate_orderbook_bins_batch(spark, symbols, interval, lookback_hours, klines):
    """Calculate orderbook bins for ALL symbols"""
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).timestamp() * 1000)

    try:
        orderbook = spark.table(f"{ORDERBOOK_TABLE}_{interval}").filter(
            (F.col("symbol").isin(symbols)) & (F.col("timestamp") >= cutoff_ms)
        )
        if orderbook.isEmpty():
            return None
    except:
        return None

    # Get ref prices
    window_latest = Window.partitionBy("symbol").orderBy(F.col("timestamp").desc())
    ref_prices = klines.withColumn("rn", F.row_number().over(window_latest)) \
        .filter(F.col("rn") == 1) \
        .select("symbol", F.col("close").alias("ref_price"))

    # Bid bins
    bid_bins = orderbook.filter(F.col("bid_avg_price").isNotNull()).select(
        "symbol", "timestamp",
        F.col("bid_avg_price").alias("price"),
        F.col("bid_total_quantity").alias("quantity")
    )

    # Ask bins
    ask_bins = orderbook.filter(F.col("ask_avg_price").isNotNull()).select(
        "symbol", "timestamp",
        F.col("ask_avg_price").alias("price"),
        F.col("ask_total_quantity").alias("quantity")
    )

    all_orders = bid_bins.union(ask_bins)
    if all_orders.isEmpty():
        return None

    all_orders = all_orders.join(F.broadcast(ref_prices), "symbol", "left")
    all_orders = all_orders.withColumn("bin_size", F.col("ref_price") * 0.5 / 100)
    all_orders = all_orders.withColumn("bin_center", F.round(F.col("price") / F.col("bin_size"), 0) * F.col("bin_size"))

    # Count total snapshots per symbol
    snapshots_per_symbol = orderbook.groupBy("symbol").agg(F.countDistinct("timestamp").alias("total_snapshots"))

    ob_bins = all_orders.groupBy("symbol", "bin_center").agg(
        F.sum("quantity").alias("ob_quantity"),
        F.countDistinct("timestamp").alias("ob_snapshots")
    )

    ob_bins = ob_bins.join(F.broadcast(snapshots_per_symbol), "symbol", "left")
    ob_bins = ob_bins.withColumn("ob_frequency", F.round((F.col("ob_snapshots") / F.col("total_snapshots")) * 100, 0))

    return add_percentile_rank(ob_bins, "ob_quantity", ["symbol"], "orderbook_score")


print("Core functions loaded")

# COMMAND ----------

# -----------------------------------------------------------------------------
# CELL 3: ZONE CALCULATOR (OPTIMIZED - DISTRIBUTED)
# -----------------------------------------------------------------------------

def calculate_zones_distributed(spark, symbols, timeframe):
    """
    Calculate liquidity zones using distributed Spark operations.
    Minimizes collect() calls and processes all symbols in parallel.
    """
    config = TIMEFRAME_CONFIG[timeframe]

    print(f"\n{'='*60}")
    print(f"LIQUIDITY ZONES: {timeframe} (DISTRIBUTED)")
    print(f"{'='*60}")
    print(f"Symbols: {len(symbols)}, Lookback: {config['lookback_hours']}h")

    if not FORCE_RECALC and not should_recalculate(spark, timeframe):
        print("Skipped (not due)")
        return

    # Load all data in batch
    klines = load_klines_batch(spark, symbols, config["klines_interval"], config["lookback_hours"]).cache()
    print(f"Loaded klines")

    # Calculate bins for all symbols at once
    bin_pct_map = {s: config["bin_pct"] for s in symbols}
    volume_bins = calculate_volume_bins_batch(klines, bin_pct_map).cache()
    oi_bins = calculate_oi_bins_batch(spark, symbols, config["klines_interval"], config["lookback_hours"], klines)
    ob_bins = calculate_orderbook_bins_batch(spark, symbols, config["klines_interval"], config["lookback_hours"], klines)

    if oi_bins:
        oi_bins = oi_bins.cache()
    if ob_bins:
        ob_bins = ob_bins.cache()

    print(f"Calculated bins")

    # Load BOS signals
    bos_df = load_bos_signals_batch(spark, symbols, timeframe, config["lookback_hours"])
    if bos_df:
        bos_df = bos_df.cache()

    # Get current prices
    current_prices = get_current_prices_batch(spark, config["klines_interval"])
    if current_prices:
        current_prices = current_prices.cache()

    # Build zones from volume bins
    # Group adjacent bins into zones using window functions
    window_order = Window.partitionBy("symbol").orderBy("bin_center")

    zones_raw = volume_bins.withColumn("prev_bin", F.lag("bin_center").over(window_order)) \
        .withColumn("gap", F.col("bin_center") - F.col("prev_bin"))

    # Join with current prices to get merge threshold
    zones_raw = zones_raw.join(F.broadcast(current_prices.select("symbol", "current_price")), "symbol", "left")
    zones_raw = zones_raw.withColumn("merge_threshold", F.col("current_price") * ZONE_MERGE_PCT / 100)

    # Mark zone boundaries (new zone when gap > threshold)
    zones_raw = zones_raw.withColumn(
        "is_new_zone",
        F.when(F.col("prev_bin").isNull(), 1)
         .when(F.col("gap") > F.col("merge_threshold"), 1)
         .otherwise(0)
    )

    # Assign zone IDs using cumsum
    zones_raw = zones_raw.withColumn("zone_num", F.sum("is_new_zone").over(window_order))

    # Aggregate bins into zones
    zones_agg = zones_raw.groupBy("symbol", "zone_num").agg(
        F.min("bin_low").alias("zone_low"),
        F.max("bin_high").alias("zone_high"),
        F.sum("volume").alias("total_volume"),
        F.max("volume_score").alias("volume_score"),
        F.sum("candle_count").alias("candle_count"),
        F.min("first_touch").alias("first_touch_ts"),
        F.max("last_touch").alias("last_touch_ts"),
        F.first("current_price").alias("current_price"),
        # POC = bin with max volume
        F.max_by("bin_center", "volume").alias("poc_price")
    )

    # Add OI scores
    if oi_bins:
        oi_zones = oi_bins.groupBy("symbol").agg(
            F.collect_list(F.struct("bin_center", "oi_score", "oi_delta_net")).alias("oi_data")
        )
        zones_agg = zones_agg.join(F.broadcast(oi_zones), "symbol", "left")
    else:
        zones_agg = zones_agg.withColumn("oi_data", F.lit(None))

    # Add OB scores
    if ob_bins:
        ob_zones = ob_bins.groupBy("symbol").agg(
            F.collect_list(F.struct("bin_center", "orderbook_score")).alias("ob_data")
        )
        zones_agg = zones_agg.join(F.broadcast(ob_zones), "symbol", "left")
    else:
        zones_agg = zones_agg.withColumn("ob_data", F.lit(None))

    # Add BOS confirmation
    if bos_df:
        bos_summary = bos_df.groupBy("symbol").agg(
            F.collect_list(F.struct("close", "bos_type", "timestamp")).alias("bos_signals")
        )
        zones_agg = zones_agg.join(F.broadcast(bos_summary), "symbol", "left")
    else:
        zones_agg = zones_agg.withColumn("bos_signals", F.lit(None))

    # Calculate derived fields
    zones_final = zones_agg.withColumn("timeframe", F.lit(timeframe)) \
        .withColumn("zone_id", F.md5(F.concat_ws("_", "symbol", "timeframe", "zone_low", "zone_high"))) \
        .withColumn("zone_mid", (F.col("zone_low") + F.col("zone_high")) / 2) \
        .withColumn("distance_from_price_pct",
            F.round(((F.col("zone_mid") - F.col("current_price")) / F.col("current_price")) * 100, 2)) \
        .withColumn("calculated_at", F.current_timestamp())

    # Simple scoring (can be refined with UDFs for complex logic)
    zones_final = zones_final.withColumn("oi_score", F.lit(50)) \
        .withColumn("orderbook_score", F.lit(50)) \
        .withColumn("bos_confirmed", F.col("bos_signals").isNotNull()) \
        .withColumn("confirmed_methods",
            F.when(F.col("volume_score") >= SCORE_THRESHOLD, 1).otherwise(0) +
            F.when(F.col("oi_score") >= SCORE_THRESHOLD, 1).otherwise(0) +
            F.when(F.col("orderbook_score") >= SCORE_THRESHOLD, 1).otherwise(0)
        ) \
        .withColumn("strength_score",
            (F.col("volume_score") * 0.4) + (F.col("oi_score") * 0.35) + (F.col("orderbook_score") * 0.25) +
            (F.col("confirmed_methods") * 5) +
            F.when(F.col("bos_confirmed"), 15).otherwise(0)
        )

    # Rank zones per symbol
    window_rank = Window.partitionBy("symbol").orderBy(F.col("strength_score").desc())
    zones_final = zones_final.withColumn("zone_rank", F.row_number().over(window_rank))

    # Select output columns
    output_cols = [
        "zone_id", "symbol", "timeframe", "zone_low", "zone_high", "poc_price",
        F.col("poc_price").alias("volume_poc"),
        "total_volume", "volume_score", "oi_score", "orderbook_score",
        F.lit(0.0).alias("oi_delta_net"),
        "confirmed_methods", "strength_score", "zone_rank",
        "candle_count",
        F.lit(0).alias("touch_count"),
        F.lit(0).alias("touch_from_above"),
        F.lit(0).alias("touch_from_below"),
        F.lit(None).cast(StringType()).alias("last_touch_direction"),
        F.lit(0).alias("bounce_count"),
        F.lit(0).alias("break_count"),
        F.lit(0).alias("bounce_up_count"),
        F.lit(0).alias("bounce_down_count"),
        F.lit(0.0).alias("avg_bounce_pct"),
        F.lit(0.0).alias("max_bounce_pct"),
        F.lit(0.0).alias("avg_hold_candles"),
        F.lit(None).cast(StringType()).alias("last_reaction"),
        F.lit("untested").alias("zone_type"),
        F.lit(None).cast(StringType()).alias("zone_role"),
        F.lit(False).alias("is_mirror"),
        F.lit(None).cast(LongType()).alias("flip_timestamp"),
        "bos_confirmed",
        F.lit(None).cast(DoubleType()).alias("bos_price"),
        F.lit(None).cast(StringType()).alias("bos_type"),
        F.lit(None).cast(LongType()).alias("bos_timestamp"),
        "first_touch_ts", "last_touch_ts",
        F.lit(0.0).alias("hours_since_touch"),
        "distance_from_price_pct", "current_price",
        F.lit(0).alias("candles_total"),
        F.lit(0).alias("candles_expected"),
        F.lit(0).alias("candles_missing"),
        F.lit(True).alias("continuity_ok"),
        "calculated_at"
    ]

    zones_output = zones_final.select(*output_cols)

    # Write to table
    output_table = config["output_table"]
    zones_output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

    zone_count = spark.table(output_table).count()
    print(f"Saved {zone_count} zones to {output_table}")

    # Cleanup
    klines.unpersist()
    volume_bins.unpersist()
    if oi_bins:
        oi_bins.unpersist()
    if ob_bins:
        ob_bins.unpersist()
    if bos_df:
        bos_df.unpersist()
    if current_prices:
        current_prices.unpersist()


print("Zone calculator loaded")

# COMMAND ----------

# CELL 4: RUN ALL TIMEFRAMES
SYMBOLS = get_all_symbols_batch(spark)
print(f"Found {len(SYMBOLS)} symbols")

for tf in ["15m", "1h", "4h", "1d"]:
    if should_run(tf):
        calculate_zones_distributed(spark, SYMBOLS, tf)
    else:
        print(f"Skipped {tf} (not scheduled)")

# COMMAND ----------

# -----------------------------------------------------------------------------
# CELL 5: CONFLUENCE + LIQ_DISTANCE (FULLY DISTRIBUTED)
# -----------------------------------------------------------------------------

def add_confluence_and_distance_distributed(spark):
    """Fully distributed confluence and distance calculation"""

    print("=" * 60)
    print("CONFLUENCE + DISTANCE MAP")
    print("=" * 60)

    # Load all zones
    all_zones_dfs = []
    for tf in ["15m", "1h", "4h", "1d"]:
        table = TIMEFRAME_CONFIG[tf]["output_table"]
        if spark.catalog.tableExists(table):
            df = spark.table(table).withColumn("source_tf", F.lit(tf))
            all_zones_dfs.append(df)
            print(f"[{tf}] Loaded zones")

    if not all_zones_dfs:
        print("No zones found")
        return

    all_zones = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), all_zones_dfs).cache()
    total_zones = all_zones.count()
    print(f"Total: {total_zones} zones")

    # Confluence via self-join
    print("\nCalculating confluence...")

    zones_a = all_zones.select(
        F.col("zone_id").alias("a_zone_id"),
        F.col("symbol").alias("a_symbol"),
        F.col("source_tf").alias("a_tf"),
        F.col("zone_low").alias("a_zone_low"),
        F.col("zone_high").alias("a_zone_high"),
        F.col("poc_price").alias("a_poc_price"),
        F.col("strength_score").alias("a_strength_score"),
        F.col("bos_confirmed").alias("a_bos_confirmed"),
        F.col("bos_type").alias("a_bos_type"),
        F.col("bos_price").alias("a_bos_price"),
        F.col("bos_timestamp").alias("a_bos_timestamp"),
        F.col("is_mirror").alias("a_is_mirror"),
        F.col("zone_role").alias("a_zone_role"),
        F.col("zone_type").alias("a_zone_type"),
        F.col("volume_score").alias("a_volume_score"),
        F.col("oi_score").alias("a_oi_score"),
        F.col("bounce_count").alias("a_bounce_count"),
        F.col("break_count").alias("a_break_count"),
        F.col("touch_count").alias("a_touch_count")
    )

    zones_b = all_zones.select(
        F.col("symbol").alias("b_symbol"),
        F.col("source_tf").alias("b_tf"),
        F.col("zone_low").alias("b_zone_low"),
        F.col("zone_high").alias("b_zone_high")
    )

    # Join for overlaps (different TFs only)
    overlaps = zones_a.join(
        zones_b,
        (F.col("a_symbol") == F.col("b_symbol")) &
        (F.col("a_zone_low") <= F.col("b_zone_high")) &
        (F.col("a_zone_high") >= F.col("b_zone_low")) &
        (F.col("a_tf") != F.col("b_tf")),
        "left"
    )

    # Aggregate overlapping TFs
    confluence_agg = overlaps.groupBy(
        "a_zone_id", "a_symbol", "a_tf", "a_zone_low", "a_zone_high",
        "a_poc_price", "a_strength_score", "a_bos_confirmed",
        "a_bos_type", "a_bos_price", "a_bos_timestamp", "a_is_mirror",
        "a_zone_role", "a_zone_type", "a_volume_score", "a_oi_score",
        "a_bounce_count", "a_break_count", "a_touch_count"
    ).agg(
        F.collect_set("b_tf").alias("overlapping_tfs")
    )

    # Calculate confluence score
    zones_with_conf = confluence_agg.withColumn(
        "all_tfs", F.array_union(F.array(F.col("a_tf")), F.coalesce(F.col("overlapping_tfs"), F.array()))
    ).withColumn(
        "confluence_score",
        F.when(F.array_contains(F.col("all_tfs"), "15m"), 1).otherwise(0) +
        F.when(F.array_contains(F.col("all_tfs"), "1h"), 2).otherwise(0) +
        F.when(F.array_contains(F.col("all_tfs"), "4h"), 3).otherwise(0) +
        F.when(F.array_contains(F.col("all_tfs"), "1d"), 4).otherwise(0)
    ).withColumn(
        "confluence_tfs", F.array_join(F.array_sort(F.col("all_tfs")), ",")
    )

    # Get current prices (fixed ambiguous column issue)
    print("Fetching prices...")
    current_prices_df = None
    for tf in ["15m", "1h", "4h"]:
        try:
            klines = spark.table(f"{KLINES_TABLE}_{tf}")
            window = Window.partitionBy("symbol").orderBy(F.col("timestamp").desc())
            current_prices_df = klines.withColumn("rn", F.row_number().over(window)) \
                .filter(F.col("rn") == 1) \
                .select(
                    F.col("symbol").alias("price_symbol"),
                    F.col("close").alias("current_price")
                )
            break
        except:
            continue

    if current_prices_df is None:
        print("No price data")
        all_zones.unpersist()
        return

    # Build liq_distance
    print("Building distance map...")

    liq_distance = zones_with_conf.filter(F.col("a_bos_confirmed") == True).join(
        current_prices_df,
        F.col("a_symbol") == F.col("price_symbol"),
        "inner"
    ).withColumn(
        "distance_pct", F.round(((F.col("a_poc_price") - F.col("current_price")) / F.col("current_price")) * 100, 4)
    ).withColumn(
        "direction", F.when(F.col("a_poc_price") > F.col("current_price"), "above").otherwise("below")
    ).withColumn(
        "calculated_at", F.current_timestamp()
    )

    # Deduplicate by POC bucket
    liq_distance = liq_distance.withColumn(
        "poc_bucket", F.round(F.col("a_poc_price") / (F.col("current_price") * 0.005), 0)
    )

    window_bucket = Window.partitionBy("a_symbol", "poc_bucket").orderBy(F.col("a_strength_score").desc())
    liq_deduped = liq_distance.withColumn("bucket_rank", F.row_number().over(window_bucket)) \
        .filter(F.col("bucket_rank") == 1) \
        .drop("bucket_rank", "poc_bucket", "price_symbol")

    # Rank by distance
    window_rank = Window.partitionBy("a_symbol").orderBy(F.abs(F.col("distance_pct")))
    liq_ranked = liq_deduped.withColumn("rank", F.row_number().over(window_rank))

    # Final output
    liq_output = liq_ranked.select(
        F.col("a_symbol").alias("symbol"),
        "current_price", "rank",
        F.col("a_poc_price").alias("poc_price"),
        F.col("a_tf").alias("timeframe"),
        "distance_pct", "direction",
        F.col("a_zone_low").alias("zone_low"),
        F.col("a_zone_high").alias("zone_high"),
        F.col("a_is_mirror").alias("is_mirror"),
        F.col("a_strength_score").alias("strength_score"),
        F.col("a_zone_role").alias("zone_role"),
        F.col("a_zone_type").alias("zone_type"),
        F.col("a_bos_type").alias("bos_type"),
        F.col("a_bos_confirmed").alias("bos_confirmed"),
        F.col("a_bos_price").alias("bos_price"),
        F.col("a_bos_timestamp").alias("bos_timestamp"),
        "confluence_score", "confluence_tfs",
        F.col("a_bounce_count").alias("bounce_count"),
        F.col("a_break_count").alias("break_count"),
        F.col("a_touch_count").alias("touch_count"),
        F.col("a_volume_score").alias("volume_score"),
        F.col("a_oi_score").alias("oi_score"),
        "calculated_at"
    )

    # Write
    liq_output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(LIQ_DISTANCE_TABLE)

    final_count = spark.table(LIQ_DISTANCE_TABLE).count()
    print(f"\nSaved {final_count} records to {LIQ_DISTANCE_TABLE}")

    # Show sample
    spark.sql(f"""
        SELECT symbol, rank, ROUND(poc_price, 2) as poc, timeframe,
               ROUND(distance_pct, 2) as dist, direction,
               ROUND(strength_score, 1) as str, bos_type
        FROM {LIQ_DISTANCE_TABLE}
        WHERE rank <= 3
        ORDER BY symbol, rank
        LIMIT 30
    """).show(truncate=False)

    all_zones.unpersist()


# Run
add_confluence_and_distance_distributed(spark)
