# Databricks notebook source
# CELL 0: CONFIGURATION
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, BooleanType
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Dict

# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------

CATALOG = "crypto"
SCHEMA = "silver"

WARNINGS_TABLE = f"{CATALOG}.{SCHEMA}.warnings"
BOS_TABLE = f"{CATALOG}.{SCHEMA}.bos_warnings"
LIQ_DISTANCE_TABLE = f"{CATALOG}.{SCHEMA}.liq_distance"

# ---
FORCE_RECALC = True
# ---

# Strict mode
STRICT_MODE = True
MAX_MISSING_PCT = 5

# Zone detection
SCORE_THRESHOLD = 80
MIN_CONFIRMED_METHODS = 2
ZONE_MERGE_PCT = 0.5
BOS_ZONE_TOLERANCE_PCT = 1.0

# TF weights for confluence
TF_WEIGHTS = {"15m": 1, "1h": 2, "4h": 3, "1d": 4}

# Recalc intervals (minutes)
RECALC_INTERVALS = {"15m": 15, "1h": 60, "4h": 240, "1d": 1440}

# Timeframe configs
TIMEFRAME_CONFIG = {
    "15m": {
        "klines_interval": "15m",
        "lookback_hours": 48,
        "bin_pct": 0.25,
        "output_table": f"{CATALOG}.{SCHEMA}.liq_15m"
    },
    "1h": {
        "klines_interval": "1h",
        "lookback_hours": 168,
        "bin_pct": 0.5,
        "output_table": f"{CATALOG}.{SCHEMA}.liq_1h"
    },
    "4h": {
        "klines_interval": "4h",
        "lookback_hours": 672,
        "bin_pct": 0.75,
        "output_table": f"{CATALOG}.{SCHEMA}.liq_4h"
    },
    "1d": {
        "klines_interval": "1d",
        "lookback_hours": 2160,
        "bin_pct": 1.0,
        "output_table": f"{CATALOG}.{SCHEMA}.liq_1d"
    }
}

# Source tables
KLINES_TABLE = f"{CATALOG}.{SCHEMA}.unified_klines"
ORDERBOOK_TABLE = f"{CATALOG}.{SCHEMA}.unified_orderbook"
OI_TABLE = f"{CATALOG}.{SCHEMA}.unified_oi"

# -----------------------------------------------------------------------------
# SCHEDULER
# -----------------------------------------------------------------------------

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

print("✓ Cell 0: Config loaded")
print(f"  FORCE_RECALC: {FORCE_RECALC}")
print(f"  Timeframes: {list(TIMEFRAME_CONFIG.keys())}")

# COMMAND ----------

# -----------------------------------------------------------------------------
# CELL 1: HELPER FUNCTIONS
# -----------------------------------------------------------------------------

# Gets all unique symbols from source tables
def get_all_symbols(spark):
    symbols = set()
    for interval in ["15m", "1h", "4h", "1d"]:
        try:
            df = spark.table(f"{KLINES_TABLE}_{interval}").select("symbol").distinct()
            for row in df.collect():
                symbols.add(row["symbol"])
        except:
            continue
    return sorted(list(symbols))


# Gets current price for symbol
def get_current_price(spark, symbol):
    for interval in ["15m", "1h", "4h", "1d"]:
        try:
            latest = spark.table(f"{KLINES_TABLE}_{interval}") \
                .filter(F.col("symbol") == symbol) \
                .orderBy(F.col("timestamp").desc()) \
                .select("close", "timestamp") \
                .first()
            if latest and latest["close"]:
                return float(latest["close"]), latest["timestamp"]
        except:
            continue
    return None, None


# Adds percentile rank column
def add_percentile_rank(df, value_col, partition_cols, output_col):
    window = Window.partitionBy(*partition_cols).orderBy(F.col(value_col))
    return df.withColumn(
        output_col,
        F.round(F.percent_rank().over(window) * 100, 0).cast(IntegerType())
    )


# Checks time series continuity (distributed - no collect)
def check_continuity(spark, df, interval_minutes):
    # Use aggregations instead of collecting all timestamps
    stats = df.agg(
        F.count("timestamp").alias("total_count"),
        F.countDistinct("timestamp").alias("unique_count"),
        F.min("timestamp").alias("min_ts"),
        F.max("timestamp").alias("max_ts")
    ).first()

    if stats["total_count"] == 0:
        return False, 0, 0, 0, 0, []

    total_count = stats["total_count"]
    unique_count = stats["unique_count"]
    min_ts = stats["min_ts"]
    max_ts = stats["max_ts"]

    if unique_count < 2:
        return True, unique_count, unique_count, 0, 0, []

    interval_ms = interval_minutes * 60 * 1000
    duplicates = total_count - unique_count
    expected = int((max_ts - min_ts) / interval_ms) + 1
    missing = expected - unique_count

    missing_pct = (missing / expected * 100) if expected > 0 else 0
    is_valid = (duplicates == 0) and (missing_pct <= MAX_MISSING_PCT)

    return is_valid, unique_count, expected, missing, duplicates, []


# Checks if timeframe needs recalculation
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


# Loads existing zones from table
def load_existing_zones(spark, timeframe):
    table = TIMEFRAME_CONFIG[timeframe]["output_table"]
    try:
        if spark.catalog.tableExists(table):
            return [row.asDict() for row in spark.table(table).collect()]
    except:
        pass
    return []


# Generates unique zone ID
def generate_zone_id(symbol, zone_low, zone_high, timeframe):
    import hashlib
    key = f"{symbol}_{timeframe}_{round(zone_low, 6)}_{round(zone_high, 6)}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


# Calculates strength score
def calculate_strength_score(volume_score, oi_score, orderbook_score, confirmed_methods, bounce_count, break_count, bos_confirmed):
    base_score = (volume_score * 0.4) + (oi_score * 0.35) + (orderbook_score * 0.25)
    confirmation_bonus = confirmed_methods * 5
    
    total_reactions = bounce_count + break_count
    reaction_bonus = (bounce_count / total_reactions * 10) if total_reactions > 0 else 0
    
    bos_bonus = 15 if bos_confirmed else 0
    
    return round(base_score + confirmation_bonus + reaction_bonus + bos_bonus, 1)


print("✓ Cell 1: Helper functions loaded")

# COMMAND ----------

# -----------------------------------------------------------------------------
# CELL 2: CORE FUNCTIONS
# -----------------------------------------------------------------------------

# Loads BOS signals for symbol/timeframe
def load_bos_signals(spark, symbol, timeframe, lookback_hours):
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    cutoff_ms = int(cutoff_time.timestamp() * 1000)
    
    try:
        if not spark.catalog.tableExists(BOS_TABLE):
            return []
        
        bos_df = spark.table(BOS_TABLE).filter(
            (F.col("symbol") == symbol) &
            (F.col("timeframe") == timeframe) &
            (F.col("timestamp") >= cutoff_ms)
        ).orderBy(F.col("timestamp").desc())
        
        return [row.asDict() for row in bos_df.collect()]
    except:
        return []


# Finds BOS inside zone
def find_bos_in_zone(zone_low, zone_high, bos_signals, tolerance_pct):
    zone_mid = (zone_low + zone_high) / 2
    tolerance = zone_mid * tolerance_pct / 100
    
    check_low = zone_low - tolerance
    check_high = zone_high + tolerance
    
    for bos in bos_signals:
        bos_price = bos.get("close") or bos.get("poc_price")
        if bos_price and check_low <= bos_price <= check_high:
            return bos
    return None


# Loads klines data
def load_klines(spark, symbol, interval, lookback_hours):
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    cutoff_ms = int(cutoff_time.timestamp() * 1000)
    
    klines_raw = spark.table(f"{KLINES_TABLE}_{interval}").filter(
        (F.col("symbol") == symbol) &
        (F.col("timestamp") >= cutoff_ms)
    )
    
    return klines_raw.groupBy("symbol", "timestamp").agg(
        F.max("high").alias("high"),
        F.min("low").alias("low"),
        F.avg("close").alias("close"),
        F.sum("volume").alias("volume")
    )


# Calculates volume bins
def calculate_volume_bins(klines, current_price, bin_pct):
    bin_size = current_price * bin_pct / 100
    
    df = klines.withColumn(
        "typical_price", (F.col("high") + F.col("low") + F.col("close")) / 3
    ).withColumn(
        "bin_center", F.round(F.col("typical_price") / bin_size, 0) * bin_size
    )
    
    volume_bins = df.groupBy("symbol", "bin_center").agg(
        F.sum("volume").alias("volume"),
        F.count("*").alias("candle_count"),
        F.min("low").alias("bin_low"),
        F.max("high").alias("bin_high"),
        F.min("timestamp").alias("first_touch"),
        F.max("timestamp").alias("last_touch")
    )
    
    return add_percentile_rank(volume_bins, "volume", ["symbol"], "volume_score")


# Calculates OI bins
def calculate_oi_bins(spark, symbol, interval, lookback_hours, klines, current_price, bin_pct):
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).timestamp() * 1000)
    
    try:
        oi_raw = spark.table(f"{OI_TABLE}_{interval}").filter(
            (F.col("symbol") == symbol) & (F.col("timestamp") >= cutoff_ms)
        )
        
        oi_data = oi_raw.groupBy("symbol", "timestamp").agg(
            F.sum("open_interest").alias("open_interest")
        ).orderBy("timestamp")
        
        if oi_data.count() == 0:
            return None
    except:
        return None
    
    klines_price = klines.select("symbol", "timestamp", "close")
    
    oi_with_price = oi_data.alias("oi").join(
        klines_price.alias("k"),
        (F.col("oi.symbol") == F.col("k.symbol")) & (F.col("oi.timestamp") == F.col("k.timestamp")),
        "inner"
    ).select(
        F.col("oi.symbol"), F.col("oi.timestamp"),
        F.col("oi.open_interest"), F.col("k.close").alias("price")
    )
    
    window = Window.partitionBy("symbol").orderBy("timestamp")
    oi_with_delta = oi_with_price.withColumn(
        "prev_oi", F.lag("open_interest", 1).over(window)
    ).withColumn(
        "oi_delta", F.col("open_interest") - F.col("prev_oi")
    ).withColumn(
        "oi_delta_abs", F.abs(F.col("oi_delta"))
    ).filter(F.col("prev_oi").isNotNull())
    
    if oi_with_delta.count() == 0:
        return None
    
    bin_size = current_price * bin_pct / 100
    
    oi_bins = oi_with_delta.withColumn(
        "bin_center", F.round(F.col("price") / bin_size, 0) * bin_size
    ).groupBy("symbol", "bin_center").agg(
        F.sum("oi_delta_abs").alias("oi_delta_abs"),
        F.sum("oi_delta").alias("oi_delta_net"),
        F.count("*").alias("oi_records")
    )
    
    return add_percentile_rank(oi_bins, "oi_delta_abs", ["symbol"], "oi_score")


# Calculates orderbook bins
def calculate_orderbook_bins(spark, symbol, interval, lookback_hours, current_price, bin_pct):
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).timestamp() * 1000)

    try:
        orderbook = spark.table(f"{ORDERBOOK_TABLE}_{interval}").filter(
            (F.col("symbol") == symbol) & (F.col("timestamp") >= cutoff_ms)
        )
        if orderbook.count() == 0:
            return None
    except:
        return None
    
    bin_size = current_price * bin_pct / 100
    
    bids = orderbook.select("symbol", "timestamp", F.explode("bids").alias("bid")).select(
        "symbol", "timestamp",
        F.col("bid").getItem(0).cast(DoubleType()).alias("price"),
        F.col("bid").getItem(1).cast(DoubleType()).alias("quantity")
    )
    
    asks = orderbook.select("symbol", "timestamp", F.explode("asks").alias("ask")).select(
        "symbol", "timestamp",
        F.col("ask").getItem(0).cast(DoubleType()).alias("price"),
        F.col("ask").getItem(1).cast(DoubleType()).alias("quantity")
    )
    
    all_orders = bids.union(asks)
    if all_orders.count() == 0:
        return None
    
    total_snapshots = orderbook.select("timestamp").distinct().count()
    
    ob_bins = all_orders.withColumn(
        "bin_center", F.round(F.col("price") / bin_size, 0) * bin_size
    ).groupBy("symbol", "bin_center").agg(
        F.sum("quantity").alias("ob_quantity"),
        F.countDistinct("timestamp").alias("ob_snapshots")
    ).withColumn(
        "ob_frequency", F.round((F.col("ob_snapshots") / total_snapshots) * 100, 0)
    )
    
    return add_percentile_rank(ob_bins, "ob_quantity", ["symbol"], "orderbook_score")


# Merges bins to zones
def merge_bins_to_zones(bins_df, current_price, zone_merge_pct):
    if bins_df is None:
        return None

    # Check count without collecting
    bin_count = bins_df.count()
    if bin_count == 0:
        return None

    bins_list = [row.asDict() for row in bins_df.orderBy("bin_center").collect()]
    if len(bins_list) == 0:
        return None
    
    merge_threshold = current_price * zone_merge_pct / 100
    zones = []
    current_zone = None
    
    for bin_row in bins_list:
        bin_center = bin_row["bin_center"]
        
        if current_zone is None:
            current_zone = {
                "symbol": bin_row["symbol"],
                "bins": [bin_row],
                "zone_low": bin_row.get("bin_low", bin_center),
                "zone_high": bin_row.get("bin_high", bin_center)
            }
        elif bin_center - current_zone["bins"][-1]["bin_center"] <= merge_threshold:
            current_zone["bins"].append(bin_row)
            if bin_row.get("bin_low"):
                current_zone["zone_low"] = min(current_zone["zone_low"], bin_row["bin_low"])
            if bin_row.get("bin_high"):
                current_zone["zone_high"] = max(current_zone["zone_high"], bin_row["bin_high"])
        else:
            zones.append(current_zone)
            current_zone = {
                "symbol": bin_row["symbol"],
                "bins": [bin_row],
                "zone_low": bin_row.get("bin_low", bin_center),
                "zone_high": bin_row.get("bin_high", bin_center)
            }
    
    if current_zone:
        zones.append(current_zone)
    
    return zones


# Aggregates zone data
def aggregate_zone(zone_data):
    bins = zone_data["bins"]
    
    total_volume = sum(b.get("volume", 0) for b in bins)
    max_score = max(b.get("volume_score", 0) for b in bins)
    candle_count = sum(b.get("candle_count", 0) for b in bins)
    
    poc_bin = max(bins, key=lambda b: b.get("volume", 0))
    poc_price = poc_bin["bin_center"]
    
    first_touch = min((b.get("first_touch") for b in bins if b.get("first_touch")), default=0)
    last_touch = max((b.get("last_touch") for b in bins if b.get("last_touch")), default=0)
    
    return {
        "total_volume": total_volume,
        "volume_score": max_score,
        "candle_count": candle_count,
        "poc_price": poc_price,
        "first_touch": first_touch,
        "last_touch": last_touch
    }


# Analyzes zone touches
def analyze_zone_touches(klines_list, zone_low, zone_high):
    """
    Analyze how price interacted with a zone.
    klines_list: pre-collected list of dicts with timestamp, high, low, close (sorted by timestamp)
    """
    default = {
        "touch_count": 0, "touch_from_above": 0, "touch_from_below": 0,
        "last_touch_direction": None, "bounce_count": 0, "break_count": 0,
        "bounce_up_count": 0, "bounce_down_count": 0, "avg_bounce_pct": 0.0,
        "max_bounce_pct": 0.0, "avg_hold_candles": 0.0, "last_reaction": None,
        "zone_type": "untested", "zone_role": None, "is_mirror": False, "flip_timestamp": None
    }

    if len(klines_list) < 2:
        return default
    
    touch_count = touch_from_above = touch_from_below = 0
    bounce_count = break_count = bounce_up_count = bounce_down_count = 0
    bounce_sizes = []
    hold_candles = []
    last_reaction = last_touch_direction = None
    was_in_zone = False
    entry_direction = entry_price = None
    candles_in_zone = 0
    reaction_sequence = []
    
    for i, candle in enumerate(klines_list):
        low, high, close = candle["low"], candle["high"], candle["close"]
        timestamp = candle["timestamp"]
        touches_zone = (low <= zone_high) and (high >= zone_low)
        
        if touches_zone:
            if not was_in_zone:
                touch_count += 1
                candles_in_zone = 1
                entry_price = close
                
                if i > 0:
                    prev_close = klines_list[i-1]["close"]
                    if prev_close > zone_high:
                        touch_from_above += 1
                        entry_direction = last_touch_direction = "above"
                    elif prev_close < zone_low:
                        touch_from_below += 1
                        entry_direction = last_touch_direction = "below"
                    else:
                        entry_direction = last_touch_direction = "inside"
                was_in_zone = True
            else:
                candles_in_zone += 1
        
        elif was_in_zone:
            hold_candles.append(candles_in_zone)
            
            if close > zone_high:
                if entry_direction == "above":
                    break_count += 1
                    last_reaction = "break_down_failed"
                else:
                    bounce_count += 1
                    bounce_up_count += 1
                    last_reaction = "bounce_up"
                    if entry_price and entry_price > 0:
                        bounce_sizes.append(abs((close - entry_price) / entry_price) * 100)
                reaction_sequence.append((timestamp, last_reaction))
            
            elif close < zone_low:
                if entry_direction == "below":
                    break_count += 1
                    last_reaction = "break_up_failed"
                else:
                    bounce_count += 1
                    bounce_down_count += 1
                    last_reaction = "bounce_down"
                    if entry_price and entry_price > 0:
                        bounce_sizes.append(abs((close - entry_price) / entry_price) * 100)
                reaction_sequence.append((timestamp, last_reaction))
            
            was_in_zone = False
            entry_direction = entry_price = None
            candles_in_zone = 0
    
    # Determine zone type
    zone_type = "untested"
    zone_role = None
    is_mirror = False
    flip_timestamp = None
    
    if touch_count == 0:
        pass
    elif bounce_up_count > 0 and bounce_down_count > 0:
        zone_type = "mirror"
        is_mirror = True
        zone_role = "support" if last_reaction == "bounce_up" else "resistance" if last_reaction == "bounce_down" else "contested"
        for i, (ts, reaction) in enumerate(reaction_sequence):
            if i > 0:
                prev = reaction_sequence[i-1][1]
                if (prev == "bounce_up" and reaction == "bounce_down") or (prev == "bounce_down" and reaction == "bounce_up"):
                    flip_timestamp = ts
    elif break_count > 0 and bounce_count == 0:
        zone_type = "broken_through"
        zone_role = "weak"
    elif touch_from_above > 0 and touch_from_below == 0:
        zone_type = "tested_one_side"
        zone_role = "resistance" if bounce_down_count > 0 else "broken_support"
    elif touch_from_below > 0 and touch_from_above == 0:
        zone_type = "tested_one_side"
        zone_role = "support" if bounce_up_count > 0 else "broken_resistance"
    else:
        zone_type = zone_role = "contested"
    
    return {
        "touch_count": touch_count,
        "touch_from_above": touch_from_above,
        "touch_from_below": touch_from_below,
        "last_touch_direction": last_touch_direction,
        "bounce_count": bounce_count,
        "break_count": break_count,
        "bounce_up_count": bounce_up_count,
        "bounce_down_count": bounce_down_count,
        "avg_bounce_pct": round(sum(bounce_sizes) / len(bounce_sizes), 2) if bounce_sizes else 0.0,
        "max_bounce_pct": round(max(bounce_sizes), 2) if bounce_sizes else 0.0,
        "avg_hold_candles": round(sum(hold_candles) / len(hold_candles), 1) if hold_candles else 0.0,
        "last_reaction": last_reaction,
        "zone_type": zone_type,
        "zone_role": zone_role,
        "is_mirror": is_mirror,
        "flip_timestamp": flip_timestamp
    }


print("✓ Cell 2: Core functions loaded")

# COMMAND ----------

# -----------------------------------------------------------------------------
# CELL 3: ZONE CALCULATOR
# -----------------------------------------------------------------------------

def calculate_zones_for_tf(spark, symbols, timeframe):
    """
    Calculates liquidity zones for a single timeframe.
    Can be run independently for parallel processing.
    """
    config = TIMEFRAME_CONFIG[timeframe]
    
    print(f"\n{'='*60}")
    print(f"LIQUIDITY ZONES: {timeframe}")
    print(f"{'='*60}")
    print(f"Symbols: {symbols}")
    print(f"Lookback: {config['lookback_hours']}h, Bin: {config['bin_pct']}%")
    
    # Check if recalc needed
    if not FORCE_RECALC and not should_recalculate(spark, timeframe):
        existing = load_existing_zones(spark, timeframe)
        print(f"⏭ Skipped (not due), loaded {len(existing)} existing zones")
        return existing, []
    
    all_zones = []
    all_significant = []
    
    for symbol in symbols:
        print(f"\n[{symbol}]", end=" ")
        
        # Get current price
        current_price, current_ts = get_current_price(spark, symbol)
        if not current_price:
            print("no price")
            continue
        
        # Load data
        klines = load_klines(spark, symbol, config["klines_interval"], config["lookback_hours"])
        interval_minutes = {"15m": 15, "1h": 60, "4h": 240, "1d": 1440}[config["klines_interval"]]
        is_valid, candles_total, candles_expected, candles_missing, duplicates, _ = check_continuity(spark, klines, interval_minutes)
        
        print(f"VP({candles_total}/{candles_expected})", end=" ")
        
        if STRICT_MODE and not is_valid:
            print(f"SKIP (missing:{candles_missing})")
            continue
        
        if candles_total == 0:
            print("no data")
            continue
        
        # Load BOS
        bos_signals = load_bos_signals(spark, symbol, timeframe, config["lookback_hours"])
        print(f"BOS({len(bos_signals)})", end=" ")
        
        # Calculate bins
        volume_bins = calculate_volume_bins(klines, current_price, config["bin_pct"])
        oi_bins = calculate_oi_bins(spark, symbol, config["klines_interval"], config["lookback_hours"], klines, current_price, config["bin_pct"])
        ob_bins = calculate_orderbook_bins(spark, symbol, config["klines_interval"], config["lookback_hours"], current_price, config["bin_pct"])
        
        # Merge to zones
        volume_zones = merge_bins_to_zones(volume_bins, current_price, ZONE_MERGE_PCT)

        if not volume_zones:
            print("no zones")
            continue

        # Pre-collect bins once for faster zone lookups (avoid repeated filter+collect)
        oi_bins_list = [r.asDict() for r in oi_bins.collect()] if oi_bins else []
        ob_bins_list = [r.asDict() for r in ob_bins.collect()] if ob_bins else []

        # Pre-collect klines once for touch analysis (avoid repeated collect per zone)
        klines_list = [row.asDict() for row in klines
            .select("timestamp", "high", "low", "close")
            .orderBy("timestamp")
            .collect()]

        zone_records = []
        bos_confirmed_count = 0

        for vz in volume_zones:
            vol_agg = aggregate_zone(vz)
            zone_low, zone_high = vz["zone_low"], vz["zone_high"]

            # BOS confirmation
            matching_bos = find_bos_in_zone(zone_low, zone_high, bos_signals, BOS_ZONE_TOLERANCE_PCT)
            bos_confirmed = matching_bos is not None

            if bos_confirmed:
                bos_confirmed_count += 1
                bos_price = matching_bos.get("close") or matching_bos.get("poc_price") or vol_agg["poc_price"]
                bos_type = matching_bos.get("bos_type") or matching_bos.get("signal_type")
                bos_timestamp = matching_bos.get("timestamp")
            else:
                bos_price = bos_type = bos_timestamp = None

            final_poc = bos_price if bos_confirmed else vol_agg["poc_price"]

            # OI score (use pre-collected list)
            oi_score = oi_delta_net = 0
            oi_in_zone = [r for r in oi_bins_list if zone_low <= r["bin_center"] <= zone_high]
            if oi_in_zone:
                oi_score = max(r["oi_score"] for r in oi_in_zone)
                oi_delta_net = sum(r["oi_delta_net"] for r in oi_in_zone)

            # OB score (use pre-collected list)
            ob_score = 0
            ob_in_zone = [r for r in ob_bins_list if zone_low <= r["bin_center"] <= zone_high]
            if ob_in_zone:
                ob_score = max(r["orderbook_score"] for r in ob_in_zone)
            
            # Confirmed methods
            confirmed = sum([
                vol_agg["volume_score"] >= SCORE_THRESHOLD,
                oi_score >= SCORE_THRESHOLD,
                ob_score >= SCORE_THRESHOLD
            ])
            
            # Touch analysis (using pre-collected klines_list)
            touch_data = analyze_zone_touches(klines_list, zone_low, zone_high)
            
            # Distance
            zone_mid = (zone_low + zone_high) / 2
            distance_pct = ((zone_mid - current_price) / current_price) * 100
            hours_since_touch = (current_ts - vol_agg["last_touch"]) / (1000 * 60 * 60)
            
            # Strength
            strength_score = calculate_strength_score(
                vol_agg["volume_score"], oi_score, ob_score, confirmed,
                touch_data["bounce_count"], touch_data["break_count"], bos_confirmed
            )
            
            zone_records.append({
                "zone_id": generate_zone_id(symbol, zone_low, zone_high, timeframe),
                "symbol": symbol,
                "timeframe": timeframe,
                "zone_low": zone_low,
                "zone_high": zone_high,
                "poc_price": final_poc,
                "volume_poc": vol_agg["poc_price"],
                "total_volume": vol_agg["total_volume"],
                "volume_score": vol_agg["volume_score"],
                "oi_score": oi_score,
                "orderbook_score": ob_score,
                "oi_delta_net": oi_delta_net,
                "confirmed_methods": confirmed,
                "strength_score": strength_score,
                "candle_count": vol_agg["candle_count"],
                **touch_data,
                "bos_confirmed": bos_confirmed,
                "bos_price": bos_price,
                "bos_type": bos_type,
                "bos_timestamp": bos_timestamp,
                "first_touch_ts": vol_agg["first_touch"],
                "last_touch_ts": vol_agg["last_touch"],
                "hours_since_touch": round(hours_since_touch, 1),
                "distance_from_price_pct": round(distance_pct, 2),
                "current_price": current_price,
                "candles_total": candles_total,
                "candles_expected": candles_expected,
                "candles_missing": candles_missing,
                "continuity_ok": candles_missing == 0 and duplicates == 0,
                "calculated_at": datetime.now(timezone.utc)
            })
        
        # Rank zones
        zone_records.sort(key=lambda x: x["strength_score"], reverse=True)
        for i, z in enumerate(zone_records):
            z["zone_rank"] = i + 1
        
        significant = [z for z in zone_records if z["confirmed_methods"] >= MIN_CONFIRMED_METHODS and z["bos_confirmed"]]
        
        print(f"-> {len(zone_records)} zones, {bos_confirmed_count} BOS, {len(significant)} sig")
        
        all_zones.extend(zone_records)
        all_significant.extend(significant)
    
    # Write to table
    if all_zones:
        write_zones_to_table(spark, all_zones, timeframe)
    
    print(f"\n✓ {timeframe}: {len(all_zones)} total, {len(all_significant)} significant")
    
    return all_zones, all_significant


def write_zones_to_table(spark, zones, timeframe):
    """Writes zones to Delta table."""
    schema = StructType([
        StructField("zone_id", StringType()), StructField("symbol", StringType()),
        StructField("timeframe", StringType()), StructField("zone_low", DoubleType()),
        StructField("zone_high", DoubleType()), StructField("poc_price", DoubleType()),
        StructField("volume_poc", DoubleType()), StructField("total_volume", DoubleType()),
        StructField("volume_score", IntegerType()), StructField("oi_score", IntegerType()),
        StructField("orderbook_score", IntegerType()), StructField("oi_delta_net", DoubleType()),
        StructField("confirmed_methods", IntegerType()), StructField("strength_score", DoubleType()),
        StructField("zone_rank", IntegerType()), StructField("candle_count", LongType()),
        StructField("touch_count", IntegerType()), StructField("touch_from_above", IntegerType()),
        StructField("touch_from_below", IntegerType()), StructField("last_touch_direction", StringType()),
        StructField("bounce_count", IntegerType()), StructField("break_count", IntegerType()),
        StructField("bounce_up_count", IntegerType()), StructField("bounce_down_count", IntegerType()),
        StructField("avg_bounce_pct", DoubleType()), StructField("max_bounce_pct", DoubleType()),
        StructField("avg_hold_candles", DoubleType()), StructField("last_reaction", StringType()),
        StructField("zone_type", StringType()), StructField("zone_role", StringType()),
        StructField("is_mirror", BooleanType()), StructField("flip_timestamp", LongType()),
        StructField("bos_confirmed", BooleanType()), StructField("bos_price", DoubleType()),
        StructField("bos_type", StringType()), StructField("bos_timestamp", LongType()),
        StructField("first_touch_ts", LongType()), StructField("last_touch_ts", LongType()),
        StructField("hours_since_touch", DoubleType()), StructField("distance_from_price_pct", DoubleType()),
        StructField("current_price", DoubleType()), StructField("candles_total", IntegerType()),
        StructField("candles_expected", IntegerType()), StructField("candles_missing", IntegerType()),
        StructField("continuity_ok", BooleanType()), StructField("calculated_at", TimestampType())
    ])
    
    df = spark.createDataFrame(zones, schema)
    output_table = TIMEFRAME_CONFIG[timeframe]["output_table"]
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)


print("✓ Cell 3: Zone calculator loaded")

# COMMAND ----------

# CELL 4: RUN ALL TIMEFRAMES (scheduled)
SYMBOLS = get_all_symbols(spark)

for tf in ["15m", "1h", "4h", "1d"]:
    if should_run(tf):
        zones, sig = calculate_zones_for_tf(spark, SYMBOLS, tf)
        print(f"✓ {tf}: {len(zones)} zones")
    else:
        print(f"⏭ {tf}: skipped (not scheduled)")

# COMMAND ----------

# -----------------------------------------------------------------------------
# CELL 8: CONFLUENCE + LIQ_DISTANCE (DISTRIBUTED - scales to 500+ symbols)
# -----------------------------------------------------------------------------


def add_confluence_and_distance_distributed(spark):
    """
    Fully distributed confluence calculation using Spark self-join.
    Replaces O(n²) Python loop with distributed range overlap join.
    """

    print("=" * 60)
    print("CONFLUENCE + DISTANCE MAP (DISTRIBUTED)")
    print("=" * 60)

    # Step 1: Load all zones into single DataFrame
    all_zones_dfs = []

    for tf in ["15m", "1h", "4h", "1d"]:
        table = TIMEFRAME_CONFIG[tf]["output_table"]
        if spark.catalog.tableExists(table):
            df = spark.table(table).withColumn("source_tf", F.lit(tf))
            all_zones_dfs.append(df)
            count = df.count()
            print(f"[{tf}] Loaded {count} zones")

    if not all_zones_dfs:
        print("No zones found")
        return

    # Union all TFs
    from functools import reduce
    all_zones = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), all_zones_dfs)

    print(f"\nTotal zones: {all_zones.count()}")

    # Step 2: Confluence via self-join with range overlap
    # Zone A overlaps Zone B if: A.zone_low <= B.zone_high AND A.zone_high >= B.zone_low
    print("\n[CONFLUENCE] Computing cross-TF overlaps via Spark join...")

    # Create aliases for self-join
    zones_a = all_zones.alias("a")
    zones_b = all_zones.select(
        F.col("symbol").alias("b_symbol"),
        F.col("source_tf").alias("b_tf"),
        F.col("zone_low").alias("b_zone_low"),
        F.col("zone_high").alias("b_zone_high")
    ).alias("b")

    # Join on symbol + range overlap + different TF
    overlaps = zones_a.join(
        zones_b,
        (F.col("a.symbol") == F.col("b.b_symbol")) &
        (F.col("a.zone_low") <= F.col("b.b_zone_high")) &
        (F.col("a.zone_high") >= F.col("b.b_zone_low")) &
        (F.col("a.source_tf") != F.col("b.b_tf")),
        "left"
    ).select(
        "a.*",
        F.col("b.b_tf").alias("overlapping_tf")
    )

    # Aggregate overlapping TFs per zone
    confluence_agg = overlaps.groupBy(
        "zone_id", "symbol", "source_tf", "zone_low", "zone_high",
        "poc_price", "volume_poc", "strength_score", "bos_confirmed",
        "bos_type", "bos_price", "bos_timestamp", "is_mirror",
        "zone_role", "zone_type", "volume_score", "oi_score",
        "bounce_count", "break_count", "touch_count"
    ).agg(
        F.collect_set("overlapping_tf").alias("overlapping_tfs")
    )

    # Calculate confluence score and flags
    zones_with_confluence = confluence_agg.withColumn(
        "all_tfs",
        F.array_union(F.array(F.col("source_tf")),
                      F.coalesce(F.col("overlapping_tfs"), F.array()))
    ).withColumn(
        "confirmed_15m",
        F.array_contains(F.col("all_tfs"), "15m")
    ).withColumn(
        "confirmed_1h",
        F.array_contains(F.col("all_tfs"), "1h")
    ).withColumn(
        "confirmed_4h",
        F.array_contains(F.col("all_tfs"), "4h")
    ).withColumn(
        "confirmed_1d",
        F.array_contains(F.col("all_tfs"), "1d")
    ).withColumn(
        "confluence_score",
        (F.when(F.col("confirmed_15m"), 1).otherwise(0) +
         F.when(F.col("confirmed_1h"), 2).otherwise(0) +
         F.when(F.col("confirmed_4h"), 3).otherwise(0) +
         F.when(F.col("confirmed_1d"), 4).otherwise(0))
    ).withColumn(
        "confluence_tfs",
        F.array_join(F.array_sort(F.col("all_tfs")), ",")
    )

    confluent_count = zones_with_confluence.filter(
        F.col("confluence_score") > F.when(F.col("source_tf") == "15m", 1)
                                       .when(F.col("source_tf") == "1h", 2)
                                       .when(F.col("source_tf") == "4h", 3)
                                       .otherwise(4)
    ).count()
    print(f"  Found {confluent_count} zones with multi-TF confluence")

    # Step 3: Get current prices (batch - single query)
    print("\n[PRICES] Fetching current prices...")

    current_prices_df = None
    for tf in ["15m", "1h", "4h"]:
        try:
            klines = spark.table(f"{KLINES_TABLE}_{tf}")

            # Get max timestamp per symbol
            max_ts = klines.groupBy("symbol").agg(F.max("timestamp").alias("max_ts"))

            # Join to get latest close
            latest = klines.join(
                max_ts,
                (klines.symbol == max_ts.symbol) & (klines.timestamp == max_ts.max_ts)
            ).select(
                klines.symbol,
                F.col("close").alias("current_price")
            )

            current_prices_df = latest
            break
        except:
            continue

    if current_prices_df is None:
        print("  No price data")
        return

    price_count = current_prices_df.count()
    print(f"  Got prices for {price_count} symbols")

    # Step 4: Build liq_distance (distributed)
    print("\n[DISTANCE MAP] Building...")

    # Join zones with current prices
    liq_distance = zones_with_confluence.filter(
        F.col("bos_confirmed") == True
    ).join(
        current_prices_df,
        "symbol",
        "inner"
    ).withColumn(
        "distance_pct",
        F.round(((F.col("poc_price") - F.col("current_price")) / F.col("current_price")) * 100, 4)
    ).withColumn(
        "direction",
        F.when(F.col("poc_price") > F.col("current_price"), "above").otherwise("below")
    ).withColumn(
        "calculated_at",
        F.current_timestamp()
    )

    # Deduplicate close zones using window functions (within 0.5%)
    # Rank by strength, keep strongest when POC prices are within 0.5%
    window_dedup = Window.partitionBy("symbol").orderBy(F.col("strength_score").desc())

    liq_distance_ranked = liq_distance.withColumn(
        "dedup_rank", F.row_number().over(window_dedup)
    )

    # For deduplication: compare each zone to all stronger zones
    # This is still O(n²) per symbol but distributed across executors
    # Alternative: bucket POC prices and deduplicate within buckets
    liq_distance_deduped = liq_distance_ranked.withColumn(
        "poc_bucket", F.round(F.col("poc_price") / (F.col("current_price") * 0.005), 0)
    )

    # Keep strongest zone per bucket
    window_bucket = Window.partitionBy("symbol", "poc_bucket").orderBy(F.col("strength_score").desc())
    liq_distance_deduped = liq_distance_deduped.withColumn(
        "bucket_rank", F.row_number().over(window_bucket)
    ).filter(F.col("bucket_rank") == 1).drop("bucket_rank", "poc_bucket", "dedup_rank")

    before_dedup = liq_distance.count()
    after_dedup = liq_distance_deduped.count()
    print(f"  Deduplicated: {before_dedup} → {after_dedup} zones")

    # Add rank by distance
    window_rank = Window.partitionBy("symbol").orderBy(F.abs(F.col("distance_pct")))
    liq_distance_final = liq_distance_deduped.withColumn(
        "rank", F.row_number().over(window_rank)
    )

    # Select final columns
    final_columns = [
        "symbol", "current_price", "rank", "poc_price",
        F.col("source_tf").alias("timeframe"),
        "distance_pct", "direction", "zone_low", "zone_high",
        "is_mirror", "strength_score", "zone_role", "zone_type",
        "bos_type", "bos_confirmed", "bos_price", "bos_timestamp",
        "confluence_score", "confluence_tfs",
        "bounce_count", "break_count", "touch_count",
        "volume_score", "oi_score", "calculated_at"
    ]

    liq_distance_output = liq_distance_final.select(*final_columns)

    # Write to table
    liq_distance_output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(LIQ_DISTANCE_TABLE)

    final_count = spark.table(LIQ_DISTANCE_TABLE).count()
    print(f"\n✓ Saved {final_count} records to {LIQ_DISTANCE_TABLE}")

    # Show sample
    print("\n" + "=" * 60)
    print("NEAREST LEVELS (rank <= 4)")
    print("=" * 60)

    spark.sql(f"""
        SELECT symbol, rank, ROUND(poc_price, 2) as poc, timeframe,
               ROUND(distance_pct, 2) as dist, direction,
               is_mirror, ROUND(strength_score, 1) as str, zone_role, bos_type
        FROM {LIQ_DISTANCE_TABLE}
        WHERE rank <= 4
        ORDER BY symbol, rank
    """).show(50, truncate=False)


# Run distributed version
add_confluence_and_distance_distributed(spark)