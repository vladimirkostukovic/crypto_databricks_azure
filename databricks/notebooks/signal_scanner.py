# Databricks notebook source
# ---
# SIGNAL SCANNER
# Finds trading setups based on:
#   1. SMC Strategy (BOS + Liquidity Zones)
#   2. Pure Liquidity Strategy (Zone confluence + Sentiment)
# Outputs: gold.signals_all (status='pending')
# ---

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timezone, timedelta
import json
import hashlib

# ---
# CONFIGURATION
# ---

CATALOG = "crypto"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

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
ENABLE_SMC = get_config("enable_smc_strategy", True)
ENABLE_LIQ = get_config("enable_liq_strategy", True)
TRADING_SYMBOLS = get_config("trading_symbols", ["BTCUSDT", "ETHUSDT"])
SCAN_TIMEFRAMES = get_config("scan_timeframes", ["15m", "1h", "4h"])
MIN_ZONE_STRENGTH = get_config("min_zone_strength", 70)
MIN_CONFLUENCE_SCORE = get_config("min_confluence_score", 4)
MIN_CONFIRMED_METHODS = get_config("min_confirmed_methods", 2)
REQUIRE_BOS = get_config("require_bos_confirmation", True)

# Signal proximity settings (tightened for better entries)
MAX_DISTANCE_PCT = {
    "15m": 0.5,   # 0.5% for 15m (~$500 on BTC)
    "1h": 1.0,    # 1% for 1h
    "4h": 1.5,    # 1.5% for 4h
    "1d": 2.0     # 2% for 1d
}
RECENT_BOS_HOURS = 12   # BOS within last N hours considered recent
MIN_ZONE_STRENGTH_OVERRIDE = 80  # Override config - require stronger zones

print("=" * 60)
print("SIGNAL SCANNER")
print("=" * 60)
print(f"Symbols: {TRADING_SYMBOLS}")
print(f"Timeframes: {SCAN_TIMEFRAMES}")
print(f"Strategies: SMC={ENABLE_SMC}, Liquidity={ENABLE_LIQ}")
print(f"Filters: strength>={MIN_ZONE_STRENGTH}, confluence>={MIN_CONFLUENCE_SCORE}")
print(f"Max distance: {MAX_DISTANCE_PCT}")
print("=" * 60)

# ---
# HELPER FUNCTIONS
# ---

def generate_signal_id(symbol, timestamp, zone_id, strategy):
    key = f"{symbol}_{timestamp}_{zone_id}_{strategy}"
    return hashlib.md5(key.encode()).hexdigest()[:16]


def get_current_prices(symbols):
    """Get latest price for each symbol"""
    prices = {}
    for tf in ["15m", "1h", "4h"]:
        try:
            df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.unified_klines_{tf}")
            latest = df.filter(F.col("symbol").isin(symbols)) \
                .groupBy("symbol") \
                .agg(F.max("timestamp").alias("max_ts"))

            latest_prices = df.join(
                latest,
                (df.symbol == latest.symbol) & (df.timestamp == latest.max_ts)
            ).select(df.symbol, "close", "high", "low").collect()

            for row in latest_prices:
                if row["symbol"] not in prices:
                    prices[row["symbol"]] = {
                        "price": float(row["close"]),
                        "high": float(row["high"]),
                        "low": float(row["low"])
                    }
            if len(prices) == len(symbols):
                break
        except:
            continue
    return prices


def calculate_atr_batch(symbols, timeframe, periods=14):
    """
    Calculate ATR for ALL symbols at once.
    Returns: {symbol: atr_value}
    Optimized for 500+ symbols - distributed computation.
    """
    try:
        df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.unified_klines_{timeframe}") \
            .filter(F.col("symbol").isin(symbols))

        # Get last N+1 candles per symbol
        window_rank = Window.partitionBy("symbol").orderBy(F.col("timestamp").desc())
        df = df.withColumn("rn", F.row_number().over(window_rank)) \
            .filter(F.col("rn") <= periods + 1)

        # Calculate TR
        window_lag = Window.partitionBy("symbol").orderBy("timestamp")
        df = df.withColumn("prev_close", F.lag("close").over(window_lag))
        df = df.withColumn("tr", F.greatest(
            F.col("high") - F.col("low"),
            F.abs(F.col("high") - F.col("prev_close")),
            F.abs(F.col("low") - F.col("prev_close"))
        ))

        # Aggregate ATR per symbol
        atr_df = df.filter(F.col("tr").isNotNull()) \
            .groupBy("symbol") \
            .agg(F.avg("tr").alias("atr")) \
            .collect()

        return {row["symbol"]: float(row["atr"]) for row in atr_df}
    except:
        return {}


def calculate_atr(symbol, timeframe, periods=14):
    """Legacy wrapper - prefer calculate_atr_batch for bulk operations"""
    result = calculate_atr_batch([symbol], timeframe, periods)
    return result.get(symbol)


# ---
# LOAD ZONE & SIGNAL DATA
# ---

def load_liquidity_zones(timeframe, symbols):
    """Load liquidity zones for given timeframe"""
    table = f"{CATALOG}.{SILVER_SCHEMA}.liq_{timeframe}"
    try:
        if not spark.catalog.tableExists(table):
            return None

        df = spark.table(table).filter(
            (F.col("symbol").isin(symbols)) &
            (F.col("strength_score") >= MIN_ZONE_STRENGTH)
        )
        return df
    except Exception as e:
        print(f"  ⚠ Error loading zones: {e}")
        return None


def load_liq_distance(symbols):
    """Load pre-calculated zone distances"""
    table = f"{CATALOG}.{SILVER_SCHEMA}.liq_distance"
    try:
        if not spark.catalog.tableExists(table):
            return None
        return spark.table(table).filter(F.col("symbol").isin(symbols))
    except:
        return None


def load_recent_bos(symbols, hours=24):
    """Load recent BOS signals"""
    table = f"{CATALOG}.{SILVER_SCHEMA}.bos_warnings"
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp() * 1000)

    try:
        if not spark.catalog.tableExists(table):
            return None

        return spark.table(table).filter(
            (F.col("symbol").isin(symbols)) &
            (F.col("timestamp") >= cutoff_ms)
        )
    except:
        return None


def load_sentiment_batch(symbols, timeframes):
    """
    Load latest sentiment for ALL symbols and timeframes at once.
    Returns: {symbol: {timeframe: sentiment_dict}}
    Optimized for 500+ symbols - single query instead of per-symbol calls.
    """
    table = f"{CATALOG}.{GOLD_SCHEMA}.sentiment_scores"
    try:
        if not spark.catalog.tableExists(table):
            return {}

        # Get latest sentiment per symbol+timeframe in ONE query
        df = spark.table(table).filter(
            (F.col("symbol").isin(symbols)) &
            (F.col("timeframe").isin(timeframes))
        )

        window = Window.partitionBy("symbol", "timeframe").orderBy(F.col("timestamp").desc())
        latest = df.withColumn("rn", F.row_number().over(window)) \
            .filter(F.col("rn") == 1) \
            .collect()

        # Build nested dict: {symbol: {timeframe: data}}
        result = {}
        for row in latest:
            symbol = row["symbol"]
            tf = row["timeframe"]
            if symbol not in result:
                result[symbol] = {}
            result[symbol][tf] = row.asDict()

        return result
    except:
        return {}


def load_sentiment(symbols, timeframe):
    """Legacy wrapper - prefer load_sentiment_batch for bulk operations"""
    all_sent = load_sentiment_batch(symbols, [timeframe])
    return {s: all_sent.get(s, {}).get(timeframe, {}) for s in symbols}


# ---
# SIGNAL DETECTION
# ---

def find_smc_signals(symbols, timeframes, current_prices):
    """
    SMC Strategy: Find setups where:
    1. Price is near a strong liquidity zone
    2. Recent BOS confirms direction
    3. Sentiment supports the trade

    Optimized for 500+ symbols - batch loads all data upfront.
    """
    signals = []

    print("\n[SMC STRATEGY]")

    # Batch load sentiment for ALL symbols and timeframes
    print("  Loading sentiment (batch)...")
    all_sentiment = load_sentiment_batch(symbols, timeframes)

    # Batch load ATR for ALL symbols and timeframes
    print("  Calculating ATR (batch)...")
    all_atr = {}
    for tf in timeframes:
        all_atr[tf] = calculate_atr_batch(symbols, tf)

    for tf in timeframes:
        print(f"\n  Scanning {tf}...")

        # Load data
        zones_df = load_liquidity_zones(tf, symbols)
        bos_df = load_recent_bos(symbols, RECENT_BOS_HOURS)

        if zones_df is None:
            print(f"    No zones data")
            continue

        # Filter zones with BOS confirmation
        if REQUIRE_BOS and bos_df is not None:
            zones_with_bos = zones_df.filter(F.col("bos_confirmed") == True)
        else:
            zones_with_bos = zones_df

        zones = zones_with_bos.collect()
        print(f"    Found {len(zones)} BOS-confirmed zones")

        for zone in zones:
            symbol = zone["symbol"]
            if symbol not in current_prices:
                continue

            cp = current_prices[symbol]["price"]
            poc = zone["poc_price"]
            zone_low = zone["zone_low"]
            zone_high = zone["zone_high"]

            # Check distance (timeframe-specific)
            distance_pct = abs((poc - cp) / cp) * 100
            max_dist = MAX_DISTANCE_PCT.get(tf, 1.0)
            if distance_pct > max_dist:
                continue

            # Determine signal type based on zone position and BOS
            bos_type = zone.get("bos_type")
            zone_role = zone.get("zone_role")

            # SMC Logic:
            # - BOS bullish + price above zone = potential long (retest of broken resistance)
            # - BOS bearish + price below zone = potential short (retest of broken support)
            signal_type = None

            if bos_type == "bullish" and cp > poc:
                # Price broke above, now retesting zone as support
                if zone_role in ["support", "broken_resistance", "mirror"]:
                    signal_type = "long"
            elif bos_type == "bearish" and cp < poc:
                # Price broke below, now retesting zone as resistance
                if zone_role in ["resistance", "broken_support", "mirror"]:
                    signal_type = "short"

            if signal_type is None:
                continue

            # Get sentiment from pre-loaded batch
            sent = all_sentiment.get(symbol, {}).get(tf, {})
            sentiment_score = sent.get("sentiment_score", 0)
            crowding = sent.get("crowding_direction", "unknown")
            smart_money = sent.get("divergence_direction", "unknown")
            taker = sent.get("aggression_direction", "unknown")

            # Sentiment confirmation (optional boost)
            sentiment_confirms = False
            if signal_type == "long":
                sentiment_confirms = (crowding == "short_crowded") or (smart_money == "smart_money_long")
            elif signal_type == "short":
                sentiment_confirms = (crowding == "long_crowded") or (smart_money == "smart_money_short")

            # Get ATR from pre-loaded batch
            atr = all_atr.get(tf, {}).get(symbol)

            # Build signal
            signal = {
                "signal_id": generate_signal_id(symbol, int(datetime.now().timestamp()*1000), zone["zone_id"], "smc"),
                "created_at": datetime.now(timezone.utc),
                "symbol": symbol,
                "timeframe": tf,
                "strategy": "smc",
                "signal_type": signal_type,
                "current_price": cp,
                "zone_id": zone["zone_id"],
                "zone_low": zone_low,
                "zone_high": zone_high,
                "poc_price": poc,
                "zone_strength": zone.get("strength_score"),
                "bos_type": bos_type,
                "bos_timestamp": zone.get("bos_timestamp"),
                "bos_price": zone.get("bos_price"),
                "confluence_score": zone.get("confluence_score"),
                "confluence_tfs": zone.get("confluence_tfs"),
                "confirmed_methods": zone.get("confirmed_methods"),
                "sentiment_score": sentiment_score,
                "crowding_direction": crowding,
                "smart_money_divergence": sent.get("smart_money_divergence"),
                "taker_bias": taker,
                "sentiment_confirms": sentiment_confirms,
                "atr": atr,
                "ai_status": "pending",
                "date": datetime.now(timezone.utc).date()
            }

            signals.append(signal)
            print(f"    ✓ {symbol} {tf} {signal_type.upper()} @ {poc:.2f} (strength={zone.get('strength_score')}, sent={sentiment_score:.1f})")

    return signals


def find_liquidity_signals(symbols, timeframes, current_prices):
    """
    Pure Liquidity Strategy: Find setups where:
    1. Price approaches strong liquidity zone
    2. High multi-TF confluence
    3. Strong sentiment confirmation

    Optimized for 500+ symbols - batch loads all data upfront.
    """
    signals = []

    print("\n[LIQUIDITY STRATEGY]")

    # Use liq_distance for efficient lookup
    liq_distance = load_liq_distance(symbols)

    if liq_distance is None:
        print("  No liq_distance data")
        return signals

    # Filter by strength and confluence (distance checked per-TF in loop)
    candidate_zones = liq_distance.filter(
        (F.col("strength_score") >= MIN_ZONE_STRENGTH_OVERRIDE) &
        (F.col("confluence_score") >= MIN_CONFLUENCE_SCORE)
    ).collect()

    print(f"  Candidate zones: {len(candidate_zones)}")

    nearby_zones = []
    for zone in candidate_zones:
        symbol = zone["symbol"]
        tf = zone["timeframe"]

        if tf not in timeframes:
            continue
        if symbol not in current_prices:
            continue

        # Check distance per timeframe
        max_dist = MAX_DISTANCE_PCT.get(tf, 1.0)
        if abs(zone["distance_pct"]) > max_dist:
            continue

        nearby_zones.append(zone)

    print(f"  After distance filter: {len(nearby_zones)} zones")

    if not nearby_zones:
        return signals

    # Batch load sentiment for ALL symbols (only those in nearby_zones)
    nearby_symbols = list(set(z["symbol"] for z in nearby_zones))
    print(f"  Loading sentiment for {len(nearby_symbols)} symbols (batch)...")
    all_sentiment = load_sentiment_batch(nearby_symbols, timeframes)

    # Batch load ATR
    print(f"  Calculating ATR (batch)...")
    all_atr = {}
    for tf in timeframes:
        all_atr[tf] = calculate_atr_batch(nearby_symbols, tf)

    for zone in nearby_zones:
        symbol = zone["symbol"]
        tf = zone["timeframe"]

        cp = current_prices[symbol]["price"]
        poc = zone["poc_price"]
        direction = zone["direction"]  # "above" or "below"
        zone_role = zone.get("zone_role")

        # Get sentiment from pre-loaded batch
        sent = all_sentiment.get(symbol, {}).get(tf, {})
        sentiment_score = sent.get("sentiment_score", 0)
        crowding = sent.get("crowding_direction", "unknown")
        smart_money = sent.get("divergence_direction", "unknown")
        taker = sent.get("aggression_direction", "unknown")

        # Determine signal type based on zone position
        signal_type = None
        sentiment_confirms = False

        if direction == "below" and zone_role in ["support", "mirror"]:
            # Zone is below price = potential support = look for long
            signal_type = "long"
            sentiment_confirms = (crowding == "short_crowded") or (smart_money == "smart_money_long")
        elif direction == "above" and zone_role in ["resistance", "mirror"]:
            # Zone is above price = potential resistance = look for short
            signal_type = "short"
            sentiment_confirms = (crowding == "long_crowded") or (smart_money == "smart_money_short")

        if signal_type is None:
            continue

        # For liquidity strategy, prefer sentiment confirmation
        if not sentiment_confirms and abs(sentiment_score) < 20:
            continue  # Skip weak sentiment setups

        # Get ATR from pre-loaded batch
        atr = all_atr.get(tf, {}).get(symbol)

        # Build signal
        signal = {
            "signal_id": generate_signal_id(symbol, int(datetime.now().timestamp()*1000), zone.get("zone_id", "liq"), "liquidity"),
            "created_at": datetime.now(timezone.utc),
            "symbol": symbol,
            "timeframe": tf,
            "strategy": "liquidity",
            "signal_type": signal_type,
            "current_price": cp,
            "zone_id": None,  # liq_distance might not have zone_id
            "zone_low": zone.get("zone_low"),
            "zone_high": zone.get("zone_high"),
            "poc_price": poc,
            "zone_strength": zone.get("strength_score"),
            "bos_type": zone.get("bos_type"),
            "bos_timestamp": zone.get("bos_timestamp"),
            "bos_price": zone.get("bos_price"),
            "confluence_score": zone.get("confluence_score"),
            "confluence_tfs": zone.get("confluence_tfs"),
            "confirmed_methods": None,
            "sentiment_score": sentiment_score,
            "crowding_direction": crowding,
            "smart_money_divergence": sent.get("smart_money_divergence"),
            "taker_bias": taker,
            "sentiment_confirms": sentiment_confirms,
            "atr": atr,
            "ai_status": "pending",
            "date": datetime.now(timezone.utc).date()
        }

        signals.append(signal)
        print(f"    ✓ {symbol} {tf} {signal_type.upper()} @ {poc:.2f} (confluence={zone.get('confluence_score')}, sent={sentiment_score:.1f})")

    return signals


# ---
# MAIN EXECUTION
# ---

# Get current prices
print("\nFetching current prices...")
current_prices = get_current_prices(TRADING_SYMBOLS)
print(f"Got prices for {len(current_prices)} symbols")

all_signals = []

# SMC Strategy
if ENABLE_SMC:
    smc_signals = find_smc_signals(TRADING_SYMBOLS, SCAN_TIMEFRAMES, current_prices)
    all_signals.extend(smc_signals)
    print(f"\nSMC signals found: {len(smc_signals)}")

# Liquidity Strategy
if ENABLE_LIQ:
    liq_signals = find_liquidity_signals(TRADING_SYMBOLS, SCAN_TIMEFRAMES, current_prices)
    all_signals.extend(liq_signals)
    print(f"Liquidity signals found: {len(liq_signals)}")

# ---
# PREPARE DATA SNAPSHOT FOR AI
# ---

def prepare_data_snapshot(signal, current_prices):
    """Prepare context data for AI validation"""
    symbol = signal["symbol"]
    tf = signal["timeframe"]

    snapshot = {
        "signal": {
            "type": signal["signal_type"],
            "strategy": signal["strategy"],
            "symbol": symbol,
            "timeframe": tf,
            "current_price": signal["current_price"],
            "zone_poc": signal["poc_price"],
            "zone_low": signal["zone_low"],
            "zone_high": signal["zone_high"],
            "zone_strength": signal["zone_strength"],
            "bos_type": signal["bos_type"],
            "confluence_score": signal["confluence_score"],
            "atr": signal["atr"]
        },
        "sentiment": {
            "score": signal["sentiment_score"],
            "crowding": signal["crowding_direction"],
            "smart_money": signal["smart_money_divergence"],
            "taker": signal["taker_bias"],
            "confirms_signal": signal["sentiment_confirms"]
        }
    }

    # Add recent klines (last 20)
    try:
        klines = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.unified_klines_{tf}") \
            .filter(F.col("symbol") == symbol) \
            .orderBy(F.col("timestamp").desc()) \
            .limit(20) \
            .select("timestamp", "open", "high", "low", "close", "volume") \
            .collect()

        snapshot["klines"] = [
            {"ts": r["timestamp"], "o": r["open"], "h": r["high"], "l": r["low"], "c": r["close"], "v": r["volume"]}
            for r in reversed(klines)
        ]
    except:
        snapshot["klines"] = []

    return json.dumps(snapshot)


# Add data snapshots
for signal in all_signals:
    signal["data_snapshot"] = prepare_data_snapshot(signal, current_prices)

# ---
# WRITE TO SIGNALS_ALL
# ---

print("\n" + "=" * 60)
print("WRITING SIGNALS")
print("=" * 60)

if not all_signals:
    print("No signals found")
else:
    # Create DataFrame
    schema = StructType([
        StructField("signal_id", StringType()),
        StructField("created_at", TimestampType()),
        StructField("symbol", StringType()),
        StructField("timeframe", StringType()),
        StructField("strategy", StringType()),
        StructField("signal_type", StringType()),
        StructField("current_price", DoubleType()),
        StructField("zone_id", StringType()),
        StructField("zone_low", DoubleType()),
        StructField("zone_high", DoubleType()),
        StructField("poc_price", DoubleType()),
        StructField("zone_strength", DoubleType()),
        StructField("bos_type", StringType()),
        StructField("bos_timestamp", LongType()),
        StructField("bos_price", DoubleType()),
        StructField("confluence_score", IntegerType()),
        StructField("confluence_tfs", StringType()),
        StructField("confirmed_methods", IntegerType()),
        StructField("sentiment_score", DoubleType()),
        StructField("crowding_direction", StringType()),
        StructField("smart_money_divergence", DoubleType()),
        StructField("taker_bias", StringType()),
        StructField("ai_status", StringType()),
        StructField("ai_confidence", DoubleType()),
        StructField("ai_reasoning", StringType()),
        StructField("ai_validated_at", TimestampType()),
        StructField("entry_price", DoubleType()),
        StructField("stop_loss", DoubleType()),
        StructField("take_profit_1", DoubleType()),
        StructField("take_profit_2", DoubleType()),
        StructField("risk_reward_ratio", DoubleType()),
        StructField("position_size_pct", DoubleType()),
        StructField("execution_status", StringType()),
        StructField("executed_at", TimestampType()),
        StructField("data_snapshot", StringType()),
        StructField("date", DateType())
    ])

    # Fill missing fields
    for s in all_signals:
        s.setdefault("ai_confidence", None)
        s.setdefault("ai_reasoning", None)
        s.setdefault("ai_validated_at", None)
        s.setdefault("entry_price", None)
        s.setdefault("stop_loss", None)
        s.setdefault("take_profit_1", None)
        s.setdefault("take_profit_2", None)
        s.setdefault("risk_reward_ratio", None)
        s.setdefault("position_size_pct", None)
        s.setdefault("execution_status", None)
        s.setdefault("executed_at", None)
        # Remove non-schema fields
        s.pop("sentiment_confirms", None)
        s.pop("atr", None)

    signals_df = spark.createDataFrame(all_signals, schema)

    # Write to gold.signals_all
    target_table = f"{CATALOG}.{GOLD_SCHEMA}.signals_all"

    if spark.catalog.tableExists(target_table):
        delta_table = DeltaTable.forName(spark, target_table)
        delta_table.alias("t").merge(
            signals_df.alias("s"),
            "t.signal_id = s.signal_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"✓ Merged {len(all_signals)} signals into {target_table}")
    else:
        signals_df.write.format("delta").partitionBy("date").saveAsTable(target_table)
        print(f"✓ Created {target_table} with {len(all_signals)} signals")

# ---
# SUMMARY
# ---

print("\n" + "=" * 60)
print("PENDING SIGNALS FOR AI VALIDATION")
print("=" * 60)

spark.sql(f"""
    SELECT
        signal_id,
        symbol,
        timeframe,
        strategy,
        signal_type,
        ROUND(current_price, 2) as price,
        ROUND(poc_price, 2) as poc,
        ROUND(zone_strength, 0) as strength,
        ROUND(sentiment_score, 1) as sentiment,
        ai_status
    FROM {CATALOG}.{GOLD_SCHEMA}.signals_all
    WHERE ai_status = 'pending'
    ORDER BY created_at DESC
    LIMIT 20
""").show(truncate=False)

print(f"\n✓ Signal scanner complete. {len(all_signals)} signals pending AI validation.")
