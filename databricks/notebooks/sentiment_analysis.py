# Databricks notebook source
# ---
# SENTIMENT ANALYSIS
# Analyzes: Long/Short Ratio, Taker Buy/Sell, Top Traders
# Outputs: gold.sentiment_scores
# ---

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timezone, timedelta

# ---
# CONFIGURATION
# ---

CATALOG = "crypto"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

TIMEFRAMES = ["15m", "1h", "4h"]
LOOKBACK_HOURS = {"15m": 24, "1h": 72, "4h": 168}

# Load config from gold.trading_config
def get_config(param_name, default=None):
    try:
        result = spark.sql(f"""
            SELECT param_value, param_type
            FROM {CATALOG}.{GOLD_SCHEMA}.trading_config
            WHERE param_name = '{param_name}'
        """).first()
        if result:
            val, ptype = result["param_value"], result["param_type"]
            if ptype == "int":
                return int(val)
            elif ptype == "float":
                return float(val)
            elif ptype == "bool":
                return val.lower() == "true"
            elif ptype == "json":
                import json
                return json.loads(val)
            return val
        return default
    except:
        return default

CROWDING_THRESHOLD = get_config("crowding_threshold", 65)
SMART_MONEY_DIV_THRESHOLD = get_config("smart_money_div_threshold", 20)
TRADING_SYMBOLS = get_config("trading_symbols", ["BTCUSDT", "ETHUSDT"])

print("=" * 60)
print("SENTIMENT ANALYSIS")
print("=" * 60)
print(f"Symbols: {TRADING_SYMBOLS}")
print(f"Timeframes: {TIMEFRAMES}")
print(f"Crowding threshold: {CROWDING_THRESHOLD}%")
print(f"Smart money divergence threshold: {SMART_MONEY_DIV_THRESHOLD}%")

# ---
# HELPER FUNCTIONS
# ---

def get_cutoff_ms(hours):
    return int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp() * 1000)


def add_percentile(df, value_col, partition_cols, output_col):
    """Add percentile rank column"""
    window = Window.partitionBy(*partition_cols).orderBy(F.col(value_col))
    return df.withColumn(
        output_col,
        F.round(F.percent_rank().over(window) * 100, 0).cast("int")
    )


# ---
# LOAD DATA FUNCTIONS
# ---

def load_long_short_ratio(timeframe, symbols, lookback_hours):
    """Load unified long/short ratio data"""
    cutoff_ms = get_cutoff_ms(lookback_hours)
    table = f"{CATALOG}.{SILVER_SCHEMA}.unified_long_short_ratio_{timeframe}"

    try:
        df = spark.table(table).filter(
            (F.col("timestamp") >= cutoff_ms) &
            (F.col("symbol").isin(symbols))
        ).select(
            "symbol", "timestamp", "exchange",
            "long_short_ratio", "long_account", "short_account",
            F.coalesce(F.col("long_ratio"), F.lit(None)).alias("long_ratio"),
            F.coalesce(F.col("short_ratio"), F.lit(None)).alias("short_ratio")
        )
        return df
    except Exception as e:
        print(f"  ⚠ Error loading L/S ratio: {e}")
        return None


def load_taker_ratio(timeframe, symbols, lookback_hours):
    """Load unified taker buy/sell ratio data"""
    cutoff_ms = get_cutoff_ms(lookback_hours)
    table = f"{CATALOG}.{SILVER_SCHEMA}.unified_taker_ratio_{timeframe}"

    try:
        df = spark.table(table).filter(
            (F.col("timestamp") >= cutoff_ms) &
            (F.col("symbol").isin(symbols))
        ).select(
            "symbol", "timestamp", "exchange",
            "buy_sell_ratio", "taker_buy_volume", "taker_sell_volume"
        )
        return df
    except Exception as e:
        print(f"  ⚠ Error loading taker ratio: {e}")
        return None


def load_top_traders_ls(timeframe, symbols, lookback_hours):
    """Load Binance top traders long/short data (accounts + positions)"""
    cutoff_ms = get_cutoff_ms(lookback_hours)

    # Try loading top accounts
    accounts_df = None
    try:
        table_accounts = f"{CATALOG}.{SILVER_SCHEMA}.binance_top_ls_accounts_{timeframe}"
        if spark.catalog.tableExists(table_accounts):
            accounts_df = spark.table(table_accounts).filter(
                (F.col("timestamp") >= cutoff_ms) &
                (F.col("symbol").isin(symbols))
            ).select(
                "symbol", "timestamp",
                F.col("long_short_ratio").alias("top_accounts_ls_ratio"),
                F.col("long_account").alias("top_accounts_long_pct"),
                F.col("short_account").alias("top_accounts_short_pct")
            )
    except:
        pass

    # Try loading top positions
    positions_df = None
    try:
        table_positions = f"{CATALOG}.{SILVER_SCHEMA}.binance_top_ls_positions_{timeframe}"
        if spark.catalog.tableExists(table_positions):
            positions_df = spark.table(table_positions).filter(
                (F.col("timestamp") >= cutoff_ms) &
                (F.col("symbol").isin(symbols))
            ).select(
                "symbol", "timestamp",
                F.col("long_short_ratio").alias("top_positions_ls_ratio"),
                F.col("long_account").alias("top_positions_long_pct"),
                F.col("short_account").alias("top_positions_short_pct")
            )
    except:
        pass

    return accounts_df, positions_df


# ---
# SENTIMENT CALCULATION
# ---

def calculate_sentiment_for_tf(timeframe, symbols):
    """Calculate sentiment scores for a timeframe"""

    print(f"\n{'─' * 40}")
    print(f"Processing {timeframe}")
    print(f"{'─' * 40}")

    lookback = LOOKBACK_HOURS[timeframe]

    # Load data
    ls_df = load_long_short_ratio(timeframe, symbols, lookback)
    taker_df = load_taker_ratio(timeframe, symbols, lookback)
    top_accounts_df, top_positions_df = load_top_traders_ls(timeframe, symbols, lookback)

    # Check what data we have
    has_ls = ls_df is not None and ls_df.count() > 0
    has_taker = taker_df is not None and taker_df.count() > 0
    has_top = top_accounts_df is not None and top_accounts_df.count() > 0

    print(f"  Data available: L/S={has_ls}, Taker={has_taker}, TopTraders={has_top}")

    if not has_ls:
        print(f"  ⚠ No L/S ratio data, skipping")
        return None

    # Aggregate L/S ratio across exchanges
    ls_agg = ls_df.groupBy("symbol", "timestamp").agg(
        F.avg("long_short_ratio").alias("long_short_ratio"),
        F.avg("long_account").alias("long_account_pct"),
        F.avg("short_account").alias("short_account_pct")
    )

    # Add percentile for L/S ratio
    ls_agg = add_percentile(ls_agg, "long_short_ratio", ["symbol"], "ls_percentile")

    # Determine crowding direction
    ls_agg = ls_agg.withColumn(
        "crowding_direction",
        F.when(F.col("long_account_pct") >= CROWDING_THRESHOLD, F.lit("long_crowded"))
         .when(F.col("short_account_pct") >= CROWDING_THRESHOLD, F.lit("short_crowded"))
         .otherwise(F.lit("neutral"))
    )

    # Start building result
    result_df = ls_agg.withColumn("timeframe", F.lit(timeframe))

    # Join taker data if available
    if has_taker:
        taker_agg = taker_df.groupBy("symbol", "timestamp").agg(
            F.avg("buy_sell_ratio").alias("taker_buy_sell_ratio"),
            F.sum("taker_buy_volume").alias("taker_buy_volume"),
            F.sum("taker_sell_volume").alias("taker_sell_volume")
        )
        taker_agg = add_percentile(taker_agg, "taker_buy_sell_ratio", ["symbol"], "taker_percentile")

        # Aggression direction
        taker_agg = taker_agg.withColumn(
            "aggression_direction",
            F.when(F.col("taker_percentile") >= 80, F.lit("aggressive_buying"))
             .when(F.col("taker_percentile") <= 20, F.lit("aggressive_selling"))
             .otherwise(F.lit("neutral"))
        )

        result_df = result_df.join(
            taker_agg,
            ["symbol", "timestamp"],
            "left"
        )
    else:
        result_df = result_df.withColumn("taker_buy_sell_ratio", F.lit(None).cast("double"))
        result_df = result_df.withColumn("taker_buy_volume", F.lit(None).cast("double"))
        result_df = result_df.withColumn("taker_sell_volume", F.lit(None).cast("double"))
        result_df = result_df.withColumn("taker_percentile", F.lit(None).cast("int"))
        result_df = result_df.withColumn("aggression_direction", F.lit("unknown"))

    # Join top traders data if available
    if has_top:
        # Use top accounts as primary
        result_df = result_df.join(
            top_accounts_df.select(
                "symbol", "timestamp",
                F.col("top_accounts_ls_ratio").alias("top_ls_ratio"),
                F.col("top_accounts_long_pct").alias("top_long_pct"),
                F.col("top_accounts_short_pct").alias("top_short_pct")
            ),
            ["symbol", "timestamp"],
            "left"
        )

        # Calculate smart money divergence
        result_df = result_df.withColumn(
            "smart_money_divergence",
            F.when(
                F.col("top_ls_ratio").isNotNull() & F.col("long_short_ratio").isNotNull(),
                F.abs(F.col("top_ls_ratio") - F.col("long_short_ratio")) / F.col("long_short_ratio") * 100
            ).otherwise(F.lit(None))
        )

        # Divergence direction: are top traders positioned opposite to retail?
        result_df = result_df.withColumn(
            "divergence_direction",
            F.when(
                (F.col("smart_money_divergence") >= SMART_MONEY_DIV_THRESHOLD) &
                (F.col("top_ls_ratio") > F.col("long_short_ratio")),
                F.lit("smart_money_long")  # Top traders more long than retail
            ).when(
                (F.col("smart_money_divergence") >= SMART_MONEY_DIV_THRESHOLD) &
                (F.col("top_ls_ratio") < F.col("long_short_ratio")),
                F.lit("smart_money_short")  # Top traders more short than retail
            ).otherwise(F.lit("aligned"))
        )
    else:
        result_df = result_df.withColumn("top_ls_ratio", F.lit(None).cast("double"))
        result_df = result_df.withColumn("top_long_pct", F.lit(None).cast("double"))
        result_df = result_df.withColumn("top_short_pct", F.lit(None).cast("double"))
        result_df = result_df.withColumn("smart_money_divergence", F.lit(None).cast("double"))
        result_df = result_df.withColumn("divergence_direction", F.lit("unknown"))

    # Calculate composite sentiment score
    # Score: -100 (extreme bearish) to +100 (extreme bullish)
    result_df = result_df.withColumn(
        "sentiment_score",
        # L/S component: high ratio = bullish, normalize to -50 to +50
        (F.col("ls_percentile") - 50) * 0.5 +
        # Taker component: high buy ratio = bullish
        F.coalesce((F.col("taker_percentile") - 50) * 0.3, F.lit(0)) +
        # Smart money component: if diverging, follow smart money
        F.when(F.col("divergence_direction") == "smart_money_long", F.lit(20))
         .when(F.col("divergence_direction") == "smart_money_short", F.lit(-20))
         .otherwise(F.lit(0))
    )

    # Sentiment bias
    result_df = result_df.withColumn(
        "sentiment_bias",
        F.when(F.col("sentiment_score") >= 30, F.lit("bullish"))
         .when(F.col("sentiment_score") <= -30, F.lit("bearish"))
         .otherwise(F.lit("neutral"))
    )

    # Add datetime and calculated_at
    result_df = result_df.withColumn(
        "datetime",
        F.from_unixtime(F.col("timestamp") / 1000).cast("timestamp")
    ).withColumn(
        "calculated_at",
        F.current_timestamp()
    )

    # Select final columns
    result_df = result_df.select(
        "symbol", "timeframe", "timestamp", "datetime",
        "long_short_ratio", "long_account_pct", "short_account_pct",
        "ls_percentile", "crowding_direction",
        "top_ls_ratio", "top_long_pct", "top_short_pct",
        "smart_money_divergence", "divergence_direction",
        "taker_buy_sell_ratio", "taker_buy_volume", "taker_sell_volume",
        "taker_percentile", "aggression_direction",
        "sentiment_score", "sentiment_bias",
        "calculated_at"
    )

    count = result_df.count()
    print(f"  ✓ Calculated {count} sentiment records")

    return result_df


# ---
# MAIN EXECUTION
# ---

all_sentiment = []

for tf in TIMEFRAMES:
    df = calculate_sentiment_for_tf(tf, TRADING_SYMBOLS)
    if df is not None:
        all_sentiment.append(df)

if not all_sentiment:
    print("\n⚠ No sentiment data generated")
else:
    # Union all timeframes
    sentiment_df = all_sentiment[0]
    for df in all_sentiment[1:]:
        sentiment_df = sentiment_df.unionByName(df)

    # Write to gold.sentiment_scores
    target_table = f"{CATALOG}.{GOLD_SCHEMA}.sentiment_scores"

    if spark.catalog.tableExists(target_table):
        delta_table = DeltaTable.forName(spark, target_table)
        delta_table.alias("t").merge(
            sentiment_df.alias("s"),
            "t.symbol = s.symbol AND t.timeframe = s.timeframe AND t.timestamp = s.timestamp"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        history = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").first()
        metrics = history["operationMetrics"]
        print(f"\n✓ Merged: +{metrics.get('numTargetRowsInserted', '?')} inserted, ~{metrics.get('numTargetRowsUpdated', '?')} updated")
    else:
        sentiment_df.write.format("delta").partitionBy("timeframe").saveAsTable(target_table)
        print(f"\n✓ Created {target_table}")

# ---
# SHOW LATEST SENTIMENT
# ---

print("\n" + "=" * 60)
print("LATEST SENTIMENT BY SYMBOL")
print("=" * 60)

spark.sql(f"""
    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol, timeframe ORDER BY timestamp DESC) as rn
        FROM {CATALOG}.{GOLD_SCHEMA}.sentiment_scores
    )
    SELECT
        symbol, timeframe,
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

print("\n✓ Sentiment analysis complete")
