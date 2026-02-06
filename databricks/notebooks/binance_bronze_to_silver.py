# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

INTERVALS = ["15m", "1h", "4h", "1d"]
CATALOG = "crypto"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
EXCHANGE = "binance"

TIMEFRAME_MS = {"15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}
CANDLES_TO_CHECK = 10
SYMBOLS = ["BTCUSDT", "ETHUSDT", "LDOUSDT", "LINKUSDT"]

# Explode symbols array from bronze table
def explode_symbols(interval):
    table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{EXCHANGE}_{interval}"
    print(f"Reading: {table_name}")
    df = spark.table(table_name)
    return (df
            .withColumn("symbol_data", F.explode("symbols"))
            .select(
                "symbol_data.*",
                F.col("interval").alias("interval"),
                F.col("timestamp").alias("ingestion_time"),
                F.col("file_date"),
                F.col("file_hour"),
                F.col("file_minute"),
                F.col("file_second"),
                F.col("file_interval")
            ))

# Upsert helper function
def upsert_to_silver(df, target_table, merge_keys):
    # Dedupe source data
    df_deduped = df.dropDuplicates(merge_keys)
    
    if spark.catalog.tableExists(target_table):
        delta_table = DeltaTable.forName(spark, target_table)
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        
        # Merge: update only if ingestion_time is newer
        delta_table.alias("target").merge(
            df_deduped.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            condition="source.ingestion_time >= target.ingestion_time",
            set={col: f"source.{col}" for col in df_deduped.columns}
        ).whenNotMatchedInsertAll().execute()
        
        print(f"✓ Merged into {target_table}")
    else:
        df_deduped.write.format("delta").mode("overwrite").saveAsTable(target_table)
        print(f"✓ Created {target_table}")

# Extract klines data
def extract_klines(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("klines").isNotNull())
          .withColumn("kline", F.explode("klines"))
          .select(
              F.col("symbol").alias("symbol"),
              F.col("kline")[0].cast("long").alias("timestamp"),
              F.col("kline")[1].cast("double").alias("open"),
              F.col("kline")[2].cast("double").alias("high"),
              F.col("kline")[3].cast("double").alias("low"),
              F.col("kline")[4].cast("double").alias("close"),
              F.col("kline")[5].cast("double").alias("volume"),
              F.col("kline")[7].cast("double").alias("quote_volume"),
              F.col("kline")[8].cast("long").alias("trades_count"),
              F.col("kline")[9].cast("double").alias("taker_buy_base_volume"),
              F.col("kline")[10].cast("double").alias("taker_buy_quote_volume"),
              F.lit(interval).alias("interval"),
              F.lit(EXCHANGE).alias("exchange"),
              F.col("ingestion_time"),
              F.col("file_date"),
              F.col("file_hour"),
              F.col("file_minute"),
              F.col("file_second"),
              F.col("file_interval")
          ))
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.{EXCHANGE}_klines_{interval}"
    upsert_to_silver(df, target_table, ["timestamp", "symbol"])

# Extract funding rate data
def extract_funding(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("funding_rate").isNotNull())
          .withColumn("funding", F.explode("funding_rate"))
          .select(
              F.col("funding.symbol").alias("symbol"),
              F.col("funding.fundingTime").cast("long").alias("timestamp"),
              F.col("funding.fundingRate").cast("double").alias("funding_rate"),
              F.col("funding.markPrice").cast("double").alias("mark_price"),
              F.lit(interval).alias("interval"),
              F.lit(EXCHANGE).alias("exchange"),
              F.col("ingestion_time"),
              F.col("file_date"),
              F.col("file_hour"),
              F.col("file_minute"),
              F.col("file_second"),
              F.col("file_interval")
          ))
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.{EXCHANGE}_funding_{interval}"
    upsert_to_silver(df, target_table, ["timestamp", "symbol"])

# Extract open interest data
def extract_oi(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("open_interest").isNotNull())
          .select(
              F.col("open_interest.symbol").alias("symbol"),
              F.col("open_interest.time").cast("long").alias("timestamp"),
              F.col("open_interest.openInterest").cast("double").alias("open_interest"),
              F.lit(None).cast("double").alias("open_interest_value"),
              F.lit(interval).alias("interval"),
              F.lit(EXCHANGE).alias("exchange"),
              F.col("ingestion_time"),
              F.col("file_date"),
              F.col("file_hour"),
              F.col("file_minute"),
              F.col("file_second"),
              F.col("file_interval")
          ))
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.{EXCHANGE}_oi_{interval}"
    upsert_to_silver(df, target_table, ["timestamp", "symbol"])

# Extract ticker data
def extract_ticker(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("ticker").isNotNull())
          .select(
              F.col("ticker.symbol").alias("symbol"),
              F.col("ticker.time").cast("long").alias("timestamp"),
              F.col("ticker.price").cast("double").alias("last_price"),
              F.lit(None).cast("double").alias("bid_price"),
              F.lit(None).cast("double").alias("ask_price"),
              F.lit(None).cast("double").alias("volume_24h"),
              F.lit(None).cast("double").alias("quote_volume_24h"),
              F.lit(None).cast("double").alias("price_change_24h"),
              F.lit(None).cast("double").alias("price_change_pct_24h"),
              F.lit(interval).alias("interval"),
              F.lit(EXCHANGE).alias("exchange"),
              F.col("ingestion_time"),
              F.col("file_date"),
              F.col("file_hour"),
              F.col("file_minute"),
              F.col("file_second"),
              F.col("file_interval")
          ))
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.{EXCHANGE}_ticker_{interval}"
    upsert_to_silver(df, target_table, ["timestamp", "symbol"])

# Extract orderbook data
def extract_orderbook(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("depth").isNotNull())
          .select(
              F.col("symbol").alias("symbol"),
              F.col("depth.lastUpdateId").cast("long").alias("timestamp"),
              F.col("depth.bids").alias("bids"),
              F.col("depth.asks").alias("asks"),
              F.lit(interval).alias("interval"),
              F.lit(EXCHANGE).alias("exchange"),
              F.col("ingestion_time"),
              F.col("file_date"),
              F.col("file_hour"),
              F.col("file_minute"),
              F.col("file_second"),
              F.col("file_interval")
          ))
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.{EXCHANGE}_orderbook_{interval}"
    upsert_to_silver(df, target_table, ["timestamp", "symbol"])

# Extract long/short ratio data
def extract_long_short_ratio(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("long_short_ratio").isNotNull())
          .withColumn("ls_record", F.explode("long_short_ratio"))
          .select(
              F.col("symbol").alias("symbol"),
              F.col("ls_record.timestamp").cast("long").alias("timestamp"),
              F.col("ls_record.longShortRatio").cast("double").alias("long_short_ratio"),
              F.col("ls_record.longAccount").cast("double").alias("long_account"),
              F.col("ls_record.shortAccount").cast("double").alias("short_account"),
              F.lit(interval).alias("interval"),
              F.lit(EXCHANGE).alias("exchange"),
              F.col("ingestion_time"),
              F.col("file_date"),
              F.col("file_hour"),
              F.col("file_minute"),
              F.col("file_second"),
              F.col("file_interval")
          ))
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.{EXCHANGE}_long_short_ratio_{interval}"
    upsert_to_silver(df, target_table, ["timestamp", "symbol"])

# Extract top traders long/short account ratio
def extract_top_long_short_accounts(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("top_long_short_accounts").isNotNull())
          .withColumn("tls_record", F.explode("top_long_short_accounts"))
          .select(
              F.col("symbol").alias("symbol"),
              F.col("tls_record.timestamp").cast("long").alias("timestamp"),
              F.col("tls_record.longShortRatio").cast("double").alias("long_short_ratio"),
              F.col("tls_record.longAccount").cast("double").alias("long_account"),
              F.col("tls_record.shortAccount").cast("double").alias("short_account"),
              F.lit(interval).alias("interval"),
              F.lit(EXCHANGE).alias("exchange"),
              F.col("ingestion_time"),
              F.col("file_date"),
              F.col("file_hour"),
              F.col("file_minute"),
              F.col("file_second"),
              F.col("file_interval")
          ))
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.{EXCHANGE}_top_ls_accounts_{interval}"
    upsert_to_silver(df, target_table, ["timestamp", "symbol"])

# Extract top traders long/short position ratio
def extract_top_long_short_positions(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("top_long_short_positions").isNotNull())
          .withColumn("tlp_record", F.explode("top_long_short_positions"))
          .select(
              F.col("symbol").alias("symbol"),
              F.col("tlp_record.timestamp").cast("long").alias("timestamp"),
              F.col("tlp_record.longShortRatio").cast("double").alias("long_short_ratio"),
              F.col("tlp_record.longAccount").cast("double").alias("long_account"),
              F.col("tlp_record.shortAccount").cast("double").alias("short_account"),
              F.lit(interval).alias("interval"),
              F.lit(EXCHANGE).alias("exchange"),
              F.col("ingestion_time"),
              F.col("file_date"),
              F.col("file_hour"),
              F.col("file_minute"),
              F.col("file_second"),
              F.col("file_interval")
          ))
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.{EXCHANGE}_top_ls_positions_{interval}"
    upsert_to_silver(df, target_table, ["timestamp", "symbol"])

# Extract taker buy/sell ratio
def extract_taker_buy_sell_ratio(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("taker_buy_sell_ratio").isNotNull())
          .withColumn("taker_record", F.explode("taker_buy_sell_ratio"))
          .select(
              F.col("symbol").alias("symbol"),
              F.col("taker_record.timestamp").cast("long").alias("timestamp"),
              F.col("taker_record.buySellRatio").cast("double").alias("buy_sell_ratio"),
              F.col("taker_record.buyVol").cast("double").alias("taker_buy_volume"),
              F.col("taker_record.sellVol").cast("double").alias("taker_sell_volume"),
              F.lit(interval).alias("interval"),
              F.lit(EXCHANGE).alias("exchange"),
              F.col("ingestion_time"),
              F.col("file_date"),
              F.col("file_hour"),
              F.col("file_minute"),
              F.col("file_second"),
              F.col("file_interval")
          ))
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.{EXCHANGE}_taker_ratio_{interval}"
    upsert_to_silver(df, target_table, ["timestamp", "symbol"])

import time

def validate_klines_completeness(interval):
    """Check that last N candles exist in silver for all symbols"""
    table = f"{CATALOG}.{SILVER_SCHEMA}.{EXCHANGE}_klines_{interval}"
    if not spark.catalog.tableExists(table):
        return

    now_ms = int(time.time() * 1000)
    interval_ms = TIMEFRAME_MS[interval]
    # Skip the current (unclosed) candle + 1 for pipeline lag
    last_closed = ((now_ms - interval_ms) // interval_ms) * interval_ms
    expected = [last_closed - (i * interval_ms) for i in range(CANDLES_TO_CHECK)]

    df = spark.table(table)
    for symbol in SYMBOLS:
        existing = set(
            row["timestamp"] for row in
            df.filter(
                (F.col("symbol") == symbol) &
                (F.col("timestamp").isin(expected))
            ).select("timestamp").distinct().collect()
        )
        missing = [ts for ts in expected if ts not in existing]
        if missing:
            print(f"  ⚠️ {symbol} [{interval}]: {len(missing)}/{CANDLES_TO_CHECK} candles missing")
        else:
            print(f"  ✓ {symbol} [{interval}]: all {CANDLES_TO_CHECK} candles present")

# Process all intervals
for interval in INTERVALS:
    print(f"\n{'='*60}")
    print(f"Processing {EXCHANGE}_{interval}")
    print(f"{'='*60}")
    try:
        df_exploded = explode_symbols(interval).cache()
        extract_klines(df_exploded, interval)
        extract_funding(df_exploded, interval)
        extract_oi(df_exploded, interval)
        extract_ticker(df_exploded, interval)
        extract_orderbook(df_exploded, interval)
        # Sentiment data
        extract_long_short_ratio(df_exploded, interval)
        extract_top_long_short_accounts(df_exploded, interval)
        extract_top_long_short_positions(df_exploded, interval)
        extract_taker_buy_sell_ratio(df_exploded, interval)
        df_exploded.unpersist()
        # Validate klines completeness
        validate_klines_completeness(interval)
    except Exception as e:
        print(f"✗ Error: {str(e)}")

print("\n" + "="*60)
print("Done")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(timestamp) FROM crypto.silver.unified_klines_15m;