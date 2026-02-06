# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

INTERVALS = ["15m", "1h", "4h", "1d"]
CATALOG = "crypto"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
EXCHANGE = "okx"

TIMEFRAME_MS = {"15m": 900000, "1h": 3600000, "4h": 14400000, "1d": 86400000}
CANDLES_TO_CHECK = 10
SYMBOLS = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "LDO-USDT-SWAP", "LINK-USDT-SWAP"]

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
    df_deduped = df.dropDuplicates(merge_keys)

    if spark.catalog.tableExists(target_table):
        delta_table = DeltaTable.forName(spark, target_table)
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])

        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        delta_table.alias("target").merge(
            df_deduped.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"✓ Merged into {target_table}")
    else:
        df_deduped.write.format("delta").mode("overwrite").saveAsTable(target_table)
        print(f"✓ Created {target_table}")

# Extract klines data - OKX format: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
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
              F.lit(None).cast("long").alias("trades_count"),
              F.lit(None).cast("double").alias("taker_buy_base_volume"),
              F.lit(None).cast("double").alias("taker_buy_quote_volume"),
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

# Extract funding rate data (current)
def extract_funding(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("funding_rate").isNotNull())
          .select(
              F.col("funding_rate.instId").alias("symbol"),
              F.col("funding_rate.fundingTime").cast("long").alias("timestamp"),
              F.col("funding_rate.fundingRate").cast("double").alias("funding_rate"),
              F.col("funding_rate.nextFundingRate").cast("double").alias("next_funding_rate"),
              F.lit(None).cast("double").alias("mark_price"),
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

# Extract funding rate history
def extract_funding_history(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("funding_rate_history").isNotNull())
          .withColumn("fr_record", F.explode("funding_rate_history"))
          .select(
              F.col("fr_record.instId").alias("symbol"),
              F.col("fr_record.fundingTime").cast("long").alias("timestamp"),
              F.col("fr_record.fundingRate").cast("double").alias("funding_rate"),
              F.col("fr_record.realizedRate").cast("double").alias("realized_rate"),
              F.lit(interval).alias("interval"),
              F.lit(EXCHANGE).alias("exchange"),
              F.col("ingestion_time"),
              F.col("file_date"),
              F.col("file_hour"),
              F.col("file_minute"),
              F.col("file_second"),
              F.col("file_interval")
          ))
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.{EXCHANGE}_funding_history_{interval}"
    upsert_to_silver(df, target_table, ["timestamp", "symbol"])

# Extract open interest data
def extract_oi(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("open_interest").isNotNull())
          .select(
              F.col("open_interest.instId").alias("symbol"),
              F.col("open_interest.ts").cast("long").alias("timestamp"),
              F.col("open_interest.oi").cast("double").alias("open_interest"),
              F.col("open_interest.oiCcy").cast("double").alias("open_interest_value"),
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
              F.col("ticker.instId").alias("symbol"),
              F.col("ticker.ts").cast("long").alias("timestamp"),
              F.col("ticker.last").cast("double").alias("last_price"),
              F.col("ticker.bidPx").cast("double").alias("bid_price"),
              F.col("ticker.askPx").cast("double").alias("ask_price"),
              F.col("ticker.vol24h").cast("double").alias("volume_24h"),
              F.col("ticker.volCcy24h").cast("double").alias("quote_volume_24h"),
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
              F.col("depth.ts").cast("long").alias("timestamp"),
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

# Extract long/short ratio data - OKX format: [ts, longShortRatio]
def extract_long_short_ratio(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("long_short_ratio").isNotNull())
          .withColumn("ls_record", F.explode("long_short_ratio"))
          .select(
              F.col("symbol").alias("symbol"),
              F.col("ls_record")[0].cast("long").alias("timestamp"),
              F.col("ls_record")[1].cast("double").alias("long_short_ratio"),
              F.lit(None).cast("double").alias("long_account"),
              F.lit(None).cast("double").alias("short_account"),
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

# Extract taker volume data - OKX format: [ts, sellVol, buyVol]
def extract_taker_volume(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("taker_volume").isNotNull())
          .withColumn("tv_record", F.explode("taker_volume"))
          .select(
              F.col("symbol").alias("symbol"),
              F.col("tv_record")[0].cast("long").alias("timestamp"),
              F.col("tv_record")[2].cast("double").alias("taker_buy_volume"),
              F.col("tv_record")[1].cast("double").alias("taker_sell_volume"),
              (F.col("tv_record")[2].cast("double") / F.col("tv_record")[1].cast("double")).alias("buy_sell_ratio"),
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
    table = f"{CATALOG}.{SILVER_SCHEMA}.{EXCHANGE}_klines_{interval}"
    if not spark.catalog.tableExists(table):
        return

    now_ms = int(time.time() * 1000)
    interval_ms = TIMEFRAME_MS[interval]
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
        extract_funding_history(df_exploded, interval)
        extract_oi(df_exploded, interval)
        extract_ticker(df_exploded, interval)
        extract_orderbook(df_exploded, interval)
        # Sentiment data
        extract_long_short_ratio(df_exploded, interval)
        extract_taker_volume(df_exploded, interval)
        df_exploded.unpersist()
        # Validate klines completeness
        validate_klines_completeness(interval)
    except Exception as e:
        print(f"✗ Error: {str(e)}")

print("\n" + "="*60)
print("Done")
