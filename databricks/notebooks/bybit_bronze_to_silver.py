# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

INTERVALS = ["15m", "1h", "4h", "1d"]
CATALOG = "crypto"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
EXCHANGE = "bybit"

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

# Extract klines data
def extract_klines(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("klines").isNotNull())
          .filter(F.col("klines.result.list").isNotNull())
          .withColumn("kline", F.explode("klines.result.list"))
          .select(
              F.col("klines.result.symbol").alias("symbol"),
              F.col("kline")[0].cast("long").alias("timestamp"),
              F.col("kline")[1].cast("double").alias("open"),
              F.col("kline")[2].cast("double").alias("high"),
              F.col("kline")[3].cast("double").alias("low"),
              F.col("kline")[4].cast("double").alias("close"),
              F.col("kline")[5].cast("double").alias("volume"),
              F.col("kline")[6].cast("double").alias("quote_volume"),
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

# Extract funding rate data
def extract_funding(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("funding_rate").isNotNull())
          .filter(F.col("funding_rate.result.list").isNotNull())
          .withColumn("funding", F.explode("funding_rate.result.list"))
          .select(
              F.col("funding.symbol").alias("symbol"),
              F.col("funding.fundingRateTimestamp").cast("long").alias("timestamp"),
              F.col("funding.fundingRate").cast("double").alias("funding_rate"),
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

# Extract open interest data
def extract_oi(df_exploded, interval):
    df = (df_exploded
          .filter(F.col("open_interest").isNotNull())
          .filter(F.col("open_interest.result.list").isNotNull())
          .withColumn("oi_record", F.explode("open_interest.result.list"))
          .select(
              F.col("open_interest.result.symbol").alias("symbol"),
              F.col("oi_record.timestamp").cast("long").alias("timestamp"),
              F.col("oi_record.openInterest").cast("double").alias("open_interest"),
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
          .filter(F.col("ticker.result.list").isNotNull())
          .withColumn("tick", F.explode("ticker.result.list"))
          .select(
              F.col("tick.symbol").alias("symbol"),
              F.col("ticker.time").cast("long").alias("timestamp"),
              F.col("tick.lastPrice").cast("double").alias("last_price"),
              F.col("tick.bid1Price").cast("double").alias("bid_price"),
              F.col("tick.ask1Price").cast("double").alias("ask_price"),
              F.col("tick.volume24h").cast("double").alias("volume_24h"),
              F.col("tick.turnover24h").cast("double").alias("quote_volume_24h"),
              F.lit(None).cast("double").alias("price_change_24h"),
              F.col("tick.price24hPcnt").cast("double").alias("price_change_pct_24h"),
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
          .filter(F.col("orderbook").isNotNull())
          .filter(F.col("orderbook.result").isNotNull())
          .select(
              F.col("orderbook.result.s").alias("symbol"),
              F.col("orderbook.result.ts").cast("long").alias("timestamp"),
              F.col("orderbook.result.b").alias("bids"),
              F.col("orderbook.result.a").alias("asks"),
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
          .filter(F.col("long_short_ratio.result.list").isNotNull())
          .withColumn("ls_record", F.explode("long_short_ratio.result.list"))
          .select(
              F.col("long_short_ratio.result.symbol").alias("symbol"),
              F.col("ls_record.timestamp").cast("long").alias("timestamp"),
              F.col("ls_record.buyRatio").cast("double").alias("long_ratio"),
              F.col("ls_record.sellRatio").cast("double").alias("short_ratio"),
              (F.col("ls_record.buyRatio").cast("double") / F.col("ls_record.sellRatio").cast("double")).alias("long_short_ratio"),
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

# Process all intervals
for interval in INTERVALS:
    print(f"\n{'='*60}")
    print(f"Processing {EXCHANGE}_{interval}")
    print(f"{'='*60}")
    try:
        df_exploded = explode_symbols(interval)
        extract_klines(df_exploded, interval)
        extract_funding(df_exploded, interval)
        extract_oi(df_exploded, interval)
        extract_ticker(df_exploded, interval)
        extract_orderbook(df_exploded, interval)
        # Sentiment data
        extract_long_short_ratio(df_exploded, interval)
    except Exception as e:
        print(f"✗ Error: {str(e)}")

print("\n" + "="*60)
print("Done")