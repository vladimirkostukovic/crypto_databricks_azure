# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from functools import reduce
from typing import List, Dict, Optional, Tuple

# ---
# CONFIGURATION
# ---

CATALOG = "crypto"
SILVER_SCHEMA = "silver"
INTERVALS = ["15m", "1h", "4h", "1d"]
DATA_TYPES = ["klines", "oi", "ticker", "orderbook", "funding", "long_short_ratio", "taker_ratio", "top_ls_accounts", "top_ls_positions"]

# Timestamp divisor per exchange/data_type (to normalize to milliseconds)
TIMESTAMP_CONFIG = {
    "binance": {
        "klines": 1,
        "oi": 1,
        "ticker": 1,
        "orderbook": 1000,  # microseconds → milliseconds
        "funding": 1,
        "long_short_ratio": 1,
        "top_ls_accounts": 1,
        "top_ls_positions": 1,
        "taker_ratio": 1
    },
    "bybit": {
        "klines": 1,
        "oi": 1,
        "ticker": 1,
        "orderbook": 1,
        "funding": 1,
        "long_short_ratio": 1
    },
    "okx": {
        "klines": 1,
        "oi": 1,
        "ticker": 1,
        "orderbook": 1,
        "funding": 1,
        "long_short_ratio": 1,
        "taker_ratio": 1
    }
}

# Column definitions per data type
COLUMN_SPECS = {
    "klines": [
        "symbol", "timestamp", "datetime", "date", "hour",
        "open", "high", "low", "close", "volume", "quote_volume",
        "trades_count", "taker_buy_base_volume", "taker_buy_quote_volume",
        "interval", "exchange", "ingestion_time"
    ],
    "oi": [
        "symbol", "timestamp", "datetime", "date", "hour",
        "open_interest", "open_interest_value",
        "interval", "exchange", "ingestion_time"
    ],
    "ticker": [
        "symbol", "timestamp", "datetime", "date", "hour",
        "last_price", "bid_price", "ask_price",
        "volume_24h", "quote_volume_24h",
        "price_change_24h", "price_change_pct_24h",
        "interval", "exchange", "ingestion_time"
    ],
    "orderbook": [
        "symbol", "timestamp", "datetime", "date", "hour",
        "best_bid", "best_ask", "spread",
        "bid_avg_price", "bid_total_quantity",
        "ask_avg_price", "ask_total_quantity",
        "interval", "exchange", "ingestion_time"
    ],
    "funding": [
        "symbol", "timestamp", "datetime", "date", "hour",
        "funding_rate", "mark_price",
        "interval", "exchange", "ingestion_time"
    ],
    "long_short_ratio": [
        "symbol", "timestamp", "datetime", "date", "hour",
        "long_short_ratio", "long_account", "short_account",
        "long_ratio", "short_ratio",
        "interval", "exchange", "ingestion_time"
    ],
    "taker_ratio": [
        "symbol", "timestamp", "datetime", "date", "hour",
        "buy_sell_ratio", "taker_buy_volume", "taker_sell_volume",
        "interval", "exchange", "ingestion_time"
    ],
    "top_ls_accounts": [
        "symbol", "timestamp", "datetime", "date", "hour",
        "long_short_ratio", "long_account", "short_account",
        "interval", "exchange", "ingestion_time"
    ],
    "top_ls_positions": [
        "symbol", "timestamp", "datetime", "date", "hour",
        "long_short_ratio", "long_account", "short_account",
        "interval", "exchange", "ingestion_time"
    ]
}

print("=" * 80)
print("UNIFIED TABLES CREATION")
print("=" * 80)
print(f"Catalog: {CATALOG}.{SILVER_SCHEMA}")
print(f"Data Types: {', '.join(DATA_TYPES)}")
print(f"Intervals: {', '.join(INTERVALS)}")
print("=" * 80)


# ---
# HELPER FUNCTIONS
# ---

def table_exists_and_has_data(table_name: str) -> Tuple[bool, int]:
    """Check if table exists and get watermark in one query"""
    if not spark.catalog.tableExists(table_name):
        return False, 0
    
    result = spark.sql(f"SELECT MAX(timestamp) as max_ts FROM {table_name}").first()
    max_ts = result["max_ts"] if result["max_ts"] is not None else 0
    return True, max_ts


def safe_select(df, columns: List[str]):
    """Select columns that exist, fill missing with NULL"""
    existing = set(df.columns)
    select_exprs = []
    
    for col in columns:
        if col in existing:
            select_exprs.append(F.col(col))
        else:
            select_exprs.append(F.lit(None).alias(col))
    
    return df.select(*select_exprs)


def safe_array_first(col_name: str, index: int = 0):
    """Safely get first element from array, return NULL if empty"""
    return F.when(
        F.size(F.col(col_name)) > index,
        F.col(col_name).getItem(index)
    ).otherwise(None)


def safe_array_agg(col_name: str, prices_idx: int = 0, qty_idx: int = 1):
    """Safely compute avg price and total qty from orderbook array"""
    # Extract prices and quantities
    prices = F.transform(F.col(col_name), lambda x: x.getItem(prices_idx).cast("double"))
    quantities = F.transform(F.col(col_name), lambda x: x.getItem(qty_idx).cast("double"))
    
    avg_price = F.when(
        F.size(F.col(col_name)) > 0,
        F.aggregate(prices, F.lit(0.0), lambda acc, x: acc + x) / F.size(F.col(col_name))
    ).otherwise(None)
    
    total_qty = F.when(
        F.size(F.col(col_name)) > 0,
        F.aggregate(quantities, F.lit(0.0), lambda acc, x: acc + x)
    ).otherwise(None)
    
    return avg_price, total_qty


# ---
# ORDERBOOK TRANSFORMATION
# ---

def transform_orderbook(df):
    """Transform orderbook with safe array handling"""
    bid_avg, bid_total = safe_array_agg("bids")
    ask_avg, ask_total = safe_array_agg("asks")
    
    return (df
        .withColumn("best_bid", safe_array_first("bids").getItem(0).cast("double"))
        .withColumn("best_ask", safe_array_first("asks").getItem(0).cast("double"))
        .withColumn("spread", F.col("best_ask") - F.col("best_bid"))
        .withColumn("bid_avg_price", bid_avg)
        .withColumn("bid_total_quantity", bid_total)
        .withColumn("ask_avg_price", ask_avg)
        .withColumn("ask_total_quantity", ask_total)
    )


# ---
# MAIN PROCESSING FUNCTION
# ---

def create_unified_table(data_type: str, interval: str) -> Optional[str]:
    """Create or update unified table for given data type and interval"""
    
    target_table = f"{CATALOG}.{SILVER_SCHEMA}.unified_{data_type}_{interval}"
    exchanges = list(TIMESTAMP_CONFIG.keys())
    
    print(f"\n{data_type}_{interval}:")
    
    # Check target table and get watermark
    target_exists, watermark = table_exists_and_has_data(target_table)
    if target_exists:
        print(f"  Watermark: {watermark}")
    
    # Read and normalize from all exchanges
    dfs = []
    for exchange in exchanges:
        source_table = f"{CATALOG}.{SILVER_SCHEMA}.{exchange}_{data_type}_{interval}"
        
        if not spark.catalog.tableExists(source_table):
            print(f"  ⚠ {exchange}: source not found")
            continue
        
        df = spark.table(source_table)
        
        # Normalize timestamp to milliseconds
        divisor = TIMESTAMP_CONFIG[exchange][data_type]
        if divisor > 1:
            df = df.withColumn("timestamp", (F.col("timestamp") / divisor).cast("long"))
        
        # Filter by watermark (incremental)
        if watermark > 0:
            df = df.filter(F.col("timestamp") > watermark)
        
        dfs.append(df)
    
    if not dfs:
        print(f"  ✗ No source tables")
        return None
    
    # Union all exchanges (handles different column orders)
    if len(dfs) == 1:
        df_unified = dfs[0]
    else:
        # Align schemas before union
        all_columns = set()
        for df in dfs:
            all_columns.update(df.columns)
        
        aligned_dfs = []
        for df in dfs:
            for col in all_columns:
                if col not in df.columns:
                    df = df.withColumn(col, F.lit(None))
            aligned_dfs.append(df.select(sorted(all_columns)))
        
        df_unified = reduce(lambda a, b: a.union(b), aligned_dfs)
    
    # Common transformations
    df_transformed = (df_unified
        .withColumn("datetime", F.from_unixtime(F.col("timestamp") / 1000).cast("timestamp"))
        .withColumn("date", F.to_date("datetime"))
        .withColumn("hour", F.hour("datetime"))
    )
    
    # Data-type specific transformations
    if data_type == "orderbook":
        df_transformed = transform_orderbook(df_transformed)
    
    # Select final columns (safe - fills missing with NULL)
    final_columns = COLUMN_SPECS[data_type]
    df_final = safe_select(df_transformed, final_columns)
    
    # Check for new data without full count
    if df_final.isEmpty():
        print(f"  ℹ No new data")
        return None
    
    # Merge or create
    if target_exists:
        delta_table = DeltaTable.forName(spark, target_table)
        
        (delta_table.alias("t")
            .merge(
                df_final.alias("s"),
                "t.symbol = s.symbol AND t.timestamp = s.timestamp AND t.exchange = s.exchange"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        
        # Get merge metrics
        history = spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").first()
        metrics = history["operationMetrics"]
        inserted = metrics.get("numTargetRowsInserted", "?")
        updated = metrics.get("numTargetRowsUpdated", "?")
        print(f"  ✓ Merged: +{inserted} inserted, ~{updated} updated")
    else:
        # Create with partitioning
        (df_final
            .write
            .format("delta")
            .partitionBy("date")
            .mode("overwrite")
            .saveAsTable(target_table)
        )
        row_count = spark.table(target_table).count()
        print(f"  ✓ Created: {row_count:,} rows")
    
    return target_table


# ---
# MAIN EXECUTION
# ---

print("\n" + "=" * 80)
print("PROCESSING")
print("=" * 80)

results = {"success": [], "failed": [], "skipped": []}

for interval in INTERVALS:
    print(f"\n{'=' * 40}")
    print(f"INTERVAL: {interval}")
    print(f"{'=' * 40}")
    
    for data_type in DATA_TYPES:
        try:
            result = create_unified_table(data_type, interval)
            if result:
                results["success"].append(f"{data_type}_{interval}")
            else:
                results["skipped"].append(f"{data_type}_{interval}")
        except Exception as e:
            print(f"  ✗ ERROR: {str(e)[:100]}")
            results["failed"].append(f"{data_type}_{interval}")


# ---
# SUMMARY
# ---

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"✓ Success: {len(results['success'])}")
print(f"ℹ Skipped: {len(results['skipped'])} (no new data)")
print(f"✗ Failed: {len(results['failed'])}")

if results["failed"]:
    print("\nFailed tables:")
    for f in results["failed"]:
        print(f"  - {f}")

print("=" * 80)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(timestamp), from_unixtime(MAX(timestamp)/1000) as time
# MAGIC FROM crypto.silver.unified_klines_15m;