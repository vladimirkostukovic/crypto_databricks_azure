# Databricks notebook source
from pyspark.sql.functions import regexp_extract, col, lit
spark.conf.set("spark.sql.ansi.enabled", "false")
LANDING_PATH = "abfss://landing-dev@stcryptomedallion.dfs.core.windows.net/"
BRONZE_PATH = "abfss://bronze-dev@stcryptomedallion.dfs.core.windows.net/"
EXCHANGES = ["binance", "bybit", "okx"]
INTERVALS = ["15m", "1h", "4h", "1d"]

# COMMAND ----------

from pyspark.sql.functions import col, size, input_file_name

def autoload_status():
    try:
        path = f"{LANDING_PATH}status/"
        checkpoint = f"{BRONZE_PATH}_checkpoints/ingestion_status"
        table = "crypto.bronze.ingestion_status"
        
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{BRONZE_PATH}status/_schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("recursiveFileLookup", "true")
            .option("multiLine", "true")
            .load(path)
        )
        
        # Add metadata
        df_final = df.withColumn("_input_file", col("_metadata.file_path")) \
            .withColumn("ingestion_time", col("timestamp"))
        
        stream = (
            df_final.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "true")
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(table)
        )
        
        print(f"✓ Status autoloader started")
        return stream
        
    except Exception as e:
        print(f"✗ Status autoloader failed: {e}")
        return None

# Run status loader
#status_stream = autoload_status()
#if status_stream:
#    status_stream.awaitTermination()

# COMMAND ----------

def autoload_interval(exchange: str, interval: str):
    try:
        path = f"{LANDING_PATH}{exchange}/"
        checkpoint = f"{BRONZE_PATH}_checkpoints/{exchange}_{interval}"
        table = f"crypto.bronze.{exchange}_{interval}"
        
        df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{BRONZE_PATH}{exchange}/_schema_{interval}")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("recursiveFileLookup", "true")
            .option("multiLine", "true")
            .load(path)
        )
        
        df_with_meta = df.withColumn("_input_file", col("_metadata.file_path")) \
            .withColumn("file_date", regexp_extract(col("_input_file"), r"date=(\d{4}-\d{2}-\d{2})", 1)) \
            .withColumn("file_hour", regexp_extract(col("_input_file"), r"hour=(\d{2})", 1)) \
            .withColumn("file_minute", regexp_extract(col("_input_file"), r"\d{8}_\d{2}(\d{2})\d{2}", 1)) \
            .withColumn("file_second", regexp_extract(col("_input_file"), r"\d{8}_\d{4}(\d{2})", 1)) \
            .withColumn("file_interval", regexp_extract(col("_input_file"), r"_(\d+[mhd])\.json", 1)) \
            .withColumn("exchange", lit(exchange))
        
        df_filtered = df_with_meta.filter(col("file_interval") == interval)
        
        stream = (
            df_filtered.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "true")
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable(table)
        )
        
        print(f"✓ Started: {exchange}_{interval}")
        return stream
        
    except Exception as e:
        print(f"✗ Failed: {exchange}_{interval} - {e}")
        return None

# Start all streams
streams = []
for exchange in EXCHANGES:
    for interval in INTERVALS:
        stream = autoload_interval(exchange, interval)
        if stream:
            streams.append(stream)

print(f"\n✓ Successfully started {len(streams)}/{len(EXCHANGES) * len(INTERVALS)} streams")

# COMMAND ----------

# Cell 3: Time Series Sanity Checks + Status Validation
from pyspark.sql.functions import col, explode, size
from datetime import datetime, timedelta

# Configuration per interval
INTERVAL_CONFIG = {
    "15m": {"expected_gap_minutes": 15, "tolerance_minutes": 1, "max_delay_minutes": 30},
    "1h":  {"expected_gap_minutes": 60, "tolerance_minutes": 5, "max_delay_minutes": 90},
    "4h":  {"expected_gap_minutes": 240, "tolerance_minutes": 15, "max_delay_minutes": 300},
    "1d":  {"expected_gap_minutes": 1440, "tolerance_minutes": 60, "max_delay_minutes": 1500}
}

LAST_N_INGESTIONS = 5

def validate_ingestion_status():
    """
    Check recent ingestion statuses for failures.
    """
    try:
        # Check if table exists
        if not spark.catalog.tableExists("crypto.bronze.ingestion_status"):
            return True, "⚠️  Status table not found yet (first run - OK)"
        
        # Get last 20 status records
        status_df = spark.table("crypto.bronze.ingestion_status") \
            .orderBy(col("timestamp").desc()) \
            .limit(20)
        
        row_count = status_df.count()
        if row_count == 0:
            return False, "No status records found"
        
        # Check for failed symbols
        failed_symbols = status_df.filter(col("symbols_failed") > 0).collect()
        
        if failed_symbols:
            errors = []
            for row in failed_symbols:
                error_msg = f"{row.exchange}_{row.interval} at {row.timestamp}: {row.symbols_failed} symbols failed"
                if row.errors and len(row.errors) > 0:
                    error_msg += f" - Errors: {row.errors}"
                errors.append(error_msg)
            
            return False, "Ingestion failures:\n" + "\n".join(errors)
        
        # Check for errors array (even if symbols_failed = 0)
        has_errors = status_df.filter(size(col("errors")) > 0).collect()
        
        if has_errors:
            errors = []
            for row in has_errors:
                errors.append(f"{row.exchange}_{row.interval} at {row.timestamp}: {row.errors}")
            
            return False, "Errors in ingestion:\n" + "\n".join(errors)
        
        return True, f"✅ All {row_count} recent ingestions successful"
        
    except Exception as e:
        # If table doesn't exist yet, it's OK
        if "Table or view not found" in str(e):
            return True, "⚠️  Status table not found (first run?)"
        return False, f"Status check error: {str(e)}"


def get_expected_symbols(exchange: str, interval: str):
    """
    Dynamically discover expected symbols from recent data.
    """
    table = f"crypto.bronze.{exchange}_{interval}"
    
    try:
        df = spark.table(table)
        symbols_df = df.select(explode(col("symbols.symbol")).alias("symbol")).distinct()
        symbols = [row.symbol for row in symbols_df.collect()]
        
        if len(symbols) == 0:
            return None, "No symbols found"
        
        return set(symbols), None
        
    except Exception as e:
        return None, f"Error: {str(e)}"


def validate_timeseries_continuity(exchange: str, interval: str, expected_symbols: set):
    """
    Validates time series continuity for LAST N ingestions only.
    """
    table = f"crypto.bronze.{exchange}_{interval}"
    config = INTERVAL_CONFIG[interval]
    
    try:
        df = spark.table(table)
        
        row_count = df.count()
        if row_count == 0:
            return False, f"Table is EMPTY"
        
        # Get LAST N ingestion times
        all_ingestions = df.select(
            "file_date", "file_hour", "file_minute", "file_second"
        ).distinct() \
         .orderBy("file_date", "file_hour", "file_minute", "file_second")
        
        total_count = all_ingestions.count()
        ingestion_times = all_ingestions.tail(LAST_N_INGESTIONS)
        
        if len(ingestion_times) == 0:
            return False, f"No ingestion timestamps found"
        
        # Check freshness
        latest = ingestion_times[-1]
        latest_time = datetime.strptime(
            f"{latest.file_date} {latest.file_hour}:{latest.file_minute}:{latest.file_second}",
            "%Y-%m-%d %H:%M:%S"
        )
        time_diff = (datetime.now() - latest_time).total_seconds() / 60
        
        if time_diff > config["max_delay_minutes"]:
            return False, f"STALE: {int(time_diff)}min ago (max {config['max_delay_minutes']}min)"
        
        # Check duplicates
        for ing_time in ingestion_times:
            dup_count = df.filter(
                (col("file_date") == ing_time.file_date) &
                (col("file_hour") == ing_time.file_hour) &
                (col("file_minute") == ing_time.file_minute) &
                (col("file_second") == ing_time.file_second)
            ).count()
            
            if dup_count > 1:
                return False, f"DUPLICATE at {ing_time.file_date} {ing_time.file_hour}:{ing_time.file_minute}"
        
        # Check symbol completeness
        for ing_time in ingestion_times:
            symbols_at_time = df.filter(
                (col("file_date") == ing_time.file_date) &
                (col("file_hour") == ing_time.file_hour) &
                (col("file_minute") == ing_time.file_minute) &
                (col("file_second") == ing_time.file_second)
            ).select(explode(col("symbols.symbol")).alias("symbol")) \
             .distinct() \
             .collect()
            
            symbols_found = set([row.symbol for row in symbols_at_time])
            missing = expected_symbols - symbols_found
            
            if missing:
                timestamp = f"{ing_time.file_date} {ing_time.file_hour}:{ing_time.file_minute}"
                return False, f"Missing symbols at {timestamp}: {missing}"
        
        # Check time gaps
        if len(ingestion_times) >= 2:
            for i in range(1, len(ingestion_times)):
                prev_time = datetime.strptime(
                    f"{ingestion_times[i-1].file_date} {ingestion_times[i-1].file_hour}:{ingestion_times[i-1].file_minute}:{ingestion_times[i-1].file_second}",
                    "%Y-%m-%d %H:%M:%S"
                )
                curr_time = datetime.strptime(
                    f"{ingestion_times[i].file_date} {ingestion_times[i].file_hour}:{ingestion_times[i].file_minute}:{ingestion_times[i].file_second}",
                    "%Y-%m-%d %H:%M:%S"
                )
                
                gap_minutes = (curr_time - prev_time).total_seconds() / 60
                expected_gap = config["expected_gap_minutes"]
                tolerance = config["tolerance_minutes"]
                
                if gap_minutes < (expected_gap - tolerance) or gap_minutes > (expected_gap + tolerance):
                    return False, f"GAP: {gap_minutes:.0f}min between {prev_time.strftime('%d %H:%M')} and {curr_time.strftime('%d %H:%M')}"
        
        return True, f"✅ Last {len(ingestion_times)}/{total_count} OK, {len(expected_symbols)} symbols, latest {int(time_diff)}min ago"
        
    except Exception as e:
        return False, f"ERROR: {str(e)}"


def run_sanity_checks():
    """
    Run comprehensive sanity checks:
    1. Ingestion status validation (CRITICAL)
    2. Symbol discovery
    3. Time series continuity
    """
    print("=" * 80)
    print(f"TIME SERIES SANITY CHECKS (LAST {LAST_N_INGESTIONS} INGESTIONS)")
    print("=" * 80)
    print()
    
    all_valid = True
    results = []
    symbol_registry = {}
    
    # STEP 1: Check ingestion status (CRITICAL)
    print("📋 Checking ingestion status...")
    is_valid, message = validate_ingestion_status()
    status_symbol = "✅" if is_valid else "❌"
    print(f"{status_symbol} Ingestion Status: {message}")
    print()
    
    if not is_valid:
        all_valid = False
        # Don't continue if ingestion failed
        print("=" * 80)
        raise Exception(
            f"❌ INGESTION STATUS CHECK FAILED\n\n"
            f"{message}\n\n"
            f"CANNOT PROCEED - FIX AZURE FUNCTION"
        )
    
    # STEP 2: Discover symbols
    print("📊 Discovering symbols...")
    for exchange in EXCHANGES:
        for interval in INTERVALS:
            expected_symbols, error = get_expected_symbols(exchange, interval)
            
            if error:
                print(f"⚠️  {exchange}_{interval}: {error}")
                all_valid = False
                results.append((exchange, interval, False, error))
            else:
                symbol_registry[f"{exchange}_{interval}"] = expected_symbols
                print(f"   {exchange}_{interval}: {len(expected_symbols)} symbols")
    
    print()
    
    # STEP 3: Validate continuity
    print("🔍 Validating continuity...")
    for exchange in EXCHANGES:
        for interval in INTERVALS:
            key = f"{exchange}_{interval}"
            
            if key not in symbol_registry:
                continue
            
            expected_symbols = symbol_registry[key]
            is_valid, message = validate_timeseries_continuity(exchange, interval, expected_symbols)
            results.append((exchange, interval, is_valid, message))
            
            status = "✅" if is_valid else "❌"
            print(f"{status} {exchange}_{interval}: {message}")
            
            if not is_valid:
                all_valid = False
    
    print()
    print("=" * 80)
    
    if not all_valid:
        failed = [f"{r[0]}_{r[1]}" for r in results if not r[2]]
        error_details = "\n".join([f"  - {r[0]}_{r[1]}: {r[3]}" for r in results if not r[2]])
        
        raise Exception(
            f"❌ SANITY CHECKS FAILED\n\n"
            f"Failed ({len(failed)}):\n{error_details}\n\n"
            f"CANNOT PROCEED WITH SILVER PROCESSING"
        )
    
    print("✅ ALL CHECKS PASSED")
    print(f"   Tables: {len(results)} | Symbols: {sum(len(v) for v in symbol_registry.values())}")
    print("=" * 80)
    return True

# Run
#run_sanity_checks()