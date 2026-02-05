# Databricks notebook source
# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import Row
from datetime import datetime

# ---
# CONFIGURATION
# ---
EXCHANGES = ["binance", "bybit", "okx"]
INTERVALS = ["15m", "1h", "4h", "1d"]

CATALOG = "crypto"
BRONZE_SCHEMA = "bronze"

STRICT_MODE = False
WARNING_MODE = True

INTERVAL_SECONDS = {
    "15m": 15 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60
}

print("=" * 60)
print("BRONZE DATA QUALITY SANITY CHECK")
print("=" * 60)
print(f"Exchanges: {', '.join(EXCHANGES)}")
print(f"Intervals: {', '.join(INTERVALS)}")
print(f"Catalog: {CATALOG}.{BRONZE_SCHEMA}")
print(f"Strict Mode: {STRICT_MODE}")
print(f"Warning Mode: {WARNING_MODE}")
print("=" * 60)


# ---
# TIMESTAMP UTILITIES
# ---
def detect_timestamp_format(df, timestamp_col="timestamp"):
    """Auto-detect timestamp format: seconds, milliseconds, or ISO string"""
    sample = df.select(timestamp_col).first()
    if sample is None:
        return None
    
    ts = sample[timestamp_col]
    
    if isinstance(ts, str):
        return "iso_string"
    if ts > 1_000_000_000_000:
        return "milliseconds"
    if ts > 1_000_000_000:
        return "seconds"
    return None


def normalize_timestamp_to_unix(df, timestamp_col="timestamp", ts_format=None):
    """
    Add 'ts_unix' column with unix timestamp in seconds.
    Handles ISO strings with microseconds, milliseconds, and seconds.
    """
    if ts_format == "iso_string":
        # Truncate microseconds: '2025-12-24T16:45:00.009970' -> '2025-12-24T16:45:00'
        # Use substring to get first 19 chars (YYYY-MM-DDTHH:MM:SS)
        df = df.withColumn(
            "ts_unix",
            F.unix_timestamp(
                F.substring(F.col(timestamp_col), 1, 19),
                "yyyy-MM-dd'T'HH:mm:ss"
            )
        )
    elif ts_format == "milliseconds":
        df = df.withColumn("ts_unix", (F.col(timestamp_col) / 1000).cast("long"))
    elif ts_format == "seconds":
        df = df.withColumn("ts_unix", F.col(timestamp_col).cast("long"))
    else:
        df = df.withColumn("ts_unix", F.col(timestamp_col).cast("long"))
    
    return df


def unix_to_datetime_col(col_name):
    """Convert unix timestamp column to datetime"""
    return F.from_unixtime(F.col(col_name)).cast("timestamp")


# ---
# VALIDATION
# ---
def check_bronze_continuity(exchange, interval):
    """
    Check Bronze table for:
    1. Gaps in ingestion timestamps
    2. Missing symbols in files
    """
    table = f"{CATALOG}.{BRONZE_SCHEMA}.{exchange}_{interval}"
    
    try:
        df = spark.table(table)
    except Exception as e:
        return None, 0, None, None, str(e), {}
    
    ts_format = detect_timestamp_format(df, "timestamp")
    expected_interval_sec = INTERVAL_SECONDS[interval]
    
    # Normalize all timestamps to unix seconds
    df = normalize_timestamp_to_unix(df, "timestamp", ts_format)
    
    # Get distinct timestamps with metadata
    df_distinct = df.select(
        "ts_unix", "timestamp", "file_date", "file_hour", "file_minute"
    ).distinct()
    
    # Check 1: Time gaps
    window = Window.orderBy("ts_unix")
    
    df_time_gaps = (
        df_distinct
        .withColumn("prev_ts_unix", F.lag("ts_unix").over(window))
        .withColumn("time_diff", F.col("ts_unix") - F.col("prev_ts_unix"))
        .filter(F.col("time_diff") > expected_interval_sec * 1.5)
        .withColumn("datetime", unix_to_datetime_col("ts_unix"))
        .withColumn("prev_datetime", unix_to_datetime_col("prev_ts_unix"))
        .select(
            F.lit(exchange).alias("exchange"),
            F.lit(interval).alias("interval"),
            F.lit("ingestion_gap").alias("issue_type"),
            "prev_datetime",
            "datetime",
            "file_date",
            "file_hour",
            F.lit(ts_format).alias("timestamp_format")
        )
    )
    
    time_gap_count = df_time_gaps.count()
    
    # Check 2: Missing symbols
    total_symbols = (
        df.withColumn("symbol_data", F.explode("symbols"))
        .select("symbol_data.symbol")
        .distinct()
        .count()
    )
    
    df_symbol_gaps = (
        df
        .withColumn("symbols_count", F.size("symbols"))
        .filter(F.col("symbols_count") < total_symbols)
        .withColumn("datetime", unix_to_datetime_col("ts_unix"))
        .select(
            F.lit(exchange).alias("exchange"),
            F.lit(interval).alias("interval"),
            F.lit("missing_symbols").alias("issue_type"),
            "datetime",
            "file_date",
            "file_hour",
            "symbols_count",
            F.lit(total_symbols).alias("expected_symbols"),
            F.lit(ts_format).alias("timestamp_format")
        )
    )
    
    symbol_gap_count = df_symbol_gaps.count()
    
    # Stats
    file_count = df.count()
    date_range = df.agg(
        F.min(unix_to_datetime_col("ts_unix")).alias("first"),
        F.max(unix_to_datetime_col("ts_unix")).alias("last")
    ).first()
    
    detailed_stats = {
        "total_files": file_count,
        "first_file": str(date_range.first),
        "last_file": str(date_range.last),
        "expected_symbols": total_symbols,
        "time_gaps": time_gap_count,
        "symbol_gaps": symbol_gap_count
    }
    
    total_issues = time_gap_count + symbol_gap_count
    is_valid = total_issues == 0
    
    # Combine issues
    issues_df = None
    common_cols = ["exchange", "interval", "issue_type", "datetime", "file_date", "file_hour", "timestamp_format"]
    
    if time_gap_count > 0 and symbol_gap_count > 0:
        issues_df = df_time_gaps.select(common_cols).union(df_symbol_gaps.select(common_cols))
    elif time_gap_count > 0:
        issues_df = df_time_gaps.select(common_cols)
    elif symbol_gap_count > 0:
        issues_df = df_symbol_gaps.select(common_cols)
    
    return is_valid, total_issues, issues_df, ts_format, None, detailed_stats


def get_bronze_stats(exchange, interval):
    """Get basic statistics for Bronze table"""
    table = f"{CATALOG}.{BRONZE_SCHEMA}.{exchange}_{interval}"
    
    try:
        df = spark.table(table)
        ts_format = detect_timestamp_format(df, "timestamp")
        df = normalize_timestamp_to_unix(df, "timestamp", ts_format)
        
        stats = df.agg(
            F.count("*").alias("total_files"),
            F.min(unix_to_datetime_col("ts_unix")).alias("first_file"),
            F.max(unix_to_datetime_col("ts_unix")).alias("last_file"),
            F.countDistinct("file_date").alias("unique_dates")
        ).first()
        
        symbols_stats = (
            df
            .withColumn("symbols_count", F.size("symbols"))
            .agg(
                F.min("symbols_count").alias("min_symbols"),
                F.max("symbols_count").alias("max_symbols"),
                F.avg("symbols_count").alias("avg_symbols")
            )
            .first()
        )
        
        return {
            "total_files": stats.total_files,
            "first_file": stats.first_file,
            "last_file": stats.last_file,
            "unique_dates": stats.unique_dates,
            "min_symbols": symbols_stats.min_symbols,
            "max_symbols": symbols_stats.max_symbols,
            "avg_symbols": round(symbols_stats.avg_symbols, 1),
            "timestamp_format": ts_format
        }
    except Exception:
        return None


# ---
# RUN VALIDATION
# ---
print("\n" + "=" * 60)
print("RUNNING BRONZE VALIDATION")
print("=" * 60)

validation_results = []
all_issues = []
timestamp_formats = {}
detailed_logs = []

for exchange in EXCHANGES:
    for interval in INTERVALS:
        print(f"\n{exchange}_{interval}:")
        
        is_valid, issue_count, issues_df, ts_format, error, stats = check_bronze_continuity(exchange, interval)
        
        if error:
            status = "ERROR"
            print(f"  ✗ {error}")
            validation_results.append({
                "exchange": exchange,
                "interval": interval,
                "status": status,
                "issue_count": 0,
                "timestamp_format": None
            })
            detailed_logs.append({
                "check_timestamp": datetime.now(),
                "exchange": exchange,
                "interval": interval,
                "status": status,
                "issue_count": 0,
                "time_gaps": 0,
                "symbol_gaps": 0,
                "total_files": 0,
                "first_file": None,
                "last_file": None,
                "expected_symbols": 0,
                "timestamp_format": None,
                "error_message": error
            })
        elif is_valid is not None:
            status = "OK" if is_valid else "WARNING"
            reason = "Valid" if is_valid else f"{stats['time_gaps']} time gaps, {stats['symbol_gaps']} symbol gaps"
            print(f"  {'✓' if is_valid else '⚠'} {reason} [{ts_format}]")
            
            validation_results.append({
                "exchange": exchange,
                "interval": interval,
                "status": status,
                "issue_count": issue_count,
                "timestamp_format": ts_format
            })
            timestamp_formats[f"{exchange}_{interval}"] = ts_format
            detailed_logs.append({
                "check_timestamp": datetime.now(),
                "exchange": exchange,
                "interval": interval,
                "status": status,
                "issue_count": issue_count,
                "time_gaps": stats["time_gaps"],
                "symbol_gaps": stats["symbol_gaps"],
                "total_files": stats["total_files"],
                "first_file": stats["first_file"],
                "last_file": stats["last_file"],
                "expected_symbols": stats["expected_symbols"],
                "timestamp_format": ts_format,
                "error_message": None if is_valid else reason
            })
            
            if issue_count > 0 and issues_df is not None:
                all_issues.append(issues_df)


# ---
# REPORTS
# ---
print("\n" + "=" * 60)
print("TIMESTAMP FORMATS")
print("=" * 60)
for key, fmt in timestamp_formats.items():
    print(f"{key}: {fmt}")

print("\n" + "=" * 60)
print("VALIDATION SUMMARY")
print("=" * 60)
results_df = spark.createDataFrame([Row(**r) for r in validation_results])
display(results_df.orderBy("exchange", "interval"))

if all_issues:
    print("\n" + "=" * 60)
    print("DETECTED ISSUES")
    print("=" * 60)
    issues_union = all_issues[0]
    for issue_df in all_issues[1:]:
        issues_union = issues_union.union(issue_df)
    display(issues_union.orderBy("exchange", "interval", "issue_type", "datetime"))
else:
    print("\n✓ NO ISSUES - ALL BRONZE DATA VALID")

print("\n" + "=" * 60)
print("BRONZE STATISTICS")
print("=" * 60)
for exchange in EXCHANGES:
    for interval in INTERVALS:
        stats = get_bronze_stats(exchange, interval)
        if stats:
            print(f"\n{exchange}_{interval} [{stats['timestamp_format']}]:")
            print(f"  Files: {stats['total_files']} | Dates: {stats['unique_dates']}")
            print(f"  Range: {stats['first_file']} → {stats['last_file']}")
            print(f"  Symbols per file: {stats['min_symbols']}-{stats['max_symbols']} (avg: {stats['avg_symbols']})")


# ---
# SAVE LOGS
# ---
print("\n" + "=" * 60)
print("SAVING DETAILED LOGS")
print("=" * 60)

detailed_log_df = spark.createDataFrame([Row(**log) for log in detailed_logs])
log_table = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_log"

try:
    detailed_log_df.write.format("delta").mode("append").saveAsTable(log_table)
    print(f"✓ Appended {len(detailed_logs)} records to {log_table}")
except Exception:
    detailed_log_df.write.format("delta").mode("overwrite").saveAsTable(log_table)
    print(f"✓ Created {log_table} with {len(detailed_logs)} records")

print(f"\n=== Latest Logs from {log_table} ===")
spark.sql(f"""
    SELECT 
        check_timestamp,
        exchange,
        interval,
        status,
        issue_count,
        time_gaps,
        symbol_gaps,
        total_files,
        error_message
    FROM {log_table}
    ORDER BY check_timestamp DESC
    LIMIT 20
""").show(truncate=False)


# ---
# HANDLE FAILURES
# ---
total_issues = sum(r["issue_count"] for r in validation_results)
failed_checks = sum(1 for r in validation_results if r["status"] in ["WARNING", "ERROR"])

if total_issues > 0:
    if WARNING_MODE:
        print(f"\n⚠ WARNING: {total_issues} issues detected in Bronze layer")
        print(f"  {failed_checks} tables affected")
    
    if STRICT_MODE:
        error_msg = f"SANITY CHECK FAILED: {total_issues} issues detected across {failed_checks} tables"
        print(f"\n✗ {error_msg}")
        raise Exception(error_msg)

print("\n" + "=" * 60)
print("BRONZE SANITY CHECK COMPLETE")
print("=" * 60)
print(f"Total checks: {len(validation_results)}")
print(f"OK: {sum(1 for r in validation_results if r['status'] == 'OK')}")
print(f"WARNING: {sum(1 for r in validation_results if r['status'] == 'WARNING')}")
print(f"ERROR: {sum(1 for r in validation_results if r['status'] == 'ERROR')}")