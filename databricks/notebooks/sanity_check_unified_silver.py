# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import Row
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# COMMAND ----------
# ---
# CONFIGURATION
# ---

CATALOG = "crypto"
SILVER_SCHEMA = "silver"
INTERVALS = ["15m", "1h", "4h", "1d"]
DATA_TYPES = ["klines", "oi", "ticker", "orderbook", "funding", "long_short_ratio", "taker_ratio"]
EXCHANGES = ["binance", "bybit", "okx"]

# Pipeline behavior
STRICT_MODE = False
WARNING_MODE = True
FAIL_ON_DUPLICATES = True
FAIL_ON_GAPS = False
FAIL_ON_NULLS = True
FAIL_ON_STALE_DATA = False

# Thresholds
MAX_DATA_AGE_HOURS = 24
MAX_EXCHANGE_IMBALANCE = 0.2
GAP_TOLERANCE_MULTIPLIER = 1.5

INTERVAL_MINUTES = {"15m": 15, "1h": 60, "4h": 240, "1d": 1440}

# Critical columns per data type
CRITICAL_COLUMNS = {
    "klines": ["open", "high", "low", "close", "volume"],
    "ticker": ["last_price", "volume_24h"],
    "oi": ["open_interest"],
    "orderbook": ["best_bid", "best_ask"],
    "funding": ["funding_rate"],
    "long_short_ratio": ["long_short_ratio"],
    "taker_ratio": ["buy_sell_ratio"]
}
BASE_CRITICAL_COLUMNS = ["symbol", "timestamp", "datetime", "exchange"]

print("=" * 80)
print("UNIFIED TABLES SANITY CHECK")
print("=" * 80)
print(f"Catalog: {CATALOG}.{SILVER_SCHEMA}")
print(f"Data Types: {', '.join(DATA_TYPES)}")
print(f"Intervals: {', '.join(INTERVALS)}")
print(f"Strict: {STRICT_MODE} | Duplicates: {FAIL_ON_DUPLICATES} | Gaps: {FAIL_ON_GAPS} | Nulls: {FAIL_ON_NULLS}")
print("=" * 80)


# COMMAND ----------
# ---
# VALIDATION TRACKING
# ---

class ValidationTracker:
    def __init__(self):
        self.results: List[Dict] = []
        self.critical_errors: List[Dict] = []
        self.warnings: List[Dict] = []
    
    def log(self, check: str, table: str, status: str, message: str, is_critical: bool = False):
        result = {
            "check": check,
            "table": table,
            "status": status,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        self.results.append(result)
        
        if status == "ERROR":
            (self.critical_errors if is_critical else self.warnings).append(result)
    
    def summary(self) -> Dict[str, int]:
        return {
            "total": len(self.results),
            "ok": sum(1 for r in self.results if r["status"] == "OK"),
            "warning": sum(1 for r in self.results if r["status"] == "WARNING"),
            "error": sum(1 for r in self.results if r["status"] == "ERROR")
        }

tracker = ValidationTracker()


# COMMAND ----------
# ---
# TABLE CACHE - Single pass for basic stats
# ---

class TableCache:
    """Cache table metadata to avoid repeated scans"""
    
    def __init__(self):
        self.cache: Dict[str, Dict] = {}
    
    def get_or_load(self, table_name: str) -> Optional[Dict]:
        if table_name in self.cache:
            return self.cache[table_name]
        
        if not spark.catalog.tableExists(table_name):
            self.cache[table_name] = None
            return None
        
        df = spark.table(table_name)
        
        # Single aggregation for all basic stats
        stats = df.agg(
            F.count("*").alias("row_count"),
            F.min("datetime").alias("min_datetime"),
            F.max("datetime").alias("max_datetime"),
            F.countDistinct("symbol").alias("symbol_count"),
            F.countDistinct("exchange").alias("exchange_count")
        ).first()
        
        self.cache[table_name] = {
            "exists": True,
            "row_count": stats["row_count"],
            "min_datetime": stats["min_datetime"],
            "max_datetime": stats["max_datetime"],
            "symbol_count": stats["symbol_count"],
            "exchange_count": stats["exchange_count"],
            "df": df
        }
        return self.cache[table_name]
    
    def get_df(self, table_name: str):
        info = self.get_or_load(table_name)
        return info["df"] if info else None

cache = TableCache()


# COMMAND ----------
# ---
# HELPER FUNCTIONS
# ---

def get_table_name(data_type: str, interval: str) -> str:
    return f"{CATALOG}.{SILVER_SCHEMA}.unified_{data_type}_{interval}"


def get_all_tables() -> List[Tuple[str, str, str]]:
    """Return list of (data_type, interval, table_name)"""
    return [
        (dt, iv, get_table_name(dt, iv))
        for dt in DATA_TYPES
        for iv in INTERVALS
    ]


# COMMAND ----------
# ---
# CHECK 1: TABLE EXISTENCE & ROW COUNTS (Combined)
# ---

print("\n" + "=" * 80)
print("CHECK 1: TABLE EXISTENCE & ROW COUNTS")
print("=" * 80)

missing_tables = []
empty_tables = []

for data_type, interval, table_name in get_all_tables():
    info = cache.get_or_load(table_name)
    
    if info is None:
        print(f"  ✗ {table_name}: MISSING")
        missing_tables.append(table_name)
        tracker.log("TABLE_EXISTENCE", table_name, "ERROR", "Table missing", is_critical=True)
    elif info["row_count"] == 0:
        print(f"  ⚠ {table_name}: EMPTY")
        empty_tables.append(table_name)
        tracker.log("ROW_COUNT", table_name, "ERROR", "Empty table", is_critical=False)
    else:
        print(f"  ✓ {table_name}: {info['row_count']:,} rows")
        tracker.log("TABLE_EXISTENCE", table_name, "OK", f"{info['row_count']:,} rows")

if missing_tables and STRICT_MODE:
    raise Exception(f"Missing tables: {', '.join(missing_tables)}")


# COMMAND ----------
# ---
# CHECK 2: DATA FRESHNESS
# ---

print("\n" + "=" * 80)
print("CHECK 2: DATA FRESHNESS")
print("=" * 80)

stale_tables = []
now = datetime.now()

for data_type, interval, table_name in get_all_tables():
    info = cache.get_or_load(table_name)
    
    if info is None or info["row_count"] == 0:
        continue
    
    latest = info["max_datetime"]
    if latest is None:
        continue
    
    # Safe datetime handling
    try:
        if isinstance(latest, str):
            latest_dt = datetime.fromisoformat(latest.replace(" ", "T"))
        else:
            latest_dt = latest
        age_hours = (now - latest_dt).total_seconds() / 3600
    except Exception:
        print(f"  ✗ {table_name}: Invalid datetime format")
        tracker.log("DATA_FRESHNESS", table_name, "ERROR", "Invalid datetime", is_critical=False)
        continue
    
    if age_hours < MAX_DATA_AGE_HOURS:
        status, log_status = "✓", "OK"
    elif age_hours < MAX_DATA_AGE_HOURS * 2:
        status, log_status = "⚠", "WARNING"
    else:
        status, log_status = "✗", "ERROR"
        stale_tables.append(table_name)
    
    print(f"  {status} {table_name}: {age_hours:.1f}h old (latest: {latest})")
    tracker.log("DATA_FRESHNESS", table_name, log_status, f"{age_hours:.1f}h old", is_critical=FAIL_ON_STALE_DATA)


# COMMAND ----------
# ---
# CHECK 3: TIMESTAMP VALIDATION
# ---

print("\n" + "=" * 80)
print("CHECK 3: TIMESTAMP VALIDATION")
print("=" * 80)

invalid_ts_tables = []

for data_type, interval, table_name in get_all_tables():
    df = cache.get_df(table_name)
    if df is None:
        continue
    
    info = cache.get_or_load(table_name)
    if info["row_count"] == 0:
        continue
    
    # Single pass for all timestamp checks
    ts_stats = df.agg(
        F.sum(F.when(F.col("datetime") > F.current_timestamp(), 1).otherwise(0)).alias("future_count"),
        F.sum(F.when(F.year("datetime") < 2020, 1).otherwise(0)).alias("old_count"),
        F.sum(F.when(F.length(F.col("timestamp").cast("string")) != 13, 1).otherwise(0)).alias("invalid_ts_len")
    ).first()
    
    issues = []
    if ts_stats["future_count"] > 0:
        issues.append(f"{ts_stats['future_count']} future")
    if ts_stats["old_count"] > 0:
        issues.append(f"{ts_stats['old_count']} pre-2020")
    if ts_stats["invalid_ts_len"] > 0:
        issues.append(f"{ts_stats['invalid_ts_len']} invalid ts length")
    
    if issues:
        print(f"  ✗ {table_name}: {', '.join(issues)}")
        invalid_ts_tables.append(table_name)
        tracker.log("TIMESTAMP", table_name, "ERROR", "; ".join(issues), is_critical=False)
    else:
        print(f"  ✓ {table_name}: OK")
        tracker.log("TIMESTAMP", table_name, "OK", "Valid")


# COMMAND ----------
# ---
# CHECK 4: TIME SERIES GAPS (Spark-native, no collect)
# ---

print("\n" + "=" * 80)
print("CHECK 4: TIME SERIES GAPS (15m only)")
print("=" * 80)

tables_with_gaps = []
expected_gap_ms = INTERVAL_MINUTES["15m"] * 60 * 1000
max_allowed_gap_ms = expected_gap_ms * GAP_TOLERANCE_MULTIPLIER

for data_type in DATA_TYPES:
    table_name = get_table_name(data_type, "15m")
    df = cache.get_df(table_name)
    
    if df is None:
        continue
    
    info = cache.get_or_load(table_name)
    if info["row_count"] == 0:
        continue
    
    print(f"\n{table_name}:")
    
    # Window per symbol+exchange partition
    window = Window.partitionBy("symbol", "exchange").orderBy("timestamp")
    
    # Detect gaps using lag
    gaps_df = (
        df
        .withColumn("prev_ts", F.lag("timestamp").over(window))
        .withColumn("gap_ms", F.col("timestamp") - F.col("prev_ts"))
        .filter(F.col("gap_ms") > max_allowed_gap_ms)
        .groupBy("symbol", "exchange")
        .agg(
            F.count("*").alias("gap_count"),
            F.max("gap_ms").alias("max_gap_ms"),
            F.min("datetime").alias("first_gap_at")
        )
    )
    
    gaps = gaps_df.collect()
    
    if gaps:
        for row in gaps:
            gap_minutes = row["max_gap_ms"] / 1000 / 60
            print(f"  ✗ {row['exchange']} {row['symbol']}: {row['gap_count']} gaps (max: {gap_minutes:.0f}min)")
            tables_with_gaps.append(f"{table_name}:{row['exchange']}:{row['symbol']}")
        tracker.log("TIME_SERIES", table_name, "WARNING", f"{len(gaps)} symbol/exchange with gaps", is_critical=FAIL_ON_GAPS)
    else:
        # Get record counts per symbol/exchange
        counts = df.groupBy("exchange", "symbol").count().collect()
        for row in counts:
            print(f"  ✓ {row['exchange']} {row['symbol']}: {row['count']} records, no gaps")
        tracker.log("TIME_SERIES", table_name, "OK", "Continuous")


# COMMAND ----------
# ---
# CHECK 5: DUPLICATES
# ---

print("\n" + "=" * 80)
print("CHECK 5: DUPLICATES")
print("=" * 80)

tables_with_duplicates = []

for data_type, interval, table_name in get_all_tables():
    df = cache.get_df(table_name)
    if df is None:
        continue
    
    info = cache.get_or_load(table_name)
    if info["row_count"] == 0:
        continue
    
    # Count duplicates
    dup_count = (
        df
        .groupBy("symbol", "timestamp", "exchange")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    
    if dup_count > 0:
        print(f"  ✗ {table_name}: {dup_count} duplicate groups")
        tables_with_duplicates.append(table_name)
        tracker.log("DUPLICATES", table_name, "ERROR", f"{dup_count} duplicates", is_critical=FAIL_ON_DUPLICATES)
    else:
        print(f"  ✓ {table_name}: No duplicates")
        tracker.log("DUPLICATES", table_name, "OK", "No duplicates")


# COMMAND ----------
# ---
# CHECK 6: NULL VALUES
# ---

print("\n" + "=" * 80)
print("CHECK 6: NULL VALUES")
print("=" * 80)

tables_with_nulls = []

for data_type, interval, table_name in get_all_tables():
    df = cache.get_df(table_name)
    if df is None:
        continue
    
    info = cache.get_or_load(table_name)
    if info["row_count"] == 0:
        continue
    
    # Get critical columns for this data type
    critical_cols = BASE_CRITICAL_COLUMNS + CRITICAL_COLUMNS.get(data_type, [])
    existing_cols = [c for c in critical_cols if c in df.columns]
    
    # Single pass null check
    null_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in existing_cols]
    null_counts = df.agg(*null_exprs).first().asDict()
    
    null_issues = {col: cnt for col, cnt in null_counts.items() if cnt > 0}
    
    if null_issues:
        details = ", ".join(f"{col}:{cnt}" for col, cnt in null_issues.items())
        print(f"  ✗ {table_name}: {details}")
        tables_with_nulls.append(table_name)
        tracker.log("NULL_VALUES", table_name, "ERROR", details, is_critical=FAIL_ON_NULLS)
    else:
        print(f"  ✓ {table_name}: No NULLs")
        tracker.log("NULL_VALUES", table_name, "OK", "No NULLs")


# COMMAND ----------
# ---
# CHECK 7: EXCHANGE BALANCE
# ---

print("\n" + "=" * 80)
print("CHECK 7: EXCHANGE BALANCE")
print("=" * 80)

imbalanced_tables = []

for data_type in DATA_TYPES:
    for interval in ["15m", "1d"]:
        table_name = get_table_name(data_type, interval)
        df = cache.get_df(table_name)
        
        if df is None:
            continue
        
        info = cache.get_or_load(table_name)
        if info["row_count"] == 0:
            continue
        
        exchange_stats = (
            df
            .groupBy("exchange")
            .agg(
                F.count("*").alias("records"),
                F.countDistinct("symbol").alias("symbols")
            )
            .collect()
        )
        
        print(f"\n{table_name}:")
        for row in exchange_stats:
            print(f"  {row['exchange']}: {row['records']:,} records, {row['symbols']} symbols")
        
        if len(exchange_stats) == 2:
            ratio = exchange_stats[0]["records"] / exchange_stats[1]["records"]
            imbalance = abs(1 - ratio)
            
            if imbalance > MAX_EXCHANGE_IMBALANCE:
                print(f"  ⚠ Imbalance: {imbalance*100:.1f}%")
                imbalanced_tables.append(table_name)
                tracker.log("EXCHANGE_BALANCE", table_name, "WARNING", f"{imbalance*100:.1f}%", is_critical=False)
            else:
                print(f"  ✓ Balanced: {imbalance*100:.1f}%")
                tracker.log("EXCHANGE_BALANCE", table_name, "OK", f"{imbalance*100:.1f}%")


# COMMAND ----------
# ---
# SUMMARY
# ---

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

summary = tracker.summary()
print(f"\nTotal: {summary['total']} | ✓ OK: {summary['ok']} | ⚠ WARNING: {summary['warning']} | ✗ ERROR: {summary['error']}")

if tracker.critical_errors:
    print(f"\nCRITICAL ERRORS ({len(tracker.critical_errors)}):")
    for err in tracker.critical_errors:
        print(f"  ✗ [{err['check']}] {err['table']}: {err['message']}")

if tracker.warnings and WARNING_MODE:
    print(f"\nWARNINGS ({len(tracker.warnings)}):")
    for warn in tracker.warnings[:10]:
        print(f"  ⚠ [{warn['check']}] {warn['table']}: {warn['message']}")
    if len(tracker.warnings) > 10:
        print(f"  ... +{len(tracker.warnings) - 10} more")


# COMMAND ----------
# ---
# FINAL DECISION
# ---

print("\n" + "=" * 80)
print("FINAL DECISION")
print("=" * 80)

failures = []

if tracker.critical_errors:
    failures.append(f"{len(tracker.critical_errors)} critical errors")
if tables_with_duplicates and FAIL_ON_DUPLICATES:
    failures.append(f"duplicates in {len(tables_with_duplicates)} tables")
if tables_with_nulls and FAIL_ON_NULLS:
    failures.append(f"NULLs in {len(tables_with_nulls)} tables")
if tables_with_gaps and FAIL_ON_GAPS:
    failures.append(f"gaps in {len(tables_with_gaps)} combinations")
if stale_tables and FAIL_ON_STALE_DATA:
    failures.append(f"stale data in {len(stale_tables)} tables")

if failures and STRICT_MODE:
    error_msg = f"PIPELINE FAILED: {'; '.join(failures)}"
    print(f"✗ {error_msg}")
    raise Exception(error_msg)
elif failures and WARNING_MODE:
    print(f"⚠ ISSUES: {'; '.join(failures)}")
    print("  Continuing (WARNING_MODE=True)")
else:
    print("✓ ALL CHECKS PASSED")

print("=" * 80)