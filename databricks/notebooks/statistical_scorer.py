# Databricks notebook source
# ---
# STATISTICAL SCORER
# Replaces AI validator with data-driven scoring model
# Based on historical outcomes, not LLM reasoning
# ---

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timezone, timedelta
import json

CATALOG = "crypto"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
TRADING_SCHEMA = "trading"

# ---
# SCORING WEIGHTS (calibrated from backtest outcomes)
# ---

# These weights should be updated based on actual trading results
SCORING_WEIGHTS = {
    # Zone quality (0-40 points)
    "zone_strength": {
        "threshold_high": 85,   # +15 points
        "threshold_mid": 75,    # +10 points
        "threshold_low": 65,    # +5 points
    },
    "confirmed_methods": {
        3: 15,  # VP + OI + OB all confirm
        2: 10,
        1: 5,
    },

    # BOS confirmation (0-20 points)
    "bos_confirmed": 15,
    "bos_recency_hours": {
        4: 5,    # BOS within 4 hours = +5
        12: 3,   # BOS within 12 hours = +3
        24: 1,   # BOS within 24 hours = +1
    },

    # Multi-TF confluence (0-20 points)
    "confluence_score": {
        10: 20,  # 1d+4h+1h+15m
        7: 15,   # 1d+4h or 4h+1h+15m
        5: 10,   # 4h+15m or 1h+4h
        3: 5,    # minimal confluence
    },

    # Sentiment alignment (0-20 points)
    "crowding_alignment": 10,      # Crowded opposite to signal direction
    "smart_money_alignment": 10,   # Smart money agrees with signal

    # Zone behavior (historical) (bonus/penalty)
    "bounce_rate_high": 5,     # Zone bounced >60% of touches
    "break_rate_high": -10,    # Zone broken >50% of times
    "is_mirror_zone": 5,       # Tested from both sides

    # Entry quality (0-10 points)
    "distance_to_poc": {
        0.5: 10,   # Within 0.5% of POC
        1.0: 7,    # Within 1%
        1.5: 3,    # Within 1.5%
        2.0: 0,    # 2% = no bonus
    },
}

# Thresholds
SCORE_THRESHOLD_EXECUTE = 70    # Score >= 70 = auto-execute
SCORE_THRESHOLD_WATCH = 50      # Score 50-70 = watchlist
MIN_RR_RATIO = 2.0              # Minimum risk/reward

print("=" * 60)
print("STATISTICAL SCORER")
print("=" * 60)
print(f"Execute threshold: {SCORE_THRESHOLD_EXECUTE}")
print(f"Watch threshold: {SCORE_THRESHOLD_WATCH}")
print(f"Min R:R ratio: {MIN_RR_RATIO}")
print("=" * 60)


# ---
# SCORING FUNCTIONS
# ---

def score_zone_quality(signal: dict) -> tuple:
    """Score zone quality (0-40 points)"""
    score = 0
    breakdown = []

    # Zone strength
    strength = signal.get("zone_strength") or 0
    if strength >= SCORING_WEIGHTS["zone_strength"]["threshold_high"]:
        score += 15
        breakdown.append(f"zone_strength_high:+15")
    elif strength >= SCORING_WEIGHTS["zone_strength"]["threshold_mid"]:
        score += 10
        breakdown.append(f"zone_strength_mid:+10")
    elif strength >= SCORING_WEIGHTS["zone_strength"]["threshold_low"]:
        score += 5
        breakdown.append(f"zone_strength_low:+5")

    # Confirmed methods
    methods = signal.get("confirmed_methods") or 0
    method_score = SCORING_WEIGHTS["confirmed_methods"].get(methods, 0)
    score += method_score
    if method_score > 0:
        breakdown.append(f"confirmed_methods_{methods}:+{method_score}")

    return score, breakdown


def score_bos(signal: dict) -> tuple:
    """Score BOS confirmation (0-20 points)"""
    score = 0
    breakdown = []

    if signal.get("bos_type"):
        score += SCORING_WEIGHTS["bos_confirmed"]
        breakdown.append(f"bos_confirmed:+{SCORING_WEIGHTS['bos_confirmed']}")

        # BOS recency
        bos_ts = signal.get("bos_timestamp")
        if bos_ts:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            hours_ago = (now_ms - bos_ts) / (1000 * 60 * 60)

            for threshold, points in sorted(SCORING_WEIGHTS["bos_recency_hours"].items()):
                if hours_ago <= threshold:
                    score += points
                    breakdown.append(f"bos_recent_{threshold}h:+{points}")
                    break

    return score, breakdown


def score_confluence(signal: dict) -> tuple:
    """Score multi-TF confluence (0-20 points)"""
    score = 0
    breakdown = []

    conf_score = signal.get("confluence_score") or 0

    for threshold, points in sorted(SCORING_WEIGHTS["confluence_score"].items(), reverse=True):
        if conf_score >= threshold:
            score += points
            breakdown.append(f"confluence_{conf_score}:+{points}")
            break

    return score, breakdown


def score_sentiment(signal: dict) -> tuple:
    """Score sentiment alignment (0-20 points)"""
    score = 0
    breakdown = []

    signal_type = signal.get("signal_type")
    crowding = signal.get("crowding_direction")
    smart_money = signal.get("divergence_direction") if "divergence_direction" in signal else None

    # Crowding alignment: want opposite crowding
    if signal_type == "long" and crowding == "short_crowded":
        score += SCORING_WEIGHTS["crowding_alignment"]
        breakdown.append(f"short_crowded_long:+{SCORING_WEIGHTS['crowding_alignment']}")
    elif signal_type == "short" and crowding == "long_crowded":
        score += SCORING_WEIGHTS["crowding_alignment"]
        breakdown.append(f"long_crowded_short:+{SCORING_WEIGHTS['crowding_alignment']}")

    # Smart money alignment
    if smart_money:
        if signal_type == "long" and smart_money == "smart_money_long":
            score += SCORING_WEIGHTS["smart_money_alignment"]
            breakdown.append(f"smart_money_long:+{SCORING_WEIGHTS['smart_money_alignment']}")
        elif signal_type == "short" and smart_money == "smart_money_short":
            score += SCORING_WEIGHTS["smart_money_alignment"]
            breakdown.append(f"smart_money_short:+{SCORING_WEIGHTS['smart_money_alignment']}")

    return score, breakdown


def score_zone_behavior(signal: dict) -> tuple:
    """Score zone historical behavior (bonus/penalty)"""
    score = 0
    breakdown = []

    bounce_count = signal.get("bounce_count") or 0
    break_count = signal.get("break_count") or 0
    total_reactions = bounce_count + break_count

    if total_reactions > 0:
        bounce_rate = bounce_count / total_reactions

        if bounce_rate >= 0.6:
            score += SCORING_WEIGHTS["bounce_rate_high"]
            breakdown.append(f"high_bounce_rate:+{SCORING_WEIGHTS['bounce_rate_high']}")
        elif bounce_rate <= 0.4:
            score += SCORING_WEIGHTS["break_rate_high"]
            breakdown.append(f"high_break_rate:{SCORING_WEIGHTS['break_rate_high']}")

    # Mirror zone bonus
    if signal.get("is_mirror"):
        score += SCORING_WEIGHTS["is_mirror_zone"]
        breakdown.append(f"mirror_zone:+{SCORING_WEIGHTS['is_mirror_zone']}")

    return score, breakdown


def score_entry_quality(signal: dict) -> tuple:
    """Score entry quality based on distance to POC (0-10 points)"""
    score = 0
    breakdown = []

    current_price = signal.get("current_price") or 0
    poc = signal.get("poc_price") or 0

    if current_price > 0 and poc > 0:
        distance_pct = abs((current_price - poc) / current_price) * 100

        for threshold, points in sorted(SCORING_WEIGHTS["distance_to_poc"].items()):
            if distance_pct <= threshold:
                score += points
                breakdown.append(f"distance_{threshold}pct:+{points}")
                break

    return score, breakdown


def calculate_levels(signal: dict, atr: float = None) -> dict:
    """Calculate entry, SL, TP levels"""
    signal_type = signal.get("signal_type")
    poc = signal.get("poc_price")
    zone_low = signal.get("zone_low")
    zone_high = signal.get("zone_high")
    current_price = signal.get("current_price")

    if not all([poc, zone_low, zone_high, current_price]):
        return None

    # Use zone boundaries for SL, not ATR
    zone_height = zone_high - zone_low

    if signal_type == "long":
        # Entry: limit at POC or slightly below
        entry = poc
        # SL: below zone low with small buffer
        stop_loss = zone_low - (zone_height * 0.2)
        risk = entry - stop_loss

        # TPs based on R:R
        take_profit_1 = entry + (risk * 1.5)
        take_profit_2 = entry + (risk * 3.0)

    else:  # short
        entry = poc
        stop_loss = zone_high + (zone_height * 0.2)
        risk = stop_loss - entry

        take_profit_1 = entry - (risk * 1.5)
        take_profit_2 = entry - (risk * 3.0)

    rr_ratio = (abs(take_profit_2 - entry) / abs(entry - stop_loss)) if abs(entry - stop_loss) > 0 else 0

    return {
        "entry_price": round(entry, 8),
        "stop_loss": round(stop_loss, 8),
        "take_profit_1": round(take_profit_1, 8),
        "take_profit_2": round(take_profit_2, 8),
        "risk_reward_ratio": round(rr_ratio, 2)
    }


def score_signal(signal: dict) -> dict:
    """Calculate total score for a signal"""

    total_score = 0
    all_breakdown = []

    # Zone quality (0-40)
    zone_score, zone_breakdown = score_zone_quality(signal)
    total_score += zone_score
    all_breakdown.extend(zone_breakdown)

    # BOS (0-20)
    bos_score, bos_breakdown = score_bos(signal)
    total_score += bos_score
    all_breakdown.extend(bos_breakdown)

    # Confluence (0-20)
    conf_score, conf_breakdown = score_confluence(signal)
    total_score += conf_score
    all_breakdown.extend(conf_breakdown)

    # Sentiment (0-20)
    sent_score, sent_breakdown = score_sentiment(signal)
    total_score += sent_score
    all_breakdown.extend(sent_breakdown)

    # Zone behavior (bonus/penalty)
    behavior_score, behavior_breakdown = score_zone_behavior(signal)
    total_score += behavior_score
    all_breakdown.extend(behavior_breakdown)

    # Entry quality (0-10)
    entry_score, entry_breakdown = score_entry_quality(signal)
    total_score += entry_score
    all_breakdown.extend(entry_breakdown)

    # Cap at 100
    total_score = min(100, max(0, total_score))

    # Calculate levels
    levels = calculate_levels(signal)

    # Determine status
    if total_score >= SCORE_THRESHOLD_EXECUTE and levels and levels["risk_reward_ratio"] >= MIN_RR_RATIO:
        status = "execute"
    elif total_score >= SCORE_THRESHOLD_WATCH:
        status = "watch"
    else:
        status = "skip"

    return {
        "score": total_score,
        "status": status,
        "breakdown": "|".join(all_breakdown),
        "levels": levels
    }


# ---
# MAIN PROCESSING
# ---

def process_pending_signals():
    """Score all pending signals"""

    signals_table = f"{CATALOG}.{GOLD_SCHEMA}.signals_all"

    # Get pending signals
    pending_df = spark.table(signals_table).filter(
        F.col("ai_status") == "pending"
    )

    pending_signals = pending_df.collect()
    print(f"\nPending signals: {len(pending_signals)}")

    if not pending_signals:
        return []

    results = []

    for signal in pending_signals:
        signal_dict = signal.asDict()
        signal_id = signal_dict["signal_id"]
        symbol = signal_dict["symbol"]

        # Load additional data if needed (zone behavior)
        # For now, use what's in the signal

        # Score the signal
        score_result = score_signal(signal_dict)

        print(f"[{signal_id[:8]}] {symbol} {signal_dict['signal_type']} -> "
              f"Score: {score_result['score']} ({score_result['status']})")

        if score_result["status"] != "skip":
            print(f"    Breakdown: {score_result['breakdown'][:80]}...")

        # Prepare update
        levels = score_result["levels"] or {}

        results.append({
            "signal_id": signal_id,
            "ai_status": "confirmed" if score_result["status"] == "execute" else "rejected",
            "ai_confidence": float(score_result["score"]),
            "ai_reasoning": f"Statistical score: {score_result['score']}. {score_result['breakdown']}",
            "ai_validated_at": datetime.now(timezone.utc),
            "entry_price": levels.get("entry_price"),
            "stop_loss": levels.get("stop_loss"),
            "take_profit_1": levels.get("take_profit_1"),
            "take_profit_2": levels.get("take_profit_2"),
            "risk_reward_ratio": levels.get("risk_reward_ratio"),
            "execution_status": "ready" if score_result["status"] == "execute" else None
        })

    return results


def update_signals(results):
    """Update signals with scores"""

    if not results:
        return

    signals_table = f"{CATALOG}.{GOLD_SCHEMA}.signals_all"

    schema = StructType([
        StructField("signal_id", StringType()),
        StructField("ai_status", StringType()),
        StructField("ai_confidence", DoubleType()),
        StructField("ai_reasoning", StringType()),
        StructField("ai_validated_at", TimestampType()),
        StructField("entry_price", DoubleType()),
        StructField("stop_loss", DoubleType()),
        StructField("take_profit_1", DoubleType()),
        StructField("take_profit_2", DoubleType()),
        StructField("risk_reward_ratio", DoubleType()),
        StructField("execution_status", StringType())
    ])

    updates_df = spark.createDataFrame(results, schema)

    delta_table = DeltaTable.forName(spark, signals_table)

    delta_table.alias("t").merge(
        updates_df.alias("s"),
        "t.signal_id = s.signal_id"
    ).whenMatchedUpdate(set={
        "ai_status": "s.ai_status",
        "ai_confidence": "s.ai_confidence",
        "ai_reasoning": "s.ai_reasoning",
        "ai_validated_at": "s.ai_validated_at",
        "entry_price": "s.entry_price",
        "stop_loss": "s.stop_loss",
        "take_profit_1": "s.take_profit_1",
        "take_profit_2": "s.take_profit_2",
        "risk_reward_ratio": "s.risk_reward_ratio",
        "execution_status": "s.execution_status"
    }).execute()

    executed = sum(1 for r in results if r["ai_status"] == "confirmed")
    skipped = sum(1 for r in results if r["ai_status"] == "rejected")

    print(f"\n✓ Updated {len(results)} signals: {executed} to execute, {skipped} skipped")


# ---
# EXECUTION
# ---

results = process_pending_signals()
update_signals(results)

# Show results
print("\n" + "=" * 60)
print("SIGNALS READY FOR EXECUTION")
print("=" * 60)

spark.sql(f"""
    SELECT
        signal_id,
        symbol,
        timeframe,
        signal_type,
        ROUND(ai_confidence, 0) as score,
        ROUND(entry_price, 2) as entry,
        ROUND(stop_loss, 2) as sl,
        ROUND(take_profit_1, 2) as tp1,
        ROUND(take_profit_2, 2) as tp2,
        ROUND(risk_reward_ratio, 2) as rr,
        execution_status
    FROM {CATALOG}.{GOLD_SCHEMA}.signals_all
    WHERE ai_status = 'confirmed'
      AND execution_status = 'ready'
    ORDER BY ai_confidence DESC
""").show(20, truncate=False)

print("\n✓ Statistical scoring complete")
