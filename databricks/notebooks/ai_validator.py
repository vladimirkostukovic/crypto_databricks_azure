# Databricks notebook source
# ---
# AI VALIDATOR
# Validates pending signals using Anthropic Claude API
# Updates gold.signals_all with AI decision and levels
# ---

import requests
import json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

# ---
# CONFIGURATION
# ---

CATALOG = "crypto"
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
ANTHROPIC_API_KEY = get_config("anthropic_api_key", "sk-ant-PLACEHOLDER")
AI_MODEL = get_config("ai_model", "claude-sonnet-4-20250514")
AI_TEMPERATURE = get_config("ai_temperature", 0.3)
MIN_AI_CONFIDENCE = get_config("min_ai_confidence", 75)
RISK_PER_TRADE = get_config("risk_per_trade_pct", 1.0)
SL_ATR_MULT = get_config("sl_atr_multiplier", 1.5)
TP1_RR = get_config("tp1_rr_ratio", 1.5)
TP2_RR = get_config("tp2_rr_ratio", 3.0)

print("=" * 60)
print("AI VALIDATOR")
print("=" * 60)
print(f"Model: {AI_MODEL}")
print(f"Min confidence: {MIN_AI_CONFIDENCE}%")
print(f"Risk per trade: {RISK_PER_TRADE}%")
print("=" * 60)

# Check API key
if "PLACEHOLDER" in ANTHROPIC_API_KEY:
    print("\n⚠ WARNING: Anthropic API key is placeholder!")
    print("Update gold.trading_config with real API key:")
    print(f"  UPDATE {CATALOG}.{GOLD_SCHEMA}.trading_config")
    print("  SET param_value = 'sk-ant-your-real-key'")
    print("  WHERE param_name = 'anthropic_api_key';")

# ---
# ANTHROPIC API CALL
# ---

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

SYSTEM_PROMPT = """You are a professional crypto trading analyst AI. Your job is to validate trading signals and calculate precise entry, stop loss, and take profit levels.

You will receive:
1. Signal details (type, symbol, timeframe, strategy)
2. Zone/level information (POC, zone boundaries, strength)
3. BOS (Break of Structure) data
4. Sentiment data (long/short ratio, crowding, smart money)
5. Recent price action (last 20 candles)

Your task:
1. Analyze if the signal is valid based on all provided data
2. If valid, calculate precise levels:
   - Entry price (optimal entry within the zone)
   - Stop loss (based on zone boundaries and ATR)
   - Take profit 1 (partial exit, ~1.5R)
   - Take profit 2 (full exit, ~3R)
3. Provide confidence score (0-100%)
4. Explain your reasoning briefly

IMPORTANT RULES:
- Be conservative. Only confirm signals with high probability.
- Check if sentiment supports the trade direction.
- Consider if crowding could lead to a squeeze against the signal.
- Verify BOS aligns with the trade direction.
- For LONG: entry should be near zone support, SL below zone, TP above.
- For SHORT: entry should be near zone resistance, SL above zone, TP below.

Respond ONLY with valid JSON in this exact format:
{
    "decision": "CONFIRM" or "REJECT",
    "confidence": 0-100,
    "entry_price": number or null,
    "stop_loss": number or null,
    "take_profit_1": number or null,
    "take_profit_2": number or null,
    "risk_reward_ratio": number or null,
    "position_size_pct": number or null,
    "reasoning": "Brief explanation"
}"""


def call_anthropic_api(signal_data):
    """Call Anthropic API to validate signal"""

    if "PLACEHOLDER" in ANTHROPIC_API_KEY:
        # Return mock response for testing
        return {
            "decision": "REJECT",
            "confidence": 0,
            "entry_price": None,
            "stop_loss": None,
            "take_profit_1": None,
            "take_profit_2": None,
            "risk_reward_ratio": None,
            "position_size_pct": None,
            "reasoning": "API key not configured - placeholder mode"
        }

    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01"
    }

    user_message = f"""Analyze this trading signal and provide your decision:

SIGNAL DATA:
{json.dumps(signal_data, indent=2)}

Provide your analysis as JSON only."""

    payload = {
        "model": AI_MODEL,
        "max_tokens": 1024,
        "temperature": AI_TEMPERATURE,
        "system": SYSTEM_PROMPT,
        "messages": [
            {"role": "user", "content": user_message}
        ]
    }

    try:
        response = requests.post(ANTHROPIC_API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()

        result = response.json()
        content = result["content"][0]["text"]

        # Parse JSON from response
        # Handle potential markdown code blocks
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]

        ai_response = json.loads(content.strip())
        return ai_response

    except requests.exceptions.RequestException as e:
        print(f"    API Error: {e}")
        return {
            "decision": "REJECT",
            "confidence": 0,
            "reasoning": f"API error: {str(e)}"
        }
    except json.JSONDecodeError as e:
        print(f"    JSON Parse Error: {e}")
        return {
            "decision": "REJECT",
            "confidence": 0,
            "reasoning": f"Response parse error: {str(e)}"
        }


# ---
# PROCESS PENDING SIGNALS
# ---

def validate_pending_signals():
    """Process all pending signals through AI"""

    signals_table = f"{CATALOG}.{GOLD_SCHEMA}.signals_all"

    # Get pending signals
    pending_df = spark.table(signals_table).filter(
        F.col("ai_status") == "pending"
    ).orderBy("created_at")

    pending_signals = pending_df.collect()
    print(f"\nPending signals to validate: {len(pending_signals)}")

    if len(pending_signals) == 0:
        print("No pending signals")
        return

    results = []

    for signal in pending_signals:
        signal_id = signal["signal_id"]
        symbol = signal["symbol"]
        tf = signal["timeframe"]
        strategy = signal["strategy"]
        signal_type = signal["signal_type"]

        print(f"\n[{signal_id[:8]}] {symbol} {tf} {strategy} {signal_type}")

        # Parse data snapshot
        try:
            data_snapshot = json.loads(signal["data_snapshot"]) if signal["data_snapshot"] else {}
        except:
            data_snapshot = {}

        # Enrich with signal info
        data_snapshot["signal_info"] = {
            "signal_id": signal_id,
            "symbol": symbol,
            "timeframe": tf,
            "strategy": strategy,
            "signal_type": signal_type,
            "current_price": signal["current_price"],
            "zone_low": signal["zone_low"],
            "zone_high": signal["zone_high"],
            "poc_price": signal["poc_price"],
            "zone_strength": signal["zone_strength"],
            "bos_type": signal["bos_type"],
            "confluence_score": signal["confluence_score"],
            "sentiment_score": signal["sentiment_score"],
            "crowding": signal["crowding_direction"],
            "smart_money_divergence": signal["smart_money_divergence"],
            "taker_bias": signal["taker_bias"]
        }

        # Call AI
        ai_response = call_anthropic_api(data_snapshot)

        decision = ai_response.get("decision", "REJECT")
        confidence = ai_response.get("confidence", 0)
        reasoning = ai_response.get("reasoning", "")

        print(f"    Decision: {decision} (confidence: {confidence}%)")
        print(f"    Reason: {reasoning[:100]}...")

        # Determine final status
        if decision == "CONFIRM" and confidence >= MIN_AI_CONFIDENCE:
            ai_status = "confirmed"
        else:
            ai_status = "rejected"

        # Prepare update
        update = {
            "signal_id": signal_id,
            "ai_status": ai_status,
            "ai_confidence": float(confidence) if confidence else None,
            "ai_reasoning": reasoning,
            "ai_validated_at": datetime.now(timezone.utc),
            "entry_price": ai_response.get("entry_price"),
            "stop_loss": ai_response.get("stop_loss"),
            "take_profit_1": ai_response.get("take_profit_1"),
            "take_profit_2": ai_response.get("take_profit_2"),
            "risk_reward_ratio": ai_response.get("risk_reward_ratio"),
            "position_size_pct": ai_response.get("position_size_pct")
        }

        results.append(update)

    return results


# ---
# UPDATE SIGNALS TABLE
# ---

def update_signals_with_ai_results(results):
    """Update signals_all with AI validation results"""

    if not results:
        print("No results to update")
        return

    signals_table = f"{CATALOG}.{GOLD_SCHEMA}.signals_all"

    # Create update DataFrame
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
        StructField("position_size_pct", DoubleType())
    ])

    updates_df = spark.createDataFrame(results, schema)

    # Merge updates
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
        "position_size_pct": "s.position_size_pct"
    }).execute()

    confirmed = sum(1 for r in results if r["ai_status"] == "confirmed")
    rejected = sum(1 for r in results if r["ai_status"] == "rejected")

    print(f"\n✓ Updated {len(results)} signals: {confirmed} confirmed, {rejected} rejected")


# ---
# MAIN EXECUTION
# ---

results = validate_pending_signals()

if results:
    update_signals_with_ai_results(results)

# ---
# SHOW RESULTS
# ---

print("\n" + "=" * 60)
print("AI VALIDATION RESULTS")
print("=" * 60)

# Confirmed signals ready for execution
print("\n[CONFIRMED SIGNALS - Ready for execution]")
spark.sql(f"""
    SELECT
        signal_id,
        symbol,
        timeframe,
        strategy,
        signal_type,
        ROUND(entry_price, 2) as entry,
        ROUND(stop_loss, 2) as sl,
        ROUND(take_profit_1, 2) as tp1,
        ROUND(take_profit_2, 2) as tp2,
        ROUND(ai_confidence, 0) as conf,
        ROUND(risk_reward_ratio, 2) as rr
    FROM {CATALOG}.{GOLD_SCHEMA}.signals_all
    WHERE ai_status = 'confirmed'
      AND execution_status IS NULL
    ORDER BY ai_confidence DESC
""").show(truncate=False)

# Recent rejected signals
print("\n[RECENTLY REJECTED]")
spark.sql(f"""
    SELECT
        signal_id,
        symbol,
        timeframe,
        strategy,
        signal_type,
        ROUND(ai_confidence, 0) as conf,
        SUBSTRING(ai_reasoning, 1, 80) as reason
    FROM {CATALOG}.{GOLD_SCHEMA}.signals_all
    WHERE ai_status = 'rejected'
      AND ai_validated_at >= current_timestamp() - INTERVAL 1 HOUR
    ORDER BY ai_validated_at DESC
    LIMIT 10
""").show(truncate=False)

# Stats
print("\n[VALIDATION STATS]")
spark.sql(f"""
    SELECT
        ai_status,
        COUNT(*) as count,
        ROUND(AVG(ai_confidence), 1) as avg_confidence,
        ROUND(AVG(zone_strength), 1) as avg_zone_strength
    FROM {CATALOG}.{GOLD_SCHEMA}.signals_all
    WHERE ai_status != 'pending'
    GROUP BY ai_status
""").show()

print("\n✓ AI validation complete")
