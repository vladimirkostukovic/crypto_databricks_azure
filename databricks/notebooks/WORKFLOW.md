# Databricks Workflow - Crypto Trading Bot

## Execution Order

### 1. Initial Setup (Run Once)
```
init_gold_trading.py
```
- Creates gold and trading schemas
- Creates all tables
- Inserts default config values

### 2. Data Ingestion (Every 15 min via Azure Functions)
```
Azure Functions:
- ingest_binance (Timer: */15 * * * *)
- ingest_bybit   (Timer: */15 * * * *)
- ingest_okx     (Timer: */15 * * * *)
```

### 3. Bronze Layer (Auto Loader - Streaming)
```
Auto_Loader_bybit_binance_to_bronze.py
```
- Runs continuously or triggered after ingestion
- Loads JSON from landing to bronze tables

### 4. Silver Layer (Scheduled - Every 15 min)
```
1. binance_bronze_to_silver.py
2. bybit_bronze_to_silver.py
3. okx_bronze_to_silver.py
4. silver_to_unified.py
```
- Can run in parallel (1-3)
- silver_to_unified must run after all exchange notebooks

### 5. Analytics Layer (Scheduled - Every 15 min)
```
1. bos_detection.py
2. liquidity_zones.py
3. sentiment_analysis.py
```
- Can run in parallel after unified tables are ready

### 6. Signal Generation (Scheduled - Every 15 min)
```
signal_scanner.py
```
- Runs after analytics layer
- Outputs to gold.signals_all (status='pending')

### 7. Statistical Scoring (Scheduled - Every 5 min)
```
statistical_scorer.py
```
- Replaces AI validator with data-driven scoring
- Score based on: zone quality, BOS, confluence, sentiment, zone behavior
- Score >= 70 = auto-execute, 50-70 = watchlist, <50 = skip
- Calculates entry/SL/TP levels

### 8. Order Execution (Scheduled - Every 1 min)
```
execution_engine.py
```
- Executes confirmed signals via exchange API
- Places limit orders at POC
- Sets SL and TP1/TP2 orders
- Calculates position size based on risk %

### 9. Position Management (Scheduled - Every 1 min)
```
position_manager.py
```
- Monitors open positions
- Detects TP1/TP2/SL hits
- Handles partial close at TP1 (50%)
- Moves SL to breakeven after TP1
- Updates positions_closed and daily_pnl

### 10. Monitoring (On-demand or Dashboard)
```
signals_monitor.py
```
- View signals, positions, P&L
- Can be run anytime for status check

---

## Databricks Jobs Configuration

### Job 1: Data Pipeline (Every 15 min)
```yaml
name: crypto_data_pipeline
schedule: "0 */15 * * * ?"
tasks:
  - task_key: bronze_binance
    notebook_path: binance_bronze_to_silver.py
  - task_key: bronze_bybit
    notebook_path: bybit_bronze_to_silver.py
  - task_key: bronze_okx
    notebook_path: okx_bronze_to_silver.py
  - task_key: unified
    notebook_path: silver_to_unified.py
    depends_on: [bronze_binance, bronze_bybit, bronze_okx]
```

### Job 2: Analytics (Every 15 min, after Job 1)
```yaml
name: crypto_analytics
schedule: "5 */15 * * * ?"
tasks:
  - task_key: bos
    notebook_path: bos_detection.py
  - task_key: liquidity
    notebook_path: liquidity_zones.py
  - task_key: sentiment
    notebook_path: sentiment_analysis.py
```

### Job 3: Signals (Every 15 min, after Job 2)
```yaml
name: crypto_signals
schedule: "10 */15 * * * ?"
tasks:
  - task_key: scan
    notebook_path: signal_scanner.py
```

### Job 4: Statistical Scoring (Every 5 min)
```yaml
name: crypto_scorer
schedule: "*/5 * * * * ?"
tasks:
  - task_key: score
    notebook_path: statistical_scorer.py
```

### Job 5: Order Execution (Every 1 min)
```yaml
name: crypto_execution
schedule: "*/1 * * * * ?"
tasks:
  - task_key: execute
    notebook_path: execution_engine.py
```

### Job 6: Position Management (Every 1 min)
```yaml
name: crypto_position_manager
schedule: "*/1 * * * * ?"
tasks:
  - task_key: manage
    notebook_path: position_manager.py
```

---

## Table Dependencies

```
bronze.binance_15m ─┐
bronze.bybit_15m  ──┼── silver.{exchange}_*_{interval}
bronze.okx_15m ────┘              │
                                  ▼
                    silver.unified_*_{interval}
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
            bos_warnings    liq_{interval}  sentiment_scores
                    │             │             │
                    └─────────────┼─────────────┘
                                  ▼
                         gold.signals_all (pending)
                                  │
                                  ▼
                    (Statistical Scorer - Data-Driven)
                    Score: zone + BOS + confluence + sentiment
                                  │
                         ┌────────┴────────┐
                         ▼                 ▼
              confirmed (score≥70)    rejected (score<50)
                         │
                         ▼
              (Execution Engine - Exchange API)
              Limit orders at POC, SL/TP orders
                         │
                         ▼
              trading.positions_open
                         │
                         ▼
              (Position Manager)
              TP1: partial close + SL→breakeven
              TP2/SL: full close
                         │
                         ▼
              trading.positions_closed
                         │
                         ▼
              trading.daily_pnl
```

---

## Config Updates

To update trading parameters:
```sql
UPDATE crypto.gold.trading_config
SET param_value = 'new_value', updated_at = current_timestamp()
WHERE param_name = 'param_name';
```

Important params to set:
- `exchange_api_key` - Your exchange API key
- `exchange_api_secret` - Your exchange API secret
- `trading_exchange` - Exchange to use (bybit, binance)
- `use_testnet` - Set to 'false' for live trading
- `trading_symbols` - JSON array of symbols to trade
- `risk_per_trade_pct` - Risk per trade (default 1%)
- `score_threshold_execute` - Min score to auto-execute (default 70)
- `min_rr_ratio` - Minimum risk/reward ratio (default 2.0)

---

## Sanity Checks

Run periodically:
```
sanity_check_for_bronze_to_silver.py
sanity_check_unified_silver.py
```

---

## Backtesting

Run on-demand to test strategies:
```
backtesting.py
```

**Features:**
- Tests SMC and Liquidity strategies on historical data
- Configurable period, symbols, timeframes
- Simulates trades with commission and slippage

**Metrics calculated:**
- Sharpe Ratio, Sortino Ratio, Calmar Ratio
- Max Drawdown
- Win Rate, Profit Factor
- Equity Curve
- Average hold time
- Consecutive wins/losses

**Results saved to:** `gold.backtest_results`

**To customize backtest parameters**, edit `BACKTEST_CONFIG` at the top of the notebook:
```python
BACKTEST_CONFIG = {
    "start_date": "2024-01-01",
    "end_date": None,  # None = up to now
    "initial_capital": 10000,
    "risk_per_trade_pct": 1.0,
    "symbols": ["BTCUSDT", "ETHUSDT"],
    "timeframes": ["15m", "1h", "4h"],
    "strategies": ["smc", "liquidity"],
    ...
}
```

**View results:**
```sql
SELECT * FROM crypto.gold.backtest_results
ORDER BY run_timestamp DESC;
```
