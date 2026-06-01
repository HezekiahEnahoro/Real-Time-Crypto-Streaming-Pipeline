# Real-Time Crypto Streaming Pipeline

A real-time streaming data pipeline that ingests live cryptocurrency prices from CoinGecko, processes them through Apache Kafka and Spark Structured Streaming, builds analytical models with dbt, and visualises everything in Grafana — all running locally in Docker.

---

## Dashboard

![Grafana Dashboard](assets/grafana_dashboard.png)

Live at `http://localhost:3000` (admin / admin) — auto-refreshes every 30 seconds.

---

## Architecture

```
CoinGecko REST API
(8 coins, polling every 12s)
         │
         ▼
  Apache Kafka — crypto.prices topic (8 partitions)
         │
         ▼
  Spark Structured Streaming
  ├── parse JSON from Kafka
  ├── write raw ticks → PostgreSQL raw.price_ticks
  ├── compute 5-min sliding OHLCV windows
  ├── add anomaly flags (large moves, high volatility)
  └── write windows → PostgreSQL raw.price_windows
         │
         ▼
  Apache Airflow — crypto_analytics DAG (hourly)
  ├── freshness check → monitoring.health_checks
  ├── dbt run  → analytics.fct_ohlcv
  │              analytics.fct_rsi
  │              analytics.fct_bollinger
  │              analytics.fct_coin_correlation
  └── dbt test → 20 data quality assertions
         │
         ▼
  Grafana — Crypto Pipeline dashboard
  ├── live price tiles (BTC / ETH / SOL / BNB)
  ├── hourly close prices — all 8 coins, log scale
  ├── volume by coin (latest hour)
  ├── pipeline health check table
  └── RSI-14 timeseries (overbought >70 / oversold <30)
```

---

## Stack

| Layer | Technology |
|---|---|
| Price feed | CoinGecko REST API |
| Message broker | Apache Kafka (Confluent 7.5, Zookeeper) |
| Stream processing | Apache Spark 3.5 Structured Streaming |
| Storage | PostgreSQL 15 |
| Transformation | dbt-postgres 1.7 |
| Orchestration | Apache Airflow 2.8 (LocalExecutor) |
| Visualisation | Grafana 10.2 |
| Containerisation | Docker Compose |

---

## Airflow DAGs

### `crypto_price_feed`

![Price Feed DAG](assets/crypto_price_feed.png)

Runs every 5 minutes. Calls the CoinGecko `/simple/price` endpoint for all 8 coins and publishes JSON events to Kafka.

### `crypto_analytics`

![Analytics DAG](assets/crypto_analytics.png)

Runs hourly. Checks that `raw.price_windows` has fresh data, then runs `dbt run` and `dbt test`.

---

## dbt Models

All models live in `dbt_project/models/` and are materialised as tables in the `analytics` schema.

### `fct_ohlcv`
Hourly OHLCV candles built from `raw.price_windows`. Columns: `symbol`, `hour`, `hour_open`, `hour_high`, `hour_low`, `hour_close`, `hour_volume_usd`, `hour_tick_count`, `had_large_move`, `had_high_volatility`, `ma_24h`, `ma_7d`, `hourly_return_pct`.

### `fct_rsi`
14-period RSI computed via window CTEs on hourly closes. Columns: `rsi_14`, `rsi_signal` (overbought / neutral / oversold), `bullish_momentum`.

### `fct_bollinger`
20-period Bollinger Bands. Columns: `sma_20`, `upper_band`, `lower_band`, `pct_b` (position within bands, 0–1), `bandwidth`. Populates only after 20 hours of data per symbol.

### `fct_coin_correlation`
Rolling 24-hour Pearson correlation between coin pairs (BTC/ETH, BTC/BNB, BTC/SOL, BTC/ADA, BTC/MATIC, BTC/AVAX, BTC/DOT, ETH/SOL) using `corr() OVER (ROWS BETWEEN 23 PRECEDING AND CURRENT ROW)`.

---

## Data Quality Tests

20 dbt tests run every hourly cycle via `dbt test`.

**Schema tests** — `not_null` and `accepted_values` on `fct_ohlcv`, `fct_rsi`, `fct_bollinger` columns, plus source-level `not_null` on `raw.price_ticks` and `raw.price_windows`.

**Singular tests** (in `dbt_project/tests/`):
- `assert_prices_positive.sql` — all OHLCV prices must be > 0
- `assert_ohlcv_ordering.sql` — `high ≥ open/close ≥ low` invariant

---

## Monitoring

### `monitoring.pipeline_failures`
Task-level failure callback in Airflow writes every failure here automatically:
```sql
SELECT dag_id, task_id, execution_date, error_message FROM monitoring.pipeline_failures ORDER BY recorded_at DESC LIMIT 20;
```

### `monitoring.health_checks`
Freshness probe writes a row each cycle:
```sql
SELECT check_name, status, details, checked_at FROM monitoring.health_checks ORDER BY checked_at DESC LIMIT 10;
```

---

## Setup

### Prerequisites
- Docker Desktop
- Docker Compose v2

### Start everything

```powershell
cd crypto-streaming-pipeline

# Copy env file (edit credentials if needed)
cp .env.example .env

# Start all services
docker compose up -d

# Wait ~60s for Airflow to initialise, then verify
docker compose ps
```

Airflow UI: `http://localhost:8080` (admin / admin)  
Spark UI:   `http://localhost:8090`  
Grafana:    `http://localhost:3000` (admin / admin)

### Start the Spark streaming job

```powershell
# Must use PowerShell (not Git Bash — it rewrites /opt/ paths)
docker exec -it crypto-streaming-pipeline-spark-1 bash /opt/spark-apps/run_streaming.sh
```

The script auto-restarts the Spark job on crash with a 15-second backoff.

### Enable the DAGs

In the Airflow UI (`http://localhost:8080`), toggle on:
1. `crypto_price_feed` — starts publishing prices to Kafka
2. `crypto_analytics` — runs dbt hourly

Or via CLI:
```powershell
docker exec crypto-streaming-pipeline-airflow-scheduler-1 airflow dags unpause crypto_price_feed
docker exec crypto-streaming-pipeline-airflow-scheduler-1 airflow dags unpause crypto_analytics
```

### Trigger a manual analytics run

```powershell
docker exec crypto-streaming-pipeline-airflow-scheduler-1 airflow dags trigger crypto_analytics
```

---

## Analytics Queries

```sql
-- Latest price per coin
SELECT DISTINCT ON (symbol)
  symbol, hour, hour_close, ma_24h, hourly_return_pct
FROM analytics.fct_ohlcv
ORDER BY symbol, hour DESC;

-- RSI signals right now
SELECT symbol, rsi_14, rsi_signal
FROM analytics.fct_rsi
WHERE hour = (SELECT MAX(hour) FROM analytics.fct_rsi)
ORDER BY rsi_14 DESC;

-- Bollinger band squeeze (low bandwidth = consolidation)
SELECT symbol, hour, bandwidth, pct_b
FROM analytics.fct_bollinger
ORDER BY bandwidth ASC
LIMIT 10;

-- BTC/ETH rolling correlation
SELECT hour, btc_eth
FROM analytics.fct_coin_correlation
ORDER BY hour DESC
LIMIT 24;

-- Most volatile coins (last 24h)
SELECT symbol,
  ROUND(STDDEV(hourly_return_pct)::numeric, 4) AS volatility,
  ROUND(SUM(hour_volume_usd)::numeric, 0)      AS volume_24h
FROM analytics.fct_ohlcv
WHERE hour > NOW() - INTERVAL '24 hours'
GROUP BY symbol
ORDER BY volatility DESC;
```

---

## Key Concepts

**Spark micro-batches** — Kafka is treated as an unbounded table; every 5 seconds Spark reads new messages since the last committed offset. Checkpoints in `/opt/spark-checkpoints` ensure exactly-once progress tracking — if the job crashes, it resumes from the last offset.

**Sliding windows** — `window("event_time", "5 minutes", "1 minute")` produces overlapping windows (5 active per symbol at any time), giving smoother OHLCV data than non-overlapping tumbling windows.

**Watermark** — `.withWatermark("event_time", "2 minutes")` caps memory: late data up to 2 minutes is accepted; anything older is dropped. Without a watermark, Spark holds every window in memory indefinitely.

**Docker internal DNS** — inside containers, `postgres-crypto:5432` and `kafka:29092` are the correct addresses. The `.env` file uses host-exposed ports (`localhost:5434`, `localhost:9092`) for local tooling. The `x-airflow-common` environment block overrides `.env` with internal addresses so Airflow tasks reach the right hosts.

**dbt singular tests** — SQL files in `dbt_project/tests/` that return rows are failures. Zero rows = the assertion holds.

---

## Project Status

- [x] CoinGecko REST polling feed (8 coins, 12s interval)
- [x] Kafka topic `crypto.prices` (8 partitions)
- [x] Spark Structured Streaming — ticks + 5-min windowed OHLCV
- [x] Anomaly flags — large moves, high volatility
- [x] PostgreSQL dual sink — `raw.price_ticks` + `raw.price_windows`
- [x] dbt mart — `fct_ohlcv` with moving averages, return pct
- [x] dbt analytics — `fct_rsi`, `fct_bollinger`, `fct_coin_correlation`
- [x] 20 dbt data quality tests (schema + singular)
- [x] Airflow hourly DAG with failure callbacks + health checks
- [x] Monitoring schema — `pipeline_failures`, `health_checks`
- [x] Spark auto-restart wrapper (`run_streaming.sh`)
- [x] Container restart policies (`restart: unless-stopped`)
- [x] Grafana dashboard — prices, volume, RSI, pipeline health
- [ ] Bollinger Bands panel in Grafana (data populates after 20h)
- [ ] MLOps Phase 2 — price movement prediction + MLflow + FastAPI
