CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Every raw price tick from feeds
CREATE TABLE IF NOT EXISTS raw.price_ticks (
    event_id        VARCHAR(36),
    symbol          VARCHAR(10)   NOT NULL,
    price_usd       NUMERIC(20,8),
    price_open      NUMERIC(20,8),
    price_high      NUMERIC(20,8),
    price_low       NUMERIC(20,8),
    volume_24h_usd  NUMERIC(24,2),
    change_24h_pct  NUMERIC(10,4),
    source          VARCHAR(50),
    event_time      TIMESTAMP,
    kafka_timestamp TIMESTAMP,
    captured_at     VARCHAR(50)
);

-- Spark windowed OHLCV aggregations (5-min windows)
CREATE TABLE IF NOT EXISTS raw.price_windows (
    symbol          VARCHAR(10)   NOT NULL,
    window_start    TIMESTAMP     NOT NULL,
    window_end      TIMESTAMP     NOT NULL,
    price_open      NUMERIC(20,8),
    price_high      NUMERIC(20,8),
    price_low       NUMERIC(20,8),
    price_close     NUMERIC(20,8),
    volume_usd      NUMERIC(24,2),
    price_avg       NUMERIC(20,8),
    price_stddev    NUMERIC(20,8),
    vwap            NUMERIC(20,8),
    tick_count      INTEGER,
    window_change_pct NUMERIC(10,4),
    flag_large_move   BOOLEAN DEFAULT FALSE,
    flag_high_volatility BOOLEAN DEFAULT FALSE,
    flag_low_liquidity BOOLEAN DEFAULT FALSE,
    computed_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol  ON raw.price_ticks(symbol);
CREATE INDEX IF NOT EXISTS idx_ticks_time    ON raw.price_ticks(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_windows_sym   ON raw.price_windows(symbol);
CREATE INDEX IF NOT EXISTS idx_windows_start ON raw.price_windows(window_start DESC);
