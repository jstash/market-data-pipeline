-- Runs once when the data volume is first created.
-- Re-running docker compose up will NOT re-execute this file.
-- To reset: make reset  (removes the postgres_data volume)

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ── OHLCV candles ──────────────────────────────────────────────────────────
-- One row per (symbol, 1-minute bucket). Primary key enforces idempotency:
-- the storage-writer can safely upsert on restart without duplicating candles.

CREATE TABLE IF NOT EXISTS ohlcv (
    symbol      TEXT           NOT NULL,
    bucket_time TIMESTAMPTZ    NOT NULL,
    open        NUMERIC(18, 8) NOT NULL,
    high        NUMERIC(18, 8) NOT NULL,
    low         NUMERIC(18, 8) NOT NULL,
    close       NUMERIC(18, 8) NOT NULL,
    volume      NUMERIC(24, 8) NOT NULL,
    trade_count INTEGER,
    PRIMARY KEY (symbol, bucket_time)
);

-- Convert to hypertable: TimescaleDB auto-partitions by bucket_time.
-- Queries scoped to a time range skip unrelated partitions entirely.
SELECT create_hypertable('ohlcv', 'bucket_time', if_not_exists => TRUE);

-- Compress chunks older than 7 days. Typical 90-95% size reduction on
-- numeric time-series. Segments by symbol so each chunk holds one symbol.
ALTER TABLE ohlcv SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby   = 'bucket_time DESC'
);
SELECT add_compression_policy('ohlcv', INTERVAL '7 days');

-- ── Anomaly events ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS anomalies (
    id           BIGSERIAL      PRIMARY KEY,
    symbol       TEXT           NOT NULL,
    detected_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    -- price_spike | volume_spike | missing_data
    anomaly_type TEXT           NOT NULL,
    -- z-score magnitude for spike types; NULL for missing_data
    severity     NUMERIC(8, 4),
    -- processor-supplied context: window stats, raw values, etc.
    details      JSONB
);

-- Descending index: most recent anomaly queries are the common case.
CREATE INDEX IF NOT EXISTS idx_anomalies_symbol_time
    ON anomalies (symbol, detected_at DESC);
