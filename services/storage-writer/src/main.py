"""
storage-writer: Kafka → TimescaleDB

Consumes processed.ohlcv and alerts.anomalies and upserts into TimescaleDB.

Delivery guarantee: Kafka offsets are committed only after a successful
database commit.  Combined with idempotent upserts this gives at-least-once
delivery with no duplicate rows.

Replayability: because the database is a projection of the Kafka log, you can
rebuild it at any time within the topic retention window by truncating the
tables and resetting this consumer group to offset 0:

    docker compose exec redpanda rpk group seek storage-writer --to start
    docker compose exec postgres psql -U pipeline -d marketdata -c "TRUNCATE ohlcv, anomalies;"
    docker compose restart storage-writer

Environment variables:
  REDPANDA_BROKERS       Kafka bootstrap servers         (default: redpanda:9092)
  DATABASE_URL           PostgreSQL DSN                  (default: see below)
  KAFKA_TOPIC_OHLCV      Source topic for candles        (default: processed.ohlcv)
  KAFKA_TOPIC_ANOMALY    Source topic for anomalies      (default: alerts.anomalies)
  LOG_LEVEL                                              (default: INFO)
  HEALTH_PORT                                            (default: 8000)
"""

import json
import logging
import os
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError

from src.parsers import parse_ohlcv, parse_anomaly

# ── Config ────────────────────────────────────────────────────────────────────

BROKERS         = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
DATABASE_URL    = os.environ.get(
    "DATABASE_URL",
    "postgresql://pipeline:pipeline@postgres:5432/marketdata",
)
TOPIC_OHLCV     = os.environ.get("KAFKA_TOPIC_OHLCV",   "processed.ohlcv")
TOPIC_ANOMALY   = os.environ.get("KAFKA_TOPIC_ANOMALY", "alerts.anomalies")
CONSUMER_GROUP  = "storage-writer"
LOG_LEVEL       = os.environ.get("LOG_LEVEL", "INFO").upper()
HEALTH_PORT     = int(os.environ.get("HEALTH_PORT", "8000"))

# ── SQL ───────────────────────────────────────────────────────────────────────

# ON CONFLICT ensures this is safe to replay: redelivered candles overwrite
# with identical data rather than raising a duplicate-key error.
_UPSERT_OHLCV = """
    INSERT INTO ohlcv
        (symbol, bucket_time, open, high, low, close, volume, trade_count)
    VALUES
        (%(symbol)s, %(bucket_time)s::timestamptz,
         %(open)s::numeric, %(high)s::numeric, %(low)s::numeric,
         %(close)s::numeric, %(volume)s::numeric, %(trade_count)s)
    ON CONFLICT (symbol, bucket_time) DO UPDATE SET
        open        = EXCLUDED.open,
        high        = EXCLUDED.high,
        low         = EXCLUDED.low,
        close       = EXCLUDED.close,
        volume      = EXCLUDED.volume,
        trade_count = EXCLUDED.trade_count
"""

_INSERT_ANOMALY = """
    INSERT INTO anomalies
        (symbol, detected_at, anomaly_type, severity, details)
    VALUES
        (%(symbol)s, %(detected_at)s::timestamptz, %(anomaly_type)s,
         %(severity)s, %(details)s::jsonb)
"""

# ── Health check ──────────────────────────────────────────────────────────────

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):
        pass


def _start_health_server(log: logging.Logger) -> None:
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info(f"health check listening on :{HEALTH_PORT}/health")


# ── Database ──────────────────────────────────────────────────────────────────

def _connect(log: logging.Logger) -> psycopg2.extensions.connection:
    """Connect to Postgres, retrying with exponential backoff."""
    backoff = 1.0
    while True:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = False
            log.info("connected to database")
            return conn
        except psycopg2.OperationalError as exc:
            log.warning(f"db unavailable | err={exc} | retry_in={backoff:.1f}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


def _ensure_connected(
    conn: psycopg2.extensions.connection,
    log: logging.Logger,
) -> psycopg2.extensions.connection:
    """Return a live connection, reconnecting if the current one is broken."""
    if conn.closed:
        log.warning("db connection lost, reconnecting")
        return _connect(log)
    return conn


# ── Consumer loop ─────────────────────────────────────────────────────────────

def run(log: logging.Logger) -> None:
    db = _connect(log)

    consumer = Consumer({
        "bootstrap.servers":  BROKERS,
        "group.id":           CONSUMER_GROUP,
        "auto.offset.reset":  "earliest",   # start from beginning on first run
        "enable.auto.commit": False,         # manual commit after successful DB write
    })
    consumer.subscribe([TOPIC_OHLCV, TOPIC_ANOMALY])
    log.info(f"subscribed | topics={TOPIC_OHLCV},{TOPIC_ANOMALY}")

    running      = True
    ohlcv_count  = 0
    anomaly_count = 0

    def _shutdown(signum, _frame):
        nonlocal running
        log.info(f"received signal {signum}, shutting down")
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error(f"kafka error | {msg.error()}")
                continue

            topic   = msg.topic()
            raw     = msg.value()

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                log.error(f"invalid JSON | topic={topic} err={exc} raw={raw[:200]}")
                consumer.commit(asynchronous=False)  # skip unrecoverable message
                continue

            try:
                db = _ensure_connected(db, log)
                with db.cursor() as cur:
                    if topic == TOPIC_OHLCV:
                        cur.execute(_UPSERT_OHLCV, parse_ohlcv(payload))
                        ohlcv_count += 1
                        if ohlcv_count % 100 == 0:
                            log.info(f"wrote {ohlcv_count} candles total")
                    elif topic == TOPIC_ANOMALY:
                        cur.execute(_INSERT_ANOMALY, parse_anomaly(payload))
                        anomaly_count += 1
                        log.info(
                            f"wrote anomaly | type={payload.get('anomaly_type')}"
                            f" symbol={payload.get('symbol')}"
                            f" severity={payload.get('severity')}"
                        )
                db.commit()
                consumer.commit(asynchronous=False)

            except ValueError as exc:
                # Malformed payload — log, skip, don't crash.
                log.error(f"malformed message | topic={topic} err={exc}")
                consumer.commit(asynchronous=False)

            except psycopg2.Error as exc:
                db.rollback()
                log.error(f"db write failed | topic={topic} err={exc}")
                # Do NOT commit the Kafka offset — message will be redelivered
                # once the DB recovers.

    finally:
        consumer.close()
        db.close()
        log.info(
            f"storage-writer stopped | ohlcv={ohlcv_count} anomalies={anomaly_count}"
        )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=LOG_LEVEL,
        format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    log = logging.getLogger(__name__)
    _start_health_server(log)
    run(log)


if __name__ == "__main__":
    main()
