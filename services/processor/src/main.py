"""
processor: raw.prices → processed.ohlcv + alerts.anomalies

Consumes raw Kraken trade events, computes 1-minute OHLCV candles via
in-memory windowing, runs Z-score anomaly detection on completed candles,
and publishes results downstream.

Design notes
────────────
Windowing state is held in memory.  On restart the current partial window
is lost and a fresh candle starts — this is acceptable because:
  1. Windows are short (1 minute) so data loss is bounded.
  2. The storage-writer uses ON CONFLICT DO UPDATE so replayed candles
     are idempotent.

At scale (multi-symbol, multi-exchange) replace this service with a Flink
job backed by RocksDB state.  The Kafka-in / Kafka-out interface is
unchanged.

Environment variables:
  REDPANDA_BROKERS          (default: redpanda:9092)
  KAFKA_TOPIC_RAW           (default: raw.prices)
  KAFKA_TOPIC_OHLCV         (default: processed.ohlcv)
  KAFKA_TOPIC_ANOMALY       (default: alerts.anomalies)
  ANOMALY_WINDOW_SIZE       rolling window for Z-score  (default: 20)
  ANOMALY_Z_THRESHOLD       standard deviations         (default: 3.0)
  MISSING_DATA_GAP_MINUTES  gap before missing_data fires (default: 5)
  FLUSH_INTERVAL_SECONDS    periodic window flush        (default: 30)
  LOG_LEVEL                 (default: INFO)
  HEALTH_PORT               (default: 8000)
"""

import json
import logging
import os
import signal
import threading
import time
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer

from confluent_kafka import Consumer, Producer, KafkaError

from src.window   import WindowAccumulator
from src.detector import AnomalyDetector

# ── Config ────────────────────────────────────────────────────────────────────

BROKERS         = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
TOPIC_RAW       = os.environ.get("KAFKA_TOPIC_RAW",     "raw.prices")
TOPIC_OHLCV     = os.environ.get("KAFKA_TOPIC_OHLCV",   "processed.ohlcv")
TOPIC_ANOMALY   = os.environ.get("KAFKA_TOPIC_ANOMALY", "alerts.anomalies")
CONSUMER_GROUP  = "processor"

WINDOW_SIZE         = int(os.environ.get("ANOMALY_WINDOW_SIZE",        "20"))
Z_THRESHOLD         = float(os.environ.get("ANOMALY_Z_THRESHOLD",      "3.0"))
MISSING_GAP_MINUTES = int(os.environ.get("MISSING_DATA_GAP_MINUTES",   "5"))
FLUSH_INTERVAL      = int(os.environ.get("FLUSH_INTERVAL_SECONDS",     "30"))

LOG_LEVEL   = os.environ.get("LOG_LEVEL", "INFO").upper()
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8000"))

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


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def _make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BROKERS,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "compression.type": "lz4",
        "linger.ms": 5,
    })


def _on_delivery(err, msg) -> None:
    if err:
        logging.getLogger(__name__).error(
            f"delivery failed | topic={msg.topic()} err={err}"
        )


def _produce(producer: Producer, topic: str, key: str, payload: dict) -> None:
    producer.produce(
        topic=topic,
        key=key.encode(),
        value=json.dumps(payload).encode(),
        on_delivery=_on_delivery,
    )
    producer.poll(0)


# ── Main loop ─────────────────────────────────────────────────────────────────

def run(log: logging.Logger) -> None:
    accumulator = WindowAccumulator()
    detector    = AnomalyDetector(window_size=WINDOW_SIZE, z_threshold=Z_THRESHOLD)
    producer    = _make_producer()

    consumer = Consumer({
        "bootstrap.servers":  BROKERS,
        "group.id":           CONSUMER_GROUP,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5_000,
    })
    consumer.subscribe([TOPIC_RAW])
    log.info(f"subscribed | topic={TOPIC_RAW}")

    running    = True
    last_flush = time.monotonic()
    # tracks the last candle emission time per symbol for missing-data detection
    last_candle_ms: dict[str, int] = {}
    candle_count   = 0
    anomaly_count  = 0

    def _shutdown(signum, _frame):
        nonlocal running
        log.info(f"received signal {signum}, shutting down")
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    try:
        while running:
            # ── Periodic flush ───────────────────────────────────────────────
            now = time.monotonic()
            if now - last_flush >= FLUSH_INTERVAL:
                cutoff_ms = int((time.time() - 90) * 1000)
                for candle in accumulator.flush_older_than(cutoff_ms):
                    _emit_candle(candle, producer, detector, log,
                                 last_candle_ms)
                    candle_count += 1

                # missing-data check
                now_ms = int(time.time() * 1000)
                for symbol, last_ms in last_candle_ms.items():
                    anomaly = detector.check_missing_data(
                        symbol, last_ms, now_ms, MISSING_GAP_MINUTES
                    )
                    if anomaly:
                        _produce(producer, TOPIC_ANOMALY, symbol, anomaly)
                        anomaly_count += 1
                        log.warning(
                            f"missing data | symbol={symbol}"
                            f" gap_min={anomaly['details']['gap_minutes']}"
                        )

                last_flush = now

            # ── Consume ──────────────────────────────────────────────────────
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error(f"kafka error | {msg.error()}")
                continue

            try:
                payload = json.loads(msg.value())
                symbol  = payload["symbol"]
                price   = Decimal(payload["price"])
                qty     = Decimal(payload["quantity"])
                ts_ms   = int(payload["trade_time_ms"])
            except (KeyError, ValueError) as exc:
                log.error(f"malformed trade | err={exc} raw={msg.value()[:200]}")
                continue

            for candle in accumulator.add_trade(symbol, price, qty, ts_ms):
                _emit_candle(candle, producer, detector, log, last_candle_ms)
                candle_count += 1
                if candle_count % 50 == 0:
                    log.info(f"emitted {candle_count} candles | anomalies={anomaly_count}")

    finally:
        producer.flush(timeout=10)
        consumer.close()
        log.info(
            f"processor stopped | candles={candle_count} anomalies={anomaly_count}"
        )


def _emit_candle(
    candle,
    producer: Producer,
    detector: AnomalyDetector,
    log: logging.Logger,
    last_candle_ms: dict[str, int],
) -> None:
    """Produce a completed candle and check it for anomalies."""
    msg = candle.to_message()
    _produce(producer, TOPIC_OHLCV, candle.symbol, msg)
    last_candle_ms[candle.symbol] = candle.bucket_ms

    anomaly = detector.check_candle(msg)
    if anomaly:
        _produce(producer, TOPIC_ANOMALY, candle.symbol, anomaly)
        log.warning(
            f"anomaly | type={anomaly['anomaly_type']}"
            f" symbol={candle.symbol}"
            f" severity={anomaly.get('severity')}"
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
