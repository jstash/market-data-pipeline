"""
ingester: Kraken trade WebSocket → Kafka raw.prices

Connects to the public Kraken trade stream for SYMBOL and publishes
normalized trade events to the raw.prices Kafka topic.  Reconnects
automatically with exponential backoff.

Environment variables:
  SYMBOL             Kraken trading pair, e.g. BTC/USD   (default: BTC/USD)
  REDPANDA_BROKERS   Kafka bootstrap servers             (default: redpanda:9092)
  KAFKA_TOPIC_RAW    Target topic                        (default: raw.prices)
  LOG_LEVEL          Python log level                    (default: INFO)
  HEALTH_PORT        Port for /health endpoint           (default: 8000)
"""

import asyncio
import json
import logging
import os
import signal
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import websockets
from confluent_kafka import Producer
from src.transform import normalize

# ── Config ────────────────────────────────────────────────────────────────────

SYMBOL      = os.environ.get("SYMBOL", "BTC/USD")   # Kraken pair format
BROKERS     = os.environ.get("REDPANDA_BROKERS", "redpanda:9092")
TOPIC       = os.environ.get("KAFKA_TOPIC_RAW", "raw.prices")
LOG_LEVEL   = os.environ.get("LOG_LEVEL", "INFO").upper()
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8000"))

WS_URL      = "wss://ws.kraken.com/v2"
KAFKA_KEY   = SYMBOL.replace("/", "").encode()  # "BTC/USD" → b"BTCUSD"

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
        pass  # suppress per-request HTTP logs


def _start_health_server(log: logging.Logger) -> None:
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info(f"health check listening on :{HEALTH_PORT}/health")


# ── Kafka producer ────────────────────────────────────────────────────────────

def make_producer() -> Producer:
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


# ── Ingestion loop ────────────────────────────────────────────────────────────

async def ingest(producer: Producer, log: logging.Logger) -> None:
    backoff     = 1.0
    max_backoff = 60.0

    subscribe_msg = json.dumps({
        "method": "subscribe",
        "params": {"channel": "trade", "symbol": [SYMBOL]},
    })

    while True:
        try:
            log.info(f"connecting | url={WS_URL}")
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                await ws.send(subscribe_msg)
                log.info(f"subscribed | symbol={SYMBOL} topic={TOPIC}")
                backoff = 1.0
                count   = 0

                async for raw_message in ws:
                    msg = json.loads(raw_message)

                    # Skip heartbeats and subscription confirmations
                    if msg.get("channel") != "trade":
                        continue
                    if msg.get("type") not in ("snapshot", "update"):
                        continue

                    for trade in msg.get("data", []):
                        payload = normalize(trade)
                        producer.produce(
                            topic=TOPIC,
                            key=KAFKA_KEY,
                            value=json.dumps(payload).encode(),
                            on_delivery=_on_delivery,
                        )
                        producer.poll(0)
                        count += 1

                    if count > 0 and count % 500 == 0:
                        log.info(f"produced {count} events | symbol={SYMBOL}")

        except websockets.exceptions.ConnectionClosed as exc:
            log.warning(f"connection closed | reason={exc} | retry_in={backoff:.1f}s")
        except OSError as exc:
            log.warning(f"network error | err={exc} | retry_in={backoff:.1f}s")
        except Exception as exc:
            log.error(f"unexpected error | err={exc} | retry_in={backoff:.1f}s")

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=LOG_LEVEL,
        format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    log = logging.getLogger(__name__)

    _start_health_server(log)
    producer = make_producer()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown(signum, _frame):
        log.info(f"received signal {signum}, shutting down")
        producer.flush(timeout=10)
        loop.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        loop.run_until_complete(ingest(producer, log))
    finally:
        producer.flush(timeout=10)
        loop.close()
        log.info("ingester stopped")


if __name__ == "__main__":
    main()
