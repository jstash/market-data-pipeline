# Market Data Pipeline

A real-time market data pipeline that streams BTC/USDT trades from Binance, processes them into OHLCV candles, detects anomalies, and surfaces everything in a Grafana dashboard.

```
Binance WebSocket → Redpanda (Kafka) → Processor → TimescaleDB → Grafana
                                                  → Anomaly Detector →
```

Everything runs locally with a single command.

## Quick start

```bash
cp .env.example .env        # already done — defaults work out of the box
docker compose up -d        # start all infrastructure
make topics                 # create Kafka topics (once, after first start)
```

| Service | URL |
|---|---|
| Grafana dashboard | http://localhost:3000 (admin / admin) |
| Redpanda Console | http://localhost:8080 |
| Kafka broker (external) | localhost:19092 |
| PostgreSQL | localhost:5432 |

## Services

| Service | Language | Purpose | Phase |
|---|---|---|---|
| `ingester` | Python | Binance WebSocket → `raw.prices` Kafka topic | 2 |
| `processor` | Python | OHLCV windowing + anomaly detection | 4 |
| `storage-writer` | Python | Kafka → TimescaleDB | 3 |
| `api` | FastAPI | REST query layer over TimescaleDB | 5 |
| `redpanda` | — | Kafka-compatible message broker | ✓ |
| `postgres` | — | TimescaleDB (time-series Postgres) | ✓ |
| `grafana` | — | Dashboard, provisioned as code | ✓ |

## Development

```bash
make logs           # stream all logs
make logs-ingester  # stream a single service
make psql           # open a psql shell
make reset          # wipe all data volumes and start fresh
make test           # run all service unit tests
```

To consume raw events for debugging:
```bash
make consume-topic TOPIC=raw.prices
```

## Roadmap

| Phase | Description | Status |
|---|---|---|
| 1 | Infrastructure — Redpanda, TimescaleDB, Grafana | ✅ complete |
| 2 | `ingester` — Binance WebSocket → Kafka | 🔜 next |
| 3 | `storage-writer` — Kafka → TimescaleDB | planned |
| 4 | `processor` — OHLCV windowing + anomaly detection | planned |
| 5 | `api` — FastAPI read layer | planned |
| 6 | Polish — GitHub Actions CI, pinned image versions | planned |

## Architecture decisions

Key design choices (Redpanda vs Confluent, TimescaleDB vs vanilla Postgres, in-memory windowing, etc.) are documented in [docs/architecture.md](docs/architecture.md).

## Requirements

- Docker Desktop 3.4+ (includes Compose v2)
- 4 GB RAM allocated to Docker is sufficient for all services
