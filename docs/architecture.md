# Architecture & Design Decisions

This document records the key design decisions made during the project, including the context, the choice, and the consequences. Written in a lightweight ADR style.

---

## ADR-1: Redpanda over Confluent Kafka

**Context.** The project needs a Kafka-compatible message broker that can be run locally with a single `docker compose up`. Confluent's standard stack (Kafka + ZooKeeper, or KRaft mode) adds significant startup time and resource overhead in a local dev environment.

**Decision.** Use Redpanda.

**Consequences.**
- Cold start is ~3–5 seconds vs. ~30+ seconds for Confluent. For demos this matters.
- No ZooKeeper dependency. Single binary. `--mode dev-container` removes the need to configure listeners manually.
- Ships with a built-in Kafka-compatible Schema Registry and Redpanda Console (web UI) — two features we get for free.
- API is fully Kafka-compatible. Any `confluent-kafka-python` or `kafka-python` client works unchanged.
- Tradeoff: in a production environment you'd run Redpanda in a multi-node cluster or use a managed Kafka service (Confluent Cloud, MSK). The migration is transparent to application code because the API is identical.

---

## ADR-2: TimescaleDB over vanilla PostgreSQL

**Context.** Price candles are a time-series workload: ingested in time order, almost always queried by time range, and grow indefinitely. Standard PostgreSQL handles this correctly but degrades as the table grows without explicit partitioning.

**Decision.** Use the `timescale/timescaledb` image, which is PostgreSQL with the TimescaleDB extension preinstalled.

**Consequences.**
- `ohlcv` is a hypertable: TimescaleDB automatically partitions it into chunks by time. Queries scoped to a time range skip unrelated chunks entirely.
- Compression policy on chunks older than 7 days achieves ~90–95% size reduction on numeric time-series data with no query-layer changes.
- `create_hypertable` is the only TimescaleDB-specific SQL in the codebase. Everything else is standard SQL and works on vanilla Postgres too.
- Tradeoff: one additional Docker image layer. The wire protocol is still PostgreSQL — every tool that speaks PostgreSQL (psql, Grafana, SQLAlchemy) works without modification.

---

## ADR-3: Separate storage-writer service

**Context.** The stream processor computes OHLCV candles and detects anomalies. It could write to the database directly.

**Decision.** The storage-writer is a separate service that consumes Kafka topics and writes to Postgres. The processor does not touch the database.

**Consequences.**
- **Replayability.** The database is a *derived projection* of the Kafka log, not the source of truth. If a bug corrupts the database, the recovery procedure is: (1) truncate the affected tables, (2) reset the consumer group offset to `earliest` on the affected topic, (3) restart the storage-writer. It rebuilds the database from the log. This works within the Kafka retention window (7 days for `processed.ohlcv`).
- **Independent scaling.** The processor and storage-writer can be scaled independently. A slow DB write does not block candle computation.
- **Testability.** The processor's logic is purely Kafka-in / Kafka-out with no database dependency, making unit tests simpler.
- Tradeoff: one more service to operate and reason about.

---

## ADR-4: Grafana over a custom dashboard

**Context.** The project needs a dashboard showing price time-series and anomaly events.

**Decision.** Use Grafana, provisioned as code (datasource + dashboard JSON checked into the repo).

**Consequences.**
- Zero frontend code. The dashboard renders the moment `docker compose up` completes.
- Provisioning as code means the dashboard is reproducible — anyone who clones the repo gets the same dashboard.
- Grafana has native TimescaleDB awareness (`timescaledb: true` in the datasource config) and excellent time-series rendering.
- Tradeoff: less control over UX than a custom frontend. For a data pipeline project this is the right tradeoff — Grafana is the actual industry tooling.

---

## ADR-5: Kraken WebSocket over REST polling (and over Binance)

**Context.** Stock/crypto data can be ingested via REST polling (CoinGecko, Alpha Vantage) or a real-time WebSocket feed. Binance was the initial candidate for its well-known aggTrade stream.

**Decision.** Use the Kraken public trade WebSocket v2 (`wss://ws.kraken.com/v2`).

**Consequences.**
- The ingester is genuinely event-driven, not simulated streaming via polling. Each Kraken trade event is published to Kafka as it arrives.
- No API key required — the public stream is unauthenticated. This is important for a public GitHub repo.
- Globally accessible: Binance.com returns HTTP 451 (geo-blocked) for US-based IPs. Kraken has no such restriction, making the project runnable anywhere without a VPN.
- The ingester must send a subscription message after connecting, then implement reconnection with exponential backoff; WebSocket connections drop periodically.
- Tradeoff: at high tick frequency (BTC generates hundreds of trades/minute), message volume is real. Kafka retention policy and TimescaleDB compression are the controls for managing disk growth.
- Tradeoff: Kraken sends prices as floats, not strings. The `normalize` function converts them to strings on ingestion to preserve decimal precision across the Kafka boundary.

---

## ADR-6: In-memory windowing in the processor

**Context.** Computing 1-minute OHLCV candles requires buffering trade events within a time window. This could be done with Flink, Spark Streaming, or a plain Python loop.

**Decision.** Use a plain Python consumer loop with an in-memory dict keyed by `(symbol, minute_bucket)`.

**Consequences.**
- No additional infrastructure. The processor is a single Python process.
- Adequate for the current scale: one symbol, ~5–500 events/second.
- If the processor restarts mid-window, the partial window is lost and that candle will be incomplete. Mitigation: candle writes use `INSERT ... ON CONFLICT DO UPDATE` so a restarted processor reopens the window and corrects the candle as new trades arrive.
- At scale (hundreds of symbols, multiple exchanges) you would replace this with Flink or Kafka Streams backed by RocksDB state. The processor's Kafka-in / Kafka-out interface makes this a drop-in replacement.

---

## Delivery guarantees

Kafka delivers messages **at least once**. Every consumer in this pipeline must handle duplicates:

- **storage-writer / ohlcv**: `INSERT ... ON CONFLICT (symbol, bucket_time) DO UPDATE` — idempotent by primary key.
- **storage-writer / anomalies**: anomaly events are append-only; duplicate detection is done by the processor (don't re-emit an anomaly for the same event).

---

## What's intentionally out of scope (v1)

| Feature | Why deferred |
|---|---|
| Schema Registry (Avro/Protobuf) | Redpanda includes one; add it when adding a second producer |
| Multiple symbols / exchanges | Single symbol validates the full pipeline; multi-symbol is additive |
| API authentication | Not relevant to the data engineering story |
| Kubernetes | Correct for production; over-engineered for a single-developer demo |
| Exactly-once semantics | At-least-once + idempotent writes achieves the same result with less complexity |
