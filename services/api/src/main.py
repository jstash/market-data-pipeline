"""
api: HTTP read layer over TimescaleDB

GET /health      — liveness probe
GET /prices      — paginated OHLCV candles for a symbol
GET /anomalies   — paginated anomaly events, filterable by symbol and type

Environment variables:
  DATABASE_URL   PostgreSQL DSN   (default: see below)
  LOG_LEVEL                       (default: INFO)
"""

import logging
import os
from datetime import datetime
from typing import Any

import psycopg2
import psycopg2.extras
import uvicorn
from fastapi import Depends, FastAPI, Query

from src.queries import anomalies_query, prices_query

# ── Config ────────────────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://pipeline:pipeline@postgres:5432/marketdata",
)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(title="market-data-pipeline API", docs_url="/docs")

# ── Database dependency ───────────────────────────────────────────────────────

def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()

# ── Response models ───────────────────────────────────────────────────────────

from pydantic import BaseModel  # noqa: E402  (after app creation for clarity)


class Candle(BaseModel):
    symbol: str
    bucket_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int | None


class Anomaly(BaseModel):
    symbol: str
    detected_at: datetime
    anomaly_type: str
    severity: float | None
    details: dict[str, Any] | None

# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/prices", response_model=list[Candle])
def get_prices(
    symbol: str = Query(..., description="Trading pair symbol, e.g. BTCUSD"),
    from_: datetime | None = Query(None, alias="from"),
    to: datetime | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    db=Depends(get_db),
):
    sql, params = prices_query(symbol, from_=from_, to=to, limit=limit)
    with db.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


@app.get("/anomalies", response_model=list[Anomaly])
def get_anomalies(
    symbol: str | None = Query(None),
    anomaly_type: str | None = Query(None),
    from_: datetime | None = Query(None, alias="from"),
    to: datetime | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    db=Depends(get_db),
):
    sql, params = anomalies_query(
        symbol=symbol,
        anomaly_type=anomaly_type,
        from_=from_,
        to=to,
        limit=limit,
    )
    with db.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()

# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=LOG_LEVEL,
        format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level=LOG_LEVEL.lower())


if __name__ == "__main__":
    main()
