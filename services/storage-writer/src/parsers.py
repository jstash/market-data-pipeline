"""
Pure parsing functions — no I/O, no external dependencies.

These validate and normalise Kafka message payloads into dicts that map
directly to named psycopg2 parameters (``%(name)s`` style).  Keeping them
separate from main.py means they can be unit-tested without a database or
Kafka broker.

Expected message schemas
────────────────────────
processed.ohlcv
    {symbol, bucket_time (ISO), open, high, low, close, volume (all str),
     trade_count (int, optional)}

alerts.anomalies
    {symbol, detected_at (ISO), anomaly_type (str),
     severity (float, optional), details (dict, optional)}
"""

import json
from typing import Any

_OHLCV_REQUIRED = {"symbol", "bucket_time", "open", "high", "low", "close", "volume"}
_ANOMALY_REQUIRED = {"symbol", "detected_at", "anomaly_type"}


def parse_ohlcv(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate and return a dict ready for the ohlcv upsert."""
    missing = _OHLCV_REQUIRED - payload.keys()
    if missing:
        raise ValueError(f"ohlcv message missing fields: {sorted(missing)}")
    return {
        "symbol":      payload["symbol"],
        "bucket_time": payload["bucket_time"],   # ISO string → ::timestamptz in SQL
        "open":        payload["open"],           # numeric string → ::numeric in SQL
        "high":        payload["high"],
        "low":         payload["low"],
        "close":       payload["close"],
        "volume":      payload["volume"],
        "trade_count": payload.get("trade_count"),
    }


def parse_anomaly(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate and return a dict ready for the anomalies insert."""
    missing = _ANOMALY_REQUIRED - payload.keys()
    if missing:
        raise ValueError(f"anomaly message missing fields: {sorted(missing)}")

    details = payload.get("details")
    return {
        "symbol":       payload["symbol"],
        "detected_at":  payload["detected_at"],   # ISO string → ::timestamptz in SQL
        "anomaly_type": payload["anomaly_type"],
        "severity":     payload.get("severity"),  # float or None → psycopg2 handles NULL
        # Serialise to JSON string; SQL casts with ::jsonb.  None stays None → NULL.
        "details":      json.dumps(details) if details is not None else None,
    }
