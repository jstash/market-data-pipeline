"""
Pure SQL query builders — no I/O, no external dependencies.

Each function returns a (sql, params) tuple ready to hand to psycopg2.
"""

from datetime import datetime
from typing import Any


def prices_query(
    symbol: str,
    from_: datetime | None = None,
    to: datetime | None = None,
    limit: int = 100,
) -> tuple[str, dict[str, Any]]:
    sql = (
        "SELECT symbol, bucket_time, open, high, low, close, volume, trade_count"
        " FROM ohlcv"
        " WHERE symbol = %(symbol)s"
    )
    params: dict[str, Any] = {"symbol": symbol}
    if from_ is not None:
        sql += " AND bucket_time >= %(from_)s"
        params["from_"] = from_
    if to is not None:
        sql += " AND bucket_time <= %(to)s"
        params["to"] = to
    sql += " ORDER BY bucket_time DESC LIMIT %(limit)s"
    params["limit"] = limit
    return sql, params


def anomalies_query(
    symbol: str | None = None,
    anomaly_type: str | None = None,
    from_: datetime | None = None,
    to: datetime | None = None,
    limit: int = 50,
) -> tuple[str, dict[str, Any]]:
    sql = (
        "SELECT symbol, detected_at, anomaly_type, severity, details"
        " FROM anomalies"
        " WHERE TRUE"
    )
    params: dict[str, Any] = {}
    if symbol is not None:
        sql += " AND symbol = %(symbol)s"
        params["symbol"] = symbol
    if anomaly_type is not None:
        sql += " AND anomaly_type = %(anomaly_type)s"
        params["anomaly_type"] = anomaly_type
    if from_ is not None:
        sql += " AND detected_at >= %(from_)s"
        params["from_"] = from_
    if to is not None:
        sql += " AND detected_at <= %(to)s"
        params["to"] = to
    sql += " ORDER BY detected_at DESC LIMIT %(limit)s"
    params["limit"] = limit
    return sql, params
