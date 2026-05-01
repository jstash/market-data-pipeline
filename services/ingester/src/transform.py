"""
Pure transformation functions — no I/O, no external dependencies.

Keeping these separate from main.py means unit tests run with only pytest
installed, without needing confluent-kafka or websockets.
"""

from datetime import datetime, timezone
from typing import Any


def normalize(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Map a Kraken trade event to the pipeline's internal schema.

    Price and quantity are emitted as strings for stable Kafka payloads and
    Decimal-friendly downstream parsing. JSON decoding may already have
    turned numbers into floats; if the feed ever sends decimals as strings,
    those are forwarded unchanged.

    Kraken symbol format ("BTC/USD") is normalised to "BTCUSD" so that
    downstream consumers don't need to know which exchange produced the event.
    """
    ts_ms = _iso_to_ms(raw["timestamp"])

    return {
        "symbol":         raw["symbol"].replace("/", ""),   # "BTC/USD" → "BTCUSD"
        "price":          _json_number_str(raw["price"]),
        "quantity":       _json_number_str(raw["qty"]),
        "trade_time_ms":  ts_ms,
        "event_time_ms":  ts_ms,    # Kraken doesn't distinguish event time from trade time
        "buyer_is_maker": raw["side"] == "sell",  # side=sell → taker sold → buyer was maker
    }


def _json_number_str(value: Any) -> str:
    """String form for Kafka; keep str payloads from the wire verbatim."""
    if isinstance(value, str):
        return value
    return str(value)


def _iso_to_ms(iso: str) -> int:
    """Convert an ISO 8601 timestamp to Unix milliseconds (UTC).

    Supports ``Z`` and numeric offsets. Naive timestamps (no zone) are
    treated as UTC so container local timezone cannot skew results.
    """
    s = iso.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)
