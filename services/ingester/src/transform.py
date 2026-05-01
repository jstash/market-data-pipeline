"""
Pure transformation functions — no I/O, no external dependencies.

Keeping these separate from main.py means unit tests run with only pytest
installed, without needing confluent-kafka or websockets.
"""

from datetime import datetime
from typing import Any


def normalize(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Map a Kraken trade event to the pipeline's internal schema.

    Kraken sends prices as floats; we convert to strings on the way in to
    preserve decimal precision across Kafka.  The processor converts back
    to Decimal when building OHLCV candles.

    Kraken symbol format ("BTC/USD") is normalised to "BTCUSD" so that
    downstream consumers don't need to know which exchange produced the event.
    """
    ts_ms = _iso_to_ms(raw["timestamp"])

    return {
        "symbol":         raw["symbol"].replace("/", ""),   # "BTC/USD" → "BTCUSD"
        "price":          str(raw["price"]),
        "quantity":       str(raw["qty"]),
        "trade_time_ms":  ts_ms,
        "event_time_ms":  ts_ms,    # Kraken doesn't distinguish event time from trade time
        "buyer_is_maker": raw["side"] == "sell",  # side=sell → taker sold → buyer was maker
    }


def _iso_to_ms(iso: str) -> int:
    """Convert an ISO 8601 timestamp string (with Z or +00:00) to Unix milliseconds."""
    return int(datetime.fromisoformat(iso).timestamp() * 1000)
