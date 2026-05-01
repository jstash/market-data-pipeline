"""Unit tests for the ingester's transformation logic."""

from datetime import datetime, timezone

from src.transform import normalize, _iso_to_ms

# A realistic Kraken trade payload (single entry from "data" array)
SAMPLE = {
    "symbol":   "BTC/USD",
    "side":     "buy",       # taker bought → buyer is taker → buyer_is_maker=False
    "price":    65432.10,
    "qty":      0.00123,
    "ord_type": "market",
    "trade_id": 12345678,
    "timestamp": "2024-05-01T00:00:00.123456Z",
}


# ── normalize ─────────────────────────────────────────────────────────────────

def test_normalize_maps_all_fields():
    result = normalize(SAMPLE)
    assert result["symbol"]   == "BTCUSD"
    assert result["price"]    == str(65432.10)
    assert result["quantity"] == str(0.00123)
    assert result["buyer_is_maker"] is False


def test_normalize_output_schema():
    # Output must contain exactly these keys — no exchange-internal fields leaked.
    result = normalize(SAMPLE)
    assert set(result.keys()) == {
        "symbol",
        "price",
        "quantity",
        "trade_time_ms",
        "event_time_ms",
        "buyer_is_maker",
    }


def test_normalize_price_and_qty_are_strings():
    # Must remain strings through Kafka to avoid IEEE 754 precision loss.
    result = normalize(SAMPLE)
    assert isinstance(result["price"], str)
    assert isinstance(result["quantity"], str)


def test_normalize_timestamp_to_ms():
    result = normalize(SAMPLE)
    expected = int(datetime(2024, 5, 1, 0, 0, 0, 123456, tzinfo=timezone.utc).timestamp() * 1000)
    assert result["trade_time_ms"] == expected
    assert result["event_time_ms"] == expected


def test_normalize_trade_and_event_time_equal():
    # Kraken doesn't distinguish event time from trade time — both should be identical.
    result = normalize(SAMPLE)
    assert result["trade_time_ms"] == result["event_time_ms"]


def test_normalize_buyer_is_maker_sell_side():
    # side=sell → taker sold → buyer was the passive/maker side
    event = {**SAMPLE, "side": "sell"}
    assert normalize(event)["buyer_is_maker"] is True


def test_normalize_strips_slash_from_symbol():
    result = normalize(SAMPLE)
    assert "/" not in result["symbol"]


def test_normalize_different_symbol():
    event = {**SAMPLE, "symbol": "ETH/USD"}
    assert normalize(event)["symbol"] == "ETHUSD"


# ── _iso_to_ms ────────────────────────────────────────────────────────────────

def test_iso_to_ms_epoch():
    assert _iso_to_ms("1970-01-01T00:00:00.000000Z") == 0


def test_iso_to_ms_known_value():
    # 2024-05-01 00:00:00 UTC
    result = _iso_to_ms("2024-05-01T00:00:00.000000Z")
    expected = int(datetime(2024, 5, 1, tzinfo=timezone.utc).timestamp() * 1000)
    assert result == expected
