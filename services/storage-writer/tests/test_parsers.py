"""Unit tests for storage-writer message parsers."""

import json
import pytest

from src.parsers import parse_ohlcv, parse_anomaly

# ── Fixtures ──────────────────────────────────────────────────────────────────

OHLCV_PAYLOAD = {
    "symbol":      "BTCUSD",
    "bucket_time": "2024-05-01T00:00:00+00:00",
    "open":        "65000.00",
    "high":        "65500.00",
    "low":         "64900.00",
    "close":       "65200.00",
    "volume":      "12.34567",
    "trade_count": 142,
}

ANOMALY_PAYLOAD = {
    "symbol":       "BTCUSD",
    "detected_at":  "2024-05-01T00:01:00+00:00",
    "anomaly_type": "price_spike",
    "severity":     3.72,
    "details":      {"z_score": 3.72, "price": "65500.00", "mean": "65100.00"},
}


# ── parse_ohlcv ───────────────────────────────────────────────────────────────

def test_parse_ohlcv_all_fields_present():
    result = parse_ohlcv(OHLCV_PAYLOAD)
    assert result["symbol"]      == "BTCUSD"
    assert result["bucket_time"] == "2024-05-01T00:00:00+00:00"
    assert result["open"]        == "65000.00"
    assert result["close"]       == "65200.00"
    assert result["volume"]      == "12.34567"
    assert result["trade_count"] == 142


def test_parse_ohlcv_output_keys():
    result = parse_ohlcv(OHLCV_PAYLOAD)
    assert set(result.keys()) == {
        "symbol", "bucket_time", "open", "high", "low",
        "close", "volume", "trade_count",
    }


def test_parse_ohlcv_trade_count_optional():
    payload = {k: v for k, v in OHLCV_PAYLOAD.items() if k != "trade_count"}
    result = parse_ohlcv(payload)
    assert result["trade_count"] is None


def test_parse_ohlcv_missing_required_field():
    for field in ("symbol", "bucket_time", "open", "high", "low", "close", "volume"):
        bad = {k: v for k, v in OHLCV_PAYLOAD.items() if k != field}
        with pytest.raises(ValueError, match="missing fields"):
            parse_ohlcv(bad)


# ── parse_anomaly ─────────────────────────────────────────────────────────────

def test_parse_anomaly_all_fields_present():
    result = parse_anomaly(ANOMALY_PAYLOAD)
    assert result["symbol"]       == "BTCUSD"
    assert result["anomaly_type"] == "price_spike"
    assert result["severity"]     == 3.72


def test_parse_anomaly_details_serialised_to_json():
    result = parse_anomaly(ANOMALY_PAYLOAD)
    # details must be a JSON string so the SQL ::jsonb cast works
    assert isinstance(result["details"], str)
    parsed = json.loads(result["details"])
    assert parsed["z_score"] == 3.72


def test_parse_anomaly_output_keys():
    result = parse_anomaly(ANOMALY_PAYLOAD)
    assert set(result.keys()) == {
        "symbol", "detected_at", "anomaly_type", "severity", "details",
    }


def test_parse_anomaly_optional_fields_none():
    payload = {
        "symbol":       "BTCUSD",
        "detected_at":  "2024-05-01T00:01:00+00:00",
        "anomaly_type": "missing_data",
    }
    result = parse_anomaly(payload)
    assert result["severity"] is None
    assert result["details"]  is None


def test_parse_anomaly_missing_required_field():
    for field in ("symbol", "detected_at", "anomaly_type"):
        bad = {k: v for k, v in ANOMALY_PAYLOAD.items() if k != field}
        with pytest.raises(ValueError, match="missing fields"):
            parse_anomaly(bad)


def test_parse_anomaly_null_details_stays_none():
    payload = {**ANOMALY_PAYLOAD, "details": None}
    assert parse_anomaly(payload)["details"] is None
