"""Unit tests for AnomalyDetector."""

import pytest

from src.detector import AnomalyDetector, take_first_missing_data_alert


def _candle(close, symbol="BTCUSD", bucket="2024-05-01T00:00:00+00:00"):
    return {"symbol": symbol, "close": str(close), "bucket_time": bucket}


def _fill(det, prices):
    """Feed a list of prices to the detector, returning all results."""
    return [det.check_candle(_candle(p)) for p in prices]


# ── Price spike detection ─────────────────────────────────────────────────────

def test_no_anomaly_with_fewer_than_3_history_points():
    det = AnomalyDetector(z_threshold=1.0)
    # history is empty before first call, then 1, then 2 — never ≥3
    results = _fill(det, [100, 200, 300])
    assert all(r is None for r in results)


def test_no_anomaly_for_normal_prices():
    det = AnomalyDetector(z_threshold=3.0)
    _fill(det, [100, 101, 100, 99, 100, 101])
    result = det.check_candle(_candle(100.5))
    assert result is None


def test_price_spike_detected():
    det = AnomalyDetector(z_threshold=2.0)
    # Stable cluster around 100
    _fill(det, [100, 100.5, 99.8, 100.2, 100.1])
    result = det.check_candle(_candle(200))   # massive spike
    assert result is not None
    assert result["anomaly_type"] == "price_spike"
    assert result["severity"] > 2.0


def test_anomaly_event_schema():
    det = AnomalyDetector(z_threshold=2.0)
    _fill(det, [100, 100.5, 99.8, 100.2, 100.1])
    result = det.check_candle(_candle(200))
    assert result is not None
    assert set(result.keys()) == {
        "symbol", "detected_at", "anomaly_type", "severity", "details"
    }
    assert result["symbol"]       == "BTCUSD"
    assert result["detected_at"]  == "2024-05-01T00:00:00+00:00"
    assert isinstance(result["details"]["window_mean"], float)
    assert isinstance(result["details"]["window_std"],  float)
    assert isinstance(result["details"]["window_n"],    int)


def test_no_spike_on_constant_prices():
    # std == 0 → z-score undefined → no anomaly should fire
    det = AnomalyDetector(z_threshold=0.1)
    _fill(det, [100, 100, 100, 100, 100])
    result = det.check_candle(_candle(100))
    assert result is None


def test_spike_below_threshold_not_flagged():
    det = AnomalyDetector(z_threshold=5.0)
    _fill(det, [100, 100.5, 99.8, 100.2, 100.1])
    # std ≈ 0.23; price=101 gives z ≈ 3.8, which is below the z=5 threshold
    result = det.check_candle(_candle(101))
    assert result is None


def test_separate_histories_per_symbol():
    det = AnomalyDetector(z_threshold=2.0)
    # Fill BTC history with stable prices
    for p in [100, 100.5, 99.8, 100.2, 100.1]:
        det.check_candle(_candle(p, symbol="BTCUSD"))
    # ETH has no history — spike on ETH should NOT fire (< 3 history points)
    result = det.check_candle(_candle(999, symbol="ETHUSD"))
    assert result is None


def test_rolling_window_drops_old_data():
    # With window_size=3, only the 3 most recent prices are kept.
    # If we fill with a spike and then normal prices, the spike leaves
    # the window and subsequent normals shouldn't trigger.
    det = AnomalyDetector(window_size=3, z_threshold=2.0)
    # Seed: one spike and two normal
    _fill(det, [100, 200, 100])   # spike enters and then gets pushed out
    _fill(det, [100, 100])        # now window is [100, 100, 100] → std=0
    result = det.check_candle(_candle(100.1))
    assert result is None


# ── Missing data detection ────────────────────────────────────────────────────

def test_missing_data_fires_after_gap():
    det = AnomalyDetector()
    result = det.check_missing_data("BTCUSD", last_ms=0, now_ms=6 * 60_000, gap_minutes=5)
    assert result is not None
    assert result["anomaly_type"] == "missing_data"
    assert result["symbol"]       == "BTCUSD"
    assert result["severity"]     is None


def test_missing_data_silent_within_gap():
    det = AnomalyDetector()
    result = det.check_missing_data("BTCUSD", last_ms=0, now_ms=4 * 60_000, gap_minutes=5)
    assert result is None


def test_missing_data_gap_minutes_in_details():
    det = AnomalyDetector()
    result = det.check_missing_data("BTCUSD", last_ms=0, now_ms=10 * 60_000, gap_minutes=5)
    assert result["details"]["gap_minutes"] == 10.0


def test_missing_data_exactly_on_boundary():
    det = AnomalyDetector()
    # Exactly 5 minutes — boundary is inclusive, so this does NOT fire.
    # The next periodic check (>5 min elapsed) will fire.
    result = det.check_missing_data("BTCUSD", last_ms=0, now_ms=5 * 60_000, gap_minutes=5)
    assert result is None


def test_missing_data_fires_just_past_boundary():
    det = AnomalyDetector()
    result = det.check_missing_data("BTCUSD", last_ms=0, now_ms=5 * 60_000 + 1, gap_minutes=5)
    assert result is not None


# ── take_first_missing_data_alert (dedupe across periodic checks) ─────────────


def test_take_first_missing_data_alert_emits_once():
    det = AnomalyDetector()
    alerted: set[str] = set()
    raw = det.check_missing_data("BTCUSD", last_ms=0, now_ms=10 * 60_000, gap_minutes=5)
    first = take_first_missing_data_alert("BTCUSD", raw, alerted)
    second = take_first_missing_data_alert("BTCUSD", raw, alerted)
    assert first is not None
    assert second is None
    assert "BTCUSD" in alerted


def test_take_first_missing_data_alert_none_stays_silent():
    alerted: set[str] = set()
    assert take_first_missing_data_alert("BTCUSD", None, alerted) is None
    assert alerted == set()


def test_take_first_missing_data_alert_cleared_after_symbol_discard():
    det = AnomalyDetector()
    alerted: set[str] = set()
    raw = det.check_missing_data("BTCUSD", last_ms=0, now_ms=10 * 60_000, gap_minutes=5)
    assert take_first_missing_data_alert("BTCUSD", raw, alerted) is not None
    alerted.discard("BTCUSD")
    again = take_first_missing_data_alert("BTCUSD", raw, alerted)
    assert again is not None
