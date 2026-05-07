"""Unit tests for query builders."""

from datetime import datetime, timezone

from src.queries import anomalies_query, prices_query


# ── prices_query ──────────────────────────────────────────────────────────────

def test_prices_selects_all_columns():
    sql, _ = prices_query("BTCUSD")
    assert "symbol, bucket_time, open, high, low, close, volume, trade_count" in sql


def test_prices_filters_by_symbol():
    sql, params = prices_query("BTCUSD")
    assert "symbol = %(symbol)s" in sql
    assert params["symbol"] == "BTCUSD"


def test_prices_ordered_descending():
    sql, _ = prices_query("BTCUSD")
    assert "ORDER BY bucket_time DESC" in sql


def test_prices_limit_in_params():
    _, params = prices_query("BTCUSD", limit=250)
    assert params["limit"] == 250


def test_prices_from_filter_added():
    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    sql, params = prices_query("BTCUSD", from_=dt)
    assert "bucket_time >= %(from_)s" in sql
    assert params["from_"] == dt


def test_prices_to_filter_added():
    dt = datetime(2024, 1, 2, tzinfo=timezone.utc)
    sql, params = prices_query("BTCUSD", to=dt)
    assert "bucket_time <= %(to)s" in sql
    assert params["to"] == dt


def test_prices_no_time_filters_when_omitted():
    sql, params = prices_query("BTCUSD")
    assert "bucket_time >=" not in sql
    assert "bucket_time <=" not in sql
    assert "from_" not in params
    assert "to" not in params


def test_prices_both_time_filters():
    from_ = datetime(2024, 1, 1, tzinfo=timezone.utc)
    to = datetime(2024, 1, 2, tzinfo=timezone.utc)
    sql, params = prices_query("BTCUSD", from_=from_, to=to)
    assert "bucket_time >= %(from_)s" in sql
    assert "bucket_time <= %(to)s" in sql


# ── anomalies_query ───────────────────────────────────────────────────────────

def test_anomalies_selects_all_columns():
    sql, _ = anomalies_query()
    assert "symbol, detected_at, anomaly_type, severity, details" in sql


def test_anomalies_no_symbol_filter_when_omitted():
    sql, params = anomalies_query()
    assert "symbol = %(symbol)s" not in sql
    assert "symbol" not in params


def test_anomalies_symbol_filter_when_provided():
    sql, params = anomalies_query(symbol="BTCUSD")
    assert "symbol = %(symbol)s" in sql
    assert params["symbol"] == "BTCUSD"


def test_anomalies_type_filter_when_provided():
    sql, params = anomalies_query(anomaly_type="price_spike")
    assert "anomaly_type = %(anomaly_type)s" in sql
    assert params["anomaly_type"] == "price_spike"


def test_anomalies_ordered_descending():
    sql, _ = anomalies_query()
    assert "ORDER BY detected_at DESC" in sql


def test_anomalies_limit_in_params():
    _, params = anomalies_query(limit=25)
    assert params["limit"] == 25


def test_anomalies_time_range_filters():
    from_ = datetime(2024, 1, 1, tzinfo=timezone.utc)
    to = datetime(2024, 1, 2, tzinfo=timezone.utc)
    sql, params = anomalies_query(from_=from_, to=to)
    assert "detected_at >= %(from_)s" in sql
    assert "detected_at <= %(to)s" in sql
    assert params["from_"] == from_
    assert params["to"] == to


def test_anomalies_all_filters_combined():
    from_ = datetime(2024, 1, 1, tzinfo=timezone.utc)
    sql, params = anomalies_query(symbol="ETHUSD", anomaly_type="missing_data", from_=from_)
    assert "symbol = %(symbol)s" in sql
    assert "anomaly_type = %(anomaly_type)s" in sql
    assert "detected_at >= %(from_)s" in sql
    assert params["symbol"] == "ETHUSD"
    assert params["anomaly_type"] == "missing_data"
