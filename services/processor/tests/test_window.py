"""Unit tests for CandleWindow and WindowAccumulator."""

from decimal import Decimal

import pytest

from src.window import CandleWindow, WindowAccumulator, _bucket_ms

# ── _bucket_ms ────────────────────────────────────────────────────────────────

def test_bucket_ms_floors_to_minute():
    assert _bucket_ms(90_000) == 60_000   # 1m30s → minute 1
    assert _bucket_ms(60_000) == 60_000   # exactly on boundary
    assert _bucket_ms(119_999) == 60_000  # last ms of minute 1
    assert _bucket_ms(120_000) == 120_000 # first ms of minute 2


# ── CandleWindow ──────────────────────────────────────────────────────────────

def _candle(price="100.00", qty="1.0", bms=60_000):
    return CandleWindow.from_trade("BTCUSD", bms, Decimal(price), Decimal(qty))


def test_open_equals_first_trade_price():
    assert _candle("100.00").open == Decimal("100.00")


def test_initial_ohlc_all_equal_first_price():
    c = _candle("100.00")
    assert c.open == c.high == c.low == c.close == Decimal("100.00")


def test_high_updates_on_higher_price():
    c = _candle("100.00")
    c.update(Decimal("110.00"), Decimal("1"))
    assert c.high == Decimal("110.00")


def test_high_does_not_drop():
    c = _candle("100.00")
    c.update(Decimal("110.00"), Decimal("1"))
    c.update(Decimal("90.00"), Decimal("1"))
    assert c.high == Decimal("110.00")


def test_low_updates_on_lower_price():
    c = _candle("100.00")
    c.update(Decimal("90.00"), Decimal("1"))
    assert c.low == Decimal("90.00")


def test_low_does_not_rise():
    c = _candle("100.00")
    c.update(Decimal("90.00"), Decimal("1"))
    c.update(Decimal("110.00"), Decimal("1"))
    assert c.low == Decimal("90.00")


def test_close_is_last_price():
    c = _candle("100.00")
    c.update(Decimal("110.00"), Decimal("1"))
    c.update(Decimal("105.00"), Decimal("1"))
    assert c.close == Decimal("105.00")


def test_volume_accumulates():
    c = _candle("100.00", "1.0")
    c.update(Decimal("101.00"), Decimal("0.5"))
    c.update(Decimal("102.00"), Decimal("0.3"))
    assert c.volume == Decimal("1.8")


def test_trade_count_includes_initial():
    c = _candle()
    c.update(Decimal("101"), Decimal("1"))
    c.update(Decimal("102"), Decimal("1"))
    assert c.trade_count == 3


def test_to_message_keys():
    msg = _candle().to_message()
    assert set(msg.keys()) == {
        "symbol", "bucket_time", "open", "high", "low",
        "close", "volume", "trade_count",
    }


def test_to_message_prices_are_strings():
    msg = _candle("65432.10", "0.00123").to_message()
    assert isinstance(msg["open"], str)
    assert isinstance(msg["volume"], str)


def test_to_message_bucket_time_is_iso():
    msg = _candle(bms=0).to_message()
    assert msg["bucket_time"].startswith("1970-01-01T00:00:00")


# ── WindowAccumulator ─────────────────────────────────────────────────────────

def _add(acc, price, ts_ms, symbol="BTCUSD", qty="1.0"):
    return acc.add_trade(symbol, Decimal(price), Decimal(qty), ts_ms)


def test_no_emission_within_same_bucket():
    acc = WindowAccumulator()
    assert _add(acc, "100", 60_000) == []
    assert _add(acc, "101", 90_000) == []   # still minute 1


def test_emits_on_new_bucket():
    acc = WindowAccumulator()
    _add(acc, "100", 60_000)
    completed = _add(acc, "101", 120_000)   # minute 2 → emits minute 1
    assert len(completed) == 1
    assert completed[0].bucket_ms == 60_000


def test_emitted_candle_has_correct_ohlc():
    acc = WindowAccumulator()
    _add(acc, "100", 60_000)
    _add(acc, "110", 90_000)   # same bucket, higher
    completed = _add(acc, "105", 120_000)
    c = completed[0]
    assert c.open  == Decimal("100")
    assert c.high  == Decimal("110")
    assert c.close == Decimal("110")  # last trade in bucket


def test_each_new_bucket_emits_previous():
    # Minute-2 trade emits minute-1; minute-3 trade emits minute-2.
    # Each advance emits exactly the one bucket it overtook.
    acc = WindowAccumulator()
    _add(acc, "100", 60_000)
    c2 = _add(acc, "101", 120_000)
    assert len(c2) == 1 and c2[0].bucket_ms == 60_000

    c3 = _add(acc, "102", 180_000)
    assert len(c3) == 1 and c3[0].bucket_ms == 120_000


def test_flush_emits_multiple_symbols_at_once():
    # flush_older_than is the mechanism that emits multiple windows together.
    acc = WindowAccumulator()
    _add(acc, "100", 60_000, symbol="BTCUSD")
    _add(acc, "200", 60_000, symbol="ETHUSD")
    completed = acc.flush_older_than(120_000)
    assert len(completed) == 2
    assert {c.symbol for c in completed} == {"BTCUSD", "ETHUSD"}


def test_late_arrival_discarded():
    acc = WindowAccumulator()
    _add(acc, "100", 60_000)
    _add(acc, "101", 120_000)   # emits minute 1
    completed = _add(acc, "99", 60_500)   # late — bucket already emitted
    assert completed == []


def test_flush_older_than():
    acc = WindowAccumulator()
    _add(acc, "100", 60_000)
    completed = acc.flush_older_than(120_000)
    assert len(completed) == 1
    assert completed[0].bucket_ms == 60_000


def test_flush_does_not_re_emit():
    acc = WindowAccumulator()
    _add(acc, "100", 60_000)
    acc.flush_older_than(120_000)
    completed = acc.flush_older_than(120_000)
    assert completed == []


def test_independent_symbols_dont_cross_emit():
    acc = WindowAccumulator()
    _add(acc, "100", 60_000, symbol="BTCUSD")
    _add(acc, "200", 60_000, symbol="ETHUSD")
    # New BTC bucket — should NOT emit the ETH candle
    completed = _add(acc, "101", 120_000, symbol="BTCUSD")
    symbols = {c.symbol for c in completed}
    assert symbols == {"BTCUSD"}
