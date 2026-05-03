"""
Candle windowing — pure logic, no I/O.

CandleWindow holds the OHLCV state for one (symbol, minute-bucket) pair.
WindowAccumulator manages all open windows and emits completed ones.

Emission rule: a bucket is complete as soon as a trade arrives for a
later bucket (event-time advance).  flush_older_than() handles the case
where trading goes quiet for a symbol.

Late-arriving trades (for already-emitted buckets) are silently discarded.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


def _bucket_ms(trade_time_ms: int) -> int:
    """Floor a trade timestamp to its 1-minute bucket (Unix ms)."""
    return (trade_time_ms // 60_000) * 60_000


@dataclass
class CandleWindow:
    symbol:      str
    bucket_ms:   int
    open:        Decimal
    high:        Decimal
    low:         Decimal
    close:       Decimal
    volume:      Decimal
    trade_count: int

    @classmethod
    def from_trade(
        cls,
        symbol: str,
        bucket_ms: int,
        price: Decimal,
        qty: Decimal,
    ) -> "CandleWindow":
        return cls(
            symbol=symbol, bucket_ms=bucket_ms,
            open=price, high=price, low=price, close=price,
            volume=qty, trade_count=1,
        )

    def update(self, price: Decimal, qty: Decimal) -> None:
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.close = price
        self.volume += qty
        self.trade_count += 1

    def to_message(self) -> dict[str, Any]:
        ts = datetime.fromtimestamp(
            self.bucket_ms / 1000, tz=timezone.utc
        ).isoformat()
        return {
            "symbol":      self.symbol,
            "bucket_time": ts,
            "open":        str(self.open),
            "high":        str(self.high),
            "low":         str(self.low),
            "close":       str(self.close),
            "volume":      str(self.volume),
            "trade_count": self.trade_count,
        }


class WindowAccumulator:
    def __init__(self) -> None:
        self._windows: dict[tuple[str, int], CandleWindow] = {}
        # tracks the most-recently emitted bucket per symbol to reject late trades
        self._last_emitted: dict[str, int] = {}

    def add_trade(
        self,
        symbol: str,
        price: Decimal,
        qty: Decimal,
        trade_time_ms: int,
    ) -> list[CandleWindow]:
        """
        Add a trade and return any newly completed candles.

        Returns an empty list for trades within the current bucket or
        late-arriving trades for already-emitted buckets.
        """
        bms = _bucket_ms(trade_time_ms)

        # Discard late arrivals for already-emitted windows
        if bms <= self._last_emitted.get(symbol, -1):
            return []

        key = (symbol, bms)
        if key in self._windows:
            self._windows[key].update(price, qty)
        else:
            self._windows[key] = CandleWindow.from_trade(symbol, bms, price, qty)

        # Emit all older buckets for this symbol
        completed: list[CandleWindow] = []
        for (s, b) in list(self._windows.keys()):
            if s == symbol and b < bms:
                completed.append(self._windows.pop((s, b)))
                self._last_emitted[symbol] = max(
                    self._last_emitted.get(symbol, 0), b
                )

        return completed

    def flush_older_than(self, cutoff_ms: int) -> list[CandleWindow]:
        """
        Emit all windows whose bucket started before cutoff_ms.

        Call periodically so candles aren't stranded when trading goes quiet.
        """
        completed: list[CandleWindow] = []
        for (s, bms) in list(self._windows.keys()):
            if bms < cutoff_ms:
                w = self._windows.pop((s, bms))
                completed.append(w)
                self._last_emitted[s] = max(self._last_emitted.get(s, 0), bms)
        return completed
