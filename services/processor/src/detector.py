"""
Anomaly detection — pure logic, no I/O.

AnomalyDetector maintains a rolling window of close prices per symbol and
flags two anomaly types:

  price_spike   — Z-score of the latest close exceeds a threshold.
                  Uses float arithmetic (not Decimal) because statistical
                  thresholds don't require sub-cent precision.

  missing_data  — No candle has been emitted for a symbol within the
                  configured gap.  Detected by the caller via
                  check_missing_data(); the caller tracks last-seen times.
"""

from collections import deque
from datetime import datetime, timezone
from typing import Any


class AnomalyDetector:
    def __init__(
        self,
        window_size: int = 20,
        z_threshold: float = 3.0,
    ) -> None:
        self._window_size = window_size
        self._z_threshold = z_threshold
        # deque(maxlen=N) automatically drops the oldest entry on append
        self._history: dict[str, deque[float]] = {}

    def check_candle(self, candle: dict[str, Any]) -> dict[str, Any] | None:
        """
        Check a completed candle for a price spike.

        The candle is added to the rolling history *after* checking so the
        current value is compared against purely historical data.

        Returns an anomaly event dict or None.
        """
        symbol = candle["symbol"]
        price  = float(candle["close"])

        if symbol not in self._history:
            self._history[symbol] = deque(maxlen=self._window_size)

        history = self._history[symbol]
        anomaly = None

        if len(history) >= 3:
            n      = len(history)
            mean   = sum(history) / n
            std    = (sum((p - mean) ** 2 for p in history) / n) ** 0.5

            if std > 0:
                z = abs(price - mean) / std
                if z >= self._z_threshold:
                    anomaly = {
                        "symbol":       symbol,
                        "detected_at":  candle["bucket_time"],
                        "anomaly_type": "price_spike",
                        "severity":     round(z, 4),
                        "details": {
                            "z_score":     round(z, 4),
                            "price":       candle["close"],
                            "window_mean": round(mean, 2),
                            "window_std":  round(std, 2),
                            "window_n":    n,
                        },
                    }

        history.append(price)
        return anomaly

    def check_missing_data(
        self,
        symbol: str,
        last_ms: int,
        now_ms: int,
        gap_minutes: int = 5,
    ) -> dict[str, Any] | None:
        """
        Return a missing_data anomaly if the gap since last_ms exceeds
        gap_minutes, else None.
        """
        gap_ms = now_ms - last_ms
        if gap_ms <= gap_minutes * 60_000:
            return None

        detected_at = datetime.fromtimestamp(
            now_ms / 1000, tz=timezone.utc
        ).isoformat()

        return {
            "symbol":       symbol,
            "detected_at":  detected_at,
            "anomaly_type": "missing_data",
            "severity":     None,
            "details":      {"gap_minutes": round(gap_ms / 60_000, 2)},
        }
