from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone


@dataclass
class CVDService:
    max_age_minutes: int = 240
    _trades: deque = field(default_factory=deque)
    cumulative_cvd: float = 0.0

    def add_trade(self, timestamp, side: str, size: float):
        ts = timestamp or datetime.now(timezone.utc)
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        signed = float(size or 0) if str(side).lower() in {"buy", "buyer", "aggressive_buy"} else -float(size or 0)
        self.cumulative_cvd += signed
        self._trades.append((ts, signed))
        self._trim(ts)
        return self.cumulative_cvd

    def _trim(self, now):
        cutoff = now - timedelta(minutes=self.max_age_minutes)
        while self._trades and self._trades[0][0] < cutoff:
            self._trades.popleft()

    def window_delta(self, minutes: int, now=None) -> float:
        now = now or datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=minutes)
        return sum(delta for ts, delta in self._trades if ts >= cutoff)

    def features(self, now=None) -> dict:
        now = now or datetime.now(timezone.utc)
        cvd_5m = self.window_delta(5, now)
        cvd_15m = self.window_delta(15, now)
        cvd_1h = self.window_delta(60, now)
        cvd_4h = self.window_delta(240, now)
        total_abs = sum(abs(delta) for _, delta in self._trades) or 0
        buy = sum(delta for _, delta in self._trades if delta > 0)
        sell = abs(sum(delta for _, delta in self._trades if delta < 0))
        return {
            "cvd_5m": cvd_5m,
            "cvd_15m": cvd_15m,
            "cvd_1h": cvd_1h,
            "cvd_4h": cvd_4h,
            "cvd_slope": cvd_15m / 15,
            "cvd_acceleration": (cvd_5m / 5) - (cvd_15m / 15),
            "buy_volume_ratio": buy / total_abs if total_abs else None,
            "sell_volume_ratio": sell / total_abs if total_abs else None,
        }

