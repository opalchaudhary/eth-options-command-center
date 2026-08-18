import math
from statistics import mean, pstdev

import pandas as pd

from probability_engine.config import get_probability_config
from probability_engine.models.market_snapshot import MarketSnapshot
from probability_engine.services.math_utils import safe_div


def _pct_change(values, periods):
    if len(values) <= periods or not values[-periods - 1]:
        return None
    return (values[-1] - values[-periods - 1]) / values[-periods - 1]


def calculate_vwap(df: pd.DataFrame) -> float | None:
    if df is None or df.empty:
        return None
    typical = (df["high"] + df["low"] + df["close"]) / 3
    volume = df["volume"].clip(lower=0)
    total_volume = volume.sum()
    if not total_volume:
        return None
    return float((typical * volume).sum() / total_volume)


def calculate_atr(df: pd.DataFrame, period: int = 14) -> float | None:
    if df is None or len(df) < 2:
        return None
    frame = df.copy()
    previous_close = frame["close"].shift(1)
    true_range = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - previous_close).abs(),
            (frame["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    value = true_range.tail(period).mean()
    return None if pd.isna(value) else float(value)


def calculate_realized_volatility(df: pd.DataFrame, periods_per_year: int = 105120) -> float | None:
    if df is None or len(df) < 3:
        return None
    returns = df["close"].pct_change().dropna()
    if returns.empty:
        return None
    return float(returns.std(ddof=0) * math.sqrt(periods_per_year) * 100)


def zscore(value, samples) -> float | None:
    clean = [float(item) for item in samples if item is not None and not pd.isna(item)]
    if len(clean) < 2:
        return None
    sigma = pstdev(clean)
    if not sigma:
        return 0.0
    return (float(value) - mean(clean)) / sigma


class FeatureEngine:
    def __init__(self, config=None):
        self.config = config or get_probability_config()

    def build_snapshot(self, market: dict, ohlcv: pd.DataFrame, option_rows=None, orderbook_insights=None, cvd_features=None):
        option_rows = option_rows or []
        cvd_features = cvd_features or {}
        orderbook_insights = orderbook_insights or {}
        closes = ohlcv["close"].tolist() if ohlcv is not None and not ohlcv.empty else []
        spot = market.get("spot_price") or (closes[-1] if closes else None)
        vwap = calculate_vwap(ohlcv)
        atr = calculate_atr(ohlcv)
        rv = calculate_realized_volatility(ohlcv)
        volume = float(ohlcv["volume"].tail(1).iloc[0]) if ohlcv is not None and not ohlcv.empty else None
        volumes = ohlcv["volume"].tail(100).tolist() if ohlcv is not None and not ohlcv.empty else []
        vwap_diff = (spot - vwap) if spot is not None and vwap is not None else None
        atm_iv = self._atm_iv(option_rows, spot)
        spread_pct = orderbook_insights.get("spread_pct")

        return MarketSnapshot(
            symbol=market.get("symbol") or self.config.symbol,
            spot_price=spot,
            future_price=market.get("mark_price") or spot,
            return_5m=_pct_change(closes, 1),
            return_15m=_pct_change(closes, 3),
            return_1h=_pct_change(closes, 12),
            return_4h=_pct_change(closes, 48),
            vwap=vwap,
            vwap_deviation_pct=safe_div(vwap_diff, vwap),
            vwap_zscore=safe_div(vwap_diff, atr),
            atr=atr,
            atr_pct=safe_div(atr, spot),
            realized_volatility=rv,
            volume=volume,
            volume_zscore=zscore(volume, volumes) if volume is not None else None,
            atm_iv=atm_iv,
            iv_rv_spread=(atm_iv - rv) if atm_iv is not None and rv is not None else None,
            book_imbalance=orderbook_insights.get("imbalance_ratio"),
            spread_bps=(float(spread_pct) * 100) if spread_pct is not None else None,
            bid_depth=orderbook_insights.get("bid_depth"),
            ask_depth=orderbook_insights.get("ask_depth"),
            cvd_5m=cvd_features.get("cvd_5m"),
            cvd_15m=cvd_features.get("cvd_15m"),
            cvd_1h=cvd_features.get("cvd_1h"),
            cvd_slope=cvd_features.get("cvd_slope"),
            cvd_acceleration=cvd_features.get("cvd_acceleration"),
            buy_volume_ratio=cvd_features.get("buy_volume_ratio"),
            feature_version=self.config.feature_version,
            regime_version=self.config.regime_version,
            delta_market_data_status="HEALTHY" if spot is not None else "STALE_DATA",
            orderflow_provider_status="HEALTHY" if cvd_features else ("DISABLED" if not self.config.orderflow_enabled else "STALE_DATA"),
        )

    def _atm_iv(self, option_rows, spot):
        if not option_rows or spot is None:
            return None
        rows = [row for row in option_rows if row.get("iv") is not None and row.get("strike") is not None]
        if not rows:
            return None
        nearest = sorted(rows, key=lambda row: abs(float(row["strike"]) - float(spot)))[:4]
        return mean(float(row["iv"]) for row in nearest)

