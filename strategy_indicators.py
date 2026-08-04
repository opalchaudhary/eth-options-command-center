from datetime import datetime, timezone

import pandas as pd


def _numeric_series(df, column):
    if df is None or df.empty or column not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").dropna()


def latest_timestamp(df):
    if df is None or df.empty:
        return None
    for column in ["timestamp", "candle_time", "created_at"]:
        if column in df.columns:
            value = pd.to_datetime(df[column], utc=True, errors="coerce").dropna()
            if not value.empty:
                return value.max().to_pydatetime()
    return None


def age_seconds(timestamp):
    if timestamp is None:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return max(0, (datetime.now(timezone.utc) - timestamp).total_seconds())


def ema(df, period=20, column="close"):
    series = _numeric_series(df, column)
    if len(series) < period:
        return None
    return float(series.ewm(span=period, adjust=False).mean().iloc[-1])


def sma(df, period=20, column="close"):
    series = _numeric_series(df, column)
    if len(series) < period:
        return None
    return float(series.tail(period).mean())


def rsi(df, period=14, column="close"):
    series = _numeric_series(df, column)
    if len(series) <= period:
        return None
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    if loss.iloc[-1] == 0:
        return 100.0
    rs = gain.iloc[-1] / loss.iloc[-1]
    return float(100 - (100 / (1 + rs)))


def atr(df, period=14):
    if df is None or df.empty or len(df) <= period:
        return None
    data = df.copy()
    for column in ["high", "low", "close"]:
        if column not in data.columns:
            return None
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data.dropna(subset=["high", "low", "close"])
    if len(data) <= period:
        return None
    previous_close = data["close"].shift(1)
    true_range = pd.concat(
        [
            data["high"] - data["low"],
            (data["high"] - previous_close).abs(),
            (data["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return float(true_range.rolling(period).mean().iloc[-1])


def vwap(df):
    if df is None or df.empty:
        return None
    data = df.copy()
    for column in ["high", "low", "close", "volume"]:
        if column not in data.columns:
            return None
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data.dropna(subset=["high", "low", "close", "volume"])
    if data.empty or data["volume"].sum() == 0:
        return None
    typical_price = (data["high"] + data["low"] + data["close"]) / 3
    return float((typical_price * data["volume"]).sum() / data["volume"].sum())


def support_resistance(df, lookback=48):
    if df is None or df.empty:
        return {"support": None, "resistance": None}
    data = df.tail(lookback).copy()
    if "low" not in data.columns or "high" not in data.columns:
        return {"support": None, "resistance": None}
    lows = pd.to_numeric(data["low"], errors="coerce").dropna()
    highs = pd.to_numeric(data["high"], errors="coerce").dropna()
    return {
        "support": float(lows.min()) if not lows.empty else None,
        "resistance": float(highs.max()) if not highs.empty else None,
    }


def realized_volatility_pct(df, lookback=60):
    closes = _numeric_series(df, "close").tail(lookback)
    if len(closes) < 20:
        return None
    returns = closes.pct_change().dropna()
    if returns.empty:
        return None
    periods_per_year = 365 * 24 * 12
    return float(returns.std() * (periods_per_year ** 0.5) * 100)


def trend_snapshot(df):
    close = _numeric_series(df, "close")
    last_close = float(close.iloc[-1]) if not close.empty else None
    ema_fast = ema(df, 9)
    ema_slow = ema(df, 21)
    sma_mid = sma(df, 50) if len(close) >= 50 else sma(df, 20)
    current_rsi = rsi(df)
    current_atr = atr(df)
    current_vwap = vwap(df.tail(96)) if df is not None and not df.empty else None
    levels = support_resistance(df)

    direction = "NEUTRAL"
    if last_close and ema_fast and ema_slow:
        if last_close > ema_fast > ema_slow:
            direction = "BULLISH"
        elif last_close < ema_fast < ema_slow:
            direction = "BEARISH"

    return {
        "last_close": last_close,
        "ema_9": ema_fast,
        "ema_21": ema_slow,
        "sma_context": sma_mid,
        "rsi": current_rsi,
        "atr": current_atr,
        "vwap": current_vwap,
        "support": levels["support"],
        "resistance": levels["resistance"],
        "direction": direction,
        "latest_timestamp": latest_timestamp(df),
        "realized_vol_pct": realized_volatility_pct(df),
    }
