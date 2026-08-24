from __future__ import annotations

import math

import numpy as np
import pandas as pd


EPS = 1e-12


def prepare_ohlcv(frame: pd.DataFrame) -> pd.DataFrame:
    data = frame.copy()
    if "candle_time" in data.columns:
        data["timestamp"] = pd.to_datetime(data["candle_time"], utc=True)
    else:
        data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
    for column in ["open", "high", "low", "close", "volume"]:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data.dropna(subset=["timestamp", "open", "high", "low", "close", "volume"])
    return data.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    data = prepare_ohlcv(frame)
    close = data["close"]
    high = data["high"]
    low = data["low"]
    open_ = data["open"]
    volume = data["volume"].clip(lower=0)
    prev_close = close.shift(1)

    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    data["true_range"] = true_range
    returns = close.pct_change()
    log_returns = np.log(close / close.shift(1)).replace([np.inf, -np.inf], np.nan)

    for window in [3, 6, 12, 24, 48, 96, 288]:
        data[f"return_{window}b"] = close.pct_change(window)
        data[f"roc_{window}b"] = close.pct_change(window)
        data[f"rolling_high_dist_{window}b"] = (close / high.rolling(window, min_periods=window).max()) - 1
        data[f"rolling_low_dist_{window}b"] = (close / low.rolling(window, min_periods=window).min()) - 1

    for window in [6, 12, 24, 48, 96, 288]:
        sma = close.rolling(window, min_periods=window).mean()
        ema = close.ewm(span=window, adjust=False, min_periods=window).mean()
        dema = 2 * ema - ema.ewm(span=window, adjust=False, min_periods=window).mean()
        ema2 = ema.ewm(span=window, adjust=False, min_periods=window).mean()
        tema = 3 * ema - 3 * ema2 + ema2.ewm(span=window, adjust=False, min_periods=window).mean()
        atr = true_range.rolling(window, min_periods=window).mean()
        rv = log_returns.rolling(window, min_periods=window).std(ddof=0) * math.sqrt(365 * 24 * 12) * 100
        vol_mean = volume.rolling(window, min_periods=window).mean()
        vol_std = volume.rolling(window, min_periods=window).std(ddof=0)

        data[f"sma_dist_{window}b"] = (close - sma) / close
        data[f"ema_dist_{window}b"] = (close - ema) / close
        data[f"dema_dist_{window}b"] = (close - dema) / close
        data[f"tema_dist_{window}b"] = (close - tema) / close
        data[f"sma_slope_{window}b"] = sma.diff(max(1, window // 4)) / sma
        data[f"ema_slope_{window}b"] = ema.diff(max(1, window // 4)) / ema
        data[f"atr_{window}b"] = atr
        data[f"atr_pct_{window}b"] = atr / close
        data[f"atr_slope_{window}b"] = atr.diff(max(1, window // 4)) / atr
        data[f"rv_{window}b"] = rv
        data[f"rv_slope_{window}b"] = rv.diff(max(1, window // 4)) / rv
        data[f"volume_rel_{window}b"] = volume / vol_mean
        data[f"volume_z_{window}b"] = (volume - vol_mean) / vol_std.replace(0, np.nan)
        data[f"price_efficiency_{window}b"] = close.diff(window).abs() / close.diff().abs().rolling(window, min_periods=window).sum()

    for fast, slow in [(6, 24), (12, 48), (24, 96), (48, 288)]:
        fast_ema = close.ewm(span=fast, adjust=False, min_periods=fast).mean()
        slow_ema = close.ewm(span=slow, adjust=False, min_periods=slow).mean()
        atr_slow = true_range.rolling(slow, min_periods=slow).mean()
        spread = fast_ema - slow_ema
        data[f"ema_spread_{fast}_{slow}b"] = spread / close
        data[f"ema_spread_atr_{fast}_{slow}b"] = spread / atr_slow.replace(0, np.nan)
        data[f"macd_hist_{fast}_{slow}b"] = spread - spread.ewm(span=max(3, fast), adjust=False, min_periods=max(3, fast)).mean()
        data[f"macd_hist_slope_{fast}_{slow}b"] = data[f"macd_hist_{fast}_{slow}b"].diff(max(1, fast // 2))

    for window in [6, 14, 24, 48]:
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(window, min_periods=window).mean()
        loss = (-delta.clip(upper=0)).rolling(window, min_periods=window).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        rsi = rsi.where(~((loss == 0) & (gain > 0)), 100.0)
        rsi = rsi.where(~((gain == 0) & (loss > 0)), 0.0)
        rsi = rsi.where(~((gain == 0) & (loss == 0)), 50.0)
        data[f"rsi_{window}b"] = rsi
        data[f"rsi_neutral_dist_{window}b"] = (rsi - 50) / 50
        data[f"rsi_slope_{window}b"] = rsi.diff(max(1, window // 4))

        low_min = low.rolling(window, min_periods=window).min()
        high_max = high.rolling(window, min_periods=window).max()
        stoch = (close - low_min) / (high_max - low_min).replace(0, np.nan)
        data[f"stoch_k_{window}b"] = stoch
        rsi_min = rsi.rolling(window, min_periods=window).min()
        rsi_max = rsi.rolling(window, min_periods=window).max()
        data[f"stoch_rsi_{window}b"] = (rsi - rsi_min) / (rsi_max - rsi_min).replace(0, np.nan)

    for window in [20, 48, 96]:
        basis = close.rolling(window, min_periods=window).mean()
        sd = close.rolling(window, min_periods=window).std(ddof=0)
        upper = basis + 2 * sd
        lower = basis - 2 * sd
        bandwidth = (upper - lower) / basis
        atr = true_range.rolling(window, min_periods=window).mean()
        keltner_width = (4 * atr) / basis
        data[f"bollinger_bandwidth_{window}b"] = bandwidth
        data[f"bollinger_pct_b_{window}b"] = (close - lower) / (upper - lower).replace(0, np.nan)
        data[f"bollinger_squeeze_pctile_{window}b"] = rolling_percentile(bandwidth, max(window * 4, 96))
        data[f"keltner_width_{window}b"] = keltner_width
        data[f"bb_keltner_width_ratio_{window}b"] = bandwidth / keltner_width.replace(0, np.nan)

    typical = (high + low + close) / 3
    raw_money_flow = typical * volume
    direction = typical.diff()
    for window in [24, 96, 288]:
        vwap = (typical * volume).rolling(window, min_periods=window).sum() / volume.rolling(window, min_periods=window).sum()
        atr = true_range.rolling(window, min_periods=window).mean()
        data[f"rolling_vwap_dist_{window}b"] = (close - vwap) / close
        data[f"rolling_vwap_z_{window}b"] = (close - vwap) / atr.replace(0, np.nan)

        donchian_low = low.rolling(window, min_periods=window).min()
        donchian_high = high.rolling(window, min_periods=window).max()
        data[f"donchian_pos_{window}b"] = (close - donchian_low) / (donchian_high - donchian_low).replace(0, np.nan)
        data[f"donchian_breakout_up_{window}b"] = (close >= donchian_high.shift(1)).astype(float)
        data[f"donchian_breakout_down_{window}b"] = (close <= donchian_low.shift(1)).astype(float)

        pos_flow = raw_money_flow.where(direction > 0, 0).rolling(window, min_periods=window).sum()
        neg_flow = raw_money_flow.where(direction < 0, 0).rolling(window, min_periods=window).sum()
        mfr = pos_flow / neg_flow.replace(0, np.nan)
        data[f"mfi_{window}b"] = 100 - (100 / (1 + mfr))
        data[f"volume_percentile_{window}b"] = rolling_percentile(volume, window)
        data[f"choppiness_{window}b"] = 100 * np.log10(true_range.rolling(window, min_periods=window).sum() / (donchian_high - donchian_low).replace(0, np.nan)) / np.log10(window)

    for window in [14, 24, 48]:
        plus_dm = (high.diff()).where((high.diff() > -low.diff()) & (high.diff() > 0), 0)
        minus_dm = (-low.diff()).where((-low.diff() > high.diff()) & (-low.diff() > 0), 0)
        atr = true_range.rolling(window, min_periods=window).mean()
        plus_di = 100 * plus_dm.rolling(window, min_periods=window).mean() / atr.replace(0, np.nan)
        minus_di = 100 * minus_dm.rolling(window, min_periods=window).mean() / atr.replace(0, np.nan)
        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
        data[f"adx_{window}b"] = dx.rolling(window, min_periods=window).mean()
        data[f"plus_di_{window}b"] = plus_di
        data[f"minus_di_{window}b"] = minus_di
        data[f"di_spread_{window}b"] = (plus_di - minus_di) / 100

        high_since = high.rolling(window + 1, min_periods=window + 1).apply(lambda x: window - int(np.argmax(x)), raw=True)
        low_since = low.rolling(window + 1, min_periods=window + 1).apply(lambda x: window - int(np.argmin(x)), raw=True)
        data[f"aroon_up_{window}b"] = 100 * (window - high_since) / window
        data[f"aroon_down_{window}b"] = 100 * (window - low_since) / window
        data[f"aroon_osc_{window}b"] = data[f"aroon_up_{window}b"] - data[f"aroon_down_{window}b"]

    body = (close - open_).abs()
    candle_range = (high - low).replace(0, np.nan)
    data["close_location_value"] = ((close - low) - (high - close)) / candle_range
    data["body_range_ratio"] = body / candle_range
    data["upper_wick_ratio"] = (high - pd.concat([open_, close], axis=1).max(axis=1)) / candle_range
    data["lower_wick_ratio"] = (pd.concat([open_, close], axis=1).min(axis=1) - low) / candle_range
    data["consecutive_up_bars"] = consecutive_count(close.diff() > 0)
    data["consecutive_down_bars"] = consecutive_count(close.diff() < 0)

    obv_direction = np.sign(close.diff()).fillna(0)
    data["obv"] = (obv_direction * volume).cumsum()
    for window in [24, 96, 288]:
        data[f"obv_slope_{window}b"] = data["obv"].diff(window) / volume.rolling(window, min_periods=window).sum().replace(0, np.nan)

    return data.replace([np.inf, -np.inf], np.nan)


def rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=window).apply(
        lambda values: float(np.sum(values <= values[-1]) / len(values)),
        raw=True,
    )


def consecutive_count(mask: pd.Series) -> pd.Series:
    groups = (mask != mask.shift()).cumsum()
    counts = mask.groupby(groups).cumcount() + 1
    return counts.where(mask, 0).astype(float)


def feature_columns(frame: pd.DataFrame) -> list[str]:
    excluded = {"timestamp", "candle_time", "epoch_time", "symbol", "resolution", "id", "open", "high", "low", "close", "volume"}
    return [
        column
        for column in frame.columns
        if column not in excluded and pd.api.types.is_numeric_dtype(frame[column])
    ]
