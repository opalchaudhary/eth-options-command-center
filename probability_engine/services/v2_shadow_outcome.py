from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from probability_engine.config import HORIZON_MINUTES


RESOLUTION_SECONDS = 300
TARGETS = {
    "realized_over_range_width_ge_1",
    "path_inside_70",
    "range_breached",
    "both_side_breach",
    "upside_breakout",
    "downside_breakdown",
    "upper_breach_only",
    "lower_breach_only",
    "up_excursion_ge_1_0_atr",
    "down_excursion_ge_1_0_atr",
}


def parse_utc(value) -> datetime:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize(timezone.utc)
    return parsed.tz_convert(timezone.utc).to_pydatetime()


def prediction_window(prediction: dict[str, Any]) -> tuple[datetime, datetime] | None:
    created_at = parse_utc(prediction["prediction_timestamp"])
    minutes = HORIZON_MINUTES.get(str(prediction.get("horizon") or "").upper())
    if minutes is None:
        return None
    return created_at, created_at + timedelta(minutes=minutes)


def is_mature(prediction: dict[str, Any], now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    window = prediction_window(prediction)
    return bool(window and now >= window[1])


def future_window_candles(candles: pd.DataFrame, start_at, end_at) -> pd.DataFrame:
    if candles is None or candles.empty:
        return pd.DataFrame()
    frame = candles.copy()
    if "timestamp" not in frame and "candle_time" in frame:
        frame["timestamp"] = frame["candle_time"]
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    eligible_end = pd.Timestamp(end_at) - pd.Timedelta(seconds=RESOLUTION_SECONDS)
    return frame[(frame["timestamp"] >= pd.Timestamp(start_at)) & (frame["timestamp"] <= eligible_end)].sort_values("timestamp").reset_index(drop=True)


def has_complete_window(candles: pd.DataFrame, start_at, end_at) -> bool:
    if candles is None or candles.empty:
        return False
    first_ts = pd.Timestamp(candles.iloc[0]["timestamp"])
    last_ts = pd.Timestamp(candles.iloc[-1]["timestamp"])
    first_allowed = pd.Timestamp(start_at).ceil("5min")
    required_last = (pd.Timestamp(end_at) - pd.Timedelta(seconds=RESOLUTION_SECONDS)).floor("5min")
    return first_ts <= first_allowed and last_ts >= required_last


def evaluate_shadow_target(prediction: dict[str, Any], candles: pd.DataFrame, feature_snapshot: dict[str, Any]) -> dict[str, Any]:
    target = prediction.get("target")
    if target not in TARGETS:
        return {"ok": False, "reason": "UNSUPPORTED_TARGET"}
    window = prediction_window(prediction)
    if not window:
        return {"ok": False, "reason": "UNSUPPORTED_HORIZON"}
    future = future_window_candles(candles, *window)
    if not has_complete_window(future, *window):
        return {"ok": False, "reason": "INCOMPLETE_WINDOW"}

    open_ = float(future.iloc[0]["open"])
    high = float(future["high"].max())
    low = float(future["low"].min())
    close = float(future.iloc[-1]["close"])
    vector = feature_snapshot.get("feature_vector_json") or {}
    metadata = prediction.get("metadata_json") or {}
    range_lower, range_upper = range_bounds_from_metadata(metadata)
    atr = float(vector.get("atr_12b") or vector.get("atr_pct_12b") or 0)
    spot_proxy = open_
    atr_abs = atr * spot_proxy if atr and atr < 1 else atr
    max_up = high - open_
    max_down = open_ - low
    realized_path_range = (high - low) / spot_proxy if spot_proxy else None
    range_width = (range_upper - range_lower) if range_lower is not None and range_upper is not None else None
    realized_over_range_width = (high - low) / range_width if range_width else None

    outcome = None
    if target == "path_inside_70":
        outcome = high <= range_upper and low >= range_lower if range_width else None
    elif target == "range_breached":
        outcome = high > range_upper or low < range_lower if range_width else None
    elif target == "both_side_breach":
        outcome = high > range_upper and low < range_lower if range_width else None
    elif target == "upper_breach_only":
        outcome = high > range_upper and low >= range_lower if range_width else None
    elif target == "lower_breach_only":
        outcome = low < range_lower and high <= range_upper if range_width else None
    elif target == "realized_over_range_width_ge_1":
        outcome = realized_over_range_width >= 1 if realized_over_range_width is not None else None
    elif target == "up_excursion_ge_1_0_atr":
        outcome = max_up >= atr_abs if atr_abs else None
    elif target == "down_excursion_ge_1_0_atr":
        outcome = max_down >= atr_abs if atr_abs else None
    elif target == "upside_breakout":
        outcome = high > range_upper if range_width else None
    elif target == "downside_breakdown":
        outcome = low < range_lower if range_width else None

    return {
        "ok": outcome is not None,
        "outcome": outcome,
        "actual_open": open_,
        "actual_high": high,
        "actual_low": low,
        "actual_close": close,
        "maximum_up_excursion": max_up,
        "maximum_down_excursion": max_down,
        "realized_path_range": realized_path_range,
        "realized_over_range_width": realized_over_range_width,
        "metadata_json": {
            "target": target,
            "window_start": window[0].isoformat(),
            "window_end": window[1].isoformat(),
            "candle_count": int(len(future)),
            "semantics": "Probability V2 candidate v1 frozen shadow outcome semantics.",
        },
    }


def range_bounds_from_metadata(metadata: dict[str, Any]) -> tuple[float | None, float | None]:
    # Step 16 stores exact range-reference details in metadata before outcome
    # activation. If unavailable, the evaluator returns incomplete for
    # range-dependent targets instead of fabricating a target.
    lower = metadata.get("range_70_lower")
    upper = metadata.get("range_70_upper")
    return (float(lower), float(upper)) if lower is not None and upper is not None else (None, None)
