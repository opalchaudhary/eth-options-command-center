from datetime import datetime, timezone

import pandas as pd

from recommendation_journal import _request, read_table
from validation_config import usdt_to_inr


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value, default=None):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _strategy_direction(strategy, directional_bias):
    if strategy in ["Bull Put Credit Spread"]:
        return "BULLISH"
    if strategy in ["Bear Call Credit Spread", "Put Broken Wing Butterfly"]:
        return "BEARISH"
    if strategy == "Debit Spread":
        if directional_bias in ["Bullish", "Mild Bullish"]:
            return "BULLISH"
        if directional_bias in ["Bearish", "Mild Bearish"]:
            return "BEARISH"
    if strategy in ["Iron Fly", "Iron Condor", "Short Strangle", "Short Straddle with Hedge"]:
        return "RANGE"
    return "WAIT"


def _latest_candles_after(created_at, hours):
    start = pd.to_datetime(created_at, utc=True, errors="coerce")

    if pd.isna(start):
        return pd.DataFrame()

    target = start + pd.Timedelta(hours=hours)

    candles = read_table(
        "eth_ohlcv",
        {
            "select": "candle_time,open,high,low,close,volume",
            "candle_time": f"gte.{start.isoformat()}",
            "order": "candle_time.asc",
            "limit": 1000,
        },
    )

    if candles.empty or "candle_time" not in candles.columns:
        return pd.DataFrame()

    candles["candle_time"] = pd.to_datetime(candles["candle_time"], utc=True, errors="coerce")
    candles = candles[candles["candle_time"] <= target].copy()
    return candles.reset_index(drop=True)


def _price_at_or_after(created_at, hours):
    candles = _latest_candles_after(created_at, hours)

    if candles.empty:
        return None

    return _safe_float(candles.iloc[-1].get("close"))


def _path_stats(created_at, hours):
    candles = _latest_candles_after(created_at, hours)

    if candles.empty:
        return {}

    for col in ["high", "low", "close"]:
        candles[col] = pd.to_numeric(candles[col], errors="coerce")

    return {
        "max_high": _safe_float(candles["high"].max()),
        "min_low": _safe_float(candles["low"].min()),
        "last_close": _safe_float(candles["close"].iloc[-1]),
    }


def _evaluate_profit(direction, entry_spot, current_spot, expected_move=None):
    if entry_spot is None or current_spot is None:
        return None

    if direction == "BULLISH":
        return current_spot > entry_spot

    if direction == "BEARISH":
        return current_spot < entry_spot

    if direction == "RANGE":
        band = expected_move if expected_move else entry_spot * 0.015
        return abs(current_spot - entry_spot) <= band

    return None


def build_outcome_payload(recommendation):
    rec_json = recommendation.get("recommendation_json") or {}
    raw = recommendation.get("raw_input_snapshot") or {}
    analytics = raw.get("analytics") or {}
    entry_spot = _safe_float(recommendation.get("spot_price"))
    expected_move = _safe_float(analytics.get("atm_straddle_price"))
    expiry_profile = raw.get("expiry_profile") or {}
    expiry_timestamp = expiry_profile.get("expiry_timestamp") or recommendation.get("expiry_label")
    expiry_time = pd.to_datetime(expiry_timestamp, utc=True, errors="coerce")
    direction = _strategy_direction(
        recommendation.get("suggested_strategy"),
        recommendation.get("directional_bias"),
    )

    spot_1h = _price_at_or_after(recommendation.get("created_at"), 1)
    spot_3h = _price_at_or_after(recommendation.get("created_at"), 3)
    path_3h = _path_stats(recommendation.get("created_at"), 3)
    spot_expiry = None

    if not pd.isna(expiry_time) and pd.Timestamp.utcnow() >= expiry_time:
        created_at = pd.to_datetime(recommendation.get("created_at"), utc=True, errors="coerce")
        if not pd.isna(created_at):
            hours_to_expiry = max(0, (expiry_time - created_at).total_seconds() / 3600)
            spot_expiry = _price_at_or_after(recommendation.get("created_at"), hours_to_expiry)

    if entry_spot and path_3h:
        max_favourable, max_adverse = _excursions(direction, entry_spot, path_3h)
    else:
        max_favourable, max_adverse = None, None

    profit_1h = _evaluate_profit(direction, entry_spot, spot_1h, expected_move)
    profit_3h = _evaluate_profit(direction, entry_spot, spot_3h, expected_move)
    profit_expiry = _evaluate_profit(direction, entry_spot, spot_expiry, expected_move)
    confidence = int(recommendation.get("confidence_score") or 0)
    confidence_matched = None

    if profit_3h is not None:
        confidence_matched = (confidence >= 60 and profit_3h) or (confidence < 60 and not profit_3h)

    return {
        "recommendation_id": recommendation.get("id"),
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "result_1h": _result_label(profit_1h),
        "result_3h": _result_label(profit_3h),
        "result_expiry": _result_label(profit_expiry),
        "spot_1h": spot_1h,
        "spot_3h": spot_3h,
        "spot_expiry": spot_expiry,
        "max_favourable_excursion": max_favourable,
        "max_adverse_excursion": max_adverse,
        "strategy_profitable_1h": profit_1h,
        "strategy_profitable_3h": profit_3h,
        "strategy_profitable_expiry": profit_expiry,
        "confidence_matched_actual": confidence_matched,
        "outcome_json": {
            "direction": direction,
            "entry_spot": entry_spot,
            "expected_move": expected_move,
            "recommendation": rec_json,
        },
    }


def _excursions(direction, entry_spot, path):
    max_high = _safe_float(path.get("max_high"), entry_spot)
    min_low = _safe_float(path.get("min_low"), entry_spot)

    if direction == "BULLISH":
        return round(max_high - entry_spot, 4), round(entry_spot - min_low, 4)

    if direction == "BEARISH":
        return round(entry_spot - min_low, 4), round(max_high - entry_spot, 4)

    if direction == "RANGE":
        max_move = max(abs(max_high - entry_spot), abs(entry_spot - min_low))
        return round(max(0, entry_spot * 0.015 - max_move), 4), round(max_move, 4)

    return None, None


def _result_label(value):
    if value is True:
        return "WIN"
    if value is False:
        return "LOSS"
    return "PENDING"


def upsert_recommendation_outcome(recommendation):
    if not recommendation.get("id"):
        return None

    payload = build_outcome_payload(recommendation)
    result = _request(
        "POST",
        "recommendation_outcomes",
        payload=payload,
        params={"on_conflict": "recommendation_id"},
        prefer="resolution=merge-duplicates,return=representation",
    )

    if isinstance(result, list) and result:
        return result[0]

    return payload


def refresh_recent_outcomes(limit=50):
    recommendations = read_table(
        "recommendation_journal",
        {
            "select": "*",
            "order": "created_at.desc",
            "limit": limit,
        },
    )

    updated = []

    for _, recommendation in recommendations.iterrows():
        outcome = upsert_recommendation_outcome(recommendation.to_dict())
        if outcome:
            updated.append(outcome)

    return updated


def get_recommendation_outcomes(limit=200):
    return read_table(
        "recommendation_outcomes",
        {
            "select": "*",
            "order": "updated_at.desc",
            "limit": limit,
        },
    )
