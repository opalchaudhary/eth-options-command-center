from probability_engine.config import get_probability_config

LABEL_VERSION_V2 = "label_v2"


def _inside(value, lower, upper):
    if value is None or lower is None or upper is None:
        return None
    return float(lower) <= float(value) <= float(upper)


def _value(source, name):
    if source is None:
        return None
    if isinstance(source, dict):
        return source.get(name)
    return getattr(source, name, None)


def _float_or_none(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _same_sign(first, second):
    return (first > 0 and second > 0) or (first < 0 and second < 0)


def _trend_direction(return_1h, return_4h, atr_pct):
    ret_1h = _float_or_none(return_1h)
    ret_4h = _float_or_none(return_4h)
    if ret_1h is None:
        return None
    if ret_4h is not None and _same_sign(ret_1h, ret_4h):
        return "UP" if ret_1h > 0 else "DOWN"

    threshold = max(_float_or_none(atr_pct) or 0, 0.001)
    candidates = [ret_1h]
    if ret_4h is not None:
        candidates.append(ret_4h)
    strongest = max(candidates, key=abs)
    if abs(strongest) >= threshold:
        return "UP" if strongest > 0 else "DOWN"
    return None


class OutcomeService:
    def __init__(self, config=None):
        self.config = config or get_probability_config()

    def evaluate_prediction(self, prediction, candles, snapshot=None):
        if candles is None or candles.empty:
            return {"ok": False, "reason": "No candles available for outcome interval."}
        open_price = float(candles.iloc[0]["open"])
        high = float(candles["high"].max())
        low = float(candles["low"].min())
        close = float(candles.iloc[-1]["close"])
        max_up = high - open_price
        max_down = open_price - low
        spot_price = _float_or_none(_value(snapshot, "spot_price"))
        vwap = _float_or_none(_value(snapshot, "vwap"))
        vwap_zscore = _float_or_none(_value(snapshot, "vwap_zscore"))
        atr = _float_or_none(_value(snapshot, "atr"))
        atr_pct = _float_or_none(_value(snapshot, "atr_pct"))
        return_1h = _float_or_none(_value(snapshot, "return_1h"))
        return_4h = _float_or_none(_value(snapshot, "return_4h"))
        upper_boundary = _float_or_none(prediction.range_70_upper)
        lower_boundary = _float_or_none(prediction.range_70_lower)

        if spot_price is None or vwap is None or vwap_zscore is None:
            return {"ok": False, "reason": "Linked snapshot is missing required Label V2 VWAP fields."}

        mean_reversion = False
        mean_reversion_eligible = abs(vwap_zscore) >= self.config.minimum_initial_vwap_zscore and spot_price != vwap
        fraction = None
        initial_distance = abs(spot_price - vwap)
        minimum_distance_to_vwap = None
        if mean_reversion_eligible:
            if low <= vwap <= high:
                minimum_distance_to_vwap = 0.0
            elif spot_price > vwap:
                minimum_distance_to_vwap = min(abs(low - vwap), abs(high - vwap))
            else:
                minimum_distance_to_vwap = min(abs(high - vwap), abs(low - vwap))
            fraction = 1 - (minimum_distance_to_vwap / initial_distance)
            fraction = max(0.0, min(1.0, fraction))
            mean_reversion = fraction >= self.config.reversion_fraction

        range_50_covered = _inside(close, prediction.range_50_lower, prediction.range_50_upper)
        range_70_covered = _inside(close, prediction.range_70_lower, prediction.range_70_upper)
        range_90_covered = _inside(close, prediction.range_90_lower, prediction.range_90_upper)
        range_held = bool(
            upper_boundary is not None
            and lower_boundary is not None
            and high <= upper_boundary
            and low >= lower_boundary
        )

        trend_direction = _trend_direction(return_1h, return_4h, atr_pct)
        trend_threshold = None
        trend_eligible = trend_direction is not None and atr is not None and spot_price > 0
        if trend_eligible:
            trend_threshold = max(0.25 * atr, 0.001 * spot_price)
        trend_continuation = bool(
            trend_eligible
            and (
                (trend_direction == "UP" and close >= spot_price + trend_threshold)
                or (trend_direction == "DOWN" and close <= spot_price - trend_threshold)
            )
        )

        return {
            "label_version": LABEL_VERSION_V2,
            "actual_open": open_price,
            "actual_high": high,
            "actual_low": low,
            "actual_close": close,
            "maximum_up_excursion": max_up,
            "maximum_down_excursion": max_down,
            "mean_reversion_occurred": mean_reversion,
            "mean_reversion_fraction": fraction,
            "upside_breakout_occurred": bool(upper_boundary is not None and high >= upper_boundary),
            "downside_breakdown_occurred": bool(lower_boundary is not None and low <= lower_boundary),
            "range_held": range_held,
            "trend_continuation_occurred": trend_continuation,
            "range_50_covered": range_50_covered,
            "range_70_covered": range_70_covered,
            "range_90_covered": range_90_covered,
            "upper_touch_occurred": bool(upper_boundary is not None and high >= upper_boundary),
            "lower_touch_occurred": bool(lower_boundary is not None and low <= lower_boundary),
            "metadata_json": {
                "label_version": LABEL_VERSION_V2,
                "mean_reversion_eligible": mean_reversion_eligible,
                "mean_reversion_target": vwap,
                "initial_equilibrium_distance": initial_distance,
                "minimum_distance_to_vwap": minimum_distance_to_vwap,
                "trend_continuation_eligible": trend_eligible,
                "trend_direction": trend_direction,
                "trend_threshold": trend_threshold,
                "trend_direction_rule": "return_1h and return_4h agree, else strongest normalized momentum above max(atr_pct, 0.001)",
                "range_continuation_event": "range_held",
                "breakout_boundary": upper_boundary,
                "breakdown_boundary": lower_boundary,
                "frozen_spot_price": spot_price,
                "frozen_vwap": vwap,
                "frozen_vwap_zscore": vwap_zscore,
                "frozen_atr": atr,
                "frozen_atr_pct": atr_pct,
                "frozen_return_1h": return_1h,
                "frozen_return_4h": return_4h,
            },
        }
