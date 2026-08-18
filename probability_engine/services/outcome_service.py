from probability_engine.config import get_probability_config


def _inside(value, lower, upper):
    if value is None or lower is None or upper is None:
        return None
    return float(lower) <= float(value) <= float(upper)


class OutcomeService:
    def __init__(self, config=None):
        self.config = config or get_probability_config()

    def evaluate_prediction(self, prediction, candles):
        if candles is None or candles.empty:
            return {"ok": False, "reason": "No candles available for outcome interval."}
        open_price = float(candles.iloc[0]["open"])
        high = float(candles["high"].max())
        low = float(candles["low"].min())
        close = float(candles.iloc[-1]["close"])
        max_up = high - open_price
        max_down = open_price - low
        metadata = prediction.metadata_json or {}
        initial_vwap_z = abs(metadata.get("initial_vwap_zscore") or 0)
        reversion_target = metadata.get("mean_reversion_target")
        upper_boundary = metadata.get("upper_boundary") or prediction.range_70_upper
        lower_boundary = metadata.get("lower_boundary") or prediction.range_70_lower
        trend_direction = metadata.get("trend_direction")
        trend_threshold = metadata.get("trend_threshold") or 0

        mean_reversion = False
        fraction = None
        if reversion_target is not None and initial_vwap_z >= self.config.minimum_initial_vwap_zscore:
            start = open_price
            required = abs(start - reversion_target) * self.config.reversion_fraction
            realized = max(0, start - low) if start > reversion_target else max(0, high - start)
            fraction = realized / abs(start - reversion_target) if start != reversion_target else 0
            mean_reversion = realized >= required

        range_50_covered = _inside(close, prediction.range_50_lower, prediction.range_50_upper)
        range_70_covered = _inside(close, prediction.range_70_lower, prediction.range_70_upper)
        range_90_covered = _inside(close, prediction.range_90_lower, prediction.range_90_upper)

        return {
            "actual_open": open_price,
            "actual_high": high,
            "actual_low": low,
            "actual_close": close,
            "maximum_up_excursion": max_up,
            "maximum_down_excursion": max_down,
            "mean_reversion_occurred": mean_reversion,
            "mean_reversion_fraction": fraction,
            "upside_breakout_occurred": bool(upper_boundary is not None and high > upper_boundary),
            "downside_breakdown_occurred": bool(lower_boundary is not None and low < lower_boundary),
            "range_held": bool((upper_boundary is None or high <= upper_boundary) and (lower_boundary is None or low >= lower_boundary)),
            "trend_continuation_occurred": bool(
                (trend_direction == "UP" and close >= open_price + trend_threshold)
                or (trend_direction == "DOWN" and close <= open_price - trend_threshold)
            ),
            "range_50_covered": range_50_covered,
            "range_70_covered": range_70_covered,
            "range_90_covered": range_90_covered,
            "upper_touch_occurred": bool(upper_boundary is not None and high >= upper_boundary),
            "lower_touch_occurred": bool(lower_boundary is not None and low <= lower_boundary),
        }
