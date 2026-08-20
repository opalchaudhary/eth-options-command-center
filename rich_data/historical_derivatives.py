from datetime import timedelta

from rich_data import delta_public
from rich_data.time_utils import utc_now


def probe_derivatives_history(symbol="ETHUSD", resolution="5m", hours=1):
    end = int(utc_now().timestamp())
    start = int((utc_now() - timedelta(hours=hours)).timestamp())
    results = {}
    for source_symbol in [f"FUNDING:{symbol}", f"OI:{symbol}", f"MARK:{symbol}"]:
        candles = delta_public.get_historical_candles(
            source_symbol,
            resolution=resolution,
            start=start,
            end=end,
        )
        results[source_symbol] = {
            "row_count": len(candles),
            "earliest_time": min((item.get("time") for item in candles), default=None),
            "latest_time": max((item.get("time") for item in candles), default=None),
            "fields": sorted(candles[0].keys()) if candles else [],
        }
    return results

