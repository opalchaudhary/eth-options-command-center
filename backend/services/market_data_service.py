import logging
import time

from data_refresh import refresh_market_structure_sources, refresh_options_sources
from rule_insights import build_rule_based_insights, get_available_expiries

from .delta_client import eth_market_snapshot, eth_option_chain, ohlcv_snapshot
from .json_utils import to_jsonable


logger = logging.getLogger(__name__)
INSIGHTS_CACHE_TTL_SECONDS = 30
_insights_cache = {}


def get_eth_market():
    return eth_market_snapshot(include_orderbook=True)


def get_option_chain(expiry=None):
    return eth_option_chain(expiry=expiry)


def get_insights(expiry=None):
    expiries = get_available_expiries(limit=20)
    selected_expiry = expiry or (expiries[0] if expiries else None)

    if not selected_expiry:
        return {
            "ok": False,
            "error": "No option expiries are available in Supabase. Refresh option sources first.",
            "expiries": [],
            "insights": None,
        }

    cache_key = str(selected_expiry)
    cached = _insights_cache.get(cache_key)
    now = time.monotonic()

    if cached and now - cached["created_at"] <= INSIGHTS_CACHE_TTL_SECONDS:
        insights = cached["insights"]
    else:
        insights = build_rule_based_insights(selected_expiry, allow_live_delta_fallback=False)
        _insights_cache[cache_key] = {
            "created_at": now,
            "insights": insights,
        }

    if not insights.get("data_flags", {}).get("option_chain"):
        return {
            "ok": False,
            "error": "No saved option-chain snapshot is available yet. The backend scheduler will refresh data in the background; please retry shortly.",
            "expiry": selected_expiry,
            "expiries": expiries,
            "insights": to_jsonable(insights),
        }

    return {
        "ok": True,
        "expiry": selected_expiry,
        "expiries": expiries,
        "insights": to_jsonable(insights),
    }


def refresh_options(expiry=None):
    _insights_cache.clear()
    return to_jsonable(refresh_options_sources(expiry_label=expiry))


def refresh_market_sources(include_smc=True):
    _insights_cache.clear()
    return to_jsonable(refresh_market_structure_sources(include_smc=include_smc))


def get_ohlcv(symbol="ETHUSD", resolution="5m", minutes_back=720):
    return ohlcv_snapshot(symbol=symbol, resolution=resolution, minutes_back=minutes_back)
