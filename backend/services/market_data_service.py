import logging

from data_refresh import refresh_market_structure_sources, refresh_options_sources
from rule_insights import build_rule_based_insights, get_available_expiries

from .delta_client import eth_market_snapshot, eth_option_chain, ohlcv_snapshot
from .json_utils import to_jsonable


logger = logging.getLogger(__name__)


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

    insights = build_rule_based_insights(selected_expiry)
    return {
        "ok": True,
        "expiry": selected_expiry,
        "expiries": expiries,
        "insights": to_jsonable(insights),
    }


def refresh_options(expiry=None):
    return to_jsonable(refresh_options_sources(expiry_label=expiry))


def refresh_market_sources():
    return to_jsonable(refresh_market_structure_sources())


def get_ohlcv(symbol="ETHUSD", resolution="5m", minutes_back=720):
    return ohlcv_snapshot(symbol=symbol, resolution=resolution, minutes_back=minutes_back)

