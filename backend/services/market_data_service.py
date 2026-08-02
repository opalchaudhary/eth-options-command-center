import logging
import time

import database_reader
from data_refresh import refresh_market_structure_sources, refresh_options_sources
from rule_insights import build_rule_based_insights, get_available_expiries

from .delta_client import eth_market_snapshot, eth_option_chain, ohlcv_snapshot
from .json_utils import to_jsonable


logger = logging.getLogger(__name__)
INSIGHTS_CACHE_TTL_SECONDS = 30
_insights_cache = {}


def get_eth_market():
    return eth_market_snapshot(include_orderbook=False)


def _compact_insights(insights):
    pricing = insights.get("strategy_pricing") or {}
    risk_reward = insights.get("strategy_risk_reward") or {}
    return {
        "generated_at": insights.get("generated_at"),
        "spot_price": insights.get("spot_price"),
        "atm_strike": insights.get("atm_strike"),
        "max_pain": insights.get("max_pain"),
        "expected_move": insights.get("expected_move"),
        "expiry_profile": insights.get("expiry_profile") or {},
        "median_iv": insights.get("median_iv"),
        "realized_vol_pct": insights.get("realized_vol_pct"),
        "iv_rv_spread": insights.get("iv_rv_spread"),
        "market_regime": insights.get("market_regime"),
        "directional_bias": insights.get("directional_bias"),
        "volatility_regime": insights.get("volatility_regime"),
        "best_strategy": insights.get("best_strategy"),
        "confidence": insights.get("confidence_score"),
        "confidence_score": insights.get("confidence_score"),
        "risk_score": insights.get("signal_conflict_score"),
        "signal_conflict_score": insights.get("signal_conflict_score"),
        "trap_risk": insights.get("trap_risk"),
        "option_selling_environment": insights.get("option_selling_environment"),
        "selected_expiry": insights.get("expiry_label"),
        "expiry_label": insights.get("expiry_label"),
        "selected_strikes": [
            leg.get("strike")
            for leg in (pricing.get("legs") or insights.get("strategy_legs") or [])
            if isinstance(leg, dict) and leg.get("strike") is not None
        ],
        "greeks_summary": insights.get("greeks_summary") or {},
        "key_warnings": insights.get("risk_warnings") or [],
        "key_insights": insights.get("key_insights") or [],
        "strategy_pricing": pricing,
        "strategy_legs": pricing.get("legs") or insights.get("strategy_legs") or [],
        "strategy_candidates": (insights.get("strategy_candidates") or [])[:10],
        "strategy_risk_reward": {
            key: risk_reward.get(key)
            for key in ["quality", "reward_risk", "effective_return_pct", "max_profit_usdt", "max_loss_usdt"]
        },
        "data_flags": insights.get("data_flags") or {},
        "missing_sources": insights.get("missing_sources") or [],
        "last_updated": insights.get("generated_at"),
    }


def get_option_chain(expiry=None, limit=500, compact=True, include_raw=False):
    limit = min(int(limit or 500), 1000)
    response = eth_option_chain(expiry=expiry)
    rows = response.get("rows") or []
    if compact and not include_raw:
        compact_rows = []
        for row in rows[:limit]:
            compact_rows.append(
                {
                    "expiry": row.get("expiry"),
                    "strike": row.get("strike"),
                    "type": row.get("type"),
                    "mark_price": row.get("mark_price"),
                    "oi": row.get("oi"),
                    "volume": row.get("volume"),
                    "iv": row.get("iv"),
                    "delta": row.get("delta"),
                    "gamma": row.get("gamma"),
                    "theta": row.get("theta"),
                    "vega": row.get("vega"),
                }
            )
        response["rows"] = compact_rows
    elif limit:
        response["rows"] = rows[:limit]
    return response


def get_insights(expiry=None, compact=True, include_raw=False):
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
            "insights": to_jsonable(insights if include_raw else _compact_insights(insights)),
        }

    return {
        "ok": True,
        "expiry": selected_expiry,
        "expiries": expiries,
        "insights": to_jsonable(insights if include_raw or not compact else _compact_insights(insights)),
    }


def get_charts(expiry=None, symbol="ETHUSD", limit=300):
    limit = min(int(limit or 300), 500)
    return {
        "ok": True,
        "analytics": to_jsonable(database_reader.get_analytics_snapshots(expiry_label=expiry, limit=limit)),
        "premium_decay": to_jsonable(database_reader.get_premium_decay_snapshots(expiry_label=expiry, limit=limit)),
        "option_chain": to_jsonable(database_reader.get_option_chain_snapshots(expiry_label=expiry, limit=limit)),
        "orderbook": to_jsonable(database_reader.get_orderbook_insight_snapshots(symbol=symbol, limit=limit)),
    }


def refresh_options(expiry=None):
    _insights_cache.clear()
    return to_jsonable(refresh_options_sources(expiry_label=expiry))


def refresh_market_sources(include_smc=True):
    _insights_cache.clear()
    return to_jsonable(refresh_market_structure_sources(include_smc=include_smc))


def get_ohlcv(symbol="ETHUSD", resolution="5m", minutes_back=720):
    return ohlcv_snapshot(symbol=symbol, resolution=resolution, minutes_back=minutes_back)
