from ohlcv_job import run_ohlcv_job
from orderbook_engine import get_eth_orderbook_insights
from smc_job import run_smc_job
import pandas as pd

from delta_api import get_eth_options, get_eth_spot_price
from analytics import (
    basic_expiry_analytics,
    calculate_atm_and_expected_move,
    calculate_max_pain,
)
from storage import (
    save_analytics_snapshot,
    save_option_chain_snapshot,
    save_orderbook_insights,
    save_premium_decay_snapshot,
)


def _expiry_key(expiry_label):
    parsed = pd.to_datetime(expiry_label, utc=True, errors="coerce")

    if pd.isna(parsed):
        return str(expiry_label)

    return parsed.isoformat()


def _matching_expiry_label(expiries, requested_expiry):
    if not requested_expiry:
        return None

    requested_key = _expiry_key(requested_expiry)

    for expiry in expiries:
        if str(expiry) == str(requested_expiry):
            return expiry

    for expiry in expiries:
        if _expiry_key(expiry) == requested_key:
            return expiry

    return None


def _save_option_sources_for_expiry(
    options_df,
    spot_price,
    source_expiry_label,
    storage_expiry_label=None,
):
    storage_expiry_label = storage_expiry_label or source_expiry_label
    expiry_df = options_df[options_df["expiry"] == source_expiry_label].copy()

    if expiry_df.empty:
        return {
            "expiry_label": storage_expiry_label,
            "matched_expiry": None,
            "analytics_saved": False,
            "premium_saved": False,
            "option_chain_saved": False,
            "rows": 0,
            "error": "No option rows matched this expiry label.",
        }

    analytics = basic_expiry_analytics(expiry_df)
    max_pain, _ = calculate_max_pain(expiry_df)
    atm_strike, expected_move, atm_ce_price, atm_pe_price = calculate_atm_and_expected_move(
        expiry_df,
        spot_price,
    )

    expected_move_pct = None
    expected_move_upper = None
    expected_move_lower = None

    if spot_price and expected_move:
        expected_move_pct = (expected_move / spot_price) * 100
        expected_move_upper = spot_price + expected_move
        expected_move_lower = spot_price - expected_move

    snapshot_analytics = {
        "spot_price": spot_price,
        "max_pain": max_pain,
        "atm_strike": atm_strike,
        "pcr": analytics.get("pcr"),
        "atm_straddle_price": expected_move,
        "expected_move_pct": expected_move_pct,
        "expected_move_upper": expected_move_upper,
        "expected_move_lower": expected_move_lower,
    }

    return {
        "expiry_label": storage_expiry_label,
        "matched_expiry": source_expiry_label,
        "analytics_saved": save_analytics_snapshot(snapshot_analytics, storage_expiry_label),
        "premium_saved": save_premium_decay_snapshot(
            storage_expiry_label,
            atm_strike,
            atm_ce_price,
            atm_pe_price,
            expected_move,
        ),
        "option_chain_saved": save_option_chain_snapshot(expiry_df, storage_expiry_label),
        "rows": len(expiry_df),
    }


def refresh_options_sources(expiry_label=None):
    """
    Populate option-chain dependent tables directly from Delta.
    This keeps Insights independent from app.py.
    """

    options_df = get_eth_options()
    spot_data = get_eth_spot_price()
    spot_price = spot_data.get("spot_price")

    if options_df.empty:
        return {
            "ok": False,
            "spot_price": spot_price,
            "expiry_count": 0,
            "row_count": 0,
            "results": [],
        }

    available_expiries = sorted(options_df["expiry"].dropna().unique())
    requested_expiry = expiry_label

    if expiry_label:
        matched_expiry = _matching_expiry_label(available_expiries, expiry_label)
        expiries = [matched_expiry] if matched_expiry else []
    else:
        expiries = available_expiries

    if requested_expiry:
        results = [
            _save_option_sources_for_expiry(
                options_df,
                spot_price,
                expiries[0],
                storage_expiry_label=requested_expiry,
            )
        ] if expiries else []
    else:
        results = [
            _save_option_sources_for_expiry(options_df, spot_price, expiry)
            for expiry in expiries
        ]

    if requested_expiry and results:
        for result in results:
            result["requested_expiry"] = requested_expiry

    if requested_expiry and not results:
        sample_expiries = [str(expiry) for expiry in available_expiries[:5]]
        return {
            "ok": False,
            "spot_price": spot_price,
            "expiry_count": 0,
            "row_count": 0,
            "requested_expiry": requested_expiry,
            "available_expiry_sample": sample_expiries,
            "results": [
                {
                    "expiry_label": requested_expiry,
                    "matched_expiry": None,
                    "analytics_saved": False,
                    "premium_saved": False,
                    "option_chain_saved": False,
                    "rows": 0,
                    "error": "Requested expiry was not present in the latest Delta option list.",
                }
            ],
        }

    return {
        "ok": bool(results) and all(item.get("option_chain_saved") for item in results),
        "spot_price": spot_price,
        "expiry_count": len(results),
        "row_count": sum(item.get("rows", 0) for item in results),
        "results": results,
    }


def refresh_market_structure_sources():
    """
    Populate the source tables used by Rule Based Insights:
    - eth_ohlcv via Delta candles
    - eth_market_events via SMC analysis
    - eth_smc_zones via SMC analysis
    - eth_volume_profile via SMC analysis
    """

    orderbook_saved = False
    ohlcv_saved = run_ohlcv_job()
    smc_saved = False

    try:
        orderbook_data = get_eth_orderbook_insights(depth=20)
        orderbook_saved = save_orderbook_insights(orderbook_data.get("insights"))
    except Exception as e:
        print("Orderbook refresh failed:", e)

    if ohlcv_saved:
        smc_saved = run_smc_job()

    return {
        "orderbook_saved": bool(orderbook_saved),
        "ohlcv_saved": bool(ohlcv_saved),
        "smc_saved": bool(smc_saved),
    }
