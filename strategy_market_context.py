from datetime import datetime, timezone

import pandas as pd

from backend.services.delta_client import eth_market_snapshot, eth_option_chain
from database_reader import get_market_events, get_smc_zones, get_volume_profile
from market_data import fetch_ohlcv
from strategy_indicators import age_seconds, latest_timestamp, trend_snapshot


def _df_from_rows(rows):
    df = pd.DataFrame(rows or [])
    if not df.empty:
        for column in ["strike", "mark_price", "oi", "volume", "iv", "delta", "gamma", "theta", "vega"]:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def _option_chain(expiry=None):
    response = eth_option_chain(expiry=expiry)
    return _df_from_rows(response.get("rows") or []), response


def _expiries(option_df):
    if option_df.empty or "expiry" not in option_df.columns:
        return []
    values = []
    for expiry in sorted(option_df["expiry"].dropna().unique()):
        if expiry not in values:
            values.append(expiry)
    return values


def build_strategy_market_context(symbol="ETHUSD", resolution="5m", minutes_back=720):
    market = eth_market_snapshot(include_orderbook=True)
    option_df, option_response = _option_chain()
    ohlcv_df = fetch_ohlcv(symbol=symbol, resolution=resolution, minutes_back=minutes_back)
    trend = trend_snapshot(ohlcv_df)
    latest_candle = latest_timestamp(ohlcv_df)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "resolution": resolution,
        "market": market,
        "spot_price": market.get("spot_price") or market.get("mark_price"),
        "mark_price": market.get("mark_price"),
        "orderbook_insights": market.get("orderbook_insights") or {},
        "option_df": option_df,
        "option_response": option_response,
        "expiries": _expiries(option_df),
        "ohlcv_df": ohlcv_df,
        "trend": trend,
        "data_freshness": {
            "latest_candle": latest_candle.isoformat() if latest_candle else None,
            "ohlcv_age_seconds": age_seconds(latest_candle),
        },
        "market_events_df": get_market_events(symbol=symbol, resolution=resolution, limit=100),
        "smc_zones_df": get_smc_zones(symbol=symbol, resolution=resolution, status="active", limit=100),
        "volume_profile_df": get_volume_profile(symbol=symbol, resolution=resolution, limit=80),
        "unavailable_inputs": [
            "funding_rate",
            "dedicated_futures_basis",
            "long_short_ratio",
            "liquidations",
            "cvd_order_flow",
        ],
    }
