import logging
from dataclasses import replace
from datetime import datetime, timezone

import pandas as pd

from backend.services.json_utils import to_jsonable
from delta_api import get_products, get_tickers, iv_to_percent, safe_float
from iron_fly_engine import build_iron_fly_recommendation
from market_data import fetch_ohlcv
from strategy_decision_config import DEFAULT_STRATEGY_CONFIG
from strategy_indicators import age_seconds, latest_timestamp, trend_snapshot


logger = logging.getLogger(__name__)

BTC_SYMBOL = "BTCUSD"
BTC_UNDERLYING = "BTC"


def _asset_symbol(value):
    if isinstance(value, dict):
        return str(value.get("symbol") or value.get("asset_symbol") or value.get("id") or "")
    return str(value or "")


def _is_btc_option(product):
    contract_type = product.get("contract_type")
    if contract_type not in ["call_options", "put_options"]:
        return False

    symbol = str(product.get("symbol") or "")
    product_underlying = _asset_symbol(
        product.get("underlying_asset_symbol")
        or product.get("underlying_asset")
        or product.get("settlement_asset")
        or product.get("quoting_asset")
        or ""
    )
    product_underlying = str(product_underlying)

    return BTC_UNDERLYING in symbol or product_underlying.upper() == BTC_UNDERLYING


def _df_from_rows(rows):
    df = pd.DataFrame(rows or [])
    if not df.empty:
        for column in ["strike", "mark_price", "oi", "volume", "iv", "delta", "gamma", "theta", "vega"]:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def _expiries(option_df):
    if option_df.empty or "expiry" not in option_df.columns:
        return []
    parsed = option_df[["expiry"]].dropna().drop_duplicates().copy()
    parsed["sort_key"] = pd.to_datetime(parsed["expiry"], utc=True, errors="coerce")
    parsed = parsed.sort_values(["sort_key", "expiry"], na_position="last")
    return [str(item) for item in parsed["expiry"].tolist()]


def _ticker_by_symbol(tickers):
    return {ticker.get("symbol"): ticker for ticker in tickers if ticker.get("symbol")}


def get_btc_market_snapshot():
    tickers = get_tickers()
    ticker = _ticker_by_symbol(tickers).get(BTC_SYMBOL, {})
    return {
        "ok": True,
        "symbol": ticker.get("symbol", BTC_SYMBOL),
        "spot_price": safe_float(ticker.get("spot_price")),
        "mark_price": safe_float(ticker.get("mark_price")),
    }


def get_btc_options():
    products = get_products()
    tickers = _ticker_by_symbol(get_tickers())
    rows = []

    for product in products:
        if not _is_btc_option(product):
            continue

        symbol = product.get("symbol")
        ticker = tickers.get(symbol, {})
        quotes = ticker.get("quotes") or {}
        greeks = ticker.get("greeks") or {}
        mark_iv = quotes.get("mark_iv") or ticker.get("mark_iv") or ticker.get("mark_vol")

        rows.append(
            {
                "symbol": symbol,
                "strike": safe_float(product.get("strike_price")),
                "type": product.get("contract_type"),
                "expiry": product.get("settlement_time"),
                "mark_price": safe_float(ticker.get("mark_price")),
                "oi": safe_float(ticker.get("oi")),
                "volume": safe_float(ticker.get("volume")),
                "iv": iv_to_percent(mark_iv),
                "delta": safe_float(greeks.get("delta")),
                "gamma": safe_float(greeks.get("gamma")),
                "theta": safe_float(greeks.get("theta")),
                "vega": safe_float(greeks.get("vega")),
            }
        )

    return _df_from_rows(rows)


def build_btc_strategy_market_context(resolution="5m", minutes_back=720):
    market = get_btc_market_snapshot()
    option_df = get_btc_options()
    ohlcv_df = fetch_ohlcv(symbol=BTC_SYMBOL, resolution=resolution, minutes_back=minutes_back)
    latest_candle = latest_timestamp(ohlcv_df)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbol": BTC_SYMBOL,
        "resolution": resolution,
        "market": market,
        "spot_price": market.get("spot_price") or market.get("mark_price"),
        "mark_price": market.get("mark_price"),
        "orderbook_insights": {},
        "option_df": option_df,
        "option_response": {
            "ok": True,
            "row_count": int(len(option_df)),
            "expiry_count": int(option_df["expiry"].nunique()) if not option_df.empty and "expiry" in option_df else 0,
            "expiries": _expiries(option_df),
            "rows": to_jsonable(option_df),
        },
        "expiries": _expiries(option_df),
        "ohlcv_df": ohlcv_df,
        "trend": trend_snapshot(ohlcv_df),
        "data_freshness": {
            "latest_candle": latest_candle.isoformat() if latest_candle else None,
            "ohlcv_age_seconds": age_seconds(latest_candle),
        },
        "market_events_df": pd.DataFrame(),
        "smc_zones_df": pd.DataFrame(),
        "volume_profile_df": pd.DataFrame(),
        "unavailable_inputs": [
            "btc_orderbook_insights",
            "funding_rate",
            "dedicated_futures_basis",
            "long_short_ratio",
            "liquidations",
            "cvd_order_flow",
        ],
    }


def _btc_strategy_config():
    btc_iron_fly = replace(
        DEFAULT_STRATEGY_CONFIG.iron_fly,
        wing_widths=(250, 500, 750, 1000, 1500, 2000, 3000, 5000),
    )
    return replace(DEFAULT_STRATEGY_CONFIG, iron_fly=btc_iron_fly)


def build_btc_iron_fly_recommendation():
    logger.info("btc_iron_fly calculation started")
    result = build_iron_fly_recommendation(
        context=build_btc_strategy_market_context(),
        config=_btc_strategy_config(),
    )
    result = dict(result)
    result["engine_name"] = "btc_iron_fly_research"
    result["symbol"] = BTC_SYMBOL
    logger.info(
        "btc_iron_fly calculation finished recommendation=%s score=%s",
        result.get("recommendation"),
        result.get("iron_fly_score"),
    )
    return result
