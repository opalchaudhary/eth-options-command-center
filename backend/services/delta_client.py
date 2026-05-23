import logging

from backend import config
from delta_api import get_eth_options, get_eth_spot_price, get_products, get_tickers
from market_data import fetch_ohlcv
from orderbook_engine import get_eth_orderbook_insights

from .json_utils import to_jsonable


logger = logging.getLogger(__name__)

PRIVATE_API_DISABLED_ERROR = (
    "Delta private API credentials are not configured. Live trading is disabled."
)


def delta_credentials_status():
    return config.delta_status()


def private_api_guard():
    status = delta_credentials_status()

    if status["private_api_configured"]:
        return {
            "ok": True,
            "delta": status,
        }

    return {
        "ok": False,
        "error": PRIVATE_API_DISABLED_ERROR,
        "delta": status,
    }


def require_private_api():
    guard = private_api_guard()

    if not guard["ok"]:
        return guard

    return None


def live_trading_guard():
    private_guard = private_api_guard()

    if not private_guard["ok"]:
        return private_guard

    status = private_guard["delta"]

    if not status["private_trading_enabled"]:
        return {
            "ok": False,
            "error": "Live trading is disabled.",
            "delta": status,
        }

    return {
        "ok": True,
        "delta": status,
    }


def eth_market_snapshot(include_orderbook=True):
    spot = get_eth_spot_price()
    response = {
        "ok": True,
        "symbol": spot.get("symbol", "ETHUSD"),
        "spot_price": spot.get("spot_price"),
        "mark_price": spot.get("mark_price"),
        "delta_credentials": delta_credentials_status(),
    }

    if include_orderbook:
        try:
            orderbook = get_eth_orderbook_insights(depth=20)
            response["orderbook"] = to_jsonable(orderbook.get("orderbook"))
            response["orderbook_insights"] = to_jsonable(orderbook.get("insights"))
            response["text_insights"] = to_jsonable(orderbook.get("text_insights"))
        except Exception as exc:
            logger.exception("Order book fetch failed")
            response["orderbook_error"] = str(exc)

    return response


def eth_option_chain(expiry=None):
    df = get_eth_options()

    if expiry and not df.empty:
        df = df[df["expiry"].astype(str) == str(expiry)].copy()

    return {
        "ok": True,
        "row_count": int(len(df)),
        "expiry_count": int(df["expiry"].nunique()) if not df.empty and "expiry" in df else 0,
        "expiries": sorted([str(item) for item in df["expiry"].dropna().unique()]) if not df.empty else [],
        "rows": to_jsonable(df),
    }


def ohlcv_snapshot(symbol="ETHUSD", resolution="5m", minutes_back=720):
    df = fetch_ohlcv(symbol=symbol, resolution=resolution, minutes_back=minutes_back)
    return {
        "ok": not df.empty,
        "symbol": symbol,
        "resolution": resolution,
        "row_count": int(len(df)),
        "rows": to_jsonable(df),
    }
