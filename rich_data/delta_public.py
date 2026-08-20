import requests

from delta_api import BASE_URL, USER_AGENT, safe_float


HEADERS = {
    "Accept": "application/json",
    "User-Agent": USER_AGENT,
}


def delta_public_get(path, params=None, timeout=10):
    response = requests.get(
        f"{BASE_URL}{path}",
        params=params or {},
        headers=HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.json()
    if not data.get("success"):
        raise ValueError(f"Delta public API error: {data}")
    return data.get("result")


def get_ticker(symbol="ETHUSD"):
    return delta_public_get(f"/tickers/{symbol}")


def get_recent_public_trades(symbol="ETHUSD"):
    return delta_public_get(f"/trades/{symbol}") or []


def get_historical_candles(symbol, resolution="5m", start=None, end=None):
    params = {
        "symbol": symbol,
        "resolution": resolution,
        "start": start,
        "end": end,
    }
    return delta_public_get("/history/candles", params=params) or []


def ticker_float(ticker, key):
    return safe_float((ticker or {}).get(key))

