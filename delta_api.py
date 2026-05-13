import requests
import pandas as pd

BASE_URL = "https://api.india.delta.exchange/v2"


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


def get_products():
    url = f"{BASE_URL}/products"

    response = requests.get(url, timeout=10)
    response.raise_for_status()

    return response.json()["result"]


def get_tickers():
    url = f"{BASE_URL}/tickers"

    response = requests.get(url, timeout=10)
    response.raise_for_status()

    return response.json()["result"]


def get_eth_spot_price():
    tickers = get_tickers()

    for ticker in tickers:
        if ticker.get("symbol") == "ETHUSD":
            return {
                "symbol": ticker.get("symbol"),
                "spot_price": safe_float(ticker.get("spot_price")),
                "mark_price": safe_float(ticker.get("mark_price")),
            }

    return {
        "symbol": "ETHUSD",
        "spot_price": None,
        "mark_price": None,
    }


def get_eth_options():
    products = get_products()
    tickers = get_tickers()

    ticker_map = {}

    for t in tickers:
        ticker_map[t["symbol"]] = t

    rows = []

    for p in products:
        symbol = p.get("symbol", "")
        contract_type = p.get("contract_type", "")

        if "ETH" not in symbol:
            continue

        if contract_type not in ["call_options", "put_options"]:
            continue

        ticker = ticker_map.get(symbol, {})
        greeks = ticker.get("greeks", {})

        rows.append({
            "symbol": symbol,
            "strike": safe_float(p.get("strike_price")),
            "type": contract_type,
            "expiry": p.get("settlement_time"),
            "mark_price": safe_float(ticker.get("mark_price")),
            "oi": safe_float(ticker.get("oi")),
            "volume": safe_float(ticker.get("volume")),
            "iv": safe_float(ticker.get("mark_iv")),
            "delta": safe_float(greeks.get("delta")),
            "gamma": safe_float(greeks.get("gamma")),
            "theta": safe_float(greeks.get("theta")),
            "vega": safe_float(greeks.get("vega")),
        })

    df = pd.DataFrame(rows)

    return df