import requests
import pandas as pd

BASE_URL = "https://api.india.delta.exchange/v2"

def get_products():
    url = f"{BASE_URL}/products"

    response = requests.get(url)

    return response.json()["result"]

def get_tickers():
    url = f"{BASE_URL}/tickers"

    response = requests.get(url)

    return response.json()["result"]

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

            "strike": p.get("strike_price"),

            "type": contract_type,

            "expiry": p.get("settlement_time"),

            "mark_price": ticker.get("mark_price"),

            "oi": ticker.get("oi"),

            "volume": ticker.get("volume"),

            "iv": ticker.get("mark_iv"),

            "delta": greeks.get("delta"),

            "gamma": greeks.get("gamma"),

            "theta": greeks.get("theta"),

            "vega": greeks.get("vega")

        })

    df = pd.DataFrame(rows)

    return df