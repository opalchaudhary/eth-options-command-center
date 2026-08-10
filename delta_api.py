import hashlib
import hmac
import json
import time
from urllib.parse import urlencode

import requests
import pandas as pd

BASE_URL = "https://api.india.delta.exchange/v2"
USER_AGENT = "python-requests/eth-options-command-center"


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


def _clean_params(params):
    if not params:
        return {}

    return {
        key: value
        for key, value in params.items()
        if value not in [None, ""]
    }


def _encoded_query(params):
    cleaned = _clean_params(params)

    if not cleaned:
        return ""

    return "?" + urlencode(cleaned)


def _signed_headers(api_key, api_secret, method, path, params=None, body=None):
    timestamp = str(int(time.time()))
    query_string = _encoded_query(params)
    payload = json.dumps(body, separators=(",", ":")) if body else ""
    signature_data = method.upper() + timestamp + path + query_string + payload
    signature = hmac.new(
        str(api_secret).encode("utf-8"),
        signature_data.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
        "api-key": str(api_key),
        "signature": signature,
        "timestamp": timestamp,
    }


def delta_private_get(path, api_key, api_secret, params=None, timeout=15):
    params = _clean_params(params)
    headers = _signed_headers(api_key, api_secret, "GET", path, params=params)
    response = requests.get(
        f"{BASE_URL}{path}",
        params=params,
        headers=headers,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def get_private_positions(api_key, api_secret, underlying_asset_symbol="ETH"):
    params = {
        "underlying_asset_symbol": underlying_asset_symbol,
    }
    return delta_private_get("/positions", api_key, api_secret, params=params).get("result")


def get_margined_positions(api_key, api_secret, contract_types=None):
    params = {}

    if contract_types:
        params["contract_types"] = contract_types

    result = delta_private_get("/positions/margined", api_key, api_secret, params=params).get("result")

    return result or []


def get_wallet_balances(api_key, api_secret):
    return delta_private_get("/wallet/balances", api_key, api_secret)


def get_sub_accounts(api_key, api_secret):
    return delta_private_get("/sub_accounts", api_key, api_secret).get("result") or []


def iv_to_percent(value):
    iv = safe_float(value)

    if iv is None:
        return None

    # Delta returns option IV as a decimal volatility value, e.g. 0.846 = 84.6%.
    return iv * 100 if abs(iv) <= 10 else iv


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
        quotes = ticker.get("quotes") or {}
        greeks = ticker.get("greeks", {})
        mark_iv = (
            quotes.get("mark_iv")
            or ticker.get("mark_iv")
            or ticker.get("mark_vol")
        )

        rows.append({
            "symbol": symbol,
            "strike": safe_float(p.get("strike_price")),
            "type": contract_type,
            "expiry": p.get("settlement_time"),
            "mark_price": safe_float(ticker.get("mark_price")),
            "oi": safe_float(ticker.get("oi")),
            "volume": safe_float(ticker.get("volume")),
            "iv": iv_to_percent(mark_iv),
            "delta": safe_float(greeks.get("delta")),
            "gamma": safe_float(greeks.get("gamma")),
            "theta": safe_float(greeks.get("theta")),
            "vega": safe_float(greeks.get("vega")),
        })

    df = pd.DataFrame(rows)

    return df
