import os
import requests
import pandas as pd
from dotenv import load_dotenv

try:
    import streamlit as st
except Exception:
    st = None


load_dotenv()


def get_secret(key):
    """
    Works both locally and on Streamlit Cloud.
    Local: reads from .env
    Streamlit Cloud: reads from st.secrets
    """
    if st is not None:
        try:
            if key in st.secrets:
                return st.secrets[key]
        except Exception:
            pass

    return os.getenv(key)


SUPABASE_URL = get_secret("SUPABASE_URL")
SUPABASE_KEY = get_secret("SUPABASE_KEY")


HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}


def get_latest_ohlcv_data(symbol="ETHUSD", resolution="5m", limit=300):
    """
    Read latest OHLCV candles from Supabase.
    Returns clean dataframe sorted oldest → newest.
    """

    url = f"{SUPABASE_URL}/rest/v1/eth_ohlcv"

    params = {
        "select": "*",
        "symbol": f"eq.{symbol}",
        "resolution": f"eq.{resolution}",
        "order": "candle_time.desc",
        "limit": limit,
    }

    try:
        response = requests.get(
            url,
            headers=HEADERS,
            params=params,
            timeout=15,
        )

        if response.status_code != 200:
            print("Failed to read OHLCV data:", response.status_code, response.text)
            return pd.DataFrame()

        data = response.json()

        if not data:
            print("No OHLCV data found in database.")
            return pd.DataFrame()

        df = pd.DataFrame(data)

        df["candle_time"] = pd.to_datetime(df["candle_time"], utc=True)

        numeric_cols = ["open", "high", "low", "close", "volume"]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.sort_values("candle_time").reset_index(drop=True)

        return df

    except Exception as e:
        print("Error reading OHLCV data:", e)
        return pd.DataFrame()
    
def get_market_events(symbol="ETHUSD", resolution="5m", limit=200):
    """
    Read latest market events from Supabase:
    swing_high, swing_low, BOS, CHoCH.
    """

    url = f"{SUPABASE_URL}/rest/v1/eth_market_events"

    params = {
        "select": "*",
        "symbol": f"eq.{symbol}",
        "resolution": f"eq.{resolution}",
        "order": "event_time.desc",
        "limit": limit,
    }

    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=15)

        if response.status_code != 200:
            print("Failed to read market events:", response.status_code, response.text)
            return pd.DataFrame()

        data = response.json()

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True)

        numeric_cols = ["price", "reference_price", "strength"]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.sort_values("event_time").reset_index(drop=True)

        return df

    except Exception as e:
        print("Error reading market events:", e)
        return pd.DataFrame()


def get_smc_zones(symbol="ETHUSD", resolution="5m", status="active", limit=200):
    """
    Read SMC zones from Supabase:
    order blocks, FVG, liquidity zones.
    """

    url = f"{SUPABASE_URL}/rest/v1/eth_smc_zones"

    params = {
        "select": "*",
        "symbol": f"eq.{symbol}",
        "resolution": f"eq.{resolution}",
        "status": f"eq.{status}",
        "order": "start_time.desc",
        "limit": limit,
    }

    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=15)

        if response.status_code != 200:
            print("Failed to read SMC zones:", response.status_code, response.text)
            return pd.DataFrame()

        data = response.json()

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
        df["end_time"] = pd.to_datetime(df["end_time"], utc=True)

        numeric_cols = ["price_low", "price_high", "strength"]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.sort_values("start_time").reset_index(drop=True)

        return df

    except Exception as e:
        print("Error reading SMC zones:", e)
        return pd.DataFrame()


def get_volume_profile(symbol="ETHUSD", resolution="5m", limit=100):
    """
    Read latest volume profile rows from Supabase.
    """

    url = f"{SUPABASE_URL}/rest/v1/eth_volume_profile"

    params = {
        "select": "*",
        "symbol": f"eq.{symbol}",
        "resolution": f"eq.{resolution}",
        "order": "price_level.asc",
        "limit": limit,
    }

    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=15)

        if response.status_code != 200:
            print("Failed to read volume profile:", response.status_code, response.text)
            return pd.DataFrame()

        data = response.json()

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        numeric_cols = ["price_level", "volume"]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.sort_values("price_level").reset_index(drop=True)

        return df

    except Exception as e:
        print("Error reading volume profile:", e)
        return pd.DataFrame()