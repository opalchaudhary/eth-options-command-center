import os
import requests
import pandas as pd
import logging
from dotenv import load_dotenv

try:
    import streamlit as st
except Exception:
    st = None


load_dotenv()
logger = logging.getLogger(__name__)


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
        "select": "symbol,resolution,candle_time,epoch_time,open,high,low,close,volume",
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
        "select": "symbol,resolution,event_type,direction,event_time,price,reference_price,strength,metadata",
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
        "select": "symbol,resolution,zone_type,direction,start_time,end_time,price_low,price_high,strength,status,metadata",
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
        "select": "symbol,resolution,price_level,volume,profile_type,metadata",
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


def read_supabase_table(table_name, params=None, timeout=15):
    """
    Generic read helper for dashboard history tables.
    Returns an empty dataframe instead of raising so pages stay usable.
    """

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Supabase credentials missing.")
        return pd.DataFrame()

    url = f"{SUPABASE_URL}/rest/v1/{table_name}"

    try:
        effective_params = params or {"select": "*"}
        if effective_params.get("select") in [None, "*"]:
            logger.warning("Wildcard Supabase read requested for %s", table_name)
            print(f"Warning: wildcard Supabase read requested for {table_name}")

        response = requests.get(
            url,
            headers=HEADERS,
            params=effective_params,
            timeout=timeout,
        )

        if response.status_code != 200:
            print(
                f"Failed to read {table_name}:",
                response.status_code,
                response.text,
            )
            return pd.DataFrame()

        data = response.json()

        if not data:
            return pd.DataFrame()

        return pd.DataFrame(data)

    except Exception as e:
        print(f"Error reading {table_name}:", e)
        return pd.DataFrame()


def get_analytics_snapshots(expiry_label=None, limit=500):
    limit = min(int(limit or 500), 500)
    params = {
        "select": "snapshot_time,expiry_label,spot_price,max_pain,atm_strike,pcr,atm_straddle_price,expected_move_pct,expected_move_upper,expected_move_lower",
        "order": "snapshot_time.asc",
        "limit": limit,
    }

    if expiry_label:
        params["expiry_label"] = f"eq.{expiry_label}"

    df = read_supabase_table("analytics_snapshots", params=params)

    if df.empty:
        return df

    if "snapshot_time" in df.columns:
        df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], utc=True)

    numeric_cols = [
        "spot_price",
        "max_pain",
        "atm_strike",
        "pcr",
        "atm_straddle_price",
        "expected_move_pct",
        "expected_move_upper",
        "expected_move_lower",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values("snapshot_time").reset_index(drop=True)


def get_premium_decay_snapshots(expiry_label=None, limit=500):
    limit = min(int(limit or 500), 500)
    params = {
        "select": "snapshot_time,expiry_label,atm_strike,atm_ce_price,atm_pe_price,atm_straddle_price",
        "order": "snapshot_time.asc",
        "limit": limit,
    }

    if expiry_label:
        params["expiry_label"] = f"eq.{expiry_label}"

    df = read_supabase_table("premium_decay_snapshots", params=params)

    if df.empty:
        return df

    if "snapshot_time" in df.columns:
        df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], utc=True)

    numeric_cols = [
        "atm_strike",
        "atm_ce_price",
        "atm_pe_price",
        "atm_straddle_price",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values("snapshot_time").reset_index(drop=True)


def get_option_chain_snapshots(expiry_label=None, limit=2000):
    limit = min(int(limit or 500), 500)
    params = {
        "select": "snapshot_time,expiry_label,expiry_date,strike,option_type,mark_price,oi,volume,iv,delta,gamma,theta,vega",
        "order": "snapshot_time.asc",
        "limit": limit,
    }

    if expiry_label:
        params["expiry_label"] = f"eq.{expiry_label}"

    df = read_supabase_table("option_chain_snapshots", params=params)

    if df.empty:
        return df

    if "snapshot_time" in df.columns:
        df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], utc=True)

    numeric_cols = [
        "strike",
        "mark_price",
        "oi",
        "volume",
        "iv",
        "delta",
        "gamma",
        "theta",
        "vega",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values("snapshot_time").reset_index(drop=True)


def get_orderbook_insight_snapshots(symbol="ETHUSD", limit=500):
    limit = min(int(limit or 500), 500)
    params = {
        "select": "timestamp,symbol,eth_price,best_bid,best_ask,spread,spread_pct,bid_depth,ask_depth,imbalance_ratio,bias,nearest_bid_wall_price,nearest_bid_wall_size,nearest_ask_wall_price,nearest_ask_wall_size,trap_risk,execution_signal",
        "symbol": f"eq.{symbol}",
        "order": "timestamp.asc",
        "limit": limit,
    }

    df = read_supabase_table("orderbook_insights", params=params)

    if df.empty:
        return df

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    numeric_cols = [
        "eth_price",
        "best_bid",
        "best_ask",
        "spread",
        "spread_pct",
        "bid_depth",
        "ask_depth",
        "imbalance_ratio",
        "nearest_bid_wall_price",
        "nearest_bid_wall_size",
        "nearest_ask_wall_price",
        "nearest_ask_wall_size",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values("timestamp").reset_index(drop=True)
