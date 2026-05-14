import requests
import pandas as pd
from datetime import datetime, timezone, timedelta


DELTA_BASE_URL = "https://api.india.delta.exchange"


def fetch_ohlcv(symbol="ETHUSD", resolution="5m", minutes_back=720):
    """
    Fetch OHLCV candle data from Delta Exchange India.

    Default setup:
    - Symbol: ETHUSD
    - Resolution: 5m
    - Lookback: 720 minutes = 12 hours

    Returns a clean pandas DataFrame:
    timestamp | time | open | high | low | close | volume
    """

    end_time = int(datetime.now(timezone.utc).timestamp())
    start_time = int(
        (datetime.now(timezone.utc) - timedelta(minutes=minutes_back)).timestamp()
    )

    url = f"{DELTA_BASE_URL}/v2/history/candles"

    params = {
        "symbol": symbol,
        "resolution": resolution,
        "start": start_time,
        "end": end_time,
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        if not data.get("success"):
            print("Delta OHLCV API error:", data)
            return pd.DataFrame()

        candles = data.get("result", [])

        if not candles:
            print("No OHLCV candles received from Delta.")
            return pd.DataFrame()

        df = pd.DataFrame(candles)

        required_columns = ["time", "open", "high", "low", "close", "volume"]

        for col in required_columns:
            if col not in df.columns:
                print(f"Missing column in OHLCV response: {col}")
                return pd.DataFrame()

        df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True)

        numeric_columns = ["open", "high", "low", "close", "volume"]

        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=numeric_columns)

        df = df[
            [
                "timestamp",
                "time",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        ]

        df = df.sort_values("timestamp").reset_index(drop=True)

        return df

    except requests.exceptions.RequestException as e:
        print("Network/API error while fetching OHLCV:", e)
        return pd.DataFrame()

    except Exception as e:
        print("Unexpected error while fetching OHLCV:", e)
        return pd.DataFrame()


def fetch_eth_5m_ohlcv(minutes_back=720):
    """
    Convenience function for ETH 5-minute candles.
    Use this everywhere in the app later.
    """

    return fetch_ohlcv(
        symbol="ETHUSD",
        resolution="5m",
        minutes_back=minutes_back,
    )


def fetch_eth_15m_ohlcv(minutes_back=1440):
    """
    Optional higher timeframe candles.
    Useful later for trend / regime / macro structure.
    """

    return fetch_ohlcv(
        symbol="ETHUSD",
        resolution="15m",
        minutes_back=minutes_back,
    )