from market_data import fetch_eth_5m_ohlcv
from storage import save_ohlcv_data


def run_ohlcv_job():
    """
    Fetch latest ETH 5m candles and save them into Supabase.
    """

    df = fetch_eth_5m_ohlcv(minutes_back=720)

    if df.empty:
        print("No OHLCV candles fetched.")
        return False

    saved = save_ohlcv_data(
        df,
        symbol="ETHUSD",
        resolution="5m",
    )

    return saved


if __name__ == "__main__":
    run_ohlcv_job()