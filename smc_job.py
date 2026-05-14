from database_reader import get_latest_ohlcv_data
from smc_engine import run_smc_analysis
from storage import save_market_events, save_smc_zones, save_volume_profile


def run_smc_job():
    df = get_latest_ohlcv_data(
        symbol="ETHUSD",
        resolution="5m",
        limit=300,
    )

    if df.empty:
        print("No OHLCV data found for SMC analysis.")
        return False

    events, zones, volume_profile = run_smc_analysis(df)

    save_market_events(events, symbol="ETHUSD", resolution="5m")
    save_smc_zones(zones, symbol="ETHUSD", resolution="5m")
    save_volume_profile(volume_profile, symbol="ETHUSD", resolution="5m")

    print("SMC analysis completed.")
    return True


if __name__ == "__main__":
    run_smc_job()