from ohlcv_job import run_ohlcv_job
from orderbook_engine import get_eth_orderbook_insights
from smc_job import run_smc_job
from storage import save_orderbook_insights


def refresh_market_structure_sources():
    """
    Populate the source tables used by Rule Based Insights:
    - eth_ohlcv via Delta candles
    - eth_market_events via SMC analysis
    - eth_smc_zones via SMC analysis
    - eth_volume_profile via SMC analysis
    """

    orderbook_saved = False
    ohlcv_saved = run_ohlcv_job()
    smc_saved = False

    try:
        orderbook_data = get_eth_orderbook_insights(depth=20)
        orderbook_saved = save_orderbook_insights(orderbook_data.get("insights"))
    except Exception as e:
        print("Orderbook refresh failed:", e)

    if ohlcv_saved:
        smc_saved = run_smc_job()

    return {
        "orderbook_saved": bool(orderbook_saved),
        "ohlcv_saved": bool(ohlcv_saved),
        "smc_saved": bool(smc_saved),
    }
