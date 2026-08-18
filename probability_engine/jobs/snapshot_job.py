from probability_engine.services.market_data_service import ProbabilityMarketDataService


def run_probability_snapshot_job():
    return ProbabilityMarketDataService().persist_snapshot()

