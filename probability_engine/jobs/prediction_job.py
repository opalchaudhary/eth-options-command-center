from probability_engine.services.market_data_service import ProbabilityMarketDataService


def run_probability_prediction_job():
    return ProbabilityMarketDataService().persist_predictions()

