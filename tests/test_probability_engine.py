from datetime import datetime, timezone

import pandas as pd

from probability_engine.config import ProbabilityEngineConfig
from probability_engine.models.market_snapshot import MarketSnapshot
from probability_engine.services.cvd_service import CVDService
from probability_engine.services.feature_engine import calculate_atr, calculate_realized_volatility, calculate_vwap
from probability_engine.services.outcome_service import OutcomeService
from probability_engine.services.performance_service import brier_score, calibration_buckets, calibration_error, log_loss
from probability_engine.services.probability_service import ProbabilityService
from probability_engine.services.regime_engine import RegimeEngine
from probability_engine.services.strike_optimizer import StrikeOptimizer


def _candles():
    return pd.DataFrame(
        [
            {"open": 100, "high": 103, "low": 99, "close": 102, "volume": 10},
            {"open": 102, "high": 106, "low": 101, "close": 105, "volume": 20},
            {"open": 105, "high": 107, "low": 100, "close": 101, "volume": 30},
            {"open": 101, "high": 104, "low": 98, "close": 99, "volume": 40},
        ]
    )


def test_feature_calculations_are_deterministic():
    df = _candles()
    assert round(calculate_vwap(df), 4) == 101.8667
    assert calculate_atr(df, period=3) > 0
    assert calculate_realized_volatility(df) > 0


def test_cvd_tracks_aggressive_delta_windows():
    service = CVDService()
    now = datetime.now(timezone.utc)
    service.add_trade(now, "buy", 10)
    service.add_trade(now, "sell", 4)
    features = service.features(now)
    assert features["cvd_5m"] == 6
    assert features["buy_volume_ratio"] == 10 / 14


def test_regime_classifies_trend_up():
    snapshot = MarketSnapshot(spot_price=100, return_1h=0.01, return_4h=0.02, realized_volatility=45, atr_pct=0.01)
    assert RegimeEngine().classify(snapshot) == "TREND_UP"


def test_probability_values_do_not_sum_to_one_and_confidence_separate():
    snapshot = MarketSnapshot(
        spot_price=4000,
        vwap=3960,
        vwap_zscore=1.4,
        atr=45,
        atr_pct=0.011,
        realized_volatility=55,
        return_1h=0.006,
        return_4h=0.01,
        volume_zscore=0.5,
        atm_iv=70,
        book_imbalance=1.1,
    )
    prediction = ProbabilityService(ProbabilityEngineConfig()).predict(snapshot, "1H")
    total = (
        prediction.mean_reversion_probability
        + prediction.upside_breakout_probability
        + prediction.downside_breakdown_probability
        + prediction.range_continuation_probability
        + prediction.trend_continuation_probability
    )
    assert total != 1
    assert 0 <= prediction.confidence <= 1


def test_outcome_labels_mean_reversion_breakout_and_range_coverage():
    prediction = ProbabilityService(ProbabilityEngineConfig()).predict(
        MarketSnapshot(spot_price=100, vwap=95, vwap_zscore=1.2, atr=5, atr_pct=0.05),
        "1H",
    )
    prediction.metadata_json.update({"initial_vwap_zscore": 1.2, "mean_reversion_target": 98, "upper_boundary": 106, "lower_boundary": 93})
    outcome = OutcomeService().evaluate_prediction(prediction, _candles())
    assert outcome["mean_reversion_occurred"] is True
    assert outcome["upside_breakout_occurred"] is True
    assert outcome["downside_breakdown_occurred"] is False
    assert outcome["range_90_covered"] is True


def test_probability_scoring_and_calibration():
    predictions = [0.8, 0.7, 0.2, 0.1]
    outcomes = [True, True, False, False]
    assert brier_score(predictions, outcomes) < 0.1
    assert log_loss(predictions, outcomes) < 0.5
    assert calibration_error(predictions, outcomes) is not None
    assert len(calibration_buckets(predictions, outcomes)) == 10


def test_strike_optimizer_allows_no_trade_and_asymmetric_put_call():
    prediction = ProbabilityService(ProbabilityEngineConfig()).predict(
        MarketSnapshot(spot_price=4000, vwap=3990, vwap_zscore=0.2, atr=40, atr_pct=0.01, return_1h=0.002),
        "4H",
    )
    optimizer = StrikeOptimizer()
    assert optimizer.optimize(prediction, [], expiry="2026-09-01").recommendation_status == "NO_ATTRACTIVE_NAKED_SELL"
    rows = [
        {"expiry": "2026-09-01", "type": "put_options", "strike": 3800, "mark_price": 10},
        {"expiry": "2026-09-01", "type": "call_options", "strike": 4200, "mark_price": 9},
    ]
    result = optimizer.optimize(prediction, rows, expiry="2026-09-01")
    assert {item.option_type for item in result} == {"put_options", "call_options"}
