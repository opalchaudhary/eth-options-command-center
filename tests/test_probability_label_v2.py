from types import SimpleNamespace

import pandas as pd

from probability_engine.config import ProbabilityEngineConfig
from probability_engine.repositories.outcome_repository import OutcomeRepository
from probability_engine.services.outcome_service import OutcomeService


def _prediction(**overrides):
    payload = {
        "range_50_lower": 97,
        "range_50_upper": 103,
        "range_70_lower": 95,
        "range_70_upper": 105,
        "range_90_lower": 90,
        "range_90_upper": 110,
        "metadata_json": {},
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _snapshot(**overrides):
    payload = {
        "spot_price": 100,
        "vwap": 90,
        "vwap_zscore": 2,
        "atr": 4,
        "atr_pct": 0.04,
        "return_1h": 0.01,
        "return_4h": 0.02,
        "regime": "TREND_UP",
    }
    payload.update(overrides)
    return payload


def _candles(open_price=100, high=104, low=96, close=101):
    return pd.DataFrame(
        [
            {"open": open_price, "high": high, "low": low, "close": close, "volume": 10},
            {"open": open_price, "high": high, "low": low, "close": close, "volume": 10},
        ]
    )


def _evaluate(prediction=None, candles=None, snapshot=None):
    return OutcomeService(ProbabilityEngineConfig()).evaluate_prediction(
        prediction or _prediction(),
        candles if candles is not None else _candles(),
        snapshot=snapshot or _snapshot(),
    )


def test_mean_reversion_above_vwap_exact_50_percent_retracement_is_true():
    outcome = _evaluate(candles=_candles(open_price=100, high=101, low=95, close=99))

    assert outcome["mean_reversion_occurred"] is True
    assert outcome["mean_reversion_fraction"] == 0.5
    assert outcome["metadata_json"]["mean_reversion_eligible"] is True
    assert outcome["metadata_json"]["mean_reversion_target"] == 90


def test_mean_reversion_above_vwap_less_than_50_percent_retracement_is_false():
    outcome = _evaluate(candles=_candles(open_price=100, high=101, low=96, close=99))

    assert outcome["mean_reversion_occurred"] is False
    assert outcome["mean_reversion_fraction"] == 0.4


def test_mean_reversion_below_vwap_exact_50_percent_retracement_is_true():
    outcome = _evaluate(
        candles=_candles(open_price=90, high=95, low=89, close=92),
        snapshot=_snapshot(spot_price=90, vwap=100, vwap_zscore=-2),
    )

    assert outcome["mean_reversion_occurred"] is True
    assert outcome["mean_reversion_fraction"] == 0.5


def test_mean_reversion_below_vwap_less_than_50_percent_retracement_is_false():
    outcome = _evaluate(
        candles=_candles(open_price=90, high=94, low=89, close=92),
        snapshot=_snapshot(spot_price=90, vwap=100, vwap_zscore=-2),
    )

    assert outcome["mean_reversion_occurred"] is False
    assert outcome["mean_reversion_fraction"] == 0.4


def test_mean_reversion_vwap_crossing_is_true_and_clamped():
    outcome = _evaluate(candles=_candles(open_price=100, high=101, low=89, close=91))

    assert outcome["mean_reversion_occurred"] is True
    assert outcome["mean_reversion_fraction"] == 1.0


def test_mean_reversion_low_initial_zscore_is_not_eligible():
    outcome = _evaluate(snapshot=_snapshot(vwap_zscore=0.99))

    assert outcome["mean_reversion_occurred"] is False
    assert outcome["mean_reversion_fraction"] is None
    assert outcome["metadata_json"]["mean_reversion_eligible"] is False


def test_mean_reversion_uses_frozen_snapshot_vwap_not_future_reference():
    outcome = _evaluate(
        candles=_candles(open_price=100, high=130, low=95, close=125),
        snapshot=_snapshot(spot_price=100, vwap=90, vwap_zscore=2),
    )

    assert outcome["metadata_json"]["frozen_vwap"] == 90
    assert outcome["mean_reversion_occurred"] is True


def test_trend_up_threshold_reached_by_close_is_true():
    outcome = _evaluate(candles=_candles(close=101))

    assert outcome["trend_continuation_occurred"] is True
    assert outcome["metadata_json"]["trend_continuation_eligible"] is True
    assert outcome["metadata_json"]["trend_direction"] == "UP"
    assert outcome["metadata_json"]["trend_threshold"] == 1.0


def test_trend_up_threshold_not_reached_by_close_is_false():
    outcome = _evaluate(candles=_candles(close=100.99))

    assert outcome["trend_continuation_occurred"] is False


def test_trend_down_threshold_reached_by_close_is_true():
    outcome = _evaluate(
        candles=_candles(close=99),
        snapshot=_snapshot(return_1h=-0.01, return_4h=-0.02, regime="TREND_DOWN"),
    )

    assert outcome["trend_continuation_occurred"] is True
    assert outcome["metadata_json"]["trend_direction"] == "DOWN"


def test_trend_down_threshold_not_reached_by_close_is_false():
    outcome = _evaluate(
        candles=_candles(close=99.01),
        snapshot=_snapshot(return_1h=-0.01, return_4h=-0.02, regime="TREND_DOWN"),
    )

    assert outcome["trend_continuation_occurred"] is False


def test_trend_neutral_is_not_eligible():
    outcome = _evaluate(snapshot=_snapshot(return_1h=0.0005, return_4h=-0.0004, atr_pct=0.02))

    assert outcome["trend_continuation_occurred"] is False
    assert outcome["metadata_json"]["trend_continuation_eligible"] is False
    assert outcome["metadata_json"]["trend_direction"] is None


def test_trend_uses_stronger_momentum_when_signs_disagree():
    outcome = _evaluate(snapshot=_snapshot(return_1h=0.002, return_4h=-0.03, atr_pct=0.01))

    assert outcome["metadata_json"]["trend_direction"] == "DOWN"


def test_trend_threshold_uses_tenth_percent_minimum_when_atr_is_small():
    outcome = _evaluate(snapshot=_snapshot(atr=0.1, spot_price=100, vwap=90, vwap_zscore=2))

    assert outcome["metadata_json"]["trend_threshold"] == 0.1


def test_breakout_breakdown_boundaries_are_inclusive_and_independent():
    outcome = _evaluate(candles=_candles(high=105, low=95, close=100))

    assert outcome["upside_breakout_occurred"] is True
    assert outcome["downside_breakdown_occurred"] is True
    assert outcome["upper_touch_occurred"] is True
    assert outcome["lower_touch_occurred"] is True


def test_breakout_breakdown_false_when_boundaries_not_reached():
    outcome = _evaluate(candles=_candles(high=104.99, low=95.01, close=100))

    assert outcome["upside_breakout_occurred"] is False
    assert outcome["downside_breakdown_occurred"] is False


def test_range_continuation_requires_full_path_inside_70_percent_range():
    outcome = _evaluate(candles=_candles(high=104, low=96, close=103))

    assert outcome["range_held"] is True
    assert outcome["range_50_covered"] is True
    assert outcome["range_70_covered"] is True
    assert outcome["range_90_covered"] is True


def test_range_continuation_false_when_close_inside_but_path_breaches():
    outcome = _evaluate(candles=_candles(high=106, low=96, close=100))

    assert outcome["range_held"] is False
    assert outcome["range_50_covered"] is True


def test_label_v2_requires_linked_snapshot_vwap_fields():
    outcome = OutcomeService().evaluate_prediction(_prediction(), _candles(), snapshot={})

    assert outcome["ok"] is False
    assert "Label V2" in outcome["reason"]


def test_outcome_repository_queries_idempotency_by_label_version(monkeypatch):
    repo = OutcomeRepository()
    calls = {}
    def fake_read(params):
        calls["params"] = params
        return []

    monkeypatch.setattr(repo, "read", fake_read)

    assert repo.existing_prediction_ids(["prediction-1"], label_version="label_v2") == set()
    assert calls["params"]["prediction_id"] == "in.(prediction-1)"
    assert calls["params"]["label_version"] == "eq.label_v2"


def test_outcome_repository_inserts_label_version(monkeypatch):
    repo = OutcomeRepository()
    calls = {}
    monkeypatch.setattr(repo, "insert", lambda payload: calls.setdefault("payload", payload) or True)

    assert repo.safe_insert_outcome("prediction-1", {"actual_open": 100}, label_version="label_v2") is True
    assert calls["payload"]["prediction_id"] == "prediction-1"
    assert calls["payload"]["label_version"] == "label_v2"
