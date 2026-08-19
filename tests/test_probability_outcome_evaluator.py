from datetime import datetime, timedelta, timezone

import pandas as pd

from probability_engine.config import ProbabilityEngineConfig
from probability_engine.services.outcome_evaluator import (
    LiveOutcomeEvaluator,
    is_mature,
    prediction_window,
)


def _prediction(prediction_id="prediction-1", horizon="1H", created_at=None):
    created_at = created_at or datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    return {
        "id": prediction_id,
        "created_at": created_at.isoformat(),
        "snapshot_id": f"snapshot-{prediction_id}",
        "symbol": "ETHUSD",
        "horizon": horizon,
        "record_type": "LIVE",
        "model_version": "probability_v1",
        "feature_version": "features_v1",
        "regime_version": "regime_v1",
        "range_model_version": "range_v1",
        "prediction_status": "LIVE",
        "mean_reversion_probability": 0.7,
        "upside_breakout_probability": 0.2,
        "downside_breakdown_probability": 0.1,
        "range_continuation_probability": 0.6,
        "trend_continuation_probability": 0.3,
        "confidence": 0.8,
        "expected_price": 100,
        "median_price": 100,
        "expected_equilibrium": 100,
        "range_50_lower": 97,
        "range_50_upper": 103,
        "range_70_lower": 95,
        "range_70_upper": 105,
        "range_90_lower": 90,
        "range_90_upper": 110,
        "analogue_sample_size": 0,
        "metadata_json": {},
    }


def _candles(start, count=12, high=104, low=96, close=101):
    rows = []
    for index in range(count):
        rows.append(
            {
                "timestamp": start + timedelta(minutes=5 * index),
                "time": int((start + timedelta(minutes=5 * index)).timestamp()),
                "open": 100 + index * 0.1,
                "high": high,
                "low": low,
                "close": close,
                "volume": 10,
            }
        )
    return pd.DataFrame(rows)


class FakePredictionRepository:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def mature_unevaluated(self, before_iso, limit=100, offset=0):
        self.calls.append({"before_iso": before_iso, "limit": limit})
        return list(self.rows)[offset : offset + limit]


class FakeOutcomeRepository:
    def __init__(self, existing=None):
        self.existing = set(existing or [])
        self.inserted = []

    def existing_prediction_ids(self, prediction_ids):
        return self.existing.intersection(prediction_ids)

    def safe_insert_outcome(self, prediction_id, outcome):
        if prediction_id in self.existing:
            return False
        self.existing.add(prediction_id)
        self.inserted.append({"prediction_id": prediction_id, **outcome})
        return True


class FakeSnapshotRepository:
    def by_ids(self, snapshot_ids):
        return {
            snapshot_id: {
                "id": snapshot_id,
                "spot_price": 100,
                "vwap": 98,
                "vwap_zscore": 1.2,
                "atr": 4,
                "atr_pct": 0.04,
                "return_1h": 0.01,
                "return_4h": 0.02,
                "regime": "TREND_UP",
            }
            for snapshot_id in snapshot_ids
            if snapshot_id
        }


def test_maturity_and_horizon_windows():
    start = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    expected_minutes = {"1H": 60, "2H": 120, "4H": 240, "8H": 480, "12H": 720, "24H": 1440}
    for horizon, minutes in expected_minutes.items():
        row = _prediction(horizon=horizon, created_at=start)
        window_start, window_end = prediction_window(row)
        assert window_start == start
        assert window_end == start + timedelta(minutes=minutes)
        assert is_mature(row, now=window_end) is True
        assert is_mature(row, now=window_end - timedelta(seconds=1)) is False


def test_mature_prediction_persists_outcome_without_mutating_prediction():
    created_at = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    row = _prediction(created_at=created_at)
    before = dict(row)
    predictions = FakePredictionRepository([row])
    outcomes = FakeOutcomeRepository()

    evaluator = LiveOutcomeEvaluator(
        config=ProbabilityEngineConfig(outcome_batch_limit=5),
        prediction_repository=predictions,
        outcome_repository=outcomes,
        snapshot_repository=FakeSnapshotRepository(),
        candle_fetcher=lambda **kwargs: _candles(created_at, count=12),
    )

    result = evaluator.run(now=created_at + timedelta(hours=2))

    assert result["created_count"] == 1
    assert row == before
    inserted = outcomes.inserted[0]
    assert inserted["prediction_id"] == "prediction-1"
    assert inserted["actual_open"] == 100
    assert inserted["actual_high"] == 104
    assert inserted["actual_low"] == 96
    assert inserted["actual_close"] == 101
    assert inserted["range_50_covered"] is True
    assert inserted["range_70_covered"] is True
    assert inserted["range_90_covered"] is True
    assert inserted["range_held"] is True
    assert inserted["metadata_json"]["window_start"] == created_at.isoformat()
    assert inserted["metadata_json"]["window_end"] == (created_at + timedelta(hours=1)).isoformat()


def test_idempotency_skips_existing_outcome():
    created_at = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    row = _prediction(created_at=created_at)
    outcomes = FakeOutcomeRepository(existing={"prediction-1"})
    evaluator = LiveOutcomeEvaluator(
        config=ProbabilityEngineConfig(outcome_batch_limit=5),
        prediction_repository=FakePredictionRepository([row]),
        outcome_repository=outcomes,
        snapshot_repository=FakeSnapshotRepository(),
        candle_fetcher=lambda **kwargs: _candles(created_at, count=12),
    )

    result = evaluator.run(now=created_at + timedelta(hours=2))

    assert result["created_count"] == 0
    assert result["skipped_existing_count"] == 1
    assert outcomes.inserted == []


def test_incomplete_candles_are_not_persisted_and_can_retry_later():
    created_at = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    row = _prediction(created_at=created_at)
    outcomes = FakeOutcomeRepository()
    evaluator = LiveOutcomeEvaluator(
        config=ProbabilityEngineConfig(outcome_batch_limit=5),
        prediction_repository=FakePredictionRepository([row]),
        outcome_repository=outcomes,
        snapshot_repository=FakeSnapshotRepository(),
        candle_fetcher=lambda **kwargs: _candles(created_at, count=3),
    )

    result = evaluator.run(now=created_at + timedelta(hours=2))

    assert result["created_count"] == 0
    assert result["skipped_incomplete_count"] == 1
    assert outcomes.inserted == []


def test_second_aligned_prediction_accepts_complete_5m_candles():
    created_at = datetime(2026, 8, 18, 0, 0, 7, tzinfo=timezone.utc)
    row = _prediction(created_at=created_at)
    candles = _candles(datetime(2026, 8, 18, 0, 5, tzinfo=timezone.utc), count=11)
    outcomes = FakeOutcomeRepository()
    evaluator = LiveOutcomeEvaluator(
        config=ProbabilityEngineConfig(outcome_batch_limit=5),
        prediction_repository=FakePredictionRepository([row]),
        outcome_repository=outcomes,
        snapshot_repository=FakeSnapshotRepository(),
        candle_fetcher=lambda **kwargs: candles,
    )

    result = evaluator.run(now=created_at + timedelta(hours=2))

    assert result["created_count"] == 1
    assert outcomes.inserted[0]["metadata_json"]["candle_count"] == 11


def test_batch_limit_bounds_backlog_processing():
    created_at = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    rows = [_prediction(f"prediction-{index}", created_at=created_at) for index in range(6)]
    outcomes = FakeOutcomeRepository()
    evaluator = LiveOutcomeEvaluator(
        config=ProbabilityEngineConfig(outcome_batch_limit=2),
        prediction_repository=FakePredictionRepository(rows),
        outcome_repository=outcomes,
        snapshot_repository=FakeSnapshotRepository(),
        candle_fetcher=lambda **kwargs: _candles(created_at, count=12),
    )

    result = evaluator.run(now=created_at + timedelta(hours=2))

    assert result["attempted_count"] == 2
    assert result["created_count"] == 2
    assert len(outcomes.inserted) == 2


def test_paged_candidate_scan_gets_past_existing_outcomes():
    created_at = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    rows = [_prediction(f"prediction-{index}", created_at=created_at) for index in range(9)]
    outcomes = FakeOutcomeRepository(existing={f"prediction-{index}" for index in range(8)})
    evaluator = LiveOutcomeEvaluator(
        config=ProbabilityEngineConfig(outcome_batch_limit=2),
        prediction_repository=FakePredictionRepository(rows),
        outcome_repository=outcomes,
        snapshot_repository=FakeSnapshotRepository(),
        candle_fetcher=lambda **kwargs: _candles(created_at, count=12),
    )

    result = evaluator.run(now=created_at + timedelta(hours=2))

    assert result["created_count"] == 1
    assert outcomes.inserted[0]["prediction_id"] == "prediction-8"
    assert result["candidate_pages_scanned"] == 2


def test_event_labels_and_close_coverage_semantics():
    created_at = datetime(2026, 8, 18, 0, 0, tzinfo=timezone.utc)
    row = _prediction(created_at=created_at)
    row["metadata_json"] = {
        "initial_vwap_zscore": 1.2,
        "mean_reversion_target": 98,
        "trend_direction": "UP",
        "trend_threshold": 3,
    }
    outcomes = FakeOutcomeRepository()
    evaluator = LiveOutcomeEvaluator(
        config=ProbabilityEngineConfig(outcome_batch_limit=5),
        prediction_repository=FakePredictionRepository([row]),
        outcome_repository=outcomes,
        snapshot_repository=FakeSnapshotRepository(),
        candle_fetcher=lambda **kwargs: _candles(created_at, count=12, high=107, low=96, close=104),
    )

    result = evaluator.run(now=created_at + timedelta(hours=2))

    assert result["created_count"] == 1
    inserted = outcomes.inserted[0]
    assert inserted["mean_reversion_occurred"] is True
    assert inserted["upside_breakout_occurred"] is True
    assert inserted["downside_breakdown_occurred"] is False
    assert inserted["trend_continuation_occurred"] is True
    assert inserted["range_50_covered"] is False
    assert inserted["range_70_covered"] is True
    assert inserted["range_90_covered"] is True
    assert inserted["range_held"] is False
