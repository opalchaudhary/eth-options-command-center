from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from probability_engine.services.v2_shadow_outcome import is_mature
from probability_engine.services.v2_shadow_service import (
    FROZEN_MANIFEST_HASH,
    clamp_probability,
    compute_v2_features_for_timestamps,
    load_ohlcv_from_supabase,
    load_manifest,
    manifest_identity_hash,
    requires_range_reference,
    semantic_manifest_hash,
)


def _ohlcv(rows=420):
    timestamps = pd.date_range("2026-01-01T00:00:00Z", periods=rows, freq="5min")
    return pd.DataFrame(
        {
            "candle_time": timestamps,
            "open": [100 + i * 0.01 for i in range(rows)],
            "high": [101 + i * 0.01 for i in range(rows)],
            "low": [99 + i * 0.01 for i in range(rows)],
            "close": [100.5 + i * 0.01 for i in range(rows)],
            "volume": [10 + (i % 7) for i in range(rows)],
        }
    )


def test_packaged_manifest_loads_and_has_frozen_counts():
    manifest = load_manifest()

    assert manifest["spec_version"] == "probability_v2_candidate_v1"
    assert len(manifest["models"]) == 26
    assert len(manifest["derived_outputs"]) == 4
    assert manifest_identity_hash() == FROZEN_MANIFEST_HASH


def test_manifest_semantic_hash_is_line_ending_independent(tmp_path):
    source = load_manifest()
    lf_path = tmp_path / "manifest.json"
    lf_path.write_text(__import__("json").dumps(source, indent=2) + "\n", encoding="utf-8", newline="\n")

    assert semantic_manifest_hash(source) == semantic_manifest_hash(load_manifest(lf_path))
    assert manifest_identity_hash(lf_path) == FROZEN_MANIFEST_HASH


def test_historical_feature_mode_is_deterministic_for_supplied_timestamp_grid():
    ohlcv = _ohlcv()
    timestamps = pd.Series(pd.date_range("2026-01-01T12:00:00Z", periods=6, freq="30min"))

    first = compute_v2_features_for_timestamps(ohlcv, timestamps)
    second = compute_v2_features_for_timestamps(ohlcv, timestamps)

    pd.testing.assert_frame_equal(first, second)
    assert (first["feature_source_timestamp"] <= first["prediction_timestamp"]).all()


def test_probability_clamp_rejects_bad_values_and_bounds_good_values():
    assert clamp_probability(-0.1) == 0.0
    assert clamp_probability(1.1) == 1.0
    assert clamp_probability(float("nan")) is None
    assert clamp_probability(0.42) == 0.42


def test_range_dependent_targets_require_range_reference():
    assert requires_range_reference("path_inside_70") is True
    assert requires_range_reference("up_excursion_ge_1_0_atr") is False


def test_live_ohlcv_loader_pages_until_current_cutoff(monkeypatch):
    import probability_engine.services.v2_shadow_service as service

    class Response:
        status_code = 200
        text = ""

        def __init__(self, rows):
            self._rows = rows

        def json(self):
            return self._rows

    pages = [
        [
            {"symbol": "ETHUSD", "resolution": "5m", "candle_time": pd.Timestamp("2026-01-01T00:00:00Z") + pd.Timedelta(minutes=5 * i), "epoch_time": i, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}
            for i in range(1000)
        ],
        [
            {"symbol": "ETHUSD", "resolution": "5m", "candle_time": pd.Timestamp("2026-01-04T12:00:00Z") + pd.Timedelta(minutes=5 * i), "epoch_time": 1000 + i, "open": 2, "high": 2, "low": 2, "close": 2, "volume": 2}
            for i in range(3)
        ],
    ]
    calls = []

    def fake_get(url, headers, params, timeout):
        calls.append(params["candle_time"])
        return Response([{**row, "candle_time": row["candle_time"].isoformat()} for row in pages[len(calls) - 1]])

    monkeypatch.setattr(service.database_reader, "SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setattr(service.requests, "get", fake_get)

    frame = load_ohlcv_from_supabase(end_at=pd.Timestamp("2026-01-04T12:10:00Z"), days=4)

    assert len(calls) == 2
    assert len(frame) == 1003
    assert frame["candle_time"].max() == pd.Timestamp("2026-01-04T12:10:00Z")
    assert frame["candle_time"].is_monotonic_increasing


def test_v2_shadow_outcome_maturity_respects_horizon():
    prediction = {"prediction_timestamp": "2026-01-01T00:00:00Z", "horizon": "2H"}

    assert is_mature(prediction, now=datetime(2026, 1, 1, 1, 59, tzinfo=timezone.utc)) is False
    assert is_mature(prediction, now=datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc)) is True


def _v2_prediction(prediction_id="v2-1", target="path_inside_70", horizon="1H", prediction_timestamp=None):
    prediction_timestamp = prediction_timestamp or datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    return {
        "id": prediction_id,
        "feature_snapshot_id": "snap-1",
        "prediction_timestamp": prediction_timestamp.isoformat(),
        "symbol": "ETHUSD",
        "record_type": "LIVE",
        "model_version": "probability_v2_candidate_v1",
        "model_id": f"probability_v2_candidate_v1__{target}__{horizon.lower()}",
        "target": target,
        "horizon": horizon,
        "feature_version": "probability_v2_features_v1",
        "label_version": "label_v2",
        "calibration_version": "calibration_v2_candidate_v1",
        "manifest_hash": FROZEN_MANIFEST_HASH,
        "metadata_json": {
            "range_70_lower": 95,
            "range_70_upper": 105,
            "range_reference_status": "OK",
        },
    }


def _v2_candles(start, count=12, high=104, low=96, close=101):
    return pd.DataFrame(
        [
            {
                "candle_time": start + timedelta(minutes=5 * index),
                "open": 100,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1,
            }
            for index in range(count)
        ]
    )


class FakeV2PredictionRepository:
    def __init__(self, rows):
        self.rows = rows

    def mature_candidates(self, before_iso, limit=100, offset=0):
        before = pd.Timestamp(before_iso)
        pending = [row for row in self.rows if pd.Timestamp(row["prediction_timestamp"]) <= before]
        return pending[offset : offset + limit]


class FakeV2OutcomeRepository:
    def __init__(self, existing=None):
        self.existing = set(existing or [])
        self.inserted = []

    def existing_prediction_ids(self, prediction_ids, label_version="label_v2"):
        return self.existing.intersection(prediction_ids)

    def safe_insert_outcome(self, prediction_id, outcome, label_version="label_v2"):
        if prediction_id in self.existing:
            return False
        self.existing.add(prediction_id)
        self.inserted.append({"prediction_id": prediction_id, "label_version": label_version, **outcome})
        return True


class FakeV2SnapshotRepository:
    def by_ids(self, snapshot_ids):
        return {
            "snap-1": {
                "id": "snap-1",
                "feature_vector_json": {"atr_pct_12b": 0.02},
                "metadata_json": {},
            }
        }


def test_v2_shadow_outcome_evaluator_persists_mature_prediction():
    from probability_engine.services.v2_shadow_outcome import V2ShadowOutcomeEvaluator

    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    outcomes = FakeV2OutcomeRepository()
    evaluator = V2ShadowOutcomeEvaluator(
        prediction_repository=FakeV2PredictionRepository([_v2_prediction(prediction_timestamp=start)]),
        outcome_repository=outcomes,
        feature_snapshot_repository=FakeV2SnapshotRepository(),
        candle_fetcher=lambda symbol, start_at, end_at: _v2_candles(start),
        batch_limit=5,
    )

    result = evaluator.run(now=start + timedelta(hours=2))

    assert result["created_count"] == 1
    inserted = outcomes.inserted[0]
    assert inserted["prediction_id"] == "v2-1"
    assert inserted["outcome"] is True
    assert inserted["metadata_json"]["target"] == "path_inside_70"
    assert inserted["metadata_json"]["horizon"] == "1H"
    assert inserted["metadata_json"]["manifest_hash"] == FROZEN_MANIFEST_HASH


def test_v2_shadow_outcome_repository_strips_non_schema_fields():
    from probability_engine.services.v2_shadow_outcome import V2ShadowOutcomeRepository

    class CapturingRepository(V2ShadowOutcomeRepository):
        def __init__(self):
            self.payload = None

        def safe_insert(self, payload):
            self.payload = payload
            return True

    repository = CapturingRepository()

    ok = repository.safe_insert_outcome(
        "v2-1",
        {
            "ok": True,
            "outcome": False,
            "actual_open": 100,
            "evaluated_at": "2026-01-01T01:00:00+00:00",
            "metadata_json": {
                "target": "path_inside_70",
                "horizon": "1H",
                "manifest_hash": FROZEN_MANIFEST_HASH,
            },
        },
    )

    assert ok is True
    assert repository.payload["prediction_id"] == "v2-1"
    assert repository.payload["target"] == "path_inside_70"
    assert repository.payload["horizon"] == "1H"
    assert repository.payload["outcome"] is False
    assert repository.payload["metadata_json"]["manifest_hash"] == FROZEN_MANIFEST_HASH
    assert "ok" not in repository.payload


def test_v2_shadow_outcome_evaluator_skips_immature_and_existing_rows():
    from probability_engine.services.v2_shadow_outcome import V2ShadowOutcomeEvaluator

    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    rows = [
        _v2_prediction("existing", prediction_timestamp=start),
        _v2_prediction("immature", prediction_timestamp=start + timedelta(minutes=30)),
    ]
    outcomes = FakeV2OutcomeRepository(existing={"existing"})
    evaluator = V2ShadowOutcomeEvaluator(
        prediction_repository=FakeV2PredictionRepository(rows),
        outcome_repository=outcomes,
        feature_snapshot_repository=FakeV2SnapshotRepository(),
        candle_fetcher=lambda symbol, start_at, end_at: _v2_candles(start),
        batch_limit=5,
    )

    result = evaluator.run(now=start + timedelta(hours=1))

    assert result["created_count"] == 0
    assert result["skipped_existing_count"] == 1
    assert outcomes.inserted == []
