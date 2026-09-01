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
        self.rows = sorted(rows, key=lambda row: (row["prediction_timestamp"], row["horizon"], row["target"], row["id"]))
        self.keyset_calls = 0

    def mature_candidates(self, before_iso, limit=100, offset=0):
        before = pd.Timestamp(before_iso)
        pending = [row for row in self.rows if pd.Timestamp(row["prediction_timestamp"]) <= before]
        return pending[offset : offset + limit]

    def mature_candidates_after(self, before_iso, after_timestamp_iso=None, limit=100):
        self.keyset_calls += 1
        before = pd.Timestamp(before_iso)
        after = pd.Timestamp(after_timestamp_iso) if after_timestamp_iso else None
        pending = [row for row in self.rows if pd.Timestamp(row["prediction_timestamp"]) <= before]
        if after is not None:
            pending = [row for row in pending if pd.Timestamp(row["prediction_timestamp"]) > after]
        return pending[:limit]


class FakeV2OutcomeRepository:
    def __init__(self, existing=None):
        self.existing = set(existing or [])
        self.inserted = []
        self.bulk_calls = 0

    def existing_prediction_ids(self, prediction_ids, label_version="label_v2"):
        return self.existing.intersection(prediction_ids)

    def safe_insert_outcome(self, prediction_id, outcome, label_version="label_v2"):
        if prediction_id in self.existing:
            return False
        self.existing.add(prediction_id)
        self.inserted.append({"prediction_id": prediction_id, "label_version": label_version, **outcome})
        return True

    def safe_insert_outcomes(self, outcomes, label_version="label_v2"):
        self.bulk_calls += 1
        created = 0
        failed = 0
        for prediction_id, outcome in outcomes:
            if self.safe_insert_outcome(prediction_id, outcome, label_version=label_version):
                created += 1
            else:
                failed += 1
        return created, failed


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
    assert result["ohlcv_fetch_count"] == 1
    assert result["outcome_group_count"] == 1


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


def test_v2_shadow_outcome_repository_bulk_insert_ignores_duplicates(monkeypatch):
    from probability_engine.services import v2_shadow_outcome
    from probability_engine.services.v2_shadow_outcome import V2ShadowOutcomeRepository

    captured = {}

    class FakeResponse:
        status_code = 201
        text = "[]"

        def json(self):
            return []

    def fake_post(url, headers, params, json, timeout):
        captured.update(
            {
                "url": url,
                "headers": headers,
                "params": params,
                "json": json,
                "timeout": timeout,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(v2_shadow_outcome.requests, "post", fake_post)
    repository = V2ShadowOutcomeRepository()

    created, failed = repository.safe_insert_outcomes(
        [
            (
                "v2-1",
                {
                    "ok": True,
                    "outcome": False,
                    "actual_open": 100,
                    "evaluated_at": "2026-01-01T01:00:00+00:00",
                    "metadata_json": {"target": "path_inside_70", "horizon": "1H"},
                },
            )
        ]
    )

    assert created == 0
    assert failed == 0
    assert captured["params"] == {"on_conflict": "prediction_id,label_version,target"}
    assert "resolution=ignore-duplicates" in captured["headers"]["Prefer"]
    assert captured["json"][0]["prediction_id"] == "v2-1"
    assert captured["json"][0]["target"] == "path_inside_70"
    assert "ok" not in captured["json"][0]


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


def test_v2_shadow_outcome_selector_reaches_beyond_completed_prefix(monkeypatch):
    from probability_engine.services import v2_shadow_outcome
    from probability_engine.services.v2_shadow_outcome import V2ShadowOutcomeEvaluator

    monkeypatch.setattr(v2_shadow_outcome, "get_probability_config", lambda: type("Config", (), {"v2_outcome_candidate_max_pages": 80})())
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    completed = [
        _v2_prediction(f"done-{index}", prediction_timestamp=start + timedelta(minutes=5 * index))
        for index in range(2000)
    ]
    pending = [
        _v2_prediction(f"pending-{index}", prediction_timestamp=start + timedelta(minutes=5 * (2000 + index)))
        for index in range(30)
    ]
    immature = [
        _v2_prediction("immature", prediction_timestamp=start + timedelta(days=20))
    ]
    repo = FakeV2PredictionRepository(completed + pending + immature)
    outcomes = FakeV2OutcomeRepository(existing={row["id"] for row in completed})
    evaluator = V2ShadowOutcomeEvaluator(
        prediction_repository=repo,
        outcome_repository=outcomes,
        feature_snapshot_repository=FakeV2SnapshotRepository(),
        candle_fetcher=lambda symbol, start_at, end_at: _v2_candles(start_at),
        batch_limit=25,
    )

    result = evaluator.run(now=start + timedelta(days=15))

    assert result["attempted_count"] == 25
    assert result["created_count"] == 25
    assert result["candidate_pages_scanned"] > 20
    assert result["prediction_query_count"] == result["candidate_pages_scanned"]
    assert result["outcome_lookup_count"] == result["candidate_pages_scanned"]
    assert result["oldest_selected_timestamp"] == pd.Timestamp(pending[0]["prediction_timestamp"]).tz_convert("UTC").isoformat()
    assert {row["prediction_id"] for row in outcomes.inserted} == {row["id"] for row in pending[:25]}


def test_v2_shadow_outcome_selector_rerun_advances_without_duplicates(monkeypatch):
    from probability_engine.services import v2_shadow_outcome
    from probability_engine.services.v2_shadow_outcome import V2ShadowOutcomeEvaluator

    monkeypatch.setattr(v2_shadow_outcome, "get_probability_config", lambda: type("Config", (), {"v2_outcome_candidate_max_pages": 20})())
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    rows = [
        _v2_prediction(f"pending-{index}", prediction_timestamp=start + timedelta(minutes=5 * index))
        for index in range(12)
    ]
    prediction_repo = FakeV2PredictionRepository(rows)
    outcomes = FakeV2OutcomeRepository()
    evaluator = V2ShadowOutcomeEvaluator(
        prediction_repository=prediction_repo,
        outcome_repository=outcomes,
        feature_snapshot_repository=FakeV2SnapshotRepository(),
        candle_fetcher=lambda symbol, start_at, end_at: _v2_candles(start_at),
        batch_limit=5,
    )

    first = evaluator.run(now=start + timedelta(days=1))
    second = evaluator.run(now=start + timedelta(days=1))

    assert first["created_count"] == 5
    assert second["created_count"] == 5
    assert {row["prediction_id"] for row in outcomes.inserted[:5]} == {row["id"] for row in rows[:5]}
    assert {row["prediction_id"] for row in outcomes.inserted[5:]} == {row["id"] for row in rows[5:10]}
    assert len({row["prediction_id"] for row in outcomes.inserted}) == 10


def test_v2_shadow_outcome_evaluator_reuses_future_path_by_timestamp_horizon():
    from probability_engine.services.v2_shadow_outcome import V2ShadowOutcomeEvaluator

    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    rows = [
        _v2_prediction("inside", target="path_inside_70", horizon="1H", prediction_timestamp=start),
        _v2_prediction("breached", target="range_breached", horizon="1H", prediction_timestamp=start),
        _v2_prediction("both", target="both_side_breach", horizon="1H", prediction_timestamp=start),
    ]
    calls = []

    def candle_fetcher(symbol, start_at, end_at):
        calls.append((symbol, start_at, end_at))
        return _v2_candles(start, high=106, low=94)

    outcomes = FakeV2OutcomeRepository()
    evaluator = V2ShadowOutcomeEvaluator(
        prediction_repository=FakeV2PredictionRepository(rows),
        outcome_repository=outcomes,
        feature_snapshot_repository=FakeV2SnapshotRepository(),
        candle_fetcher=candle_fetcher,
        batch_limit=10,
    )

    result = evaluator.run(now=start + timedelta(hours=2))

    assert result["attempted_count"] == 3
    assert result["created_count"] == 3
    assert result["ohlcv_fetch_count"] == 1
    assert result["outcome_group_count"] == 1
    assert len(calls) == 1
    by_id = {row["prediction_id"]: row["outcome"] for row in outcomes.inserted}
    assert by_id == {"inside": False, "breached": True, "both": True}


def test_v2_shadow_grouped_evaluator_matches_reference_per_row_semantics():
    from probability_engine.services.v2_shadow_outcome import (
        V2ShadowOutcomeEvaluator,
        evaluate_shadow_target,
    )

    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    targets = [
        "path_inside_70",
        "range_breached",
        "both_side_breach",
        "upside_breakout",
        "downside_breakdown",
        "upper_breach_only",
        "lower_breach_only",
        "realized_over_range_width_ge_1",
        "up_excursion_ge_1_0_atr",
        "down_excursion_ge_1_0_atr",
    ]
    rows = [
        _v2_prediction(target, target=target, horizon="1H", prediction_timestamp=start)
        for target in targets
    ]
    candles = _v2_candles(start, high=106, low=94)
    snapshot = FakeV2SnapshotRepository().by_ids(["snap-1"])["snap-1"]
    expected = {
        row["id"]: evaluate_shadow_target(row, candles, snapshot)["outcome"]
        for row in rows
    }
    outcomes = FakeV2OutcomeRepository()
    evaluator = V2ShadowOutcomeEvaluator(
        prediction_repository=FakeV2PredictionRepository(rows),
        outcome_repository=outcomes,
        feature_snapshot_repository=FakeV2SnapshotRepository(),
        candle_fetcher=lambda symbol, start_at, end_at: candles,
        batch_limit=20,
    )

    result = evaluator.run(now=start + timedelta(hours=2))

    actual = {row["prediction_id"]: row["outcome"] for row in outcomes.inserted}
    assert result["created_count"] == len(rows)
    assert actual == expected
    assert result["ohlcv_fetch_count"] == 1
    assert outcomes.bulk_calls == 1


def test_v2_shadow_grouped_evaluator_marks_group_incomplete_without_persistence():
    from probability_engine.services.v2_shadow_outcome import V2ShadowOutcomeEvaluator

    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    rows = [
        _v2_prediction("inside", target="path_inside_70", horizon="1H", prediction_timestamp=start),
        _v2_prediction("breached", target="range_breached", horizon="1H", prediction_timestamp=start),
    ]
    outcomes = FakeV2OutcomeRepository()
    evaluator = V2ShadowOutcomeEvaluator(
        prediction_repository=FakeV2PredictionRepository(rows),
        outcome_repository=outcomes,
        feature_snapshot_repository=FakeV2SnapshotRepository(),
        candle_fetcher=lambda symbol, start_at, end_at: _v2_candles(start, count=3),
        batch_limit=10,
    )

    result = evaluator.run(now=start + timedelta(hours=2))

    assert result["created_count"] == 0
    assert result["skipped_incomplete_count"] == 2
    assert outcomes.inserted == []


def test_v2_shadow_outcome_evaluator_uses_v2_specific_batch_env(monkeypatch):
    from probability_engine.services.v2_shadow_outcome import V2ShadowOutcomeEvaluator

    monkeypatch.setenv("PROBABILITY_OUTCOME_BATCH_LIMIT", "25")
    monkeypatch.setenv("PROBABILITY_V2_OUTCOME_BATCH_SIZE", "75")

    evaluator = V2ShadowOutcomeEvaluator(
        prediction_repository=FakeV2PredictionRepository([]),
        outcome_repository=FakeV2OutcomeRepository(),
        feature_snapshot_repository=FakeV2SnapshotRepository(),
        candle_fetcher=lambda symbol, start_at, end_at: pd.DataFrame(),
    )

    assert evaluator.batch_limit == 75


def test_v2_shadow_outcome_evaluator_falls_back_to_shared_batch_env(monkeypatch):
    from probability_engine.services.v2_shadow_outcome import V2ShadowOutcomeEvaluator

    monkeypatch.setenv("PROBABILITY_OUTCOME_BATCH_LIMIT", "50")
    monkeypatch.delenv("PROBABILITY_V2_OUTCOME_BATCH_SIZE", raising=False)

    evaluator = V2ShadowOutcomeEvaluator(
        prediction_repository=FakeV2PredictionRepository([]),
        outcome_repository=FakeV2OutcomeRepository(),
        feature_snapshot_repository=FakeV2SnapshotRepository(),
        candle_fetcher=lambda symbol, start_at, end_at: pd.DataFrame(),
    )

    assert evaluator.batch_limit == 50
