from __future__ import annotations

import math
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from probability_engine.services import v2_1_shadow_outcome as outcome
from probability_engine.services import v2_1_shadow_service as svc


EXPECTED_HASHES = {
    ("realized_over_range_width_ge_1", "12H"): "fb9eda0fba033a1f7a024f96d5daf9275e67b0dc6d85c9c4e60941327bc1310f",
    ("up_excursion_ge_1_0_atr", "1H"): "eaa2b4db1da44c104245f335da9f8b7e8d17b8fb1f26348c90b32d37c777f499",
}


def _baseline_rows():
    ts = "2026-09-22T10:00:00+00:00"
    rows = []
    targets = [
        ("realized_over_range_width_ge_1", "12H"),
        ("up_excursion_ge_1_0_atr", "1H"),
        ("path_inside_70", "4H"),
    ]
    for index, (target, horizon) in enumerate(targets):
        rows.append(
            {
                "id": f"00000000-0000-0000-0000-00000000000{index}",
                "prediction_timestamp": ts,
                "symbol": "ETHUSD",
                "record_type": "LIVE",
                "model_version": svc.V2_MODEL_VERSION,
                "model_id": f"v2__{target}__{horizon.lower()}",
                "target": target,
                "horizon": horizon,
                "calibrated_probability": 0.55 + index * 0.05,
                "feature_snapshot_id": f"snapshot-{index}",
                "manifest_hash": "v2-manifest",
            }
        )
    return rows


def _orderflow():
    return {
        "bucket_timestamp": "2026-09-22T09:59:00+00:00",
        "source_status": "COMPLETE",
        "cvd_increment": 10.0,
        "cvd_5m": 20.0,
        "cvd_15m": -5.0,
        "cvd_1h": 100.0,
        "taker_buy_ratio": 0.53,
        "total_volume": 5000.0,
    }


class FakeBaselineRepository:
    def latest_batch(self, symbol="ETHUSD", limit=80):
        return _baseline_rows()


class FakePredictionRepository:
    def insert_many_returning(self, payloads):
        return [{**row, "id": f"v21-{index}"} for index, row in enumerate(payloads)]


@pytest.fixture(autouse=True)
def clear_model_caches():
    svc.load_manifest.cache_clear()
    svc.load_model.cache_clear()
    yield
    svc.load_manifest.cache_clear()
    svc.load_model.cache_clear()


def test_frozen_v2_1_manifest_hashes_match_champion_contract():
    manifest = svc.load_manifest()
    observed = {
        (entry["target"], entry["horizon"]): entry["sha256"]
        for entry in manifest["models"]
    }
    assert observed == EXPECTED_HASHES


def test_shadow_engine_routes_rich_champions_and_v2_fallback(monkeypatch):
    monkeypatch.setattr(svc, "get_probability_config", lambda: SimpleNamespace(v21_shadow_enabled=True, symbol="ETHUSD"))
    monkeypatch.setattr(svc, "latest_orderflow_asof", lambda prediction_timestamp: (_orderflow(), None, 60.0))
    engine = svc.V21ShadowEngine(FakeBaselineRepository(), FakePredictionRepository())

    result = engine.run_shadow_prediction(persist=False)

    assert result["ok"] is True
    assert result["action"] == "DRY_RUN"
    assert result["candidate_count"] == 3
    assert result["rich_count"] == 2
    assert result["fallback_count"] == 1
    assert result["fallback_reasons"] == {"V2_0_CHAMPION": 1}


def test_rich_champion_falls_back_when_orderflow_is_stale(monkeypatch):
    monkeypatch.setattr(svc, "get_probability_config", lambda: SimpleNamespace(v21_shadow_enabled=True, symbol="ETHUSD"))
    monkeypatch.setattr(svc, "latest_orderflow_asof", lambda prediction_timestamp: (_orderflow(), "ORDERFLOW_STALE", 600.0))
    engine = svc.V21ShadowEngine(FakeBaselineRepository(), FakePredictionRepository())

    result = engine.run_shadow_prediction(persist=False)

    assert result["ok"] is True
    assert result["rich_count"] == 0
    assert result["fallback_count"] == 3
    assert result["fallback_reasons"]["ORDERFLOW_STALE"] == 2
    assert result["fallback_reasons"]["V2_0_CHAMPION"] == 1


def test_prediction_probability_is_finite_and_bounded():
    model = svc.load_model("up_excursion_ge_1_0_atr", "1H")
    probability = svc.predict_model_probability(model, 0.61, _orderflow())

    assert math.isfinite(probability)
    assert 0.0 <= probability <= 1.0


def test_shadow_disabled_does_not_read_or_persist(monkeypatch):
    monkeypatch.setattr(svc, "get_probability_config", lambda: SimpleNamespace(v21_shadow_enabled=False, symbol="ETHUSD"))
    engine = svc.V21ShadowEngine(FakeBaselineRepository(), FakePredictionRepository())

    result = engine.run_shadow_prediction(persist=True)

    assert result == {
        "ok": True,
        "enabled": False,
        "action": "DISABLED",
        "reason": "PROBABILITY_V2_1_SHADOW_ENABLED=false",
    }


def test_v2_1_outcome_evaluator_reuses_v2_baseline_label_semantics(monkeypatch):
    now = datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)
    v21_prediction = {
        "id": "v21-prediction",
        "prediction_timestamp": "2026-09-22T10:00:00+00:00",
        "symbol": "ETHUSD",
        "record_type": "LIVE",
        "model_version": svc.MODEL_VERSION,
        "target": "up_excursion_ge_1_0_atr",
        "horizon": "1H",
        "v2_baseline_prediction_id": "v2-prediction",
    }
    v2_prediction = {
        "id": "v2-prediction",
        "feature_snapshot_id": "snapshot-1",
        "prediction_timestamp": "2026-09-22T10:00:00+00:00",
        "symbol": "ETHUSD",
        "target": "up_excursion_ge_1_0_atr",
        "horizon": "1H",
    }

    class PredictionRepo:
        def recent_mature_candidates(self, before_iso, limit=100):
            return [v21_prediction]

    class OutcomeRepo:
        saved = []

        def existing_prediction_ids(self, prediction_ids):
            return set()

        def safe_insert_outcomes(self, outcomes):
            self.saved = outcomes
            return len(outcomes), 0

    class SnapshotRepo:
        def by_ids(self, ids):
            return {"snapshot-1": {"id": "snapshot-1"}}

    outcome_repo = OutcomeRepo()
    monkeypatch.setattr(outcome, "fetch_baseline_predictions", lambda ids: {"v2-prediction": v2_prediction})
    monkeypatch.setattr(outcome, "prediction_window", lambda prediction: ("start", "end"))
    monkeypatch.setattr(outcome, "load_future_ohlcv", lambda symbol, start, end: [{"close": 1}])
    monkeypatch.setattr(
        outcome,
        "evaluate_shadow_target",
        lambda prediction, candles, snapshot: {
            "ok": True,
            "outcome": True,
            "metadata_json": {"target": prediction["target"], "horizon": prediction["horizon"]},
        },
    )

    result = outcome.V21ShadowOutcomeEvaluator(
        prediction_repository=PredictionRepo(),
        outcome_repository=outcome_repo,
        feature_snapshot_repository=SnapshotRepo(),
        batch_limit=5,
    ).run(now=now)

    assert result["ok"] is True
    assert result["created_count"] == 1
    saved_prediction_id, saved_outcome = outcome_repo.saved[0]
    assert saved_prediction_id == "v21-prediction"
    assert saved_outcome["metadata_json"]["v2_baseline_prediction_id"] == "v2-prediction"
    assert "exact V2.0 label semantics" in saved_outcome["metadata_json"]["semantics"]
