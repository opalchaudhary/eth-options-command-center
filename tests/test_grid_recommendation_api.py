from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from backend.routers import grid as grid_router
from grid_bot.recommendation_service import (
    GridRecommendationService,
    GridRecommendationStorageError,
    GridRecommendationUnavailable,
)


NOW = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)


def _row(target, probability, horizon="12H", ts=None, **updates):
    ts = ts or NOW - timedelta(minutes=5)
    payload = {
        "id": f"{target}-{horizon}",
        "feature_snapshot_id": "snap-1",
        "prediction_timestamp": ts.isoformat(),
        "symbol": "ETHUSD",
        "record_type": "LIVE",
        "model_version": "probability_v2_candidate_v1",
        "model_id": f"probability_v2_candidate_v1__{target}__{horizon.lower()}",
        "target": target,
        "horizon": horizon,
        "calibrated_probability": probability,
        "manifest_hash": "aa59ecc3c4a036ff1309d617ecd20566c2378582b687d5f163ae64370907019b",
        "abstained": False,
        "ood_status": "OK",
        "metadata_json": {
            "range_reference_status": "OK",
            "range_70_lower": 4400,
            "range_70_upper": 4600,
            "range_model_version": "probability_v1",
        },
    }
    payload.update(updates)
    return payload


def _rows(horizon="12H", ts=None, inside=0.68, expansion=0.35, up=None, down=None):
    rows = [
        _row("path_inside_70", inside, horizon, ts),
        _row("realized_over_range_width_ge_1", expansion, horizon, ts),
    ]
    if up is not None:
        rows.append(_row("upside_breakout", up, horizon, ts))
    if down is not None:
        rows.append(_row("downside_breakdown", down, horizon, ts))
    return rows


class FakePredictionRepository:
    def __init__(self, rows=None, error=None):
        self.rows = rows or []
        self.error = error
        self.calls = []

    def latest(self, symbol="ETHUSD", limit=120):
        self.calls.append({"symbol": symbol, "limit": limit})
        if self.error:
            raise self.error
        return self.rows


class FakeGridRepository:
    enabled = True

    def __init__(self, snapshot=None):
        self.snapshot = snapshot
        self.calls = 0

    def active_config_snapshot(self):
        self.calls += 1
        return self.snapshot


def _grid(lower=4400, upper=4600, grid_type="neutral", grid_count=9, spacing_type="arithmetic"):
    return {
        "run_id": "run-1",
        "bot_id": "bot-1",
        "status": "RUNNING",
        "config_version": 1,
        "product_symbol": "ETHUSD",
        "grid_type": grid_type,
        "lower_price": str(lower),
        "upper_price": str(upper),
        "grid_count": grid_count,
        "spacing_type": spacing_type,
    }


def _market(price=4500):
    return lambda include_orderbook=False: {"ok": True, "symbol": "ETHUSD", "spot_price": price}


def _service(rows=None, grid=None, market=None, now=NOW, error=None):
    return GridRecommendationService(
        prediction_repository=FakePredictionRepository(rows, error=error),
        grid_repository=FakeGridRepository(grid),
        market_snapshot_fn=market or _market(),
        now_fn=lambda: now,
    )


def test_complete_valid_12h_v2_data_returns_complete_recommendation():
    payload = _service(_rows()).recommendation()

    assert payload["ok"] is True
    assert payload["symbol"] == "ETHUSD"
    assert payload["recommendation"]["operating_horizon"] == "12H"
    assert payload["recommendation"]["grid_type"] == "neutral"
    assert payload["recommendation"]["confidence_type"] == "RECOMMENDER_CONFIDENCE"
    assert payload["recommendation"]["inputs_summary"]["path_inside_70"] == 0.68
    assert payload["sources"]["probability_source"] == "probability_v2_shadow_predictions"


def test_current_grid_absent_still_returns_complete_recommendation():
    payload = _service(_rows(), grid=None).recommendation()

    rec = payload["recommendation"]
    assert payload["current_grid"] is None
    assert rec["lower_price"] is not None
    assert rec["upper_price"] is not None
    assert rec["grid_count"] is not None


def test_current_grid_within_tolerance_keeps_current():
    payload = _service(_rows(), grid=_grid()).recommendation()

    assert payload["recommendation"]["action"] == "KEEP_CURRENT"


def test_mild_current_grid_drift_consider_edit():
    payload = _service(_rows(), grid=_grid(lower=4370, upper=4630)).recommendation()

    assert payload["recommendation"]["action"] == "CONSIDER_EDIT"


def test_material_current_grid_mismatch_regrid():
    payload = _service(_rows(), grid=_grid(grid_type="long_bias")).recommendation()

    assert payload["recommendation"]["action"] == "REGRID"


def test_strong_bullish_v2_directional_data_recommends_long_bias():
    payload = _service(_rows(horizon="24H", up=0.72, down=0.35)).recommendation()

    assert payload["recommendation"]["grid_type"] == "long_bias"


def test_strong_bearish_v2_directional_data_recommends_short_bias():
    payload = _service(_rows(horizon="24H", up=0.31, down=0.66)).recommendation()

    assert payload["recommendation"]["grid_type"] == "short_bias"


def test_weak_direction_remains_neutral():
    payload = _service(_rows(horizon="24H", up=0.53, down=0.45)).recommendation()

    assert payload["recommendation"]["grid_type"] == "neutral"


def test_12h_unavailable_but_valid_fallback_exists_uses_8h():
    payload = _service(_rows(horizon="8H")).recommendation()

    assert payload["recommendation"]["operating_horizon"] == "8H"


def test_required_grid_specific_signal_missing_returns_no_grid():
    payload = _service([_row("path_inside_70", 0.68)]).recommendation()

    assert payload["recommendation"]["action"] == "NO_GRID"
    assert "MISSING_CRITICAL_V2_SIGNAL" in payload["recommendation"]["reason_codes"]


def test_stale_v2_returns_no_grid(monkeypatch):
    monkeypatch.setenv("GRID_RECOMMENDATION_MAX_PREDICTION_AGE_MINUTES", "20")
    payload = _service(_rows(ts=NOW - timedelta(minutes=30))).recommendation()

    assert payload["recommendation"]["action"] == "NO_GRID"
    assert payload["recommendation"]["stale"] is True


def test_abstained_v2_returns_no_grid():
    rows = _rows()
    rows[0]["abstained"] = True
    rows[0]["abstention_reason"] = "MISSING_RANGE_REFERENCE"

    payload = _service(rows).recommendation()

    assert payload["recommendation"]["action"] == "NO_GRID"
    assert payload["recommendation"]["v2_abstained"] is True


def test_ood_v2_returns_no_grid():
    rows = _rows()
    rows[0]["ood_status"] = "FLAGGED"
    rows[0]["ood_reason"] = "MISSING_OR_EXTREME_FEATURES"

    payload = _service(rows).recommendation()

    assert payload["recommendation"]["action"] == "NO_GRID"
    assert payload["recommendation"]["v2_ood"] is True


def test_prediction_timestamp_mismatch_does_not_combine_rows_unsafely():
    latest = [_row("path_inside_70", 0.68, ts=NOW)]
    older = _rows(ts=NOW - timedelta(minutes=5))

    payload = _service(latest + older, now=NOW).recommendation()

    assert payload["prediction_timestamp"] == NOW.isoformat()
    assert payload["recommendation"]["action"] == "NO_GRID"
    assert "OLDER_V2_ROWS_NOT_COMBINED" in payload["recommendation"]["reason_codes"]


def test_missing_range_reference_returns_safe_no_grid():
    rows = _rows()
    for row in rows:
        row["metadata_json"] = {"range_reference_status": "MISSING"}

    payload = _service(rows).recommendation()

    assert payload["recommendation"]["action"] == "NO_GRID"
    assert "MISSING_REFERENCE_RANGE" in payload["recommendation"]["reason_codes"]


def test_missing_current_spot_fails_clearly():
    with pytest.raises(GridRecommendationUnavailable):
        _service(_rows(), market=lambda include_orderbook=False: {"ok": True}).recommendation()


def test_v2_repository_infrastructure_failure_maps_to_storage_error():
    with pytest.raises(GridRecommendationStorageError):
        _service(error=RuntimeError("database offline")).recommendation()


def test_endpoint_maps_storage_failure_to_503(monkeypatch):
    class FailingService:
        def recommendation(self, symbol="ETHUSD"):
            raise GridRecommendationStorageError("V2 shadow storage unavailable")

    monkeypatch.setattr(grid_router, "GridRecommendationService", lambda: FailingService())

    with pytest.raises(grid_router.HTTPException) as excinfo:
        grid_router.grid_v01_recommendation()

    assert excinfo.value.status_code == 503


def test_endpoint_performs_no_exchange_or_order_mutation(monkeypatch):
    def fail(*_args, **_kwargs):
        raise AssertionError("mutation attempted")

    monkeypatch.setattr("grid_bot.delta_testnet_client.DeltaTestnetClient.private_post", fail)
    payload = _service(_rows(), grid=_grid()).recommendation()

    assert payload["ok"] is True


def test_endpoint_does_not_invoke_v2_model_inference(monkeypatch):
    def fail(*_args, **_kwargs):
        raise AssertionError("V2 inference attempted")

    monkeypatch.setattr("probability_engine.services.v2_shadow_service.V2ShadowEngine.run_shadow_prediction", fail)
    payload = _service(_rows()).recommendation()

    assert payload["ok"] is True


def test_stable_response_schema():
    payload = _service(_rows()).recommendation()

    assert set(payload) == {"ok", "symbol", "spot_price", "prediction_timestamp", "recommendation", "current_grid", "sources"}
    assert {
        "recommender_version",
        "grid_type",
        "lower_price",
        "upper_price",
        "grid_count",
        "spacing_type",
        "grid_step",
        "confidence",
        "confidence_label",
        "confidence_type",
        "action",
        "operating_horizon",
        "reason_codes",
        "reasons",
        "v2_model_version",
        "v2_manifest_hash",
        "v2_ood",
        "v2_abstained",
        "stale",
        "inputs_summary",
    } <= set(payload["recommendation"])


def test_recommender_confidence_is_clearly_labelled():
    payload = _service(_rows()).recommendation()

    assert payload["recommendation"]["confidence_type"] == "RECOMMENDER_CONFIDENCE"


def test_same_persisted_inputs_produce_deterministic_outputs():
    first = _service(_rows()).recommendation()
    second = _service(_rows()).recommendation()

    assert first == second
