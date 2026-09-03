from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.routers import grid as grid_router
from grid_bot.recommendation_service import GridRecommendationService
from grid_bot.recommendation_snapshot_repository import GridRecommendationSnapshotRepository, TABLE_NAME


NOW = datetime(2026, 9, 3, 10, 0, tzinfo=timezone.utc)
PAGE = Path("pages/DeltaGridBot_V01.py")


def _row(target, probability, horizon="12H", ts=None, **updates):
    ts = ts or NOW - timedelta(minutes=4)
    payload = {
        "id": f"pred-{target}-{horizon}",
        "feature_snapshot_id": f"feature-{target}-{horizon}",
        "prediction_timestamp": ts.isoformat(),
        "symbol": "ETHUSD",
        "record_type": "LIVE",
        "model_version": "probability_v2_candidate_v1",
        "target": target,
        "horizon": horizon,
        "calibrated_probability": probability,
        "manifest_hash": "manifest-123",
        "abstained": False,
        "ood_status": "OK",
        "metadata_json": {
            "range_reference_status": "OK",
            "range_70_lower": 4400,
            "range_70_upper": 4600,
        },
    }
    payload.update(updates)
    return payload


def _rows(horizon="12H", inside=0.68, expansion=0.35, up=0.52, down=0.44, **updates):
    return [
        _row("path_inside_70", inside, horizon, **updates),
        _row("realized_over_range_width_ge_1", expansion, horizon, **updates),
        _row("upside_breakout", up, horizon, **updates),
        _row("downside_breakdown", down, horizon, **updates),
    ]


def _no_grid_rows():
    return [
        _row("path_inside_70", 0.2),
        _row("realized_over_range_width_ge_1", 0.9),
    ]


def _grid():
    return {
        "run_id": "run-active",
        "bot_id": "bot-active",
        "status": "RUNNING",
        "config_version": 7,
        "product_symbol": "ETHUSD",
        "grid_type": "neutral",
        "lower_price": "4400",
        "upper_price": "4600",
        "grid_count": 9,
        "spacing_type": "arithmetic",
        "lot_size": "10",
        "max_inventory_lots": "200",
    }


class FakePredictionRepository:
    def __init__(self, rows):
        self.rows = rows
        self.calls = 0

    def latest(self, symbol="ETHUSD", limit=120):
        self.calls += 1
        return self.rows


class FakeGridRepository:
    enabled = True

    def __init__(self, snapshot=None):
        self.snapshot = snapshot

    def active_config_snapshot(self):
        return self.snapshot

    def patch(self, *_args, **_kwargs):
        raise AssertionError("Grid mutation attempted")

    def upsert(self, *_args, **_kwargs):
        raise AssertionError("Grid mutation attempted")


class RecordingDb:
    enabled = True

    def __init__(self):
        self.inserts = []
        self.selects = []
        self.patches = []
        self.deletes = []

    def insert_once(self, table, payload, on_conflict=None):
        self.inserts.append({"table": table, "payload": payload, "on_conflict": on_conflict})
        return True

    def select(self, table, params=None):
        self.selects.append({"table": table, "params": params or {}})
        return [
            {"recommendation_id": "rec-new", "created_at": "2026-09-03T10:02:00+00:00"},
            {"recommendation_id": "rec-old", "created_at": "2026-09-03T10:01:00+00:00"},
        ]

    def patch(self, *_args, **_kwargs):
        self.patches.append((_args, _kwargs))
        raise AssertionError("Snapshot update attempted")

    def delete(self, *_args, **_kwargs):
        self.deletes.append((_args, _kwargs))
        raise AssertionError("Snapshot delete attempted")


class FailingInsertDb(RecordingDb):
    def insert_once(self, table, payload, on_conflict=None):
        self.inserts.append({"table": table, "payload": payload, "on_conflict": on_conflict})
        raise RuntimeError("database offline")


def _service(rows=None, grid=None, snapshot_db=None, prediction_repo=None):
    prediction_repository = prediction_repo or FakePredictionRepository(rows or _rows())
    snapshot_repository = GridRecommendationSnapshotRepository(db=snapshot_db or RecordingDb())
    return GridRecommendationService(
        prediction_repository=prediction_repository,
        grid_repository=FakeGridRepository(grid),
        snapshot_repository=snapshot_repository,
        market_snapshot_fn=lambda include_orderbook=False: {"ok": True, "spot_price": 4500},
        now_fn=lambda: NOW,
    )


def test_manual_recommendation_request_inserts_exactly_one_snapshot():
    db = RecordingDb()
    payload = _service(snapshot_db=db, grid=_grid()).recommendation(persist=True)

    assert payload["persistence"]["saved"] is True
    assert len(db.inserts) == 1
    assert db.inserts[0]["table"] == TABLE_NAME


def test_two_manual_requests_insert_two_rows_even_when_identical():
    db = RecordingDb()
    service = _service(snapshot_db=db, grid=_grid())

    service.recommendation(persist=True)
    service.recommendation(persist=True)

    ids = [row["payload"]["recommendation_id"] for row in db.inserts]
    assert len(db.inserts) == 2
    assert len(set(ids)) == 2


def test_non_persistent_recommendation_inserts_zero_rows():
    db = RecordingDb()
    _service(snapshot_db=db, grid=_grid()).recommendation()

    assert db.inserts == []


def test_ordinary_page_load_and_rerender_do_not_request_recommendation():
    text = PAGE.read_text()
    fragment_body = text.split("@fragment(run_every=\"5s\")", maxsplit=1)[1].split("def render_idle", maxsplit=1)[0]
    dashboard_body = text.split("def render_live_dashboard", maxsplit=1)[1].split("def render_actions", maxsplit=1)[0]

    assert "/api/grid/v01/recommendation" not in fragment_body
    assert "render_grid_recommendation()" in dashboard_body
    assert "safe_post(\"/api/grid/v01/recommendation\"" in text


def test_snapshot_includes_exact_v2_inputs_used_and_flags():
    db = RecordingDb()
    payload = _service(rows=_rows(horizon="24H"), snapshot_db=db, grid=_grid()).recommendation(persist=True)
    snapshot = db.inserts[0]["payload"]

    assert snapshot["path_inside_70"] == "0.68"
    assert snapshot["realized_over_range_width_ge_1"] == "0.35"
    assert snapshot["upside_probability"] == "0.52"
    assert snapshot["downside_probability"] == "0.44"
    assert snapshot["range_70_lower"] == "4400.0"
    assert snapshot["range_70_upper"] == "4600.0"
    assert snapshot["v2_ood"] is False
    assert snapshot["v2_abstained"] is False
    assert snapshot["v2_stale"] is False
    assert snapshot["source_prediction_id"] == "pred-path_inside_70-24H"
    assert payload["sources"]["source_feature_snapshot_ids"]["path_inside_70"] == "feature-path_inside_70-24H"


def test_snapshot_includes_recommended_parameters_confidence_and_reasons():
    db = RecordingDb()
    _service(snapshot_db=db, grid=_grid()).recommendation(persist=True)
    snapshot = db.inserts[0]["payload"]

    assert snapshot["recommended_grid_type"] == "neutral"
    assert snapshot["recommended_lower_price"] is not None
    assert snapshot["recommended_upper_price"] is not None
    assert snapshot["recommended_grid_count"] is not None
    assert snapshot["recommended_spacing_type"] == "arithmetic"
    assert snapshot["recommended_grid_step"] is not None
    assert snapshot["recommender_confidence"] is not None
    assert snapshot["recommendation_action"] == "KEEP_CURRENT"
    assert snapshot["reason_codes"]


def test_snapshot_includes_current_grid_when_active():
    db = RecordingDb()
    _service(snapshot_db=db, grid=_grid()).recommendation(persist=True)
    snapshot = db.inserts[0]["payload"]

    assert snapshot["active_run_id"] == "run-active"
    assert snapshot["bot_id"] == "bot-active"
    assert snapshot["config_version"] == 7
    assert snapshot["current_grid_type"] == "neutral"
    assert snapshot["current_lower_price"] == "4400.0"
    assert snapshot["current_upper_price"] == "4600.0"
    assert snapshot["current_grid_count"] == 9
    assert snapshot["current_spacing_type"] == "arithmetic"
    assert snapshot["current_lot_size"] == "10.0"
    assert snapshot["current_max_inventory_lots"] == "200.0"


def test_snapshot_current_grid_fields_are_null_when_no_active_grid():
    db = RecordingDb()
    _service(snapshot_db=db, grid=None).recommendation(persist=True)
    snapshot = db.inserts[0]["payload"]

    assert snapshot["active_run_id"] is None
    assert snapshot["bot_id"] is None
    assert snapshot["config_version"] is None
    assert snapshot["current_grid_type"] is None
    assert snapshot["current_lot_size"] is None


def test_snapshot_records_versions_horizon_prediction_timestamp_and_sources():
    db = RecordingDb()
    _service(snapshot_db=db, grid=_grid()).recommendation(persist=True)
    snapshot = db.inserts[0]["payload"]

    assert snapshot["recommender_version"] == "grid_parameter_recommender_v0_1"
    assert snapshot["probability_model_version"] == "probability_v2_candidate_v1"
    assert snapshot["selected_operating_horizon"] == "12H"
    assert snapshot["prediction_timestamp"] == (NOW - timedelta(minutes=4)).isoformat()
    assert snapshot["probability_source"] == "probability_v2_shadow_predictions"
    assert "range_70_lower" in snapshot["range_source"]


def test_no_grid_recommendations_are_persisted():
    db = RecordingDb()
    _service(rows=_no_grid_rows(), snapshot_db=db, grid=_grid()).recommendation(persist=True)
    snapshot = db.inserts[0]["payload"]

    assert snapshot["recommendation_action"] == "NO_GRID"
    assert snapshot["recommended_grid_type"] is None
    assert snapshot["reason_codes"]


def test_snapshot_insert_failure_does_not_affect_gridbot_or_hide_recommendation():
    db = FailingInsertDb()
    payload = _service(snapshot_db=db, grid=_grid()).recommendation(persist=True)

    assert payload["ok"] is True
    assert payload["recommendation"]["grid_type"] == "neutral"
    assert payload["persistence"] == {
        "saved": False,
        "recommendation_id": None,
        "error": "Recommendation snapshot could not be saved.",
    }
    assert len(db.inserts) == 1


def test_snapshot_insert_failure_does_not_trigger_exchange_actions(monkeypatch):
    def fail(*_args, **_kwargs):
        raise AssertionError("exchange mutation attempted")

    monkeypatch.setattr("grid_bot.delta_testnet_client.DeltaTestnetClient.private_post", fail)
    payload = _service(snapshot_db=FailingInsertDb(), grid=_grid()).recommendation(persist=True)

    assert payload["ok"] is True
    assert payload["persistence"]["saved"] is False


def test_persistence_does_not_trigger_v2_inference(monkeypatch):
    def fail(*_args, **_kwargs):
        raise AssertionError("V2 inference attempted")

    monkeypatch.setattr("probability_engine.services.v2_shadow_service.V2ShadowEngine.run_shadow_prediction", fail)
    payload = _service(grid=_grid()).recommendation(persist=True)

    assert payload["ok"] is True


def test_history_endpoint_returns_newest_first_and_honors_limit(monkeypatch):
    class FakeService:
        def history(self, limit=50, recommender_version=None, horizon=None, action=None):
            assert limit == 2
            assert horizon == "12H"
            return {
                "ok": True,
                "rows": [
                    {"recommendation_id": "rec-new", "created_at": "2026-09-03T10:02:00+00:00"},
                    {"recommendation_id": "rec-old", "created_at": "2026-09-03T10:01:00+00:00"},
                ],
            }

    monkeypatch.setattr(grid_router, "GridRecommendationService", lambda: FakeService())

    payload = grid_router.grid_v01_recommendation_history(limit=2, horizon="12H")

    assert [row["recommendation_id"] for row in payload["rows"]] == ["rec-new", "rec-old"]


def test_repository_history_uses_newest_first_limit_and_filters():
    db = RecordingDb()
    repo = GridRecommendationSnapshotRepository(db=db)

    rows = repo.latest(limit=2, recommender_version="v1", horizon="12H", action="REGRID")

    assert [row["recommendation_id"] for row in rows] == ["rec-new", "rec-old"]
    params = db.selects[0]["params"]
    assert params["order"] == "created_at.desc"
    assert params["limit"] == 2
    assert params["recommender_version"] == "eq.v1"
    assert params["selected_operating_horizon"] == "eq.12H"
    assert params["recommendation_action"] == "eq.REGRID"


def test_repository_insert_is_append_only_in_tested_flows():
    db = RecordingDb()
    repo = GridRecommendationSnapshotRepository(db=db)
    payload = _service(snapshot_db=RecordingDb(), grid=_grid()).recommendation()
    snapshot = repo.build_snapshot(payload, requested_at=NOW)

    recommendation_id = repo.insert(snapshot)

    assert recommendation_id == snapshot["recommendation_id"]
    assert db.inserts[0]["on_conflict"] == "recommendation_id"
    assert db.patches == []
    assert db.deletes == []


def test_post_api_is_persistence_enabled_get_remains_read_only(monkeypatch):
    calls = []

    class FakeService:
        def recommendation(self, symbol="ETHUSD", persist=False):
            calls.append({"symbol": symbol, "persist": persist})
            return {"ok": True, "persistence": {"saved": persist}}

    monkeypatch.setattr(grid_router, "GridRecommendationService", lambda: FakeService())

    assert grid_router.grid_v01_recommendation()["persistence"]["saved"] is False
    assert grid_router.grid_v01_recommendation_request()["persistence"]["saved"] is True
    assert calls == [
        {"symbol": "ETHUSD", "persist": False},
        {"symbol": "ETHUSD", "persist": True},
    ]


def test_public_post_and_history_routes_are_registered():
    routes = {
        (route.path, tuple(sorted(getattr(route, "methods", set()))))
        for route in grid_router.public_grid_router.routes
    }

    assert ("/grid/v01/recommendation", ("GET",)) in routes
    assert ("/grid/v01/recommendation", ("POST",)) in routes
    assert ("/grid/v01/recommendations/history", ("GET",)) in routes


def test_ui_button_uses_exactly_one_persistence_enabled_request():
    text = PAGE.read_text()
    recommendation_body = text.split("def render_grid_recommendation()", maxsplit=1)[1].split("def render_orders", maxsplit=1)[0]

    assert recommendation_body.count('safe_post("/api/grid/v01/recommendation"') == 1
    assert 'safe_get("/api/grid/v01/recommendation"' not in recommendation_body
