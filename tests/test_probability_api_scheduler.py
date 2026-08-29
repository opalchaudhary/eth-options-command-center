import types

import pandas as pd

from probability_engine.config import ProbabilityEngineConfig
from probability_engine.models.market_snapshot import MarketSnapshot
from probability_engine.routers import probability_router


def test_probability_health_defaults_retention_off(monkeypatch):
    monkeypatch.delenv("PROBABILITY_ENGINE_ENABLED", raising=False)
    monkeypatch.delenv("PROBABILITY_RETENTION_ENABLED", raising=False)
    payload = probability_router.probability_health()
    assert payload["enabled"] is False
    assert payload["retention_enabled"] is False
    assert payload["automatic_destructive_retention"] is False


def test_probability_current_api_shape(monkeypatch):
    class FakeService:
        def current_intelligence(self):
            return {
                "ok": True,
                "symbol": "ETHUSD",
                "spot_price": 4000,
                "regime": "NORMAL_RANGE",
                "horizons": {"1H": {"confidence": 0.8, "range_70": [3900, 4100]}},
            }

    monkeypatch.setattr(probability_router, "_service", lambda: FakeService())
    payload = probability_router.probability_current_horizon("1h")
    assert payload["horizon"] == "1H"
    assert payload["prediction"]["confidence"] == 0.8


def test_scheduler_does_not_register_probability_jobs_when_disabled(monkeypatch):
    import backend.services.scheduler_service as scheduler_service

    added = []
    monkeypatch.setattr(scheduler_service.config, "BACKEND_SCHEDULER_ENABLED", True)
    monkeypatch.setattr(scheduler_service, "get_probability_config", lambda: ProbabilityEngineConfig(enabled=False))

    class FakeScheduler:
        running = False

        def add_job(self, *args, **kwargs):
            added.append(kwargs["id"])

        def start(self):
            self.running = True

        def get_jobs(self):
            return []

    monkeypatch.setattr(scheduler_service, "BackgroundScheduler", lambda timezone: FakeScheduler())
    scheduler_service._scheduler = None
    scheduler_service.start_scheduler()
    assert "probability_prediction_v1" not in added
    assert "probability_snapshot_v1" not in added
    assert "retention_cleanup" in added


def test_scheduler_registers_canonical_probability_jobs_when_enabled(monkeypatch):
    import backend.services.scheduler_service as scheduler_service

    added = []
    monkeypatch.setattr(scheduler_service.config, "BACKEND_SCHEDULER_ENABLED", True)
    monkeypatch.setattr(scheduler_service, "get_probability_config", lambda: ProbabilityEngineConfig(enabled=True))

    class FakeScheduler:
        running = False

        def add_job(self, *args, **kwargs):
            added.append(kwargs["id"])

        def start(self):
            self.running = True

        def get_jobs(self):
            return []

    monkeypatch.setattr(scheduler_service, "BackgroundScheduler", lambda timezone: FakeScheduler())
    scheduler_service._scheduler = None
    scheduler_service.start_scheduler()

    assert "probability_snapshot_v1" not in added
    assert added.count("probability_prediction_v1") == 1
    assert added.count("probability_outcome_evaluator_v1") == 1
    assert added.count("probability_strike_scan_v1") == 1
    assert added.count("probability_performance_daily_v1") == 1
    assert "market_refresh" in added
    assert "retention_cleanup" in added


def test_scheduler_registers_v2_shadow_outcome_job_when_v2_enabled(monkeypatch):
    import backend.services.scheduler_service as scheduler_service

    added = []
    monkeypatch.setattr(scheduler_service.config, "BACKEND_SCHEDULER_ENABLED", True)
    monkeypatch.setattr(
        scheduler_service,
        "get_probability_config",
        lambda: ProbabilityEngineConfig(enabled=True, v2_shadow_enabled=True),
    )

    class FakeScheduler:
        running = False

        def add_job(self, *args, **kwargs):
            added.append(kwargs["id"])

        def start(self):
            self.running = True

        def get_jobs(self):
            return []

    monkeypatch.setattr(scheduler_service, "BackgroundScheduler", lambda timezone: FakeScheduler())
    scheduler_service._scheduler = None
    scheduler_service.start_scheduler()

    assert added.count("probability_v2_shadow_prediction_v1") == 1
    assert added.count("probability_v2_shadow_outcome_evaluator_v1") == 1


def test_repository_safe_insert_uses_model_record(monkeypatch):
    from probability_engine.repositories.snapshot_repository import SnapshotRepository

    calls = {}
    repo = SnapshotRepository()
    monkeypatch.setattr(repo, "insert", lambda payload: calls.setdefault("payload", payload) or True)
    ok = repo.safe_insert(MarketSnapshot(spot_price=1))
    assert ok is True
    assert calls["payload"]["spot_price"] == 1


def test_live_prediction_insert_requires_snapshot_id():
    from probability_engine.models.prediction import ProbabilityPrediction
    from probability_engine.repositories.prediction_repository import PredictionRepository

    repo = PredictionRepository()
    ok = repo.safe_insert(ProbabilityPrediction(record_type="LIVE", snapshot_id=None))
    assert ok is False


def test_mature_unevaluated_uses_label_version_anti_join(monkeypatch):
    from probability_engine.repositories.prediction_repository import PredictionRepository

    repo = PredictionRepository()
    calls = {}
    def fake_read(params):
        calls["params"] = params
        return []

    monkeypatch.setattr(repo, "read", fake_read)

    rows = repo.mature_unevaluated(
        before_iso="2026-08-20T02:00:00+00:00",
        limit=25,
        offset=0,
        label_version="label_v2",
    )

    assert rows == []
    assert "pending_outcome:probability_outcomes!left()" in calls["params"]["select"]
    assert calls["params"]["pending_outcome.label_version"] == "eq.label_v2"
    assert calls["params"]["pending_outcome"] == "is.null"
    assert calls["params"]["record_type"] == "eq.LIVE"
    assert calls["params"]["order"] == "created_at.asc"
    assert calls["params"]["limit"] == "25"


def test_persist_predictions_creates_one_canonical_snapshot_for_all_horizons(monkeypatch):
    from probability_engine.services.market_data_service import ProbabilityMarketDataService

    saved_predictions = []
    saved_snapshots = []
    saved_snapshot_id = "8af7bb6f-d5ac-4d27-9fdf-d6cb2a932d41"
    horizons = ["1H", "2H", "4H", "8H", "12H", "24H"]
    service = ProbabilityMarketDataService(config=ProbabilityEngineConfig(horizons=horizons))

    class FakeSnapshotRepository:
        def safe_insert_returning(self, snapshot):
            saved_snapshots.append(snapshot.to_record())
            return {"id": saved_snapshot_id, **snapshot.to_record()}

    class FakePredictionRepository:
        def safe_insert(self, prediction):
            saved_predictions.append(prediction.to_record())
            return True

    monkeypatch.setattr(service, "build_current_snapshot", lambda: MarketSnapshot(spot_price=4000))
    service.snapshot_repository = FakeSnapshotRepository()
    service.prediction_repository = FakePredictionRepository()

    result = service.persist_predictions()

    assert result["ok"] is True
    assert result["snapshot_id"] == saved_snapshot_id
    assert len(saved_snapshots) == 1
    assert len(saved_predictions) == 6
    assert {item["horizon"] for item in result["results"]} == set(horizons)
    assert {row["snapshot_id"] for row in saved_predictions} == {saved_snapshot_id}
    assert all(row["snapshot_id"] for row in saved_predictions)


def test_outcome_repository_for_prediction_handles_empty_dataframe(monkeypatch):
    from probability_engine.repositories.outcome_repository import OutcomeRepository

    repo = OutcomeRepository()
    monkeypatch.setattr(repo, "read", lambda params: pd.DataFrame())

    assert repo.for_prediction("prediction-1") is None


def test_outcome_repository_for_prediction_handles_one_row_dataframe(monkeypatch):
    from probability_engine.repositories.outcome_repository import OutcomeRepository

    repo = OutcomeRepository()
    monkeypatch.setattr(
        repo,
        "read",
        lambda params: pd.DataFrame([{"id": "outcome-1", "prediction_id": "prediction-1"}]),
    )

    assert repo.for_prediction("prediction-1") == {"id": "outcome-1", "prediction_id": "prediction-1"}
