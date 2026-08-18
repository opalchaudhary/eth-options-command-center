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
    assert "retention_cleanup" in added


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


def test_persist_predictions_reuses_saved_snapshot_id_for_all_horizons(monkeypatch):
    from probability_engine.services.market_data_service import ProbabilityMarketDataService

    saved_predictions = []
    saved_snapshot_id = "8af7bb6f-d5ac-4d27-9fdf-d6cb2a932d41"
    service = ProbabilityMarketDataService(config=ProbabilityEngineConfig(horizons=["1H", "4H"]))

    class FakeSnapshotRepository:
        def safe_insert_returning(self, snapshot):
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
    assert {item["horizon"] for item in result["results"]} == {"1H", "4H"}
    assert {row["snapshot_id"] for row in saved_predictions} == {saved_snapshot_id}


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
