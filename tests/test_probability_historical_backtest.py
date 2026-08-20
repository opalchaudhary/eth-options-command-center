from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from probability_engine.repositories.prediction_repository import PredictionRepository
from probability_engine.services.historical_backtest import HistoricalBacktestPilot, normalize_ohlcv, read_stored_ohlcv


def _stored_rows(start, count=700, gap_at=None):
    rows = []
    for index in range(count):
        if gap_at is not None and index == gap_at:
            continue
        ts = start + timedelta(minutes=5 * index)
        price = 100 + index * 0.05
        rows.append(
            {
                "symbol": "ETHUSD",
                "resolution": "5m",
                "candle_time": ts.isoformat(),
                "epoch_time": int(ts.timestamp()),
                "open": price,
                "high": price + 1,
                "low": price - 1,
                "close": price + 0.25,
                "volume": 10 + index % 20,
            }
        )
    return rows


class FakeSnapshotRepository:
    def __init__(self):
        self.rows = []

    def safe_insert_returning(self, snapshot):
        row = snapshot.to_record() if hasattr(snapshot, "to_record") else snapshot.copy()
        row["id"] = f"snapshot-{len(self.rows) + 1}"
        self.rows.append(row)
        return row

    def insert_many_returning(self, snapshots):
        return [self.safe_insert_returning(snapshot) for snapshot in snapshots]

    def read(self, params=None):
        params = params or {}
        range_filter = params.get("and", "")
        timestamp = ""
        if "timestamp.gte." in range_filter:
            timestamp = range_filter.split("timestamp.gte.", 1)[1].split(",", 1)[0]
        feature_version = (params or {}).get("feature_version", "").replace("eq.", "")
        return [
            row
            for row in self.rows
            if row.get("timestamp") >= timestamp and row.get("feature_version") == feature_version
        ]


class FakePredictionRepository:
    def __init__(self):
        self.rows = []
        self.latest_params = None

    def safe_insert_returning(self, prediction):
        row = prediction.to_record() if hasattr(prediction, "to_record") else prediction.copy()
        row["id"] = f"prediction-{len(self.rows) + 1}"
        self.rows.append(row)
        return row

    def insert_many_returning(self, predictions):
        return [self.safe_insert_returning(prediction) for prediction in predictions]

    def read(self, params=None):
        params = params or {}
        snapshot_filter = params.get("snapshot_id", "")
        if snapshot_filter.startswith("in.("):
            snapshot_ids = set(snapshot_filter[4:-1].split(","))
        else:
            snapshot_ids = {snapshot_filter.replace("eq.", "")}
        horizon = params.get("horizon", "").replace("eq.", "")
        record_type = params.get("record_type", "").replace("eq.", "")
        return [
            row
            for row in self.rows
            if row.get("snapshot_id") in snapshot_ids
            and (not horizon or row.get("horizon") == horizon)
            and row.get("record_type") == record_type
        ]


class FakeOutcomeRepository:
    def __init__(self):
        self.rows = []

    def for_prediction(self, prediction_id, label_version=None):
        for row in self.rows:
            if row["prediction_id"] == prediction_id and row["label_version"] == label_version:
                return row
        return None

    def safe_insert_outcome(self, prediction_id, outcome, label_version="label_v2"):
        self.rows.append({"prediction_id": prediction_id, "label_version": label_version, **outcome})
        return True

    def insert_many_returning(self, outcomes):
        saved = []
        for outcome in outcomes:
            row = outcome.copy()
            row["id"] = f"outcome-{len(self.rows) + 1}"
            self.rows.append(row)
            saved.append(row)
        return saved

    def read(self, params=None):
        params = params or {}
        prediction_filter = params.get("prediction_id", "")
        if prediction_filter.startswith("in.("):
            prediction_ids = set(prediction_filter[4:-1].split(","))
        else:
            prediction_ids = {prediction_filter.replace("eq.", "")}
        label_version = params.get("label_version", "").replace("eq.", "")
        return [
            row
            for row in self.rows
            if row.get("prediction_id") in prediction_ids
            and (not label_version or row.get("label_version") == label_version)
        ]


def _pilot(monkeypatch, rows):
    monkeypatch.setattr(
        "probability_engine.services.historical_backtest.read_stored_ohlcv",
        lambda *args, **kwargs: normalize_ohlcv(rows),
    )
    return HistoricalBacktestPilot(
        snapshot_repository=FakeSnapshotRepository(),
        prediction_repository=FakePredictionRepository(),
        outcome_repository=FakeOutcomeRepository(),
    )


def test_historical_pilot_uses_only_cutoff_lookback_and_future_after_freeze(monkeypatch):
    start = datetime(2026, 5, 15, tzinfo=timezone.utc)
    rows = _stored_rows(start - timedelta(hours=24), count=700)
    pilot = _pilot(monkeypatch, rows)

    result = pilot.run(start=start, end=start, dry_run=True, sample_minutes=30, horizons=("1H",))

    assert result.ok is True
    assert result.predictions_generated == 1
    check = result.manual_checks[0]
    assert check["timestamp"] == start.isoformat()
    assert check["future_first_candle"] >= start.isoformat()
    assert check["no_lookahead"] is True
    assert result.diagnostics["feature_version"] == "historical_reconstructible_v1"


def test_gap_crossing_lookback_is_rejected(monkeypatch):
    start = datetime(2026, 5, 15, tzinfo=timezone.utc)
    rows = _stored_rows(start - timedelta(hours=24), count=700, gap_at=20)
    pilot = _pilot(monkeypatch, rows)

    result = pilot.run(start=start, end=start, dry_run=True, sample_minutes=30, horizons=("1H",))

    assert result.predictions_generated == 0
    assert result.skipped_timestamps[0]["reason"] == "incomplete_lookback_or_gap"


def test_backtest_provenance_and_unavailable_features_are_explicit(monkeypatch):
    start = datetime(2026, 5, 15, tzinfo=timezone.utc)
    rows = _stored_rows(start - timedelta(hours=24), count=700)
    pilot = _pilot(monkeypatch, rows)

    result = pilot.run(start=start, end=start, dry_run=True, sample_minutes=30, horizons=("1H",))

    matrix = result.diagnostics["feature_matrix"]
    assert {"feature": "book_imbalance", "live_available": "yes", "historical_available": "no", "parity": "unavailable"} in matrix
    assert result.candle_source == "EXISTING_ETH_OHLCV"
    assert result.delta_api_calls == 0


def test_persisted_pilot_is_idempotent(monkeypatch):
    start = datetime(2026, 5, 15, tzinfo=timezone.utc)
    rows = _stored_rows(start - timedelta(hours=24), count=700)
    pilot = _pilot(monkeypatch, rows)

    first = pilot.run(start=start, end=start, dry_run=False, persist=True, sample_minutes=30, horizons=("1H",))
    second = pilot.run(start=start, end=start, dry_run=False, persist=True, sample_minutes=30, horizons=("1H",))

    assert first.snapshots_inserted == 1
    assert first.predictions_inserted == 1
    assert first.outcomes_inserted == 1
    assert second.snapshots_inserted == 0
    assert second.predictions_inserted == 0
    assert second.outcomes_inserted == 0
    assert second.skipped_existing_predictions == 1
    assert second.skipped_existing_outcomes == 1


def test_prediction_latest_filters_live_by_default(monkeypatch):
    calls = {}

    def fake_read(self, params=None):
        calls["params"] = params
        return []

    monkeypatch.setattr(PredictionRepository, "read", fake_read)

    PredictionRepository().latest(horizon="1H", limit=5)

    assert calls["params"]["record_type"] == "eq.LIVE"


def test_manual_uniqueness_migration_scopes_backtest_and_allows_versions():
    sql = Path("migrations/probability_backtest_snapshot_uniqueness_manual.sql").read_text()

    assert "where metadata_json->>'record_type' = 'BACKTEST'" in sql
    assert "metadata_json->>'backtest_version'" in sql
    assert "idx_probability_backtest_snapshot_unique" in sql
    assert "idx_probability_backtest_prediction_unique" in sql


def test_stored_ohlcv_reader_pages_past_supabase_default_limit(monkeypatch):
    start = datetime(2026, 5, 15, tzinfo=timezone.utc)
    rows = _stored_rows(start, count=1205)
    calls = []

    def fake_read(table_name, params=None, timeout=15):
        calls.append(params)
        offset = int(params.get("offset", 0))
        limit = int(params.get("limit", 1000))
        return pd.DataFrame(rows[offset : offset + limit])

    monkeypatch.setattr("probability_engine.services.historical_backtest.database_reader.read_supabase_table", fake_read)

    frame = read_stored_ohlcv("ETHUSD", start, start + timedelta(days=5))

    assert len(frame) == 1205
    assert [call["offset"] for call in calls] == ["0", "1000"]
