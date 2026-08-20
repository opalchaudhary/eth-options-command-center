from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pandas as pd

import database_reader
from probability_engine.config import HORIZON_MINUTES, ProbabilityEngineConfig
from probability_engine.models.market_snapshot import MarketSnapshot
from probability_engine.repositories.outcome_repository import OutcomeRepository
from probability_engine.repositories.prediction_repository import PredictionRepository
from probability_engine.repositories.snapshot_repository import SnapshotRepository
from probability_engine.services.feature_engine import FeatureEngine
from probability_engine.services.outcome_evaluator import _has_complete_window, _window_candles
from probability_engine.services.outcome_service import OutcomeService
from probability_engine.services.probability_service import ProbabilityService
from probability_engine.services.regime_engine import RegimeEngine


HISTORICAL_FEATURE_VERSION = "historical_reconstructible_v1"
BACKTEST_VERSION = "backtest_v1"
DATA_PROVENANCE = "EXISTING_ETH_OHLCV"
LABEL_VERSION = "label_v2"
DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_SAMPLE_MINUTES = 30
DEFAULT_HORIZONS = ("1H", "2H", "4H", "8H", "12H", "24H")
EXPECTED_CANDLE_DELTA = pd.Timedelta(minutes=5)


@dataclass
class BacktestPilotResult:
    ok: bool
    dry_run: bool
    persisted: bool
    symbol: str
    start: str
    end: str
    sample_minutes: int
    horizons: list[str]
    selected_timestamps: list[str] = field(default_factory=list)
    skipped_timestamps: list[dict[str, Any]] = field(default_factory=list)
    expected_prediction_count: int = 0
    snapshots_generated: int = 0
    predictions_generated: int = 0
    outcomes_generated: int = 0
    snapshots_inserted: int = 0
    predictions_inserted: int = 0
    outcomes_inserted: int = 0
    skipped_existing_snapshots: int = 0
    skipped_existing_predictions: int = 0
    skipped_existing_outcomes: int = 0
    skipped_incomplete: int = 0
    failed_count: int = 0
    delta_api_calls: int = 0
    candle_source: str = DATA_PROVENANCE
    supabase_reads: int = 0
    supabase_writes: int = 0
    eth_ohlcv_rows_read: int = 0
    existing_duplicate_snapshot_timestamps: int = 0
    manual_checks: list[dict[str, Any]] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def parse_utc(value) -> datetime:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize(timezone.utc)
    return parsed.tz_convert(timezone.utc).to_pydatetime()


def normalize_ohlcv(rows) -> pd.DataFrame:
    if rows is None:
        frame = pd.DataFrame()
    elif isinstance(rows, pd.DataFrame):
        frame = rows.copy()
    else:
        frame = pd.DataFrame(rows or [])
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "time", "open", "high", "low", "close", "volume"])
    if "candle_time" in frame:
        frame["timestamp"] = pd.to_datetime(frame["candle_time"], utc=True)
    elif "timestamp" in frame:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    if "epoch_time" in frame:
        frame["time"] = pd.to_numeric(frame["epoch_time"], errors="coerce").astype("Int64")
    elif "time" not in frame:
        frame["time"] = (frame["timestamp"].astype("int64") // 1_000_000_000).astype("Int64")
    for column in ["open", "high", "low", "close", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["timestamp", "open", "high", "low", "close", "volume"])
    return frame[["timestamp", "time", "open", "high", "low", "close", "volume"]].sort_values("timestamp").reset_index(drop=True)


def read_stored_ohlcv(symbol: str, start_at: datetime, end_at: datetime, resolution: str = "5m") -> pd.DataFrame:
    page_size = 1000
    frames = []
    offset = 0
    while True:
        rows = database_reader.read_supabase_table(
            "eth_ohlcv",
            params={
                "select": "symbol,resolution,candle_time,epoch_time,open,high,low,close,volume",
                "symbol": f"eq.{symbol}",
                "resolution": f"eq.{resolution}",
                "candle_time": f"gte.{start_at.isoformat()}",
                "order": "candle_time.asc",
                "limit": str(page_size),
                "offset": str(offset),
            },
            timeout=30,
        )
        if rows.empty:
            break
        frames.append(rows)
        if len(rows) < page_size:
            break
        max_time = pd.to_datetime(rows["candle_time"], utc=True).max()
        if max_time > pd.Timestamp(end_at):
            break
        offset += page_size
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not combined.empty:
        combined = combined[pd.to_datetime(combined["candle_time"], utc=True) <= pd.Timestamp(end_at)]
    return normalize_ohlcv(combined)


def has_gap(frame: pd.DataFrame) -> bool:
    if frame is None or len(frame) < 2:
        return True
    diffs = frame["timestamp"].sort_values().diff().dropna()
    return bool((diffs != EXPECTED_CANDLE_DELTA).any())


def complete_lookback(frame: pd.DataFrame, timestamp: datetime, lookback: timedelta) -> pd.DataFrame:
    start = pd.Timestamp(timestamp - lookback)
    end = pd.Timestamp(timestamp)
    window = frame[(frame["timestamp"] >= start) & (frame["timestamp"] <= end)].copy()
    if window.empty or window["timestamp"].min() > start or window["timestamp"].max() < end:
        return pd.DataFrame()
    return pd.DataFrame() if has_gap(window) else window.reset_index(drop=True)


def future_window(frame: pd.DataFrame, timestamp: datetime, horizon: str) -> pd.DataFrame:
    start = parse_utc(timestamp)
    end = start + timedelta(minutes=HORIZON_MINUTES[horizon])
    candles = _window_candles(frame, start, end)
    if not _has_complete_window(candles, start, end) or has_gap(candles):
        return pd.DataFrame()
    return candles


def historical_feature_matrix() -> list[dict[str, str]]:
    exact = ["spot_price", "return_5m", "return_15m", "return_1h", "return_4h", "vwap", "vwap_deviation_pct", "vwap_zscore", "atr", "atr_pct", "realized_volatility", "volume", "volume_zscore", "regime"]
    unavailable = ["funding_rate", "funding_percentile", "open_interest", "oi_change_5m", "oi_change_1h", "oi_change_4h", "basis", "cvd_5m", "cvd_15m", "cvd_1h", "cvd_slope", "cvd_acceleration", "price_cvd_divergence", "buy_volume_ratio", "book_imbalance", "spread_bps", "bid_depth", "ask_depth", "atm_iv", "iv_rv_spread", "iv_percentile", "put_call_skew", "term_structure_signal"]
    rows = [{"feature": item, "live_available": "yes", "historical_available": "yes", "parity": "exact"} for item in exact]
    rows.extend({"feature": item, "live_available": "yes", "historical_available": "no", "parity": "unavailable"} for item in unavailable)
    return rows


class HistoricalBacktestPilot:
    def __init__(
        self,
        config: ProbabilityEngineConfig | None = None,
        snapshot_repository: SnapshotRepository | None = None,
        prediction_repository: PredictionRepository | None = None,
        outcome_repository: OutcomeRepository | None = None,
    ):
        self.config = config or ProbabilityEngineConfig(feature_version=HISTORICAL_FEATURE_VERSION)
        self.feature_engine = FeatureEngine(self.config)
        self.regime_engine = RegimeEngine()
        self.probability_service = ProbabilityService(self.config)
        self.outcome_service = OutcomeService(self.config)
        self.snapshot_repository = snapshot_repository or SnapshotRepository()
        self.prediction_repository = prediction_repository or PredictionRepository()
        self.outcome_repository = outcome_repository or OutcomeRepository()

    def run(
        self,
        start: datetime,
        end: datetime,
        symbol: str = "ETHUSD",
        sample_minutes: int = DEFAULT_SAMPLE_MINUTES,
        horizons: tuple[str, ...] = DEFAULT_HORIZONS,
        dry_run: bool = True,
        persist: bool = False,
        chunk_size: int = 100,
    ) -> BacktestPilotResult:
        horizons = tuple(item.upper() for item in horizons if item.upper() in HORIZON_MINUTES)
        read_start = start - timedelta(hours=DEFAULT_LOOKBACK_HOURS)
        read_end = end + timedelta(minutes=max(HORIZON_MINUTES[item] for item in horizons))
        candles = read_stored_ohlcv(symbol, read_start, read_end)
        result = BacktestPilotResult(
            ok=True,
            dry_run=dry_run,
            persisted=bool(persist and not dry_run),
            symbol=symbol,
            start=start.isoformat(),
            end=end.isoformat(),
            sample_minutes=sample_minutes,
            horizons=list(horizons),
            supabase_reads=1,
            eth_ohlcv_rows_read=len(candles),
            diagnostics={
                "feature_version": HISTORICAL_FEATURE_VERSION,
                "backtest_version": BACKTEST_VERSION,
                "feature_matrix": historical_feature_matrix(),
            },
        )
        if candles.empty:
            result.ok = False
            result.errors.append("No stored eth_ohlcv rows found for pilot window.")
            return result

        persistence_plan = []
        for timestamp in pd.date_range(start=pd.Timestamp(start), end=pd.Timestamp(end), freq=f"{sample_minutes}min"):
            ts = timestamp.to_pydatetime()
            lookback = complete_lookback(candles, ts, timedelta(hours=DEFAULT_LOOKBACK_HOURS))
            if lookback.empty:
                result.skipped_timestamps.append({"timestamp": ts.isoformat(), "reason": "incomplete_lookback_or_gap"})
                continue
            snapshot = self._snapshot_from_lookback(symbol, ts, lookback)
            complete_horizons = [horizon for horizon in horizons if not future_window(candles, ts, horizon).empty]
            if not complete_horizons:
                result.skipped_timestamps.append({"timestamp": ts.isoformat(), "reason": "no_complete_future_horizon"})
                continue
            result.selected_timestamps.append(ts.isoformat())
            result.snapshots_generated += 1
            plan_entry = {"timestamp": ts, "snapshot": snapshot, "predictions": []}

            for horizon in complete_horizons:
                result.expected_prediction_count += 1
                prediction = self.probability_service.predict(snapshot, horizon, record_type="BACKTEST")
                prediction.created_at = ts
                prediction.metadata_json = {
                    **(prediction.metadata_json or {}),
                    "record_type": "BACKTEST",
                    "data_provenance": DATA_PROVENANCE,
                    "historical_feature_set": HISTORICAL_FEATURE_VERSION,
                    "backtest_version": BACKTEST_VERSION,
                    "feature_cutoff": ts.isoformat(),
                    "lookback_start": lookback.iloc[0]["timestamp"].isoformat(),
                    "lookback_end": lookback.iloc[-1]["timestamp"].isoformat(),
                }
                result.predictions_generated += 1
                candles_for_outcome = future_window(candles, ts, horizon)
                outcome = self.outcome_service.evaluate_prediction(prediction, candles_for_outcome, snapshot=snapshot)
                if outcome.get("ok") is False:
                    result.skipped_incomplete += 1
                    continue
                outcome["evaluated_at"] = datetime.now(timezone.utc).isoformat()
                outcome["metadata_json"] = {
                    **(outcome.get("metadata_json") or {}),
                    "evaluation_mode": "HISTORICAL_BACKTEST",
                    "source": "reconstructed_historical_snapshot",
                    "candle_source": DATA_PROVENANCE,
                    "backtest_version": BACKTEST_VERSION,
                    "label_version": LABEL_VERSION,
                    "window_start": ts.isoformat(),
                    "window_end": (ts + timedelta(minutes=HORIZON_MINUTES[horizon])).isoformat(),
                    "candle_count": int(len(candles_for_outcome)),
                    "no_lookahead": True,
                }
                result.outcomes_generated += 1
                self._add_manual_check(result, snapshot, prediction, outcome, candles_for_outcome, horizon)
                plan_entry["predictions"].append(
                    {"horizon": horizon, "prediction": prediction, "outcome": outcome}
                )
            persistence_plan.append(plan_entry)

        if persist and not dry_run:
            self._persist_plan_batched(result, persistence_plan, symbol=symbol, chunk_size=chunk_size)

        result.ok = result.failed_count == 0 and not result.errors
        result.diagnostics["event_rates"] = self._event_rates(result.manual_checks)
        result.diagnostics["persistence_mode"] = "batched" if persist and not dry_run else "dry_run"
        result.diagnostics["chunk_size"] = chunk_size
        result.manual_checks = result.manual_checks[:12]
        return result

    def _persist_plan_batched(self, result: BacktestPilotResult, plan: list[dict[str, Any]], symbol: str, chunk_size: int) -> None:
        chunk_size = max(1, min(int(chunk_size or 100), 500))
        if not plan:
            return
        start = min(item["timestamp"] for item in plan)
        end = max(item["timestamp"] for item in plan)

        snapshot_by_key = self._read_existing_snapshots(symbol, start, end, result)
        missing_snapshots = []
        for item in plan:
            key = self._snapshot_semantic_key(symbol, item["timestamp"])
            if key in snapshot_by_key:
                result.skipped_existing_snapshots += 1
                continue
            missing_snapshots.append(item["snapshot"].to_record())

        for rows in _chunks(missing_snapshots, chunk_size):
            inserted = self._insert_many_returning(self.snapshot_repository, rows, result)
            result.snapshots_inserted += len(inserted)

        if missing_snapshots:
            snapshot_by_key = self._read_existing_snapshots(symbol, start, end, result)

        prediction_items = []
        for item in plan:
            snapshot_row = snapshot_by_key.get(self._snapshot_semantic_key(symbol, item["timestamp"]))
            if not snapshot_row:
                result.failed_count += len(item["predictions"]) or 1
                result.errors.append(f"Missing persisted BACKTEST snapshot for {item['timestamp'].isoformat()}")
                continue
            snapshot_id = snapshot_row.get("id")
            for pred_item in item["predictions"]:
                pred_item["prediction"].snapshot_id = snapshot_id
                prediction_items.append(pred_item)

        prediction_by_key = self._read_existing_predictions(
            [row.get("id") for row in snapshot_by_key.values() if row.get("id")],
            result,
            chunk_size,
        )
        missing_predictions = []
        for item in prediction_items:
            prediction = item["prediction"]
            key = self._prediction_semantic_key(prediction.snapshot_id, prediction.horizon)
            if key in prediction_by_key:
                result.skipped_existing_predictions += 1
                continue
            missing_predictions.append(prediction.to_record())

        for rows in _chunks(missing_predictions, chunk_size):
            inserted = self._insert_many_returning(self.prediction_repository, rows, result)
            result.predictions_inserted += len(inserted)

        if missing_predictions:
            prediction_by_key = self._read_existing_predictions(
                [row.get("id") for row in snapshot_by_key.values() if row.get("id")],
                result,
                chunk_size,
            )

        outcome_payloads = []
        prediction_ids = []
        for item in prediction_items:
            prediction = item["prediction"]
            prediction_row = prediction_by_key.get(self._prediction_semantic_key(prediction.snapshot_id, prediction.horizon))
            if not prediction_row:
                result.failed_count += 1
                result.errors.append(
                    f"Missing persisted BACKTEST prediction for snapshot {prediction.snapshot_id} horizon {prediction.horizon}"
                )
                continue
            prediction_id = prediction_row.get("id")
            prediction_ids.append(prediction_id)
            outcome_payloads.append({"prediction_id": prediction_id, "label_version": LABEL_VERSION, **item["outcome"]})

        existing_outcome_ids = self._read_existing_outcome_prediction_ids(prediction_ids, result, chunk_size)
        missing_outcomes = []
        for payload in outcome_payloads:
            if payload["prediction_id"] in existing_outcome_ids:
                result.skipped_existing_outcomes += 1
                continue
            missing_outcomes.append(payload)

        for rows in _chunks(missing_outcomes, chunk_size):
            inserted = self._insert_many_returning(self.outcome_repository, rows, result)
            result.outcomes_inserted += len(inserted)

    def _read_existing_snapshots(self, symbol: str, start: datetime, end: datetime, result: BacktestPilotResult) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        rows = self.snapshot_repository.read(
            params={
                "select": "id,symbol,timestamp,feature_version,metadata_json",
                "symbol": f"eq.{symbol}",
                "feature_version": f"eq.{HISTORICAL_FEATURE_VERSION}",
                "and": f"(timestamp.gte.{start.isoformat()},timestamp.lte.{end.isoformat()})",
                "order": "timestamp.asc",
                "limit": "1000",
            }
        )
        result.supabase_reads += 1
        records = {}
        for row in _records(rows):
            metadata = row.get("metadata_json") or {}
            if metadata.get("record_type") != "BACKTEST" or metadata.get("backtest_version") != BACKTEST_VERSION:
                continue
            records[self._snapshot_semantic_key(row.get("symbol"), parse_utc(row.get("timestamp")))] = row
        return records

    def _read_existing_predictions(self, snapshot_ids: list[str], result: BacktestPilotResult, chunk_size: int) -> dict[tuple[str, str, str, str], dict[str, Any]]:
        records = {}
        for ids in _chunks([item for item in snapshot_ids if item], max(1, min(chunk_size, 100))):
            rows = self.prediction_repository.read(
                params={
                    "select": "id,snapshot_id,horizon,record_type,model_version",
                    "snapshot_id": f"in.({','.join(ids)})",
                    "record_type": "eq.BACKTEST",
                    "model_version": f"eq.{self.config.model_version}",
                    "limit": str(max(len(ids) * len(DEFAULT_HORIZONS), 1)),
                }
            )
            result.supabase_reads += 1
            for row in _records(rows):
                records[self._prediction_semantic_key(row.get("snapshot_id"), row.get("horizon"))] = row
        return records

    def _read_existing_outcome_prediction_ids(self, prediction_ids: list[str], result: BacktestPilotResult, chunk_size: int) -> set[str]:
        existing = set()
        for ids in _chunks([item for item in prediction_ids if item], max(1, min(chunk_size, 100))):
            rows = self.outcome_repository.read(
                params={
                    "select": "prediction_id,label_version",
                    "prediction_id": f"in.({','.join(ids)})",
                    "label_version": f"eq.{LABEL_VERSION}",
                    "limit": str(len(ids)),
                }
            )
            result.supabase_reads += 1
            existing.update(row.get("prediction_id") for row in _records(rows) if row.get("prediction_id"))
        return existing

    def _insert_many_returning(self, repository, payloads: list[dict[str, Any]], result: BacktestPilotResult) -> list[dict[str, Any]]:
        if not payloads:
            return []
        try:
            if hasattr(repository, "insert_many_returning"):
                rows = repository.insert_many_returning(payloads)
            else:
                rows = [repository.safe_insert_returning(payload) for payload in payloads]
        except Exception as exc:
            result.failed_count += len(payloads)
            result.errors.append(f"Bulk insert failed for {getattr(repository, 'table_name', 'repository')}: {exc}")
            return []
        result.supabase_writes += 1
        inserted = [row for row in _records(rows) if row]
        if len(inserted) != len(payloads):
            result.failed_count += len(payloads) - len(inserted)
        return inserted

    def _snapshot_semantic_key(self, symbol: str, timestamp: datetime) -> tuple[str, str, str, str]:
        return (symbol, parse_utc(timestamp).isoformat(), HISTORICAL_FEATURE_VERSION, BACKTEST_VERSION)

    def _prediction_semantic_key(self, snapshot_id: str, horizon: str) -> tuple[str, str, str, str]:
        return (snapshot_id, horizon, "BACKTEST", self.config.model_version)

    def _snapshot_from_lookback(self, symbol: str, timestamp: datetime, lookback: pd.DataFrame) -> MarketSnapshot:
        snapshot = self.feature_engine.build_snapshot(
            market={"symbol": symbol, "spot_price": float(lookback.iloc[-1]["close"]), "mark_price": float(lookback.iloc[-1]["close"])},
            ohlcv=lookback,
            option_rows=[],
            orderbook_insights={},
            cvd_features={},
        )
        snapshot.timestamp = timestamp
        snapshot.feature_version = HISTORICAL_FEATURE_VERSION
        snapshot.regime_version = self.config.regime_version
        snapshot.regime = self.regime_engine.classify(snapshot)
        snapshot.delta_market_data_status = "HISTORICAL"
        snapshot.orderflow_provider_status = "UNAVAILABLE"
        snapshot.metadata_json = {
            "record_type": "BACKTEST",
            "data_provenance": DATA_PROVENANCE,
            "historical_feature_set": HISTORICAL_FEATURE_VERSION,
            "backtest_version": BACKTEST_VERSION,
            "backtest_key": self._backtest_key(symbol, timestamp),
            "feature_cutoff": timestamp.isoformat(),
            "lookback_start": lookback.iloc[0]["timestamp"].isoformat(),
            "lookback_end": lookback.iloc[-1]["timestamp"].isoformat(),
            "unavailable_features": [row["feature"] for row in historical_feature_matrix() if row["parity"] == "unavailable"],
            "no_lookahead": True,
        }
        return snapshot

    def _existing_backtest_snapshot(self, symbol: str, timestamp: datetime) -> dict[str, Any] | None:
        upper = timestamp + timedelta(seconds=1)
        rows = self.snapshot_repository.read(
            params={
                "select": "id,timestamp,metadata_json",
                "symbol": f"eq.{symbol}",
                "feature_version": f"eq.{HISTORICAL_FEATURE_VERSION}",
                "and": f"(timestamp.gte.{timestamp.isoformat()},timestamp.lt.{upper.isoformat()})",
                "limit": "1",
            }
        )
        records = _records(rows)
        return records[0] if records else None

    def _existing_backtest_prediction(self, snapshot_id: str, horizon: str) -> dict[str, Any] | None:
        rows = self.prediction_repository.read(
            params={
                "select": "id,snapshot_id,horizon,record_type,model_version",
                "snapshot_id": f"eq.{snapshot_id}",
                "horizon": f"eq.{horizon}",
                "record_type": "eq.BACKTEST",
                "model_version": f"eq.{self.config.model_version}",
                "limit": "1",
            }
        )
        records = _records(rows)
        return records[0] if records else None

    def _backtest_key(self, symbol: str, timestamp: datetime) -> str:
        return f"{BACKTEST_VERSION}:{symbol}:{timestamp.isoformat()}"

    def _add_manual_check(self, result, snapshot, prediction, outcome, candles, horizon):
        if len(result.manual_checks) >= 24:
            return
        result.manual_checks.append(
            {
                "timestamp": snapshot.timestamp.isoformat(),
                "horizon": horizon,
                "spot": snapshot.spot_price,
                "vwap": snapshot.vwap,
                "vwap_zscore": snapshot.vwap_zscore,
                "atr": snapshot.atr,
                "return_1h": snapshot.return_1h,
                "return_4h": snapshot.return_4h,
                "regime": snapshot.regime,
                "mean_reversion_probability": prediction.mean_reversion_probability,
                "range_70": [prediction.range_70_lower, prediction.range_70_upper],
                "future_first_candle": candles.iloc[0]["timestamp"].isoformat(),
                "future_last_candle": candles.iloc[-1]["timestamp"].isoformat(),
                "actual_close": outcome.get("actual_close"),
                "range_70_covered": outcome.get("range_70_covered"),
                "no_lookahead": candles.iloc[0]["timestamp"] >= pd.Timestamp(snapshot.timestamp),
            }
        )

    def _event_rates(self, checks):
        if not checks:
            return {}
        fields = ["range_70_covered"]
        return {
            field: sum(1 for row in checks if row.get(field) is True) / len(checks)
            for field in fields
        }


def _records(rows):
    if rows is None:
        return []
    if hasattr(rows, "empty") and hasattr(rows, "to_dict"):
        return [] if rows.empty else rows.to_dict("records")
    if isinstance(rows, dict):
        return [rows]
    return list(rows or [])


def _chunks(items, size):
    size = max(1, int(size or 1))
    for index in range(0, len(items), size):
        yield items[index : index + size]
