from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
import time

import pandas as pd
import requests

from probability_engine.config import HORIZON_MINUTES, get_probability_config
from probability_engine.repositories.base_repository import SupabaseRepository
import database_reader


RESOLUTION_SECONDS = 300
TARGETS = {
    "realized_over_range_width_ge_1",
    "path_inside_70",
    "range_breached",
    "both_side_breach",
    "upside_breakout",
    "downside_breakdown",
    "upper_breach_only",
    "lower_breach_only",
    "up_excursion_ge_1_0_atr",
    "down_excursion_ge_1_0_atr",
}

PREDICTION_SELECT = (
    "id,created_at,feature_snapshot_id,prediction_timestamp,symbol,record_type,"
    "model_version,model_id,target,horizon,feature_version,label_version,"
    "calibration_version,manifest_hash,metadata_json"
)


def _records(rows):
    if rows is None:
        return []
    if hasattr(rows, "empty") and hasattr(rows, "to_dict"):
        return [] if rows.empty else rows.to_dict("records")
    if isinstance(rows, dict):
        return [rows]
    return list(rows or [])


class V2ShadowOutcomeRepository(SupabaseRepository):
    table_name = "probability_v2_shadow_outcomes"
    outcome_columns = {
        "evaluated_at",
        "outcome",
        "actual_open",
        "actual_high",
        "actual_low",
        "actual_close",
        "maximum_up_excursion",
        "maximum_down_excursion",
        "realized_path_range",
        "realized_over_range_width",
        "metadata_json",
    }

    def existing_prediction_ids(self, prediction_ids, label_version="label_v2"):
        clean_ids = [str(item) for item in prediction_ids if item]
        if not clean_ids:
            return set()
        existing = set()
        for index in range(0, len(clean_ids), 50):
            chunk = clean_ids[index : index + 50]
            rows = self.read(
                params={
                    "select": "prediction_id",
                    "prediction_id": f"in.({','.join(chunk)})",
                    "label_version": f"eq.{label_version}",
                    "limit": str(len(chunk)),
                }
            )
            existing.update(row.get("prediction_id") for row in _records(rows) if row.get("prediction_id"))
        return existing

    def outcome_payload(self, prediction_id, outcome, label_version="label_v2"):
        outcome_payload = {key: value for key, value in outcome.items() if key in self.outcome_columns}
        return {
            "prediction_id": prediction_id,
            "label_version": label_version,
            "target": outcome.get("metadata_json", {}).get("target"),
            "horizon": outcome.get("metadata_json", {}).get("horizon"),
            **outcome_payload,
        }

    def safe_insert_outcome(self, prediction_id, outcome, label_version="label_v2"):
        payload = self.outcome_payload(prediction_id, outcome, label_version=label_version)
        return self.safe_insert(payload)

    def safe_insert_outcomes(self, outcomes, label_version="label_v2"):
        payloads = [
            self.outcome_payload(prediction_id, outcome, label_version=label_version)
            for prediction_id, outcome in outcomes
        ]
        if not payloads:
            return 0, 0
        try:
            response = requests.post(
                f"{database_reader.SUPABASE_URL}/rest/v1/{self.table_name}",
                headers={
                    **database_reader.HEADERS,
                    "Content-Type": "application/json",
                    "Prefer": "resolution=ignore-duplicates,return=representation",
                },
                params={"on_conflict": "prediction_id,label_version,target"},
                json=payloads,
                timeout=20,
            )
            if response.status_code in [200, 201]:
                rows = response.json() if response.text else []
                return len(rows), 0
            if response.status_code == 204:
                return len(payloads), 0
            raise RuntimeError(f"bulk outcome insert failed: {response.status_code} {response.text[:200]}")
        except Exception:
            created = 0
            failed = 0
            for prediction_id, outcome in outcomes:
                if self.safe_insert_outcome(prediction_id, outcome, label_version=label_version):
                    created += 1
                else:
                    failed += 1
            return created, failed


class V2ShadowPredictionEvaluationRepository(SupabaseRepository):
    table_name = "probability_v2_shadow_predictions"

    def mature_candidates(self, before_iso, limit=100, offset=0):
        return self.read(
            params={
                "select": PREDICTION_SELECT,
                "prediction_timestamp": f"lte.{before_iso}",
                "record_type": "eq.LIVE",
                "abstained": "eq.false",
                "order": "prediction_timestamp.asc,horizon.asc,target.asc,id.asc",
                "limit": str(limit),
                "offset": str(offset),
            }
        )

    def mature_candidates_after(self, before_iso, after_timestamp_iso=None, limit=100):
        params = {
            "select": PREDICTION_SELECT,
            "prediction_timestamp": f"lte.{before_iso}",
            "record_type": "eq.LIVE",
            "abstained": "eq.false",
            "order": "prediction_timestamp.asc,horizon.asc,target.asc,id.asc",
            "limit": str(limit),
        }
        if after_timestamp_iso:
            params["prediction_timestamp"] = f"gt.{after_timestamp_iso}"
            params["and"] = f"(prediction_timestamp.lte.{before_iso})"
        return self.read(params=params)

    def pending_mature_candidates(self, before_iso, limit=100, label_version="label_v2"):
        rows = self.read(
            params={
                "select": (
                    f"{PREDICTION_SELECT},"
                    "probability_v2_shadow_outcomes!left(prediction_id,label_version,target)"
                ),
                "prediction_timestamp": f"lte.{before_iso}",
                "record_type": "eq.LIVE",
                "abstained": "eq.false",
                "probability_v2_shadow_outcomes": "is.null",
                "order": "prediction_timestamp.asc,horizon.asc,target.asc,id.asc",
                "limit": str(limit),
            }
        )
        clean_rows = []
        for row in _records(rows):
            row.pop("probability_v2_shadow_outcomes", None)
            clean_rows.append(row)
        return clean_rows

    def mature_candidates_window(self, start_iso, before_iso, horizon=None, limit=100, offset=0):
        params = {
            "select": PREDICTION_SELECT,
            "prediction_timestamp": f"gte.{start_iso}",
            "and": f"(prediction_timestamp.lte.{before_iso})",
            "record_type": "eq.LIVE",
            "abstained": "eq.false",
            "order": "prediction_timestamp.desc,target.asc,id.asc",
            "limit": str(limit),
            "offset": str(offset),
        }
        if horizon:
            params["horizon"] = f"eq.{str(horizon).upper()}"
        return self.read(params=params)

    def frontier_candidates(self, cursor_iso, before_iso, horizon, limit=100):
        return self.read(
            params={
                "select": PREDICTION_SELECT,
                "prediction_timestamp": f"gt.{cursor_iso}",
                "and": f"(prediction_timestamp.lte.{before_iso})",
                "record_type": "eq.LIVE",
                "abstained": "eq.false",
                "horizon": f"eq.{str(horizon).upper()}",
                "order": "prediction_timestamp.asc,target.asc,id.asc",
                "limit": str(limit),
            }
        )

    def by_ids(self, prediction_ids):
        clean_ids = [str(item) for item in prediction_ids if item]
        if not clean_ids:
            return []
        rows = []
        for index in range(0, len(clean_ids), 50):
            chunk = clean_ids[index : index + 50]
            page = self.read(
                params={
                    "select": PREDICTION_SELECT,
                    "id": f"in.({','.join(chunk)})",
                    "limit": str(len(chunk)),
                }
            )
            rows.extend(_records(page))
        return rows


class V2OutcomeFrontierRepository(SupabaseRepository):
    table_name = "probability_v2_outcome_frontier_state"
    retry_table_name = "probability_v2_outcome_retry_queue"

    def state_for_horizon(self, horizon: str) -> dict[str, Any] | None:
        rows = self.read(
            params={
                "select": "horizon,cursor_prediction_timestamp,cursor_prediction_id,updated_at,metadata_json",
                "horizon": f"eq.{str(horizon).upper()}",
                "limit": "1",
            }
        )
        records = _records(rows)
        return records[0] if records else None

    def initialize_state(self, horizon: str, cursor: datetime) -> dict[str, Any]:
        payload = {
            "horizon": str(horizon).upper(),
            "cursor_prediction_timestamp": cursor.isoformat(),
            "metadata_json": {"initialized_by": "step18b3_frontier", "mode": "routine_new_work_only"},
        }
        response = requests.post(
            f"{database_reader.SUPABASE_URL}/rest/v1/{self.table_name}",
            headers={
                **database_reader.HEADERS,
                "Content-Type": "application/json",
                "Prefer": "resolution=ignore-duplicates,return=representation",
            },
            params={"on_conflict": "horizon"},
            json=payload,
            timeout=15,
        )
        if response.status_code not in [200, 201, 204]:
            raise RuntimeError(f"frontier state initialize failed: {response.status_code} {response.text[:200]}")
        return self.state_for_horizon(horizon) or payload

    def advance_state(self, horizon: str, cursor: datetime, metadata: dict[str, Any] | None = None) -> bool:
        payload = {
            "horizon": str(horizon).upper(),
            "cursor_prediction_timestamp": cursor.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "metadata_json": metadata or {},
        }
        response = requests.post(
            f"{database_reader.SUPABASE_URL}/rest/v1/{self.table_name}",
            headers={
                **database_reader.HEADERS,
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=minimal",
            },
            params={"on_conflict": "horizon"},
            json=payload,
            timeout=15,
        )
        return response.status_code in [200, 201, 204]

    def due_retry_prediction_ids(self, now: datetime, limit: int = 100) -> list[str]:
        rows = database_reader.read_supabase_table(
            self.retry_table_name,
            params={
                "select": "prediction_id",
                "retry_after": f"lte.{now.isoformat()}",
                "order": "prediction_timestamp.asc,horizon.asc,prediction_id.asc",
                "limit": str(limit),
            },
        )
        return [row.get("prediction_id") for row in _records(rows) if row.get("prediction_id")]

    def enqueue_retry(self, prediction: dict[str, Any], reason: str, retry_after: datetime, error: str | None = None) -> bool:
        payload = {
            "prediction_id": prediction.get("id"),
            "retry_after": retry_after.isoformat(),
            "attempt_count": int((prediction.get("retry_attempt_count") or 0)) + 1,
            "horizon": str(prediction.get("horizon") or "").upper(),
            "target": prediction.get("target"),
            "prediction_timestamp": prediction.get("prediction_timestamp"),
            "reason": reason,
            "last_error": error,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "metadata_json": {
                "model_version": prediction.get("model_version"),
                "manifest_hash": prediction.get("manifest_hash"),
            },
        }
        response = requests.post(
            f"{database_reader.SUPABASE_URL}/rest/v1/{self.retry_table_name}",
            headers={
                **database_reader.HEADERS,
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=minimal",
            },
            params={"on_conflict": "prediction_id"},
            json=payload,
            timeout=15,
        )
        return response.status_code in [200, 201, 204]

    def clear_retries(self, prediction_ids) -> int:
        clean_ids = [str(item) for item in prediction_ids if item]
        if not clean_ids:
            return 0
        cleared = 0
        for index in range(0, len(clean_ids), 50):
            chunk = clean_ids[index : index + 50]
            response = requests.delete(
                f"{database_reader.SUPABASE_URL}/rest/v1/{self.retry_table_name}",
                headers=database_reader.HEADERS,
                params={"prediction_id": f"in.({','.join(chunk)})"},
                timeout=15,
            )
            if response.status_code in [200, 202, 204]:
                cleared += len(chunk)
        return cleared


class V2FeatureSnapshotEvaluationRepository(SupabaseRepository):
    table_name = "probability_v2_feature_snapshots"

    def by_ids(self, snapshot_ids):
        clean_ids = [str(item) for item in snapshot_ids if item]
        if not clean_ids:
            return {}
        rows = self.read(
            params={
                "select": "id,feature_vector_json,metadata_json",
                "id": f"in.({','.join(clean_ids)})",
                "limit": str(len(clean_ids)),
            }
        )
        return {row.get("id"): row for row in _records(rows) if row.get("id")}


def load_future_ohlcv(symbol, start_at, end_at) -> pd.DataFrame:
    rows = []
    page_start = pd.Timestamp(start_at).tz_convert("UTC")
    end = pd.Timestamp(end_at).tz_convert("UTC")
    url = f"{database_reader.SUPABASE_URL}/rest/v1/eth_ohlcv"
    for _page in range(4):
        response = requests.get(
            url,
            headers=database_reader.HEADERS,
            params={
                "select": "symbol,resolution,candle_time,open,high,low,close,volume",
                "symbol": f"eq.{symbol}",
                "resolution": "eq.5m",
                "candle_time": f"gte.{page_start.isoformat()}",
                "order": "candle_time.asc",
                "limit": "1000",
            },
            timeout=20,
        )
        if response.status_code != 200:
            break
        page_rows = response.json()
        if not page_rows:
            break
        rows.extend(page_rows)
        last_time = pd.Timestamp(page_rows[-1]["candle_time"]).tz_convert("UTC")
        if last_time >= end or len(page_rows) < 1000:
            break
        page_start = last_time + pd.Timedelta(microseconds=1)
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["candle_time"] = pd.to_datetime(frame["candle_time"], utc=True)
    return frame[frame["candle_time"] < end].sort_values("candle_time").reset_index(drop=True)


def parse_utc(value) -> datetime:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize(timezone.utc)
    return parsed.tz_convert(timezone.utc).to_pydatetime()


def prediction_window(prediction: dict[str, Any]) -> tuple[datetime, datetime] | None:
    created_at = parse_utc(prediction["prediction_timestamp"])
    minutes = HORIZON_MINUTES.get(str(prediction.get("horizon") or "").upper())
    if minutes is None:
        return None
    return created_at, created_at + timedelta(minutes=minutes)


def is_mature(prediction: dict[str, Any], now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    window = prediction_window(prediction)
    return bool(window and now >= window[1])


def future_window_candles(candles: pd.DataFrame, start_at, end_at) -> pd.DataFrame:
    if candles is None or candles.empty:
        return pd.DataFrame()
    frame = candles.copy()
    if "timestamp" not in frame and "candle_time" in frame:
        frame["timestamp"] = frame["candle_time"]
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    eligible_end = pd.Timestamp(end_at) - pd.Timedelta(seconds=RESOLUTION_SECONDS)
    return frame[(frame["timestamp"] >= pd.Timestamp(start_at)) & (frame["timestamp"] <= eligible_end)].sort_values("timestamp").reset_index(drop=True)


def has_complete_window(candles: pd.DataFrame, start_at, end_at) -> bool:
    if candles is None or candles.empty:
        return False
    first_ts = pd.Timestamp(candles.iloc[0]["timestamp"])
    last_ts = pd.Timestamp(candles.iloc[-1]["timestamp"])
    first_allowed = pd.Timestamp(start_at).ceil("5min")
    required_last = (pd.Timestamp(end_at) - pd.Timedelta(seconds=RESOLUTION_SECONDS)).floor("5min")
    return first_ts <= first_allowed and last_ts >= required_last


def evaluate_shadow_target(prediction: dict[str, Any], candles: pd.DataFrame, feature_snapshot: dict[str, Any]) -> dict[str, Any]:
    target = prediction.get("target")
    if target not in TARGETS:
        return {"ok": False, "reason": "UNSUPPORTED_TARGET"}
    window = prediction_window(prediction)
    if not window:
        return {"ok": False, "reason": "UNSUPPORTED_HORIZON"}
    future = future_window_candles(candles, *window)
    if not has_complete_window(future, *window):
        return {"ok": False, "reason": "INCOMPLETE_WINDOW"}

    open_ = float(future.iloc[0]["open"])
    high = float(future["high"].max())
    low = float(future["low"].min())
    close = float(future.iloc[-1]["close"])
    vector = feature_snapshot.get("feature_vector_json") or {}
    metadata = prediction.get("metadata_json") or {}
    range_lower, range_upper = range_bounds_from_metadata(metadata)
    atr = float(vector.get("atr_12b") or vector.get("atr_pct_12b") or 0)
    spot_proxy = open_
    atr_abs = atr * spot_proxy if atr and atr < 1 else atr
    max_up = high - open_
    max_down = open_ - low
    realized_path_range = (high - low) / spot_proxy if spot_proxy else None
    range_width = (range_upper - range_lower) if range_lower is not None and range_upper is not None else None
    realized_over_range_width = (high - low) / range_width if range_width else None

    outcome = None
    if target == "path_inside_70":
        outcome = high <= range_upper and low >= range_lower if range_width else None
    elif target == "range_breached":
        outcome = high > range_upper or low < range_lower if range_width else None
    elif target == "both_side_breach":
        outcome = high > range_upper and low < range_lower if range_width else None
    elif target == "upper_breach_only":
        outcome = high > range_upper and low >= range_lower if range_width else None
    elif target == "lower_breach_only":
        outcome = low < range_lower and high <= range_upper if range_width else None
    elif target == "realized_over_range_width_ge_1":
        outcome = realized_over_range_width >= 1 if realized_over_range_width is not None else None
    elif target == "up_excursion_ge_1_0_atr":
        outcome = max_up >= atr_abs if atr_abs else None
    elif target == "down_excursion_ge_1_0_atr":
        outcome = max_down >= atr_abs if atr_abs else None
    elif target == "upside_breakout":
        outcome = high > range_upper if range_width else None
    elif target == "downside_breakdown":
        outcome = low < range_lower if range_width else None

    return {
        "ok": outcome is not None,
        "outcome": outcome,
        "actual_open": open_,
        "actual_high": high,
        "actual_low": low,
        "actual_close": close,
        "maximum_up_excursion": max_up,
        "maximum_down_excursion": max_down,
        "realized_path_range": realized_path_range,
        "realized_over_range_width": realized_over_range_width,
        "metadata_json": {
            "target": target,
            "horizon": prediction.get("horizon"),
            "manifest_hash": prediction.get("manifest_hash"),
            "model_version": prediction.get("model_version"),
            "feature_version": prediction.get("feature_version"),
            "calibration_version": prediction.get("calibration_version"),
            "prediction_timestamp": prediction.get("prediction_timestamp"),
            "window_start": window[0].isoformat(),
            "window_end": window[1].isoformat(),
            "candle_count": int(len(future)),
            "semantics": "Probability V2 candidate v1 frozen shadow outcome semantics.",
        },
    }


def range_bounds_from_metadata(metadata: dict[str, Any]) -> tuple[float | None, float | None]:
    # Step 16 stores exact range-reference details in metadata before outcome
    # activation. If unavailable, the evaluator returns incomplete for
    # range-dependent targets instead of fabricating a target.
    lower = metadata.get("range_70_lower")
    upper = metadata.get("range_70_upper")
    return (float(lower), float(upper)) if lower is not None and upper is not None else (None, None)


def outcome_group_key(prediction: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(prediction.get("symbol") or "ETHUSD"),
        pd.Timestamp(prediction.get("prediction_timestamp")).tz_convert("UTC").isoformat(),
        str(prediction.get("horizon") or "").upper(),
    )


class V2ShadowOutcomeEvaluator:
    def __init__(
        self,
        prediction_repository=None,
        outcome_repository=None,
        feature_snapshot_repository=None,
        frontier_repository=None,
        candle_fetcher=None,
        batch_limit=None,
    ):
        using_default_prediction_repository = prediction_repository is None
        self.prediction_repository = prediction_repository or V2ShadowPredictionEvaluationRepository()
        self.outcome_repository = outcome_repository or V2ShadowOutcomeRepository()
        self.feature_snapshot_repository = feature_snapshot_repository or V2FeatureSnapshotEvaluationRepository()
        config = get_probability_config()
        if frontier_repository is not None:
            self.frontier_repository = frontier_repository
        elif using_default_prediction_repository and getattr(config, "v2_outcome_frontier_enabled", True):
            self.frontier_repository = V2OutcomeFrontierRepository()
        else:
            self.frontier_repository = None
        self.candle_fetcher = candle_fetcher or load_future_ohlcv
        self.batch_limit = batch_limit if batch_limit is not None else config.v2_outcome_batch_limit
        self.max_candidate_pages = config.v2_outcome_candidate_max_pages
        self.selector_lookback_hours = max(1, int(getattr(config, "v2_outcome_selector_lookback_hours", 12) or 12))
        self.candidate_page_size = max(25, min(1000, int(getattr(config, "v2_outcome_candidate_page_size", 250) or 250)))
        self.frontier_bootstrap_minutes = max(5, int(getattr(config, "v2_outcome_frontier_bootstrap_minutes", 60) or 60))
        self.retry_delay_seconds = max(60, int(getattr(config, "v2_outcome_retry_delay_seconds", 900) or 900))
        self.horizons = {str(item).upper() for item in getattr(config, "horizons", ["1H", "2H", "4H", "8H", "12H", "24H"])}

    def select_pending(self, now: datetime, batch_limit: int) -> dict[str, Any]:
        if self.frontier_repository is not None:
            return self.select_pending_frontier(now=now, batch_limit=batch_limit)
        return self.select_pending_bounded_windows(now=now, batch_limit=batch_limit)

    def select_pending_frontier(self, now: datetime, batch_limit: int) -> dict[str, Any]:
        candidate_limit = max(self.candidate_page_size, batch_limit * 2)
        started = time.perf_counter()
        candidates = []
        pending = []
        existing_ids = set()
        retry_ids = self.frontier_repository.due_retry_prediction_ids(now, limit=batch_limit)
        prediction_query_count = 0
        outcome_lookup_count = 0
        frontier_before = {}
        frontier_boundaries: dict[str, str] = {}

        if retry_ids and hasattr(self.prediction_repository, "by_ids"):
            retry_rows = _records(self.prediction_repository.by_ids(retry_ids))
            for row in retry_rows:
                row["frontier_source"] = "retry"
            candidates.extend(retry_rows)

        for horizon, minutes in sorted(HORIZON_MINUTES.items(), key=lambda item: item[1]):
            if horizon not in self.horizons:
                continue
            cutoff = now - timedelta(minutes=minutes)
            state = self.frontier_repository.state_for_horizon(horizon)
            if not state:
                state = self.frontier_repository.initialize_state(
                    horizon,
                    cutoff - timedelta(minutes=self.frontier_bootstrap_minutes),
                )
            cursor_iso = state.get("cursor_prediction_timestamp")
            frontier_before[horizon] = cursor_iso
            rows = _records(
                self.prediction_repository.frontier_candidates(
                    cursor_iso,
                    cutoff.isoformat(),
                    horizon=horizon,
                    limit=candidate_limit,
                )
            )
            prediction_query_count += 1
            for row in rows:
                row["frontier_source"] = "frontier"
            candidates.extend(rows)
            if rows:
                frontier_boundaries[horizon] = max(
                    pd.Timestamp(row.get("prediction_timestamp")).tz_convert("UTC").isoformat()
                    for row in rows
                )
            if len(candidates) >= batch_limit:
                break

        mature = [row for row in candidates if is_mature(row, now=now)]
        page_existing = self.outcome_repository.existing_prediction_ids([row.get("id") for row in mature])
        outcome_lookup_count += 1 if mature else 0
        existing_ids.update(page_existing)
        pending = [row for row in mature if row.get("id") not in existing_ids]

        pending = sorted(
            pending,
            key=lambda row: (
                pd.Timestamp(row.get("prediction_timestamp")).tz_convert("UTC")
                + pd.Timedelta(minutes=HORIZON_MINUTES.get(str(row.get("horizon") or "").upper(), 0)),
                pd.Timestamp(row.get("prediction_timestamp")).tz_convert("UTC"),
                str(row.get("horizon") or ""),
                str(row.get("target") or ""),
                str(row.get("id") or ""),
            ),
        )
        selected_timestamps = [pd.Timestamp(row.get("prediction_timestamp")).tz_convert("UTC") for row in pending]
        return {
            "pending": pending,
            "candidates": candidates,
            "mature": mature,
            "existing_ids": existing_ids,
            "candidate_pages_scanned": prediction_query_count,
            "prediction_query_count": prediction_query_count,
            "outcome_lookup_count": outcome_lookup_count,
            "selector_runtime_seconds": time.perf_counter() - started,
            "selector_exhausted": True,
            "oldest_selected_timestamp": min(selected_timestamps).isoformat() if selected_timestamps else None,
            "newest_selected_timestamp": max(selected_timestamps).isoformat() if selected_timestamps else None,
            "selector_strategy": "frontier_cursor_retry_queue",
            "selector_lookback_hours": None,
            "candidate_page_size": candidate_limit,
            "frontier_before": frontier_before,
            "frontier_boundaries": frontier_boundaries,
            "retry_candidate_count": len(retry_ids),
        }

    def select_pending_bounded_windows(self, now: datetime, batch_limit: int) -> dict[str, Any]:
        candidate_limit = max(self.candidate_page_size, batch_limit * 4)
        max_candidate_pages = max(1, int(self.max_candidate_pages or 800))
        started = time.perf_counter()
        candidates = []
        mature = []
        pending = []
        existing_ids = set()
        outcome_lookup_count = 0
        prediction_query_count = 0
        exhausted = False

        horizon_windows: list[tuple[str, datetime, datetime]] = []
        for horizon, minutes in sorted(HORIZON_MINUTES.items(), key=lambda item: item[1]):
            if horizon not in self.horizons:
                continue
            window_end = now - timedelta(minutes=minutes)
            window_start = window_end - timedelta(hours=self.selector_lookback_hours)
            horizon_windows.append((horizon, window_start, window_end))

        pages_scanned = 0
        for horizon, window_start, window_end in horizon_windows:
            offset = 0
            horizon_exhausted = False
            while pages_scanned < max_candidate_pages:
                if hasattr(self.prediction_repository, "mature_candidates_window"):
                    rows = _records(
                        self.prediction_repository.mature_candidates_window(
                            window_start.isoformat(),
                            window_end.isoformat(),
                            horizon=horizon,
                            limit=candidate_limit,
                            offset=offset,
                        )
                    )
                else:
                    rows = _records(
                        self.prediction_repository.mature_candidates(
                            window_end.isoformat(),
                            limit=candidate_limit,
                            offset=offset,
                        )
                    )
                    rows = [
                        row for row in rows
                        if str(row.get("horizon") or "").upper() == horizon
                        and pd.Timestamp(row.get("prediction_timestamp")).tz_convert("UTC") >= pd.Timestamp(window_start)
                    ]
                prediction_query_count += 1
                pages_scanned += 1
                if not rows:
                    horizon_exhausted = True
                    break

                candidates.extend(rows)
                page_mature = [row for row in rows if is_mature(row, now=now)]
                mature.extend(page_mature)
                page_existing = self.outcome_repository.existing_prediction_ids([row.get("id") for row in page_mature])
                outcome_lookup_count += 1
                existing_ids.update(page_existing)
                pending.extend(row for row in page_mature if row.get("id") not in page_existing)
                if len(rows) < candidate_limit:
                    horizon_exhausted = True
                    break
                offset += candidate_limit
            exhausted = exhausted or horizon_exhausted

        pending = sorted(
            pending,
            key=lambda row: (
                pd.Timestamp(row.get("prediction_timestamp")).tz_convert("UTC")
                + pd.Timedelta(minutes=HORIZON_MINUTES.get(str(row.get("horizon") or "").upper(), 0)),
                pd.Timestamp(row.get("prediction_timestamp")).tz_convert("UTC"),
                str(row.get("horizon") or ""),
                str(row.get("target") or ""),
                str(row.get("id") or ""),
            ),
        )[:batch_limit]

        selected_timestamps = [pd.Timestamp(row.get("prediction_timestamp")).tz_convert("UTC") for row in pending]
        return {
            "pending": pending,
            "candidates": candidates,
            "mature": mature,
            "existing_ids": existing_ids,
            "candidate_pages_scanned": pages_scanned,
            "prediction_query_count": prediction_query_count,
            "outcome_lookup_count": outcome_lookup_count,
            "selector_runtime_seconds": time.perf_counter() - started,
            "selector_exhausted": exhausted,
            "oldest_selected_timestamp": min(selected_timestamps).isoformat() if selected_timestamps else None,
            "newest_selected_timestamp": max(selected_timestamps).isoformat() if selected_timestamps else None,
            "selector_strategy": "bounded_horizon_maturity_windows",
            "selector_lookback_hours": self.selector_lookback_hours,
            "candidate_page_size": candidate_limit,
        }

    def _enqueue_incomplete_retries(self, rows, now: datetime, reason: str) -> int:
        if self.frontier_repository is None:
            return 0
        retry_after = now + timedelta(seconds=self.retry_delay_seconds)
        queued = 0
        for row in rows:
            if self.frontier_repository.enqueue_retry(row, reason=reason, retry_after=retry_after):
                queued += 1
        return queued

    def _advance_frontiers_after_success(self, selection, now: datetime) -> int:
        if self.frontier_repository is None:
            return 0
        advanced = 0
        for horizon, timestamp_iso in (selection.get("frontier_boundaries") or {}).items():
            ok = self.frontier_repository.advance_state(
                horizon,
                pd.Timestamp(timestamp_iso).tz_convert("UTC").to_pydatetime(),
                metadata={
                    "advanced_by": "step18b3_frontier",
                    "advanced_at": now.isoformat(),
                    "selector_strategy": selection.get("selector_strategy"),
                },
            )
            advanced += 1 if ok else 0
        return advanced

    def run(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        batch_limit = max(1, int(self.batch_limit or 25))
        selection = self.select_pending(now=now, batch_limit=batch_limit)
        candidates = selection["candidates"]
        mature = selection["mature"]
        pending = selection["pending"]
        existing_ids = selection["existing_ids"]

        if not pending:
            retry_queued = 0
            frontier_advanced = self._advance_frontiers_after_success(selection, now=now)
            return {
                "ok": True,
                "action": "EVALUATED",
                "candidate_count": len(candidates),
                "mature_count": len(mature),
                "attempted_count": 0,
                "created_count": 0,
                "skipped_existing_count": len(existing_ids),
                "skipped_incomplete_count": 0,
                "failed_count": 0,
                "batch_limit": batch_limit,
                "candidate_pages_scanned": selection["candidate_pages_scanned"],
                "prediction_query_count": selection["prediction_query_count"],
                "outcome_lookup_count": selection["outcome_lookup_count"],
                "selector_runtime_seconds": round(selection["selector_runtime_seconds"], 3),
                "selector_exhausted": selection["selector_exhausted"],
                "oldest_selected_timestamp": selection["oldest_selected_timestamp"],
                "newest_selected_timestamp": selection["newest_selected_timestamp"],
                "selector_strategy": selection.get("selector_strategy"),
                "selector_lookback_hours": selection.get("selector_lookback_hours"),
                "candidate_page_size": selection.get("candidate_page_size"),
                "retry_queued_count": retry_queued,
                "retries_cleared_count": 0,
                "frontier_advanced_count": frontier_advanced,
                "frontier_before": selection.get("frontier_before"),
                "frontier_boundaries": selection.get("frontier_boundaries"),
                "retry_candidate_count": selection.get("retry_candidate_count", 0),
            }

        snapshots = self.feature_snapshot_repository.by_ids([row.get("feature_snapshot_id") for row in pending])
        ready_outcomes = []
        ready_prediction_ids = []
        incomplete_rows = []
        incomplete = 0
        failed = 0
        ohlcv_fetch_count = 0
        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in pending:
            groups.setdefault(outcome_group_key(row), []).append(row)

        for (_symbol, _prediction_timestamp, _horizon), group_rows in groups.items():
            try:
                window = prediction_window(group_rows[0])
                if not window:
                    incomplete += len(group_rows)
                    continue
                candles = self.candle_fetcher(group_rows[0].get("symbol") or "ETHUSD", *window)
                ohlcv_fetch_count += 1
                for row in group_rows:
                    snapshot = snapshots.get(row.get("feature_snapshot_id"))
                    if not snapshot:
                        incomplete += 1
                        incomplete_rows.append(row)
                        continue
                    outcome = evaluate_shadow_target(row, candles, snapshot)
                    if not outcome.get("ok"):
                        incomplete += 1
                        incomplete_rows.append(row)
                        continue
                    outcome["evaluated_at"] = now.isoformat()
                    ready_outcomes.append((row.get("id"), outcome))
                    ready_prediction_ids.append(row.get("id"))
            except Exception:
                failed += len(group_rows)

        if hasattr(self.outcome_repository, "safe_insert_outcomes"):
            created, persistence_failed = self.outcome_repository.safe_insert_outcomes(ready_outcomes)
            failed += persistence_failed
        else:
            created = 0
            for prediction_id, outcome in ready_outcomes:
                if self.outcome_repository.safe_insert_outcome(prediction_id, outcome):
                    created += 1
                else:
                    failed += 1

        retry_queued = self._enqueue_incomplete_retries(incomplete_rows, now=now, reason="INCOMPLETE_WINDOW")
        retries_cleared = 0
        frontier_advanced = 0
        if self.frontier_repository is not None and failed == 0 and retry_queued == len(incomplete_rows):
            retries_cleared = self.frontier_repository.clear_retries(ready_prediction_ids)
            frontier_advanced = self._advance_frontiers_after_success(selection, now=now)

        return {
            "ok": failed == 0,
            "action": "EVALUATED",
            "candidate_count": len(candidates),
            "mature_count": len(mature),
            "attempted_count": len(pending),
            "created_count": created,
            "skipped_existing_count": len(existing_ids),
            "skipped_incomplete_count": incomplete,
            "failed_count": failed,
            "batch_limit": batch_limit,
            "candidate_pages_scanned": selection["candidate_pages_scanned"],
            "prediction_query_count": selection["prediction_query_count"],
            "outcome_lookup_count": selection["outcome_lookup_count"],
            "selector_runtime_seconds": round(selection["selector_runtime_seconds"], 3),
            "selector_exhausted": selection["selector_exhausted"],
            "oldest_selected_timestamp": selection["oldest_selected_timestamp"],
            "newest_selected_timestamp": selection["newest_selected_timestamp"],
            "selector_strategy": selection.get("selector_strategy"),
            "selector_lookback_hours": selection.get("selector_lookback_hours"),
            "candidate_page_size": selection.get("candidate_page_size"),
            "ohlcv_fetch_count": ohlcv_fetch_count,
            "outcome_group_count": len(groups),
            "bulk_persistence": hasattr(self.outcome_repository, "safe_insert_outcomes"),
            "retry_queued_count": retry_queued,
            "retries_cleared_count": retries_cleared,
            "frontier_advanced_count": frontier_advanced,
            "frontier_before": selection.get("frontier_before"),
            "frontier_boundaries": selection.get("frontier_boundaries"),
            "retry_candidate_count": selection.get("retry_candidate_count", 0),
        }
