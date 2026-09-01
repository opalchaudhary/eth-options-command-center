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
        rows = self.read(
            params={
                "select": "prediction_id",
                "prediction_id": f"in.({','.join(clean_ids)})",
                "label_version": f"eq.{label_version}",
                "limit": str(len(clean_ids)),
            }
        )
        return {row.get("prediction_id") for row in _records(rows) if row.get("prediction_id")}

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
        candle_fetcher=None,
        batch_limit=None,
    ):
        self.prediction_repository = prediction_repository or V2ShadowPredictionEvaluationRepository()
        self.outcome_repository = outcome_repository or V2ShadowOutcomeRepository()
        self.feature_snapshot_repository = feature_snapshot_repository or V2FeatureSnapshotEvaluationRepository()
        self.candle_fetcher = candle_fetcher or load_future_ohlcv
        self.batch_limit = batch_limit if batch_limit is not None else get_probability_config().v2_outcome_batch_limit
        self.max_candidate_pages = get_probability_config().v2_outcome_candidate_max_pages

    def select_pending(self, now: datetime, batch_limit: int) -> dict[str, Any]:
        candidate_limit = batch_limit * 4
        max_candidate_pages = max(1, int(self.max_candidate_pages or 800))
        before_iso = (now - timedelta(minutes=min(HORIZON_MINUTES.values()))).isoformat()
        started = time.perf_counter()
        candidates = []
        mature = []
        pending = []
        existing_ids = set()
        outcome_lookup_count = 0
        prediction_query_count = 0
        cursor_timestamp = None
        exhausted = False

        if hasattr(self.prediction_repository, "pending_mature_candidates"):
            rows = _records(
                self.prediction_repository.pending_mature_candidates(
                    before_iso,
                    limit=batch_limit,
                    label_version="label_v2",
                )
            )
            prediction_query_count = 1
            candidates.extend(rows)
            mature.extend(row for row in rows if is_mature(row, now=now))
            existing_ids = self.outcome_repository.existing_prediction_ids([row.get("id") for row in mature])
            outcome_lookup_count = 1
            pending = [row for row in mature if row.get("id") not in existing_ids][:batch_limit]
            exhausted = len(rows) < batch_limit
            selected_timestamps = [pd.Timestamp(row.get("prediction_timestamp")).tz_convert("UTC") for row in pending]
            return {
                "pending": pending,
                "candidates": candidates,
                "mature": mature,
                "existing_ids": existing_ids,
                "candidate_pages_scanned": 1,
                "prediction_query_count": prediction_query_count,
                "outcome_lookup_count": outcome_lookup_count,
                "selector_runtime_seconds": time.perf_counter() - started,
                "selector_exhausted": exhausted,
                "oldest_selected_timestamp": min(selected_timestamps).isoformat() if selected_timestamps else None,
                "newest_selected_timestamp": max(selected_timestamps).isoformat() if selected_timestamps else None,
            }

        for page in range(max_candidate_pages):
            if hasattr(self.prediction_repository, "mature_candidates_after"):
                rows = _records(
                    self.prediction_repository.mature_candidates_after(
                        before_iso,
                        after_timestamp_iso=cursor_timestamp,
                        limit=candidate_limit,
                    )
                )
            else:
                rows = _records(
                    self.prediction_repository.mature_candidates(
                        before_iso,
                        limit=candidate_limit,
                        offset=page * candidate_limit,
                    )
                )
            prediction_query_count += 1
            if not rows:
                exhausted = True
                break

            candidates.extend(rows)
            page_mature = [row for row in rows if is_mature(row, now=now)]
            mature.extend(page_mature)
            page_existing = self.outcome_repository.existing_prediction_ids([row.get("id") for row in page_mature])
            outcome_lookup_count += 1
            existing_ids.update(page_existing)
            pending.extend(row for row in page_mature if row.get("id") not in page_existing)
            pending = pending[:batch_limit]
            cursor_timestamp = pd.Timestamp(rows[-1].get("prediction_timestamp")).tz_convert("UTC").isoformat()
            if len(pending) >= batch_limit:
                break
            if len(rows) < candidate_limit:
                exhausted = True
                break

        selected_timestamps = [pd.Timestamp(row.get("prediction_timestamp")).tz_convert("UTC") for row in pending]
        return {
            "pending": pending,
            "candidates": candidates,
            "mature": mature,
            "existing_ids": existing_ids,
            "candidate_pages_scanned": page + 1 if "page" in locals() else 0,
            "prediction_query_count": prediction_query_count,
            "outcome_lookup_count": outcome_lookup_count,
            "selector_runtime_seconds": time.perf_counter() - started,
            "selector_exhausted": exhausted,
            "oldest_selected_timestamp": min(selected_timestamps).isoformat() if selected_timestamps else None,
            "newest_selected_timestamp": max(selected_timestamps).isoformat() if selected_timestamps else None,
        }

    def run(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        batch_limit = max(1, int(self.batch_limit or 25))
        selection = self.select_pending(now=now, batch_limit=batch_limit)
        candidates = selection["candidates"]
        mature = selection["mature"]
        pending = selection["pending"]
        existing_ids = selection["existing_ids"]

        if not pending:
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
            }

        snapshots = self.feature_snapshot_repository.by_ids([row.get("feature_snapshot_id") for row in pending])
        ready_outcomes = []
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
                        continue
                    outcome = evaluate_shadow_target(row, candles, snapshot)
                    if not outcome.get("ok"):
                        incomplete += 1
                        continue
                    outcome["evaluated_at"] = now.isoformat()
                    ready_outcomes.append((row.get("id"), outcome))
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
            "ohlcv_fetch_count": ohlcv_fetch_count,
            "outcome_group_count": len(groups),
            "bulk_persistence": hasattr(self.outcome_repository, "safe_insert_outcomes"),
        }
