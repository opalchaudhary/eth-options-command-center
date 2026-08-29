from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

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

    def safe_insert_outcome(self, prediction_id, outcome, label_version="label_v2"):
        payload = {
            "prediction_id": prediction_id,
            "label_version": label_version,
            "target": outcome.get("metadata_json", {}).get("target"),
            "horizon": outcome.get("metadata_json", {}).get("horizon"),
            **outcome,
        }
        return self.safe_insert(payload)


class V2ShadowPredictionEvaluationRepository(SupabaseRepository):
    table_name = "probability_v2_shadow_predictions"

    def mature_candidates(self, before_iso, limit=100, offset=0):
        return self.read(
            params={
                "select": PREDICTION_SELECT,
                "prediction_timestamp": f"lte.{before_iso}",
                "record_type": "eq.LIVE",
                "abstained": "eq.false",
                "order": "prediction_timestamp.asc,target.asc,horizon.asc",
                "limit": str(limit),
                "offset": str(offset),
            }
        )


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
        self.batch_limit = batch_limit if batch_limit is not None else get_probability_config().outcome_batch_limit

    def run(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        batch_limit = max(1, int(self.batch_limit or 25))
        candidate_limit = batch_limit * 4
        before_iso = (now - timedelta(minutes=min(HORIZON_MINUTES.values()))).isoformat()
        candidates = []
        mature = []
        pending = []
        existing_ids = set()

        for page in range(20):
            rows = _records(self.prediction_repository.mature_candidates(before_iso, limit=candidate_limit, offset=page * candidate_limit))
            candidates.extend(rows)
            page_mature = [row for row in rows if is_mature(row, now=now)]
            mature.extend(page_mature)
            page_existing = self.outcome_repository.existing_prediction_ids([row.get("id") for row in page_mature])
            existing_ids.update(page_existing)
            pending.extend(row for row in page_mature if row.get("id") not in page_existing)
            pending = pending[:batch_limit]
            if len(pending) >= batch_limit or len(rows) < candidate_limit:
                break

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
                "candidate_pages_scanned": page + 1 if "page" in locals() else 0,
            }

        snapshots = self.feature_snapshot_repository.by_ids([row.get("feature_snapshot_id") for row in pending])
        created = 0
        incomplete = 0
        failed = 0
        for row in pending:
            try:
                snapshot = snapshots.get(row.get("feature_snapshot_id"))
                if not snapshot:
                    incomplete += 1
                    continue
                start_at, end_at = prediction_window(row)
                candles = self.candle_fetcher(row.get("symbol") or "ETHUSD", start_at, end_at)
                outcome = evaluate_shadow_target(row, candles, snapshot)
                if not outcome.get("ok"):
                    incomplete += 1
                    continue
                outcome["evaluated_at"] = now.isoformat()
                if self.outcome_repository.safe_insert_outcome(row.get("id"), outcome):
                    created += 1
                else:
                    failed += 1
            except Exception:
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
            "candidate_pages_scanned": page + 1 if "page" in locals() else 0,
        }
