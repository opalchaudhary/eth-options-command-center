from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

import database_reader
from probability_engine.config import HORIZON_MINUTES, get_probability_config
from probability_engine.repositories.base_repository import SupabaseRepository
from probability_engine.services.v2_shadow_outcome import (
    V2FeatureSnapshotEvaluationRepository,
    evaluate_shadow_target,
    is_mature,
    load_future_ohlcv,
    outcome_group_key,
    prediction_window,
)


LABEL_VERSION = "label_v2"


def _records(rows):
    if rows is None:
        return []
    if hasattr(rows, "empty") and hasattr(rows, "to_dict"):
        return [] if rows.empty else rows.to_dict("records")
    if isinstance(rows, dict):
        return [rows]
    return list(rows or [])


class V21ShadowPredictionEvaluationRepository(SupabaseRepository):
    table_name = "probability_v2_1_shadow_predictions"

    def recent_mature_candidates(self, before_iso: str, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.read(
            params={
                "select": (
                    "id,created_at,prediction_timestamp,symbol,record_type,model_version,target,"
                    "horizon,v2_baseline_prediction_id,label_version,metadata_json,"
                    "probability_v2_1_shadow_outcomes!left(prediction_id,label_version,target)"
                ),
                "prediction_timestamp": f"lte.{before_iso}",
                "record_type": "eq.LIVE",
                "probability_v2_1_shadow_outcomes": "is.null",
                "order": "prediction_timestamp.asc,horizon.asc,target.asc,id.asc",
                "limit": str(limit),
            }
        )
        clean_rows = []
        for row in _records(rows):
            row.pop("probability_v2_1_shadow_outcomes", None)
            clean_rows.append(row)
        return clean_rows


class V21ShadowOutcomeRepository(SupabaseRepository):
    table_name = "probability_v2_1_shadow_outcomes"

    def existing_prediction_ids(self, prediction_ids) -> set[str]:
        clean = [str(item) for item in prediction_ids if item]
        if not clean:
            return set()
        found = set()
        for index in range(0, len(clean), 50):
            chunk = clean[index : index + 50]
            rows = self.read(
                params={
                    "select": "prediction_id",
                    "prediction_id": f"in.({','.join(chunk)})",
                    "label_version": f"eq.{LABEL_VERSION}",
                    "limit": str(len(chunk)),
                }
            )
            found.update(str(row.get("prediction_id")) for row in _records(rows) if row.get("prediction_id"))
        return found

    def safe_insert_outcomes(self, outcomes) -> tuple[int, int]:
        payloads = [self.outcome_payload(prediction_id, outcome) for prediction_id, outcome in outcomes]
        if not payloads:
            return 0, 0
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
            return len(response.json() if response.text else []), 0
        if response.status_code == 204:
            return len(payloads), 0
        return 0, len(payloads)

    def outcome_payload(self, prediction_id, outcome):
        allowed = {
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
        base = {key: value for key, value in outcome.items() if key in allowed}
        return {
            "prediction_id": prediction_id,
            "label_version": LABEL_VERSION,
            "target": outcome.get("metadata_json", {}).get("target"),
            "horizon": outcome.get("metadata_json", {}).get("horizon"),
            **base,
        }


def fetch_baseline_predictions(prediction_ids: list[str]) -> dict[str, dict[str, Any]]:
    clean = [str(item) for item in prediction_ids if item]
    found: dict[str, dict[str, Any]] = {}
    for index in range(0, len(clean), 50):
        chunk = clean[index : index + 50]
        rows = database_reader.read_supabase_table(
            "probability_v2_shadow_predictions",
            params={
                "select": "id,created_at,feature_snapshot_id,prediction_timestamp,symbol,record_type,model_version,model_id,target,horizon,feature_version,label_version,calibration_version,manifest_hash,metadata_json",
                "id": f"in.({','.join(chunk)})",
                "limit": str(len(chunk)),
            },
        )
        for row in _records(rows):
            if row.get("id"):
                found[str(row["id"])] = row
    return found


class V21ShadowOutcomeEvaluator:
    def __init__(
        self,
        prediction_repository=None,
        outcome_repository=None,
        feature_snapshot_repository=None,
        candle_fetcher=None,
        batch_limit=None,
    ):
        self.prediction_repository = prediction_repository or V21ShadowPredictionEvaluationRepository()
        self.outcome_repository = outcome_repository or V21ShadowOutcomeRepository()
        self.feature_snapshot_repository = feature_snapshot_repository or V2FeatureSnapshotEvaluationRepository()
        self.candle_fetcher = candle_fetcher or load_future_ohlcv
        config = get_probability_config()
        self.batch_limit = batch_limit if batch_limit is not None else getattr(config, "v21_outcome_batch_limit", 25)

    def run(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        limit = max(1, int(self.batch_limit or 25) * 4)
        max_minutes = max(HORIZON_MINUTES.values())
        before = now
        candidates = self.prediction_repository.recent_mature_candidates(before.isoformat(), limit=limit)
        mature = [row for row in candidates if is_mature(row, now=now)]
        existing = self.outcome_repository.existing_prediction_ids([row.get("id") for row in mature])
        pending = [row for row in mature if str(row.get("id")) not in existing]
        pending = sorted(
            pending,
            key=lambda row: (
                pd.Timestamp(row.get("prediction_timestamp")).tz_convert("UTC")
                + pd.Timedelta(minutes=HORIZON_MINUTES.get(str(row.get("horizon") or "").upper(), max_minutes)),
                str(row.get("target") or ""),
            ),
        )[: max(1, int(self.batch_limit or 25))]
        if not pending:
            return {
                "ok": True,
                "action": "EVALUATED",
                "candidate_count": len(candidates),
                "mature_count": len(mature),
                "attempted_count": 0,
                "created_count": 0,
                "skipped_existing_count": len(existing),
                "skipped_incomplete_count": 0,
                "failed_count": 0,
            }

        baseline = fetch_baseline_predictions([row.get("v2_baseline_prediction_id") for row in pending])
        snapshots = self.feature_snapshot_repository.by_ids([row.get("feature_snapshot_id") for row in baseline.values()])
        ready = []
        incomplete = 0
        failed = 0
        groups: dict[tuple[str, str, str], list[tuple[dict[str, Any], dict[str, Any]]]] = {}
        for row in pending:
            base = baseline.get(str(row.get("v2_baseline_prediction_id")))
            if not base:
                incomplete += 1
                continue
            groups.setdefault(outcome_group_key(base), []).append((row, base))

        for (_symbol, _prediction_timestamp, _horizon), pairs in groups.items():
            try:
                window = prediction_window(pairs[0][1])
                if not window:
                    incomplete += len(pairs)
                    continue
                candles = self.candle_fetcher(pairs[0][1].get("symbol") or "ETHUSD", *window)
                for v21_row, base in pairs:
                    snapshot = snapshots.get(base.get("feature_snapshot_id"))
                    if not snapshot:
                        incomplete += 1
                        continue
                    outcome = evaluate_shadow_target(base, candles, snapshot)
                    if not outcome.get("ok"):
                        incomplete += 1
                        continue
                    outcome["evaluated_at"] = now.isoformat()
                    outcome["metadata_json"] = {
                        **(outcome.get("metadata_json") or {}),
                        "v21_prediction_id": v21_row.get("id"),
                        "v2_baseline_prediction_id": base.get("id"),
                        "semantics": "Probability V2.1 shadow outcome uses exact V2.0 label semantics.",
                    }
                    ready.append((v21_row.get("id"), outcome))
            except Exception:
                failed += len(pairs)

        created, insert_failed = self.outcome_repository.safe_insert_outcomes(ready)
        failed += insert_failed
        return {
            "ok": failed == 0,
            "action": "EVALUATED",
            "candidate_count": len(candidates),
            "mature_count": len(mature),
            "attempted_count": len(pending),
            "created_count": created,
            "skipped_existing_count": len(existing),
            "skipped_incomplete_count": incomplete,
            "failed_count": failed,
            "outcome_group_count": len(groups),
        }
