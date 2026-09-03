from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .models import new_id, utc_now
from .supabase_repository import SupabaseGridRepository


TABLE_NAME = "grid_parameter_recommendations"


def _iso(value: Any) -> str | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return str(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _numeric(value: Any) -> str | None:
    if value in [None, ""]:
        return None
    return str(value)


def _bool(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


class GridRecommendationSnapshotRepository:
    """Append-only persistence for Grid Parameter recommendation research snapshots."""

    def __init__(self, db: SupabaseGridRepository | None = None):
        self.db = db or SupabaseGridRepository()
        self.enabled = bool(getattr(self.db, "enabled", False))

    def build_snapshot(self, payload: dict, *, requested_at: datetime | str | None = None) -> dict:
        recommendation = payload.get("recommendation") or {}
        inputs = recommendation.get("inputs_summary") or {}
        current = payload.get("current_grid") or {}
        sources = payload.get("sources") or {}
        metadata = {
            "confidence_type": recommendation.get("confidence_type"),
            "v2_manifest_hash": recommendation.get("v2_manifest_hash"),
            "source_v2_row_ids": sources.get("source_v2_row_ids") or {},
            "source_feature_snapshot_ids": sources.get("source_feature_snapshot_ids") or {},
            "spot_source": sources.get("current_spot_source"),
            "source_payload_schema": "grid_recommendation_v0_1",
        }
        return {
            "recommendation_id": new_id("grec"),
            "created_at": utc_now(),
            "requested_at": _iso(requested_at) or utc_now(),
            "prediction_timestamp": _iso(payload.get("prediction_timestamp")),
            "symbol": payload.get("symbol"),
            "recommender_version": recommendation.get("recommender_version"),
            "policy_version": recommendation.get("policy_version"),
            "probability_model_version": recommendation.get("v2_model_version"),
            "spot_price": _numeric(payload.get("spot_price")),
            "selected_operating_horizon": recommendation.get("operating_horizon"),
            "path_inside_70": _numeric(inputs.get("path_inside_70")),
            "realized_over_range_width_ge_1": _numeric(inputs.get("range_expansion")),
            "upside_probability": _numeric(inputs.get("upside_probability")),
            "downside_probability": _numeric(inputs.get("downside_probability")),
            "range_70_lower": _numeric(inputs.get("range_70_lower")),
            "range_70_upper": _numeric(inputs.get("range_70_upper")),
            "v2_ood": _bool(recommendation.get("v2_ood")),
            "v2_abstained": _bool(recommendation.get("v2_abstained")),
            "v2_stale": _bool(recommendation.get("stale")),
            "source_prediction_id": sources.get("source_prediction_id"),
            "recommended_grid_type": recommendation.get("grid_type"),
            "recommended_lower_price": _numeric(recommendation.get("lower_price")),
            "recommended_upper_price": _numeric(recommendation.get("upper_price")),
            "recommended_grid_count": recommendation.get("grid_count"),
            "recommended_spacing_type": recommendation.get("spacing_type"),
            "recommended_grid_step": _numeric(recommendation.get("grid_step")),
            "recommender_confidence": _numeric(recommendation.get("confidence")),
            "recommendation_action": recommendation.get("action"),
            "reason_codes": recommendation.get("reason_codes") or [],
            "reasons": recommendation.get("reasons") or [],
            "active_run_id": current.get("run_id"),
            "bot_id": current.get("bot_id"),
            "config_version": current.get("config_version"),
            "current_grid_type": current.get("grid_type"),
            "current_lower_price": _numeric(current.get("lower_price")),
            "current_upper_price": _numeric(current.get("upper_price")),
            "current_grid_count": current.get("grid_count"),
            "current_spacing_type": current.get("spacing_type"),
            "current_grid_step": _numeric(current.get("step")),
            "current_lot_size": _numeric(current.get("lot_size")),
            "current_max_inventory_lots": _numeric(current.get("max_inventory_lots")),
            "probability_source": sources.get("probability_source"),
            "range_source": sources.get("range_source"),
            "spot_source": sources.get("current_spot_source"),
            "current_grid_source": sources.get("current_grid_source"),
            "metadata_json": metadata,
        }

    def insert(self, snapshot: dict) -> str:
        self.db.insert_once(TABLE_NAME, snapshot, on_conflict="recommendation_id")
        return str(snapshot["recommendation_id"])

    def latest(
        self,
        *,
        limit: int = 50,
        recommender_version: str | None = None,
        horizon: str | None = None,
        action: str | None = None,
    ) -> list[dict]:
        params = {"select": "*", "order": "created_at.desc", "limit": max(1, min(int(limit), 500))}
        if recommender_version:
            params["recommender_version"] = f"eq.{recommender_version}"
        if horizon:
            params["selected_operating_horizon"] = f"eq.{horizon}"
        if action:
            params["recommendation_action"] = f"eq.{action}"
        return self.db.select(TABLE_NAME, params)

    def by_time_window(
        self,
        *,
        start: str,
        end: str,
        limit: int = 500,
    ) -> list[dict]:
        return self.db.select(
            TABLE_NAME,
            {
                "select": "*",
                "and": f"(created_at.gte.{start},created_at.lte.{end})",
                "order": "created_at.desc",
                "limit": max(1, min(int(limit), 1000)),
            },
        )
