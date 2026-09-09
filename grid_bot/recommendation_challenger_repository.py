from __future__ import annotations

from datetime import timedelta
from typing import Any

from research.grid_intelligence_policy_challenger_v01 import (
    as_float,
    expansion_widen_policy,
    parse_ts,
    stress_no_grid_policy,
    trailing_features,
    v2_volatility_buffer_policy,
)

from .models import new_id, utc_now
from .supabase_repository import SupabaseGridRepository


CHALLENGER_TABLE = "grid_parameter_recommendation_challengers"
CHALLENGER_POLICY_VERSION = "grid_intelligence_shadow_challenger_v0_1"
TRAILING_LOOKBACK_MINUTES = 12 * 60
TRAILING_RESOLUTION = "5m"
SHADOW_POLICIES = (
    ("expansion_widen", expansion_widen_policy),
    ("v2_range70_trailing_buffer", v2_volatility_buffer_policy),
    ("stress_filter_no_grid", stress_no_grid_policy),
)


def _numeric(value: Any) -> str | None:
    if value in [None, ""]:
        return None
    return str(value)


class GridRecommendationChallengerRepository:
    """Append-only persistence for research-only Grid Intelligence shadow challengers."""

    def __init__(self, db: SupabaseGridRepository | None = None):
        self.db = db or SupabaseGridRepository()
        self.enabled = bool(getattr(self.db, "enabled", False))

    def build_challengers(self, champion: dict[str, Any]) -> list[dict[str, Any]]:
        trailing = self._trailing_inputs(champion)
        generated_at = utc_now()
        rows = []
        for policy_name, policy_fn in SHADOW_POLICIES:
            policy_range = policy_fn(champion, trailing)
            metadata = self._metadata(policy_name, champion, trailing, policy_range)
            rows.append(
                {
                    "challenger_id": new_id("gchall"),
                    "recommendation_id": champion.get("recommendation_id"),
                    "challenger_policy": policy_name,
                    "challenger_policy_version": CHALLENGER_POLICY_VERSION,
                    "generated_at": generated_at,
                    "prediction_timestamp": champion.get("prediction_timestamp"),
                    "symbol": champion.get("symbol") or "ETHUSD",
                    "horizon": champion.get("selected_operating_horizon"),
                    "shadow_action": "NO_GRID" if policy_range.no_grid else champion.get("recommendation_action"),
                    "shadow_grid_type": None if policy_range.no_grid else champion.get("recommended_grid_type"),
                    "shadow_lower_price": _numeric(policy_range.lower),
                    "shadow_upper_price": _numeric(policy_range.upper),
                    "shadow_grid_count": None if policy_range.no_grid else champion.get("recommended_grid_count"),
                    "shadow_spacing_type": None if policy_range.no_grid else champion.get("recommended_spacing_type"),
                    "shadow_grid_step": None if policy_range.no_grid else champion.get("recommended_grid_step"),
                    "no_grid": bool(policy_range.no_grid),
                    "no_grid_reason": policy_range.reason,
                    "metadata_json": metadata,
                    "immutable": True,
                }
            )
        return rows

    def insert_for_recommendation(self, champion: dict[str, Any]) -> list[dict[str, Any]]:
        rows = self.build_challengers(champion)
        for row in rows:
            self.db.insert_once(CHALLENGER_TABLE, row, on_conflict="recommendation_id,challenger_policy")
        return rows

    def _trailing_inputs(self, champion: dict[str, Any]) -> dict[str, float]:
        start = parse_ts(champion.get("requested_at") or champion.get("created_at"))
        if not start:
            return {}
        try:
            rows = self.db.select(
                "eth_ohlcv",
                {
                    "select": "candle_time,open,high,low,close,volume",
                    "symbol": f"eq.{champion.get('symbol') or 'ETHUSD'}",
                    "resolution": f"eq.{TRAILING_RESOLUTION}",
                    "candle_time": f"gt.{(start - timedelta(minutes=TRAILING_LOOKBACK_MINUTES)).isoformat()}",
                    "order": "candle_time.asc",
                    "limit": 2000,
                },
            )
        except Exception:
            return {}
        candles = [row for row in rows if (ts := parse_ts(row.get("candle_time"))) and start - timedelta(minutes=TRAILING_LOOKBACK_MINUTES) < ts <= start]
        return trailing_features(candles)

    def _metadata(self, policy_name: str, champion: dict[str, Any], trailing: dict[str, float], policy_range: Any) -> dict[str, Any]:
        champion_lower = as_float(champion.get("recommended_lower_price"))
        champion_upper = as_float(champion.get("recommended_upper_price"))
        champion_width = champion_upper - champion_lower if champion_lower is not None and champion_upper is not None else None
        shadow_width = policy_range.width
        expansion = as_float(champion.get("realized_over_range_width_ge_1"))
        factor = None
        if policy_name == "expansion_widen" and champion_width and shadow_width:
            factor = shadow_width / champion_width
        buffer_width = None
        if policy_name == "v2_range70_trailing_buffer":
            ref_lower = as_float(champion.get("range_70_lower"))
            ref_upper = as_float(champion.get("range_70_upper"))
            base_width = champion_width
            if ref_lower is not None and ref_upper is not None and ref_upper > ref_lower:
                base_width = max(base_width or 0.0, ref_upper - ref_lower)
            if base_width is not None and shadow_width is not None:
                buffer_width = shadow_width - base_width
        return {
            "source": "research.grid_intelligence_policy_challenger_v01",
            "policy_version": CHALLENGER_POLICY_VERSION,
            "champion_recommender_version": champion.get("recommender_version"),
            "champion_action": champion.get("recommendation_action"),
            "champion_lower_price": _numeric(champion_lower),
            "champion_upper_price": _numeric(champion_upper),
            "champion_width": _numeric(champion_width),
            "shadow_width": _numeric(shadow_width),
            "width_vs_champion": shadow_width / champion_width if champion_width and shadow_width is not None else None,
            "path_inside_70": _numeric(champion.get("path_inside_70")),
            "expansion_probability": _numeric(expansion),
            "range_70_lower": _numeric(champion.get("range_70_lower")),
            "range_70_upper": _numeric(champion.get("range_70_upper")),
            "trailing_12h_path_width": _numeric(trailing.get("trailing_12h_path_width")),
            "trailing_12h_close_vol": _numeric(trailing.get("trailing_12h_close_vol")),
            "expansion_widen_factor": factor,
            "trailing_buffer_width": buffer_width,
            "no_grid_reason": policy_range.reason,
            "math_source": {
                "expansion_widen": "expansion_widen_policy",
                "v2_range70_trailing_buffer": "v2_volatility_buffer_policy",
                "stress_filter_no_grid": "stress_no_grid_policy",
            }.get(policy_name),
        }
