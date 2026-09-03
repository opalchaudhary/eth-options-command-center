from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Any

from backend.services.delta_client import eth_market_snapshot
from probability_engine.services.v2_shadow_service import V2ShadowPredictionRepository

from .models import GridType, SpacingType
from .recommendation import (
    PREFERRED_HORIZONS,
    CurrentGridSnapshot,
    GridParameterRecommendation,
    GridProbabilityInputs,
    HorizonProbability,
    RecommendationAction,
    as_utc,
    no_grid,
    recommend_grid_parameters,
)
from .recommendation_snapshot_repository import GridRecommendationSnapshotRepository
from .supabase_repository import SupabaseGridRepository


SYMBOL = "ETHUSD"
REQUIRED_GRID_TARGETS = {"path_inside_70", "realized_over_range_width_ge_1"}
OPTIONAL_DIRECTIONAL_TARGETS = {
    "upside_breakout",
    "downside_breakdown",
    "upper_breach_only",
    "lower_breach_only",
}
GRID_INTERFACE_PATH = Path(__file__).resolve().parents[1] / "probability_engine" / "models" / "v2_candidate_v1" / "probability_interface.json"


class GridRecommendationUnavailable(RuntimeError):
    pass


class GridRecommendationStorageError(RuntimeError):
    pass


def _max_age_seconds() -> int:
    try:
        minutes = int(os.getenv("GRID_RECOMMENDATION_MAX_PREDICTION_AGE_MINUTES", "20"))
    except (TypeError, ValueError):
        minutes = 20
    return max(1, minutes) * 60


def _records(rows: Any) -> list[dict]:
    if rows is None:
        return []
    if hasattr(rows, "empty") and hasattr(rows, "to_dict"):
        return [] if rows.empty else rows.to_dict("records")
    if isinstance(rows, dict):
        return [rows]
    return list(rows or [])


@lru_cache(maxsize=1)
def grid_interface_targets() -> dict[str, set[str]]:
    rows = json.loads(GRID_INTERFACE_PATH.read_text(encoding="utf-8"))
    targets: dict[str, set[str]] = {}
    for row in rows:
        target = str(row.get("target") or "")
        horizon = str(row.get("horizon") or "").upper()
        if not target or not horizon:
            continue
        targets.setdefault(horizon, set()).add(target)
    return targets


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _decimal(value: Any) -> Decimal | None:
    if value in [None, ""]:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _probability(value: Any) -> float | None:
    if value in [None, ""]:
        return None
    try:
        probability = float(value)
    except (TypeError, ValueError):
        return None
    if probability < 0 or probability > 1:
        return None
    return probability


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return as_utc(value).isoformat()
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


class GridRecommendationService:
    def __init__(
        self,
        *,
        prediction_repository: Any | None = None,
        grid_repository: Any | None = None,
        snapshot_repository: Any | None = None,
        market_snapshot_fn: Any | None = None,
        now_fn: Any | None = None,
    ):
        self.prediction_repository = prediction_repository or V2ShadowPredictionRepository()
        self.grid_repository = grid_repository if grid_repository is not None else SupabaseGridRepository()
        self.snapshot_repository = snapshot_repository
        self.market_snapshot_fn = market_snapshot_fn or eth_market_snapshot
        self.now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    def recommendation(self, symbol: str = SYMBOL, *, persist: bool = False) -> dict:
        as_of = as_utc(self.now_fn())
        spot = self._current_spot()
        if spot is None:
            raise GridRecommendationUnavailable("Current ETH spot is unavailable.")

        rows = self._latest_v2_rows(symbol)
        current_grid = self._current_grid(spot)
        inputs, source_timestamp, input_codes = self._inputs_from_rows(rows, spot, as_of)
        if inputs is None:
            inputs = GridProbabilityInputs(spot_price=spot, predictions={}, as_of=as_of)
            recommendation = no_grid(inputs, input_codes or ["NO_V2_PREDICTIONS"])
        else:
            recommendation = recommend_grid_parameters(inputs, current_grid=current_grid)
            if input_codes and recommendation.action == RecommendationAction.NO_GRID:
                recommendation = no_grid(inputs, list(recommendation.reason_codes) + input_codes)

        payload = self._payload(symbol, spot, source_timestamp, recommendation, current_grid, inputs)
        if persist:
            payload["persistence"] = self._persist_snapshot(payload, requested_at=as_of)
        return payload

    def history(
        self,
        *,
        limit: int = 50,
        recommender_version: str | None = None,
        horizon: str | None = None,
        action: str | None = None,
    ) -> dict:
        repository = self._snapshot_repository()
        rows = repository.latest(
            limit=limit,
            recommender_version=recommender_version,
            horizon=horizon,
            action=action,
        )
        return {"ok": True, "rows": rows}

    def _latest_v2_rows(self, symbol: str) -> list[dict]:
        try:
            return _records(self.prediction_repository.latest(symbol=symbol, limit=120))
        except Exception as exc:
            raise GridRecommendationStorageError(f"V2 shadow storage unavailable: {exc}") from exc

    def _current_spot(self) -> Decimal | None:
        try:
            market = self.market_snapshot_fn(include_orderbook=False)
        except TypeError:
            market = self.market_snapshot_fn()
        except Exception as exc:
            raise GridRecommendationUnavailable(f"Current ETH spot is unavailable: {exc}") from exc
        return _decimal((market or {}).get("spot_price") or (market or {}).get("mark_price"))

    def _current_grid(self, spot: Decimal) -> CurrentGridSnapshot | None:
        if not self.grid_repository or not getattr(self.grid_repository, "enabled", True):
            return None
        try:
            row = self.grid_repository.active_config_snapshot()
        except Exception:
            return None
        if not row:
            return None
        try:
            return CurrentGridSnapshot(
                run_id=row.get("run_id"),
                bot_id=row.get("bot_id"),
                config_version=int(row["config_version"]) if row.get("config_version") not in [None, ""] else None,
                grid_type=GridType(row["grid_type"]),
                lower_price=Decimal(str(row["lower_price"])),
                upper_price=Decimal(str(row["upper_price"])),
                grid_count=int(row["grid_count"]),
                spacing_type=SpacingType(row["spacing_type"]),
                lot_size=Decimal(str(row["lot_size"])) if row.get("lot_size") not in [None, ""] else None,
                max_inventory_lots=Decimal(str(row["max_inventory_lots"])) if row.get("max_inventory_lots") not in [None, ""] else None,
                spot_price=spot,
            )
        except Exception:
            return None

    def _inputs_from_rows(self, rows: list[dict], spot: Decimal, as_of: datetime) -> tuple[GridProbabilityInputs | None, datetime | None, list[str]]:
        live_rows = [row for row in rows if str(row.get("record_type") or "LIVE").upper() == "LIVE"]
        timestamps = sorted({ts for ts in (_parse_ts(row.get("prediction_timestamp")) for row in live_rows) if ts}, reverse=True)
        if not timestamps:
            return None, None, ["NO_V2_PREDICTIONS"]
        latest_ts = timestamps[0]
        snapshot_rows = [row for row in live_rows if _parse_ts(row.get("prediction_timestamp")) == latest_ts]
        if len(timestamps) > 1:
            older_targets = {(str(row.get("horizon") or "").upper(), row.get("target")) for row in live_rows if _parse_ts(row.get("prediction_timestamp")) != latest_ts}
            latest_targets = {(str(row.get("horizon") or "").upper(), row.get("target")) for row in snapshot_rows}
            if older_targets - latest_targets:
                mismatch_code = ["OLDER_V2_ROWS_NOT_COMBINED"]
            else:
                mismatch_code = []
        else:
            mismatch_code = []

        supported = grid_interface_targets()
        by_horizon: dict[str, dict[str, dict]] = {}
        for row in snapshot_rows:
            horizon = str(row.get("horizon") or "").upper()
            target = str(row.get("target") or "")
            if horizon not in PREFERRED_HORIZONS or target not in supported.get(horizon, set()):
                continue
            if target not in REQUIRED_GRID_TARGETS and target not in OPTIONAL_DIRECTIONAL_TARGETS:
                continue
            targets = by_horizon.setdefault(horizon, {})
            if target in targets:
                return None, latest_ts, ["DUPLICATE_V2_SIGNAL"]
            targets[target] = row

        predictions = {}
        stale = (as_utc(as_of) - latest_ts).total_seconds() > _max_age_seconds()
        malformed = False
        for horizon in PREFERRED_HORIZONS:
            targets = by_horizon.get(horizon)
            if not targets:
                continue
            inside_row = targets.get("path_inside_70")
            expansion_row = targets.get("realized_over_range_width_ge_1")
            metadata = (inside_row or expansion_row or {}).get("metadata_json") or {}
            if not isinstance(metadata, dict):
                metadata = {}
                malformed = True
            range_lower = _decimal(metadata.get("range_70_lower"))
            range_upper = _decimal(metadata.get("range_70_upper"))
            inside = _probability((inside_row or {}).get("calibrated_probability"))
            expansion = _probability((expansion_row or {}).get("calibrated_probability"))
            if (inside_row and inside is None) or (expansion_row and expansion is None):
                malformed = True
            predictions[horizon] = HorizonProbability(
                horizon=horizon,
                path_inside_70=inside,
                realized_over_range_width_ge_1=expansion,
                range_70_lower=range_lower,
                range_70_upper=range_upper,
                upside_breakout=_probability((targets.get("upside_breakout") or {}).get("calibrated_probability")),
                downside_breakdown=_probability((targets.get("downside_breakdown") or {}).get("calibrated_probability")),
                upper_breach_only=_probability((targets.get("upper_breach_only") or {}).get("calibrated_probability")),
                lower_breach_only=_probability((targets.get("lower_breach_only") or {}).get("calibrated_probability")),
                prediction_timestamp=latest_ts,
                model_version=(inside_row or expansion_row or {}).get("model_version"),
                manifest_hash=(inside_row or expansion_row or {}).get("manifest_hash"),
                abstained=any(bool(row.get("abstained")) for row in targets.values()),
                abstention_reason=next((row.get("abstention_reason") for row in targets.values() if row.get("abstention_reason")), None),
                ood_status=next((row.get("ood_status") for row in targets.values() if row.get("ood_status")), None),
                ood_reason=next((row.get("ood_reason") for row in targets.values() if row.get("ood_reason")), None),
                stale=stale,
                metadata={
                    "v2_rows": {target: row.get("id") for target, row in targets.items()},
                    "feature_snapshots": {target: row.get("feature_snapshot_id") for target, row in targets.items()},
                },
            )
        if malformed:
            return None, latest_ts, ["MALFORMED_V2_ROW"]
        if not predictions:
            return None, latest_ts, ["MISSING_GRID_V2_SIGNALS"]
        return GridProbabilityInputs(spot_price=spot, predictions=predictions, as_of=as_of), latest_ts, mismatch_code

    def _payload(
        self,
        symbol: str,
        spot: Decimal,
        prediction_timestamp: datetime | None,
        recommendation: GridParameterRecommendation,
        current_grid: CurrentGridSnapshot | None,
        inputs: GridProbabilityInputs,
    ) -> dict:
        metadata = recommendation.metadata or {}
        any_v2_ood = any(
            str(prediction.ood_status or "").upper() in {"FLAGGED", "OOD", "OUT_OF_DISTRIBUTION"}
            for prediction in inputs.predictions.values()
        )
        any_v2_abstained = any(bool(prediction.abstained) for prediction in inputs.predictions.values())
        any_v2_stale = any(bool(prediction.stale) for prediction in inputs.predictions.values())
        current_summary = None
        if current_grid:
            current_summary = _json_value(asdict(current_grid))
        return {
            "ok": True,
            "symbol": symbol,
            "spot_price": float(spot),
            "prediction_timestamp": prediction_timestamp.isoformat() if prediction_timestamp else None,
            "recommendation": {
                "recommender_version": recommendation.recommender_version,
                "grid_type": recommendation.grid_type.value if recommendation.grid_type else None,
                "lower_price": _json_value(recommendation.lower_price),
                "upper_price": _json_value(recommendation.upper_price),
                "grid_count": recommendation.grid_count,
                "spacing_type": recommendation.spacing_type.value if recommendation.spacing_type else None,
                "grid_step": _json_value(recommendation.step),
                "confidence": recommendation.confidence,
                "confidence_label": recommendation.confidence_label.value,
                "confidence_type": "RECOMMENDER_CONFIDENCE",
                "action": recommendation.action.value,
                "operating_horizon": metadata.get("selected_operating_horizon"),
                "reason_codes": list(recommendation.reason_codes),
                "reasons": list(recommendation.reasons),
                "v2_model_version": metadata.get("probability_model_version"),
                "v2_manifest_hash": metadata.get("manifest_hash"),
                "v2_ood": any_v2_ood or str(metadata.get("ood_status") or "").upper() in {"FLAGGED", "OOD", "OUT_OF_DISTRIBUTION"},
                "v2_abstained": any_v2_abstained or bool(metadata.get("abstained")),
                "stale": any_v2_stale or "V2_STALE" in recommendation.reason_codes,
                "inputs_summary": {
                    "path_inside_70": self._selected_probability(inputs, recommendation, "path_inside_70"),
                    "range_expansion": self._selected_probability(inputs, recommendation, "realized_over_range_width_ge_1"),
                    "upside_probability": self._selected_probability(inputs, recommendation, "upside_breakout"),
                    "downside_probability": self._selected_probability(inputs, recommendation, "downside_breakdown"),
                    "range_70_lower": _json_value(_decimal(metadata.get("reference_range_lower"))),
                    "range_70_upper": _json_value(_decimal(metadata.get("reference_range_upper"))),
                },
            },
            "current_grid": current_summary,
            "sources": {
                "probability_source": "probability_v2_shadow_predictions",
                "range_source": "probability_v2_shadow_predictions.metadata_json.range_70_lower/range_70_upper",
                "current_spot_source": "backend.services.delta_client.eth_market_snapshot(include_orderbook=False)",
                "current_grid_source": "grid_active_run_locks + grid_runs + grid_config_versions",
                "source_prediction_id": self._selected_source_prediction_id(inputs, recommendation),
                "source_v2_row_ids": self._selected_v2_row_ids(inputs, recommendation),
                "source_feature_snapshot_ids": self._selected_feature_snapshot_ids(inputs, recommendation),
            },
        }

    def _snapshot_repository(self):
        if self.snapshot_repository is not None:
            return self.snapshot_repository
        return GridRecommendationSnapshotRepository()

    def _persist_snapshot(self, payload: dict, *, requested_at: datetime) -> dict:
        try:
            repository = self._snapshot_repository()
            if not getattr(repository, "enabled", True):
                return {"saved": False, "recommendation_id": None, "error": "Recommendation snapshot storage is not configured."}
            snapshot = repository.build_snapshot(payload, requested_at=requested_at)
            recommendation_id = repository.insert(snapshot)
            return {"saved": True, "recommendation_id": recommendation_id}
        except Exception:
            return {"saved": False, "recommendation_id": None, "error": "Recommendation snapshot could not be saved."}

    def _selected_prediction(self, inputs: GridProbabilityInputs, recommendation: GridParameterRecommendation) -> HorizonProbability | None:
        horizon = (recommendation.metadata or {}).get("selected_operating_horizon")
        if not horizon:
            return None
        return inputs.predictions.get(str(horizon))

    def _selected_probability(self, inputs: GridProbabilityInputs, recommendation: GridParameterRecommendation, key: str) -> float | None:
        prediction = self._selected_prediction(inputs, recommendation)
        return getattr(prediction, key, None) if prediction else None

    def _selected_v2_row_ids(self, inputs: GridProbabilityInputs, recommendation: GridParameterRecommendation) -> dict:
        prediction = self._selected_prediction(inputs, recommendation)
        rows = ((prediction.metadata or {}).get("v2_rows") if prediction else None) or {}
        return {key: value for key, value in rows.items() if value}

    def _selected_feature_snapshot_ids(self, inputs: GridProbabilityInputs, recommendation: GridParameterRecommendation) -> dict:
        prediction = self._selected_prediction(inputs, recommendation)
        rows = ((prediction.metadata or {}).get("feature_snapshots") if prediction else None) or {}
        return {key: value for key, value in rows.items() if value}

    def _selected_source_prediction_id(self, inputs: GridProbabilityInputs, recommendation: GridParameterRecommendation) -> str | None:
        rows = self._selected_v2_row_ids(inputs, recommendation)
        for target in ("path_inside_70", "realized_over_range_width_ge_1"):
            if rows.get(target):
                return rows[target]
        return next(iter(rows.values()), None)
