from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from statistics import median
from typing import Any

from .supabase_repository import SupabaseGridRepository, SupabasePersistenceError


RECOMMENDATION_TABLE = "grid_parameter_recommendations"
OUTCOME_TABLE = "grid_parameter_recommendation_outcomes"
CHALLENGER_TABLE = "grid_parameter_recommendation_challengers"
CHALLENGER_OUTCOME_TABLE = "grid_parameter_recommendation_challenger_outcomes"
OHLCV_TABLE = "eth_ohlcv"
HORIZON_MINUTES = {
    "1H": 60,
    "4H": 240,
    "8H": 480,
    "12H": 720,
    "24H": 1440,
}
RESOLUTION_MINUTES = {
    "1m": 1,
    "5m": 5,
}


@dataclass(frozen=True)
class EvaluationResult:
    rows: list[dict[str, Any]]
    challenger_rows: list[dict[str, Any]]
    persisted: int
    challenger_persisted: int
    skipped_existing: int
    challenger_skipped_existing: int
    skipped_unmatured: dict[str, int]
    mature_counts: dict[str, int]
    ohlcv_source: str
    ohlcv_resolution: str | None
    recommendations_found: int


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


def _float(value: Any) -> float | None:
    decimal_value = _decimal(value)
    return float(decimal_value) if decimal_value is not None else None


def _records(rows: Any) -> list[dict[str, Any]]:
    if rows is None:
        return []
    if isinstance(rows, dict):
        return [rows]
    return list(rows or [])


class GridRecommendationOutcomeEvaluator:
    def __init__(
        self,
        *,
        db: Any | None = None,
        now_fn: Any | None = None,
        resolution_preference: tuple[str, ...] = ("1m", "5m"),
    ):
        self.db = db if db is not None else SupabaseGridRepository()
        self.now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self.resolution_preference = resolution_preference

    def evaluate_pending(self, *, symbol: str = "ETHUSD", limit: int = 500, persist: bool = True) -> EvaluationResult:
        recommendations = self._recommendations(symbol=symbol, limit=limit)
        recommendations_by_id = {str(row.get("recommendation_id")): row for row in recommendations if row.get("recommendation_id")}
        challengers = self._challengers(symbol=symbol, limit=limit) if recommendations_by_id else []
        existing = self._existing_keys() if persist else set()
        existing_challengers = self._existing_challenger_keys() if persist else set()
        latest_resolution, latest_time = self._latest_ohlcv_time(symbol)
        rows: list[dict[str, Any]] = []
        challenger_rows: list[dict[str, Any]] = []
        persisted = 0
        challenger_persisted = 0
        skipped_existing = 0
        challenger_skipped_existing = 0
        skipped_unmatured = {horizon: 0 for horizon in HORIZON_MINUTES}
        mature_counts = {horizon: 0 for horizon in HORIZON_MINUTES}

        for recommendation in recommendations:
            start = self._evaluation_start(recommendation)
            lower = _decimal(recommendation.get("recommended_lower_price"))
            upper = _decimal(recommendation.get("recommended_upper_price"))
            if not start or lower is None or upper is None or upper <= lower:
                continue
            recommendation_id = str(recommendation.get("recommendation_id") or "")
            if not recommendation_id:
                continue
            for horizon, minutes in HORIZON_MINUTES.items():
                end = start + timedelta(minutes=minutes)
                if latest_time is None or latest_time < end:
                    skipped_unmatured[horizon] += 1
                    continue
                mature_counts[horizon] += 1
                key = (recommendation_id, horizon)
                if key in existing:
                    skipped_existing += 1
                    continue
                candles, resolution = self._forward_candles(symbol, start, end)
                row = self._outcome_row(recommendation, horizon, start, end, candles, resolution)
                rows.append(row)
                if persist:
                    if self.db.insert_once(OUTCOME_TABLE, row, on_conflict="recommendation_id,horizon"):
                        persisted += 1

        for challenger in challengers:
            recommendation_id = str(challenger.get("recommendation_id") or "")
            recommendation = recommendations_by_id.get(recommendation_id)
            horizon = str(challenger.get("horizon") or "").upper()
            if not recommendation or horizon not in HORIZON_MINUTES:
                continue
            start = self._evaluation_start(recommendation)
            if not start:
                continue
            end = start + timedelta(minutes=HORIZON_MINUTES[horizon])
            if latest_time is None or latest_time < end:
                skipped_unmatured[horizon] += 1
                continue
            mature_counts[horizon] += 1
            challenger_id = str(challenger.get("challenger_id") or "")
            key = (challenger_id, horizon)
            if not challenger_id or key in existing_challengers:
                challenger_skipped_existing += 1
                continue
            candles, resolution = self._forward_candles(symbol, start, end)
            row = self._challenger_outcome_row(challenger, recommendation, horizon, start, end, candles, resolution)
            challenger_rows.append(row)
            if persist:
                if self.db.insert_once(CHALLENGER_OUTCOME_TABLE, row, on_conflict="challenger_id,horizon"):
                    challenger_persisted += 1

        return EvaluationResult(
            rows=rows,
            challenger_rows=challenger_rows,
            persisted=persisted,
            challenger_persisted=challenger_persisted,
            skipped_existing=skipped_existing,
            challenger_skipped_existing=challenger_skipped_existing,
            skipped_unmatured=skipped_unmatured,
            mature_counts=mature_counts,
            ohlcv_source=OHLCV_TABLE,
            ohlcv_resolution=latest_resolution,
            recommendations_found=len(recommendations),
        )

    def summary(self, *, limit: int = 5000) -> dict[str, Any]:
        rows = _records(
            self.db.select(
                OUTCOME_TABLE,
                {
                    "select": "horizon,recommender_version,stayed_inside_recommended_range,upper_breached,lower_breached,minutes_to_first_breach,actual_recommended_width_ratio",
                    "order": "evaluation_end.desc",
                    "limit": max(1, min(int(limit), 10000)),
                },
            )
        )
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault((str(row.get("horizon") or ""), str(row.get("recommender_version") or "")), []).append(row)

        aggregates = []
        for (horizon, version), group in sorted(grouped.items()):
            breach_rows = [row for row in group if row.get("upper_breached") or row.get("lower_breached")]
            breach_times = [_float(row.get("minutes_to_first_breach")) for row in breach_rows]
            ratios = [_float(row.get("actual_recommended_width_ratio")) for row in group]
            breach_times = [value for value in breach_times if value is not None]
            ratios = [value for value in ratios if value is not None]
            total = len(group)
            aggregates.append(
                {
                    "horizon": horizon,
                    "recommender_version": version,
                    "total_evaluated": total,
                    "containment_rate": sum(1 for row in group if row.get("stayed_inside_recommended_range") is True) / total if total else None,
                    "breach_rate": len(breach_rows) / total if total else None,
                    "median_time_to_breach_minutes": median(breach_times) if breach_times else None,
                    "median_actual_recommended_width_ratio": median(ratios) if ratios else None,
                }
            )
        try:
            challenger_rows = _records(
                self.db.select(
                    CHALLENGER_OUTCOME_TABLE,
                    {
                        "select": "horizon,challenger_policy,no_grid,stayed_inside_shadow_range,upper_breached,lower_breached,minutes_to_first_breach,actual_recommended_width_ratio,width_vs_champion",
                        "order": "evaluation_end.desc",
                        "limit": max(1, min(int(limit), 10000)),
                    },
                )
            )
        except SupabasePersistenceError as exc:
            missing_table = CHALLENGER_OUTCOME_TABLE in str(exc) and any(token in str(exc) for token in ["42P01", "PGRST205", "PGRST204"])
            if not missing_table:
                raise
            challenger_rows = []
        policy_rows = [
            {
                "policy": "champion_v0_1",
                "horizon": row["horizon"],
                "total_evaluated": row["total_evaluated"],
                "participation_rate": 1.0,
                "no_grid_rate": 0.0,
                "containment_rate": row["containment_rate"],
                "breach_rate": row["breach_rate"],
                "median_time_to_breach_minutes": row["median_time_to_breach_minutes"],
                "median_actual_recommended_width_ratio": row["median_actual_recommended_width_ratio"],
                "median_width_vs_champion": 1.0,
            }
            for row in aggregates
        ]
        policy_rows.extend(self._challenger_summary_rows(challenger_rows))
        return {"ok": True, "rows": aggregates, "policy_rows": policy_rows}

    def _recommendations(self, *, symbol: str, limit: int) -> list[dict[str, Any]]:
        return _records(
            self.db.select(
                RECOMMENDATION_TABLE,
                {
                    "select": "*",
                    "symbol": f"eq.{symbol}",
                    "recommended_lower_price": "not.is.null",
                    "recommended_upper_price": "not.is.null",
                    "order": "requested_at.asc",
                    "limit": max(1, min(int(limit), 5000)),
                },
            )
        )

    def _challengers(self, *, symbol: str, limit: int) -> list[dict[str, Any]]:
        try:
            return _records(
                self.db.select(
                    CHALLENGER_TABLE,
                    {
                        "select": "*",
                        "symbol": f"eq.{symbol}",
                        "order": "generated_at.asc",
                        "limit": max(1, min(int(limit) * 4, 20000)),
                    },
                )
            )
        except SupabasePersistenceError as exc:
            missing_table = CHALLENGER_TABLE in str(exc) and any(token in str(exc) for token in ["42P01", "PGRST205", "PGRST204"])
            if missing_table:
                return []
            raise

    def _existing_keys(self) -> set[tuple[str, str]]:
        try:
            rows = _records(self.db.select(OUTCOME_TABLE, {"select": "recommendation_id,horizon", "limit": 10000}))
        except SupabasePersistenceError as exc:
            missing_table = OUTCOME_TABLE in str(exc) and any(token in str(exc) for token in ["42P01", "PGRST205", "PGRST204"])
            if missing_table:
                return set()
            raise
        return {(str(row.get("recommendation_id")), str(row.get("horizon"))) for row in rows}

    def _existing_challenger_keys(self) -> set[tuple[str, str]]:
        try:
            rows = _records(self.db.select(CHALLENGER_OUTCOME_TABLE, {"select": "challenger_id,horizon", "limit": 20000}))
        except SupabasePersistenceError as exc:
            missing_table = CHALLENGER_OUTCOME_TABLE in str(exc) and any(token in str(exc) for token in ["42P01", "PGRST205", "PGRST204"])
            if missing_table:
                return set()
            raise
        return {(str(row.get("challenger_id")), str(row.get("horizon"))) for row in rows}

    def _latest_ohlcv_time(self, symbol: str) -> tuple[str | None, datetime | None]:
        for resolution in self.resolution_preference:
            rows = _records(
                self.db.select(
                    OHLCV_TABLE,
                    {
                        "select": "candle_time,resolution",
                        "symbol": f"eq.{symbol}",
                        "resolution": f"eq.{resolution}",
                        "order": "candle_time.desc",
                        "limit": 1,
                    },
                )
            )
            if rows:
                return resolution, _parse_ts(rows[0].get("candle_time"))
        return None, None

    def _forward_candles(self, symbol: str, start: datetime, end: datetime) -> tuple[list[dict[str, Any]], str | None]:
        for resolution in self.resolution_preference:
            rows = _records(
                self.db.select(
                    OHLCV_TABLE,
                    {
                        "select": "candle_time,open,high,low,close,volume,resolution",
                        "symbol": f"eq.{symbol}",
                        "resolution": f"eq.{resolution}",
                        "candle_time": f"gt.{start.isoformat()}",
                        "order": "candle_time.asc",
                        "limit": self._ohlcv_limit(resolution, start, end),
                    },
                )
            )
            rows = [row for row in rows if (ts := _parse_ts(row.get("candle_time"))) and start < ts <= end]
            if rows:
                return rows, resolution
        return [], None

    def _ohlcv_limit(self, resolution: str, start: datetime, end: datetime) -> int:
        minutes = max(1, int((end - start).total_seconds() // 60))
        resolution_minutes = RESOLUTION_MINUTES.get(resolution, 5)
        return min(2000, minutes // resolution_minutes + 5)

    def _evaluation_start(self, recommendation: dict[str, Any]) -> datetime | None:
        return _parse_ts(recommendation.get("requested_at")) or _parse_ts(recommendation.get("created_at"))

    def _outcome_row(
        self,
        recommendation: dict[str, Any],
        horizon: str,
        start: datetime,
        end: datetime,
        candles: list[dict[str, Any]],
        resolution: str | None,
    ) -> dict[str, Any]:
        lower = float(_decimal(recommendation["recommended_lower_price"]) or Decimal("0"))
        upper = float(_decimal(recommendation["recommended_upper_price"]) or Decimal("0"))
        recommended_width = upper - lower
        lows = [_float(row.get("low")) for row in candles]
        highs = [_float(row.get("high")) for row in candles]
        lows = [value for value in lows if value is not None]
        highs = [value for value in highs if value is not None]
        actual_low = min(lows) if lows else None
        actual_high = max(highs) if highs else None
        actual_width = actual_high - actual_low if actual_high is not None and actual_low is not None else None
        upper_breached = bool(actual_high is not None and actual_high > upper)
        lower_breached = bool(actual_low is not None and actual_low < lower)
        first_side, first_time = self._first_breach(candles, lower, upper)
        minutes_to_first = (first_time - start).total_seconds() / 60 if first_time else None
        spot_at_end = _float(candles[-1].get("close")) if candles else None
        expected_candles = self._expected_candles(resolution, horizon)
        actual_candles = len(candles)
        completeness = actual_candles / expected_candles if expected_candles else 0
        clipped_low = max(actual_low, lower) if actual_low is not None else None
        clipped_high = min(actual_high, upper) if actual_high is not None else None
        in_range_covered = max(0.0, clipped_high - clipped_low) if clipped_low is not None and clipped_high is not None else None
        return {
            "recommendation_id": recommendation.get("recommendation_id"),
            "horizon": horizon,
            "evaluated_at": self.now_fn().astimezone(timezone.utc).isoformat(),
            "evaluation_start": start.isoformat(),
            "evaluation_end": end.isoformat(),
            "symbol": recommendation.get("symbol") or "ETHUSD",
            "recommender_version": recommendation.get("recommender_version"),
            "recommended_lower_price": lower,
            "recommended_upper_price": upper,
            "recommended_width": recommended_width,
            "actual_forward_low": actual_low,
            "actual_forward_high": actual_high,
            "actual_path_width": actual_width,
            "stayed_inside_recommended_range": bool(candles and not upper_breached and not lower_breached),
            "upper_breached": upper_breached,
            "lower_breached": lower_breached,
            "both_sides_breached": upper_breached and lower_breached,
            "first_breach_side": first_side,
            "first_breach_time": first_time.isoformat() if first_time else None,
            "minutes_to_first_breach": minutes_to_first,
            "max_excursion_above_upper": max(0.0, (actual_high or upper) - upper),
            "max_excursion_below_lower": max(0.0, lower - (actual_low or lower)),
            "actual_recommended_width_ratio": actual_width / recommended_width if actual_width is not None and recommended_width > 0 else None,
            "range_utilization": in_range_covered / recommended_width if in_range_covered is not None and recommended_width > 0 else None,
            "spot_at_evaluation_end": spot_at_end,
            "ohlcv_source": OHLCV_TABLE,
            "ohlcv_resolution": resolution,
            "expected_candles": expected_candles,
            "actual_candles": actual_candles,
            "data_completeness_pct": 100 * completeness,
            "data_quality_flag": self._quality_flag(completeness, actual_candles),
            "metadata_json": {
                "source_recommendation_created_at": recommendation.get("created_at"),
                "source_recommendation_requested_at": recommendation.get("requested_at"),
                "selected_operating_horizon": recommendation.get("selected_operating_horizon"),
            },
            "immutable": True,
        }

    def _challenger_outcome_row(
        self,
        challenger: dict[str, Any],
        recommendation: dict[str, Any],
        horizon: str,
        start: datetime,
        end: datetime,
        candles: list[dict[str, Any]],
        resolution: str | None,
    ) -> dict[str, Any]:
        lower_decimal = _decimal(challenger.get("shadow_lower_price"))
        upper_decimal = _decimal(challenger.get("shadow_upper_price"))
        lower = float(lower_decimal) if lower_decimal is not None else None
        upper = float(upper_decimal) if upper_decimal is not None else None
        recommended_width = upper - lower if lower is not None and upper is not None else None
        lows = [_float(row.get("low")) for row in candles]
        highs = [_float(row.get("high")) for row in candles]
        lows = [value for value in lows if value is not None]
        highs = [value for value in highs if value is not None]
        actual_low = min(lows) if lows else None
        actual_high = max(highs) if highs else None
        actual_width = actual_high - actual_low if actual_high is not None and actual_low is not None else None
        no_grid = bool(challenger.get("no_grid"))
        upper_breached = bool(not no_grid and actual_high is not None and upper is not None and actual_high > upper)
        lower_breached = bool(not no_grid and actual_low is not None and lower is not None and actual_low < lower)
        first_side, first_time = self._first_breach(candles, lower, upper) if lower is not None and upper is not None else (None, None)
        minutes_to_first = (first_time - start).total_seconds() / 60 if first_time else None
        spot_at_end = _float(candles[-1].get("close")) if candles else None
        expected_candles = self._expected_candles(resolution, horizon)
        actual_candles = len(candles)
        completeness = actual_candles / expected_candles if expected_candles else 0
        clipped_low = max(actual_low, lower) if actual_low is not None and lower is not None else None
        clipped_high = min(actual_high, upper) if actual_high is not None and upper is not None else None
        in_range_covered = max(0.0, clipped_high - clipped_low) if clipped_low is not None and clipped_high is not None else None
        metadata = challenger.get("metadata_json") or {}
        return {
            "challenger_id": challenger.get("challenger_id"),
            "recommendation_id": challenger.get("recommendation_id"),
            "challenger_policy": challenger.get("challenger_policy"),
            "challenger_policy_version": challenger.get("challenger_policy_version"),
            "horizon": horizon,
            "evaluated_at": self.now_fn().astimezone(timezone.utc).isoformat(),
            "evaluation_start": start.isoformat(),
            "evaluation_end": end.isoformat(),
            "symbol": challenger.get("symbol") or recommendation.get("symbol") or "ETHUSD",
            "no_grid": no_grid,
            "shadow_action": challenger.get("shadow_action"),
            "shadow_lower_price": lower,
            "shadow_upper_price": upper,
            "shadow_width": recommended_width,
            "width_vs_champion": _float(metadata.get("width_vs_champion")),
            "actual_forward_low": actual_low,
            "actual_forward_high": actual_high,
            "actual_path_width": actual_width,
            "stayed_inside_shadow_range": bool(candles and not no_grid and not upper_breached and not lower_breached),
            "upper_breached": upper_breached,
            "lower_breached": lower_breached,
            "both_sides_breached": upper_breached and lower_breached,
            "first_breach_side": first_side,
            "first_breach_time": first_time.isoformat() if first_time else None,
            "minutes_to_first_breach": minutes_to_first,
            "max_excursion_above_upper": max(0.0, (actual_high or upper or 0) - upper) if upper is not None else None,
            "max_excursion_below_lower": max(0.0, lower - (actual_low or lower)) if lower is not None else None,
            "actual_recommended_width_ratio": actual_width / recommended_width if actual_width is not None and recommended_width and recommended_width > 0 else None,
            "range_utilization": in_range_covered / recommended_width if in_range_covered is not None and recommended_width and recommended_width > 0 else None,
            "spot_at_evaluation_end": spot_at_end,
            "ohlcv_source": OHLCV_TABLE,
            "ohlcv_resolution": resolution,
            "expected_candles": expected_candles,
            "actual_candles": actual_candles,
            "data_completeness_pct": 100 * completeness,
            "data_quality_flag": self._quality_flag(completeness, actual_candles),
            "metadata_json": {
                "source_recommendation_created_at": recommendation.get("created_at"),
                "source_recommendation_requested_at": recommendation.get("requested_at"),
                "challenger_generated_at": challenger.get("generated_at"),
                "selected_operating_horizon": recommendation.get("selected_operating_horizon"),
                "no_grid_reason": challenger.get("no_grid_reason"),
            },
            "immutable": True,
        }

    def _challenger_summary_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault((str(row.get("horizon") or ""), str(row.get("challenger_policy") or "")), []).append(row)
        aggregates = []
        for (horizon, policy), group in sorted(grouped.items()):
            grid_rows = [row for row in group if not row.get("no_grid")]
            no_grid_rows = [row for row in group if row.get("no_grid")]
            breach_rows = [row for row in grid_rows if row.get("upper_breached") or row.get("lower_breached")]
            breach_times = [_float(row.get("minutes_to_first_breach")) for row in breach_rows]
            ratios = [_float(row.get("actual_recommended_width_ratio")) for row in grid_rows]
            width_vs_champion = [_float(row.get("width_vs_champion")) for row in grid_rows]
            breach_times = [value for value in breach_times if value is not None]
            ratios = [value for value in ratios if value is not None]
            width_vs_champion = [value for value in width_vs_champion if value is not None]
            total = len(group)
            aggregates.append(
                {
                    "policy": policy,
                    "horizon": horizon,
                    "total_evaluated": total,
                    "participation_rate": len(grid_rows) / total if total else None,
                    "no_grid_rate": len(no_grid_rows) / total if total else None,
                    "containment_rate": sum(1 for row in grid_rows if row.get("stayed_inside_shadow_range") is True) / len(grid_rows) if grid_rows else None,
                    "breach_rate": len(breach_rows) / len(grid_rows) if grid_rows else None,
                    "median_time_to_breach_minutes": median(breach_times) if breach_times else None,
                    "median_actual_recommended_width_ratio": median(ratios) if ratios else None,
                    "median_width_vs_champion": median(width_vs_champion) if width_vs_champion else None,
                }
            )
        return aggregates

    def _first_breach(self, candles: list[dict[str, Any]], lower: float, upper: float) -> tuple[str | None, datetime | None]:
        for row in candles:
            ts = _parse_ts(row.get("candle_time"))
            if ts is None:
                continue
            open_price = _float(row.get("open"))
            high = _float(row.get("high"))
            low = _float(row.get("low"))
            close = _float(row.get("close"))
            if None in {open_price, high, low, close}:
                continue
            if open_price > upper:
                return "upper", ts
            if open_price < lower:
                return "lower", ts
            path = [open_price, low, high, close] if close >= open_price else [open_price, high, low, close]
            for start_price, end_price in zip(path, path[1:]):
                if end_price > start_price and end_price > upper and start_price <= upper:
                    return "upper", ts
                if end_price < start_price and end_price < lower and start_price >= lower:
                    return "lower", ts
            if high > upper and low < lower:
                return "both_same_candle", ts
            if high > upper:
                return "upper", ts
            if low < lower:
                return "lower", ts
        return None, None

    def _expected_candles(self, resolution: str | None, horizon: str) -> int:
        minutes = HORIZON_MINUTES[horizon]
        if not resolution:
            return minutes
        return max(1, minutes // RESOLUTION_MINUTES.get(resolution, 1))

    def _quality_flag(self, completeness: float, actual_candles: int) -> str:
        if actual_candles <= 0:
            return "NO_DATA"
        if completeness >= 0.95:
            return "COMPLETE"
        if completeness >= 0.75:
            return "PARTIAL"
        return "SPARSE"
