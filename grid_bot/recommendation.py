from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Any

from .models import GridConfig, GridType, SpacingType


RECOMMENDER_VERSION = "grid_parameter_recommender_v0_1"
PREFERRED_HORIZONS = ("12H", "8H", "4H", "24H")

# V0.1 heuristic policy constants. These are not learned Grid Intelligence;
# keep them centralized so later empirical tuning can replace them cleanly.
MAX_PREDICTION_AGE_SECONDS = 20 * 60
MIN_RANGE_WIDTH_PCT = Decimal("0.004")
MAX_RANGE_WIDTH_PCT = Decimal("0.18")
RANGE_EXPANSION_WIDEN_FACTOR = Decimal("0.25")
DIRECTIONAL_ASYMMETRY_SHIFT_FACTOR = Decimal("0.15")
GEOMETRIC_RELATIVE_WIDTH_THRESHOLD = Decimal("0.12")
TARGET_STEP_PCT = Decimal("0.006")
EXPANSION_TARGET_STEP_PCT = Decimal("0.009")
MIN_GRID_COUNT = 5
MAX_GRID_COUNT = 31

HIGH_CONTAINMENT_THRESHOLD = 0.62
LOW_CONTAINMENT_THRESHOLD = 0.38
HIGH_EXPANSION_THRESHOLD = 0.72
EXTREME_EXPANSION_THRESHOLD = 0.86
DIRECTIONAL_EDGE_THRESHOLD = 0.18
DIRECTIONAL_MIN_PROBABILITY = 0.58

RANGE_DRIFT_TOLERANCE = Decimal("0.12")
SPACING_DRIFT_TOLERANCE = Decimal("0.18")
SPOT_EDGE_BUFFER = Decimal("0.02")


class RecommendationAction(str, Enum):
    KEEP_CURRENT = "KEEP_CURRENT"
    CONSIDER_EDIT = "CONSIDER_EDIT"
    REGRID = "REGRID"
    NO_GRID = "NO_GRID"


class ConfidenceLabel(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass(frozen=True)
class HorizonProbability:
    horizon: str
    path_inside_70: float | None = None
    realized_over_range_width_ge_1: float | None = None
    range_70_lower: Decimal | None = None
    range_70_upper: Decimal | None = None
    upside_breakout: float | None = None
    downside_breakdown: float | None = None
    upper_breach_only: float | None = None
    lower_breach_only: float | None = None
    prediction_timestamp: datetime | None = None
    model_version: str | None = None
    manifest_hash: str | None = None
    abstained: bool = False
    abstention_reason: str | None = None
    ood_status: str | None = None
    ood_reason: str | None = None
    stale: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GridProbabilityInputs:
    spot_price: Decimal
    predictions: dict[str, HorizonProbability]
    as_of: datetime
    tick_size: Decimal = Decimal("0.05")


@dataclass(frozen=True)
class CurrentGridSnapshot:
    grid_type: GridType
    lower_price: Decimal
    upper_price: Decimal
    grid_count: int
    spacing_type: SpacingType
    step: Decimal | None = None
    spot_price: Decimal | None = None

    @classmethod
    def from_grid_config(cls, config: GridConfig, spot_price: Decimal | None = None) -> "CurrentGridSnapshot":
        return cls(
            grid_type=config.grid_type,
            lower_price=config.lower_price,
            upper_price=config.upper_price,
            grid_count=config.grid_count,
            spacing_type=config.spacing_type,
            step=compute_step(config.lower_price, config.upper_price, config.grid_count, config.spacing_type),
            spot_price=spot_price,
        )


@dataclass(frozen=True)
class GridParameterRecommendation:
    recommender_version: str
    grid_type: GridType | None
    lower_price: Decimal | None
    upper_price: Decimal | None
    grid_count: int | None
    spacing_type: SpacingType | None
    step: Decimal | None
    confidence: float
    confidence_label: ConfidenceLabel
    action: RecommendationAction
    reason_codes: tuple[str, ...]
    reasons: tuple[str, ...]
    metadata: dict[str, Any]


def recommend_grid_parameters(
    inputs: GridProbabilityInputs,
    current_grid: CurrentGridSnapshot | GridConfig | None = None,
) -> GridParameterRecommendation:
    current = normalize_current_grid(current_grid, inputs.spot_price)
    selected, invalid_reasons = select_operating_horizon(inputs)
    if selected is None:
        return no_grid(inputs, invalid_reasons or ["MISSING_CRITICAL_V2_SIGNAL"])

    reasons: list[str] = []
    codes: list[str] = []
    containment = selected.path_inside_70
    expansion = selected.realized_over_range_width_ge_1
    if containment is None or expansion is None:
        return no_grid(inputs, ["MISSING_CRITICAL_V2_SIGNAL"])
    if expansion >= EXTREME_EXPANSION_THRESHOLD and containment <= LOW_CONTAINMENT_THRESHOLD:
        return no_grid(inputs, ["RANGE_EXPANSION_RISK", "LOW_PATH_CONTAINMENT"], selected)

    lower, upper = derive_range(inputs.spot_price, selected, inputs.tick_size, codes, reasons)
    if lower is None or upper is None or lower >= upper:
        return no_grid(inputs, ["INVALID_REFERENCE_RANGE"], selected)

    grid_type = choose_grid_type(selected, codes, reasons)
    spacing_type = choose_spacing_type(lower, upper)
    grid_count = choose_grid_count(lower, upper, inputs.spot_price, expansion)
    step = compute_step(lower, upper, grid_count, spacing_type, inputs.tick_size)
    confidence = confidence_score(containment, expansion, selected, lower, upper, inputs.spot_price)
    confidence_label = label_confidence(confidence)
    action = classify_action(current, grid_type, lower, upper, grid_count, spacing_type, step, inputs.spot_price, codes, reasons)

    if containment >= HIGH_CONTAINMENT_THRESHOLD:
        codes.append("HIGH_PATH_CONTAINMENT")
        reasons.append("V2 path-containment probability supports a grid within the supplied reference range.")
    if expansion >= HIGH_EXPANSION_THRESHOLD:
        codes.append("RANGE_EXPANSION_RISK")
        reasons.append("Range-expansion probability is elevated, so the grid is widened and made sparser.")
    if grid_type == GridType.NEUTRAL and "DIRECTIONAL_SIGNAL_WEAK" not in codes:
        codes.append("DIRECTIONAL_SIGNAL_WEAK")
        reasons.append("Directional probabilities are absent or not asymmetric enough to justify directional bias.")

    return GridParameterRecommendation(
        recommender_version=RECOMMENDER_VERSION,
        grid_type=grid_type,
        lower_price=lower,
        upper_price=upper,
        grid_count=grid_count,
        spacing_type=spacing_type,
        step=step,
        confidence=confidence,
        confidence_label=confidence_label,
        action=action,
        reason_codes=tuple(dict.fromkeys(codes)),
        reasons=tuple(dict.fromkeys(reasons)),
        metadata=metadata(inputs, selected),
    )


def normalize_current_grid(current_grid, spot_price: Decimal) -> CurrentGridSnapshot | None:
    if current_grid is None:
        return None
    if isinstance(current_grid, CurrentGridSnapshot):
        return current_grid
    if isinstance(current_grid, GridConfig):
        return CurrentGridSnapshot.from_grid_config(current_grid, spot_price)
    raise TypeError("current_grid must be CurrentGridSnapshot, GridConfig, or None")


def select_operating_horizon(inputs: GridProbabilityInputs) -> tuple[HorizonProbability | None, list[str]]:
    invalid = []
    for horizon in PREFERRED_HORIZONS:
        pred = inputs.predictions.get(horizon)
        if pred is None:
            invalid.append(f"MISSING_{horizon}")
            continue
        reason = unsafe_prediction_reason(pred, inputs.as_of)
        if reason:
            invalid.append(reason)
            continue
        if pred.path_inside_70 is None or pred.realized_over_range_width_ge_1 is None:
            invalid.append("MISSING_CRITICAL_V2_SIGNAL")
            continue
        if pred.range_70_lower is None or pred.range_70_upper is None:
            invalid.append("MISSING_REFERENCE_RANGE")
            continue
        return pred, []
    return None, invalid


def unsafe_prediction_reason(prediction: HorizonProbability, as_of: datetime) -> str | None:
    if prediction.abstained:
        return "V2_ABSTAINED"
    if str(prediction.ood_status or "").upper() in {"FLAGGED", "OOD", "OUT_OF_DISTRIBUTION"}:
        return "V2_OOD"
    if prediction.stale:
        return "V2_STALE"
    if prediction.prediction_timestamp is None:
        return "MISSING_PREDICTION_TIMESTAMP"
    age = (as_utc(as_of) - as_utc(prediction.prediction_timestamp)).total_seconds()
    if age < -1:
        return "PREDICTION_FROM_FUTURE"
    if age > MAX_PREDICTION_AGE_SECONDS:
        return "V2_STALE"
    return None


def derive_range(spot: Decimal, pred: HorizonProbability, tick_size: Decimal, codes: list[str], reasons: list[str]) -> tuple[Decimal | None, Decimal | None]:
    lower = pred.range_70_lower
    upper = pred.range_70_upper
    if lower is None or upper is None or lower <= 0 or upper <= 0 or lower >= upper:
        return None, None
    if not (lower < spot < upper):
        codes.append("SPOT_OUTSIDE_REFERENCE_RANGE")
        reasons.append("Current spot is outside the supplied reference range, so the range is centered conservatively around spot.")
        width = upper - lower
        lower = spot - width / Decimal("2")
        upper = spot + width / Decimal("2")
    width = upper - lower
    width_pct = width / spot
    if width_pct < MIN_RANGE_WIDTH_PCT:
        expand = (spot * MIN_RANGE_WIDTH_PCT - width) / Decimal("2")
        lower -= expand
        upper += expand
        codes.append("REFERENCE_RANGE_TOO_NARROW")
    if width_pct > MAX_RANGE_WIDTH_PCT:
        codes.append("REFERENCE_RANGE_TOO_WIDE")
        reasons.append("Supplied reference range is unusually wide; no arbitrary clipping was applied.")
    expansion = Decimal(str(pred.realized_over_range_width_ge_1 or 0))
    if expansion >= Decimal(str(HIGH_EXPANSION_THRESHOLD)):
        widen = (upper - lower) * RANGE_EXPANSION_WIDEN_FACTOR
        lower -= widen / Decimal("2")
        upper += widen / Decimal("2")
    lower, upper = apply_directional_asymmetry(lower, upper, pred, codes, reasons)
    return quantize(lower, tick_size), quantize(upper, tick_size)


def apply_directional_asymmetry(lower: Decimal, upper: Decimal, pred: HorizonProbability, codes: list[str], reasons: list[str]) -> tuple[Decimal, Decimal]:
    up = pred.upside_breakout
    down = pred.downside_breakdown
    if up is None or down is None:
        return lower, upper
    edge = up - down
    if abs(edge) < DIRECTIONAL_EDGE_THRESHOLD:
        return lower, upper
    shift = (upper - lower) * DIRECTIONAL_ASYMMETRY_SHIFT_FACTOR
    if edge > 0 and up >= DIRECTIONAL_MIN_PROBABILITY:
        codes.append("DIRECTIONAL_UPSIDE_BIAS")
        reasons.append("Upside breakout probability is materially stronger than downside risk.")
        return lower + shift * Decimal("0.35"), upper + shift
    if edge < 0 and down >= DIRECTIONAL_MIN_PROBABILITY:
        codes.append("DIRECTIONAL_DOWNSIDE_BIAS")
        reasons.append("Downside breakdown probability is materially stronger than upside risk.")
        return lower - shift, upper - shift * Decimal("0.35")
    return lower, upper


def choose_grid_type(pred: HorizonProbability, codes: list[str], reasons: list[str]) -> GridType:
    up = pred.upside_breakout
    down = pred.downside_breakdown
    if up is None or down is None:
        return GridType.NEUTRAL
    if up - down >= DIRECTIONAL_EDGE_THRESHOLD and up >= DIRECTIONAL_MIN_PROBABILITY:
        codes.append("DIRECTIONAL_UPSIDE_BIAS")
        reasons.append("Directional V2 probabilities support a long-bias grid.")
        return GridType.LONG_BIAS
    if down - up >= DIRECTIONAL_EDGE_THRESHOLD and down >= DIRECTIONAL_MIN_PROBABILITY:
        codes.append("DIRECTIONAL_DOWNSIDE_BIAS")
        reasons.append("Directional V2 probabilities support a short-bias grid.")
        return GridType.SHORT_BIAS
    return GridType.NEUTRAL


def choose_spacing_type(lower: Decimal, upper: Decimal) -> SpacingType:
    midpoint = (lower + upper) / Decimal("2")
    relative_width = (upper - lower) / midpoint
    if relative_width >= GEOMETRIC_RELATIVE_WIDTH_THRESHOLD:
        return SpacingType.GEOMETRIC
    return SpacingType.ARITHMETIC


def choose_grid_count(lower: Decimal, upper: Decimal, spot: Decimal, expansion_probability: float | None) -> int:
    width = upper - lower
    target_pct = EXPANSION_TARGET_STEP_PCT if (expansion_probability or 0) >= HIGH_EXPANSION_THRESHOLD else TARGET_STEP_PCT
    target_step = max(spot * target_pct, Decimal("1"))
    count = int((width / target_step).to_integral_value(rounding=ROUND_HALF_UP)) + 1
    if count % 2 == 0:
        count += 1
    return max(MIN_GRID_COUNT, min(MAX_GRID_COUNT, count))


def compute_step(lower: Decimal, upper: Decimal, grid_count: int, spacing_type: SpacingType, tick_size: Decimal = Decimal("0.05")) -> Decimal:
    if spacing_type == SpacingType.GEOMETRIC:
        ratio = (upper / lower) ** (Decimal(1) / Decimal(grid_count - 1))
        return quantize(lower * (ratio - Decimal("1")), tick_size)
    return arithmetic_step(lower, upper, grid_count, tick_size)


def arithmetic_step(lower: Decimal, upper: Decimal, grid_count: int, tick_size: Decimal = Decimal("0.05")) -> Decimal:
    if grid_count <= 1:
        return Decimal("0")
    return quantize((upper - lower) / Decimal(grid_count - 1), tick_size)


def classify_action(
    current: CurrentGridSnapshot | None,
    grid_type: GridType,
    lower: Decimal,
    upper: Decimal,
    grid_count: int,
    spacing_type: SpacingType,
    step: Decimal,
    spot: Decimal,
    codes: list[str],
    reasons: list[str],
) -> RecommendationAction:
    if current is None:
        codes.append("NO_CURRENT_GRID")
        reasons.append("No current grid was supplied; returning a complete recommendation for operator review.")
        return RecommendationAction.CONSIDER_EDIT
    current_spot = current.spot_price or spot
    if not (current.lower_price < current_spot < current.upper_price):
        codes.append("SPOT_OUTSIDE_CURRENT_RANGE")
        reasons.append("Current spot is outside the active grid range.")
        return RecommendationAction.REGRID
    current_width = current.upper_price - current.lower_price
    if current_width <= 0:
        codes.append("INVALID_CURRENT_RANGE")
        reasons.append("Current grid range is invalid.")
        return RecommendationAction.REGRID
    edge_distance = min(current_spot - current.lower_price, current.upper_price - current_spot)
    if edge_distance / current_width <= SPOT_EDGE_BUFFER:
        codes.append("SPOT_NEAR_CURRENT_RANGE_EDGE")
        reasons.append("Current spot is too close to an active grid boundary.")
        return RecommendationAction.REGRID
    if current.grid_type != grid_type:
        codes.append("GRID_TYPE_MISMATCH")
        reasons.append("Recommended grid type differs materially from the current grid.")
        return RecommendationAction.REGRID

    rec_width = upper - lower
    lower_drift = abs(current.lower_price - lower) / rec_width
    upper_drift = abs(current.upper_price - upper) / rec_width
    level_diff = abs(current.grid_count - grid_count)
    current_step = current.step or arithmetic_step(current.lower_price, current.upper_price, current.grid_count)
    spacing_drift = abs(current_step - step) / step if step else Decimal("1")

    if lower_drift <= RANGE_DRIFT_TOLERANCE and upper_drift <= RANGE_DRIFT_TOLERANCE and level_diff <= 1 and current.spacing_type == spacing_type and spacing_drift <= SPACING_DRIFT_TOLERANCE:
        codes.append("CURRENT_GRID_WITHIN_TOLERANCE")
        reasons.append("Current grid parameters are within V0.1 hysteresis tolerances.")
        return RecommendationAction.KEEP_CURRENT
    if lower_drift > Decimal("0.28") or upper_drift > Decimal("0.28") or level_diff >= 5 or current.spacing_type != spacing_type:
        codes.append("CURRENT_RANGE_MATERIAL_DRIFT")
        reasons.append("Current grid differs materially from the recommendation.")
        return RecommendationAction.REGRID
    codes.append("CURRENT_GRID_MILD_DRIFT")
    reasons.append("Current grid has meaningful but non-critical drift from the recommendation.")
    return RecommendationAction.CONSIDER_EDIT


def confidence_score(containment: float, expansion: float, pred: HorizonProbability, lower: Decimal, upper: Decimal, spot: Decimal) -> float:
    score = 0.35
    score += max(0.0, min(0.3, (containment - 0.45) * 0.8))
    score += max(0.0, min(0.2, (0.78 - expansion) * 0.5))
    if lower < spot < upper:
        score += 0.1
    if pred.upside_breakout is not None and pred.downside_breakdown is not None:
        score += 0.05
    return round(max(0.0, min(1.0, score)), 4)


def label_confidence(confidence: float) -> ConfidenceLabel:
    if confidence >= 0.7:
        return ConfidenceLabel.HIGH
    if confidence >= 0.45:
        return ConfidenceLabel.MEDIUM
    return ConfidenceLabel.LOW


def no_grid(inputs: GridProbabilityInputs, reason_codes: list[str], selected: HorizonProbability | None = None) -> GridParameterRecommendation:
    reasons = [human_reason(code) for code in reason_codes]
    return GridParameterRecommendation(
        recommender_version=RECOMMENDER_VERSION,
        grid_type=None,
        lower_price=None,
        upper_price=None,
        grid_count=None,
        spacing_type=None,
        step=None,
        confidence=0.0,
        confidence_label=ConfidenceLabel.LOW,
        action=RecommendationAction.NO_GRID,
        reason_codes=tuple(dict.fromkeys(reason_codes)),
        reasons=tuple(dict.fromkeys(reasons)),
        metadata=metadata(inputs, selected),
    )


def metadata(inputs: GridProbabilityInputs, selected: HorizonProbability | None) -> dict[str, Any]:
    return {
        "recommender_version": RECOMMENDER_VERSION,
        "probability_model_version": selected.model_version if selected else None,
        "manifest_hash": selected.manifest_hash if selected else None,
        "source_prediction_timestamp": as_utc(selected.prediction_timestamp).isoformat() if selected and selected.prediction_timestamp else None,
        "selected_operating_horizon": selected.horizon if selected else None,
        "horizon_fallback_order": list(PREFERRED_HORIZONS),
        "as_of": as_utc(inputs.as_of).isoformat(),
        "abstained": selected.abstained if selected else None,
        "abstention_reason": selected.abstention_reason if selected else None,
        "ood_status": selected.ood_status if selected else None,
        "ood_reason": selected.ood_reason if selected else None,
        "reference_range_lower": str(selected.range_70_lower) if selected and selected.range_70_lower is not None else None,
        "reference_range_upper": str(selected.range_70_upper) if selected and selected.range_70_upper is not None else None,
        "confidence_semantics": "Recommender confidence only; not Probability V2 model confidence.",
    }


def human_reason(code: str) -> str:
    mapping = {
        "MISSING_CRITICAL_V2_SIGNAL": "Required frozen V2 grid probabilities are missing.",
        "MISSING_REFERENCE_RANGE": "Required supplied probability range boundaries are missing.",
        "V2_ABSTAINED": "Frozen V2 abstained, so no grid parameters should be manufactured.",
        "V2_OOD": "Frozen V2 marked the prediction out-of-distribution.",
        "V2_STALE": "Frozen V2 prediction is stale for the recommender freshness policy.",
        "RANGE_EXPANSION_RISK": "Range expansion risk is too high for a safe grid recommendation.",
        "LOW_PATH_CONTAINMENT": "Path-containment probability is too low for a safe grid recommendation.",
        "INVALID_REFERENCE_RANGE": "Supplied reference range is invalid.",
    }
    return mapping.get(code, code.replace("_", " ").lower())


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def quantize(value: Decimal, tick_size: Decimal = Decimal("0.05")) -> Decimal:
    ticks = (value / tick_size).to_integral_value(rounding=ROUND_HALF_UP)
    return ticks * tick_size
