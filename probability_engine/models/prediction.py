from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class ProbabilityPrediction:
    snapshot_id: str | None = None
    symbol: str = "ETHUSD"
    horizon: str = "1H"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    record_type: str = "LIVE"
    model_version: str = "probability_v1"
    feature_version: str = "features_v1"
    regime_version: str = "regime_v1"
    range_model_version: str = "range_v1"
    prediction_status: str = "LIVE"
    mean_reversion_probability: float | None = None
    upside_breakout_probability: float | None = None
    downside_breakdown_probability: float | None = None
    range_continuation_probability: float | None = None
    trend_continuation_probability: float | None = None
    confidence: float | None = None
    expected_price: float | None = None
    median_price: float | None = None
    expected_equilibrium: float | None = None
    range_50_lower: float | None = None
    range_50_upper: float | None = None
    range_70_lower: float | None = None
    range_70_upper: float | None = None
    range_90_lower: float | None = None
    range_90_upper: float | None = None
    analogue_sample_size: int = 0
    metadata_json: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        record = self.__dict__.copy()
        record["created_at"] = self.created_at.isoformat()
        return record

