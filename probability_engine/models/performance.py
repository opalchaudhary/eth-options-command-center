from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class ProbabilityPerformance:
    calculated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    event_name: str = "MEAN_REVERSION"
    horizon: str = "1H"
    model_version: str = "probability_v1"
    regime: str = "UNKNOWN"
    sample_count: int = 0
    brier_score: float | None = None
    log_loss: float | None = None
    calibration_error: float | None = None
    range_50_coverage: float | None = None
    range_70_coverage: float | None = None
    range_90_coverage: float | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        record = self.__dict__.copy()
        record["calculated_at"] = self.calculated_at.isoformat()
        return record

