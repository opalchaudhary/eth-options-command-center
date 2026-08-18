from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class OptionStrikeRecommendation:
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    symbol: str = "ETHUSD"
    expiry: str | None = None
    option_type: str = "put_options"
    strike: float | None = None
    risk_tier: str = "BALANCED"
    recommendation_status: str = "NO_ATTRACTIVE_NAKED_SELL"
    touch_probability: float | None = None
    itm_probability: float | None = None
    premium: float | None = None
    premium_efficiency: float | None = None
    range_buffer_pct: float | None = None
    risk_score: float | None = None
    model_version: str = "probability_v1"
    metadata_json: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        record = self.__dict__.copy()
        record["timestamp"] = self.timestamp.isoformat()
        return record

