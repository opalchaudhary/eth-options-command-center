from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class MarketSnapshot:
    symbol: str = "ETHUSD"
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    spot_price: float | None = None
    future_price: float | None = None
    return_5m: float | None = None
    return_15m: float | None = None
    return_1h: float | None = None
    return_4h: float | None = None
    vwap: float | None = None
    vwap_deviation_pct: float | None = None
    vwap_zscore: float | None = None
    atr: float | None = None
    atr_pct: float | None = None
    realized_volatility: float | None = None
    volume: float | None = None
    volume_zscore: float | None = None
    funding_rate: float | None = None
    funding_percentile: float | None = None
    open_interest: float | None = None
    oi_change_5m: float | None = None
    oi_change_1h: float | None = None
    oi_change_4h: float | None = None
    basis: float | None = None
    cvd_5m: float | None = None
    cvd_15m: float | None = None
    cvd_1h: float | None = None
    cvd_slope: float | None = None
    cvd_acceleration: float | None = None
    price_cvd_divergence: float | None = None
    buy_volume_ratio: float | None = None
    book_imbalance: float | None = None
    spread_bps: float | None = None
    bid_depth: float | None = None
    ask_depth: float | None = None
    atm_iv: float | None = None
    iv_rv_spread: float | None = None
    iv_percentile: float | None = None
    put_call_skew: float | None = None
    term_structure_signal: float | None = None
    regime: str = "UNKNOWN"
    feature_version: str = "features_v1"
    regime_version: str = "regime_v1"
    delta_market_data_status: str = "UNKNOWN"
    orderflow_provider_status: str = "DISABLED"
    last_delta_update: datetime | None = None
    last_orderflow_update: datetime | None = None
    data_age_seconds: float | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        record = self.__dict__.copy()
        for key, value in list(record.items()):
            if isinstance(value, datetime):
                record[key] = value.isoformat()
        return record

