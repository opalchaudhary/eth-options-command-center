import os
from dataclasses import dataclass, field


def _bool_env(key: str, default: bool = False) -> bool:
    value = os.getenv(key)
    if value in [None, ""]:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _int_env(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, default))
    except (TypeError, ValueError):
        return default


def _float_env(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, default))
    except (TypeError, ValueError):
        return default


def _csv_env(key: str, default: str) -> list[str]:
    return [item.strip().upper() for item in os.getenv(key, default).split(",") if item.strip()]


@dataclass(frozen=True)
class ProbabilityEngineConfig:
    enabled: bool = field(default_factory=lambda: _bool_env("PROBABILITY_ENGINE_ENABLED", False))
    snapshot_interval_seconds: int = field(default_factory=lambda: _int_env("PROBABILITY_SNAPSHOT_INTERVAL", 300))
    prediction_interval_seconds: int = field(default_factory=lambda: _int_env("PROBABILITY_PREDICTION_INTERVAL", 300))
    strike_scan_interval_seconds: int = field(default_factory=lambda: _int_env("PROBABILITY_STRIKE_SCAN_INTERVAL", 900))
    outcome_interval_seconds: int = field(default_factory=lambda: _int_env("PROBABILITY_OUTCOME_INTERVAL", 300))
    performance_interval_seconds: int = field(default_factory=lambda: _int_env("PROBABILITY_PERFORMANCE_INTERVAL", 86400))
    outcome_batch_limit: int = field(default_factory=lambda: _int_env("PROBABILITY_OUTCOME_BATCH_LIMIT", 25))
    horizons: list[str] = field(default_factory=lambda: _csv_env("PROBABILITY_HORIZONS", "1h,2h,4h,8h,12h,24h"))
    symbol: str = field(default_factory=lambda: os.getenv("PROBABILITY_SYMBOL", "ETHUSD"))
    feature_version: str = field(default_factory=lambda: os.getenv("PROBABILITY_FEATURE_VERSION", "features_v1"))
    regime_version: str = field(default_factory=lambda: os.getenv("PROBABILITY_REGIME_VERSION", "regime_v1"))
    model_version: str = field(default_factory=lambda: os.getenv("PROBABILITY_MODEL_VERSION", "probability_v1"))
    range_model_version: str = field(default_factory=lambda: os.getenv("PROBABILITY_RANGE_MODEL_VERSION", "range_v1"))
    analogue_count: int = field(default_factory=lambda: _int_env("ANALOGUE_COUNT", 300))
    min_model_sample_size: int = field(default_factory=lambda: _int_env("MIN_MODEL_SAMPLE_SIZE", 100))
    orderflow_enabled: bool = field(default_factory=lambda: _bool_env("ORDERFLOW_ENABLED", False))
    orderbook_enabled: bool = field(default_factory=lambda: _bool_env("ORDERBOOK_ENABLED", True))
    retention_enabled: bool = field(default_factory=lambda: _bool_env("PROBABILITY_RETENTION_ENABLED", False))
    stale_data_seconds: int = field(default_factory=lambda: _int_env("PROBABILITY_STALE_DATA_SECONDS", 900))
    minimum_initial_vwap_zscore: float = field(default_factory=lambda: _float_env("PROBABILITY_MIN_VWAP_ZSCORE", 1.0))
    reversion_fraction: float = field(default_factory=lambda: _float_env("PROBABILITY_REVERSION_FRACTION", 0.5))
    breakout_atr_multiple: float = field(default_factory=lambda: _float_env("PROBABILITY_BREAKOUT_ATR_MULTIPLE", 0.6))
    trend_atr_multiple: float = field(default_factory=lambda: _float_env("PROBABILITY_TREND_ATR_MULTIPLE", 0.5))


def get_probability_config() -> ProbabilityEngineConfig:
    return ProbabilityEngineConfig()


HORIZON_MINUTES = {
    "1H": 60,
    "2H": 120,
    "4H": 240,
    "8H": 480,
    "12H": 720,
    "24H": 1440,
    "48H": 2880,
    "D3": 4320,
    "D7": 10080,
}
