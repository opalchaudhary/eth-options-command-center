from dataclasses import dataclass, field


@dataclass(frozen=True)
class FuturesConfig:
    min_candles: int = 40
    max_spread_pct: float = 0.08
    min_rr: float = 1.35
    atr_period: int = 14
    atr_stop_multiplier: float = 1.4
    tp1_atr_multiplier: float = 1.5
    tp2_atr_multiplier: float = 2.4
    tp3_atr_multiplier: float = 3.2
    max_stop_atr_multiple: float = 2.2
    min_score_for_trade: float = 58
    min_data_quality_for_trade: float = 55
    stale_data_seconds: int = 180
    suggested_risk_pct_low: float = 0.25
    suggested_risk_pct_medium: float = 0.5
    suggested_risk_pct_high: float = 0.75
    weights: dict = field(
        default_factory=lambda: {
            "trend": 0.30,
            "momentum": 0.18,
            "structure": 0.18,
            "orderbook": 0.14,
            "volatility": 0.12,
            "options": 0.08,
        }
    )


@dataclass(frozen=True)
class CoveredConfig:
    min_option_oi: float = 5
    min_option_volume: float = 1
    max_spread_pct: float = 8.0
    preferred_abs_delta_low: float = 0.18
    preferred_abs_delta_high: float = 0.38
    min_yield_pct: float = 0.20
    breakout_conflict_score: float = 65


@dataclass(frozen=True)
class IronFlyConfig:
    min_option_oi: float = 5
    min_option_volume: float = 1
    max_leg_spread_pct: float = 12.0
    min_credit_to_width: float = 0.18
    min_return_on_risk_pct: float = 15.0
    max_expiries: int = 8
    center_strike_count: int = 5
    wing_widths: tuple = (40, 60, 80, 100, 120, 160, 200)
    max_dte: float = 35.0
    min_score_recommended: float = 68
    min_score_watchlist: float = 52
    stale_data_seconds: int = 180
    weights: dict = field(
        default_factory=lambda: {
            "credit": 0.22,
            "return_on_risk": 0.20,
            "pinning": 0.18,
            "liquidity": 0.18,
            "volatility": 0.12,
            "greeks": 0.10,
        }
    )


@dataclass(frozen=True)
class PersistenceConfig:
    min_interval_seconds: int = 15 * 60
    history_limit: int = 50


@dataclass(frozen=True)
class StrategyDecisionConfig:
    futures: FuturesConfig = field(default_factory=FuturesConfig)
    covered: CoveredConfig = field(default_factory=CoveredConfig)
    iron_fly: IronFlyConfig = field(default_factory=IronFlyConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)


DEFAULT_STRATEGY_CONFIG = StrategyDecisionConfig()
