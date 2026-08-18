from probability_engine.config import get_probability_config
from probability_engine.models.prediction import ProbabilityPrediction
from probability_engine.services.math_utils import clamp, sigmoid
from probability_engine.services.range_service import RangeService


class ProbabilityService:
    def __init__(self, config=None, range_service=None):
        self.config = config or get_probability_config()
        self.range_service = range_service or RangeService()

    def predict(self, snapshot, horizon: str, analogue_stats=None, record_type="LIVE"):
        analogue_stats = analogue_stats or {}
        vwap_z = snapshot.vwap_zscore or 0
        atr_pct = snapshot.atr_pct or 0
        rv = snapshot.realized_volatility or 50
        momentum = snapshot.return_1h or 0
        cvd = snapshot.cvd_slope or 0
        book = (snapshot.book_imbalance or 1) - 1
        missing = self._missing_feature_count(snapshot)

        mean_reversion = sigmoid(abs(vwap_z) * 0.8 - abs(momentum) * 18 - max(0, rv - 80) * 0.01)
        upside_breakout = sigmoid(momentum * 55 + max(0, vwap_z) * 0.15 + cvd * 0.001 + book * 0.35 - 1.25)
        downside_breakdown = sigmoid(-momentum * 55 + max(0, -vwap_z) * 0.15 - cvd * 0.001 - book * 0.35 - 1.25)
        range_continuation = sigmoid(1.2 - abs(vwap_z) * 0.45 - atr_pct * 75 - abs(momentum) * 35)
        trend_continuation = sigmoid(abs(momentum) * 70 + max(0, rv - 45) * 0.008 - 1.2)

        sample_size = int(analogue_stats.get("sample_size") or 0)
        confidence = clamp(0.86 - missing * 0.08 + min(sample_size, self.config.analogue_count) / max(self.config.analogue_count, 1) * 0.08)
        if sample_size and sample_size < self.config.min_model_sample_size:
            confidence = clamp(confidence - 0.18)

        distribution = self.range_service.expected_distribution(snapshot, horizon, analogue_stats.get("returns"))
        status = "LIVE"
        if snapshot.delta_market_data_status == "STALE_DATA" or snapshot.spot_price is None:
            status = "STALE_DATA"
            confidence = clamp((confidence or 0) * 0.55)

        return ProbabilityPrediction(
            snapshot_id=getattr(snapshot, "id", None),
            symbol=snapshot.symbol,
            horizon=horizon.upper(),
            record_type=record_type,
            model_version=self.config.model_version,
            feature_version=self.config.feature_version,
            regime_version=self.config.regime_version,
            range_model_version=self.config.range_model_version,
            prediction_status=status,
            mean_reversion_probability=round(clamp(mean_reversion), 4),
            upside_breakout_probability=round(clamp(upside_breakout), 4),
            downside_breakdown_probability=round(clamp(downside_breakdown), 4),
            range_continuation_probability=round(clamp(range_continuation), 4),
            trend_continuation_probability=round(clamp(trend_continuation), 4),
            confidence=round(confidence, 4) if confidence is not None else None,
            analogue_sample_size=sample_size,
            metadata_json={"probabilities_are_independent": True, "missing_feature_count": missing},
            **distribution,
        )

    def _missing_feature_count(self, snapshot):
        tracked = ["vwap_zscore", "atr", "realized_volatility", "volume_zscore", "atm_iv", "cvd_slope", "book_imbalance"]
        return sum(1 for name in tracked if getattr(snapshot, name, None) is None)

