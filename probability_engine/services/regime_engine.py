from probability_engine.models.regime import MarketRegime


class RegimeEngine:
    def classify(self, snapshot) -> str:
        if not snapshot or snapshot.spot_price is None:
            return MarketRegime.UNKNOWN.value

        rv = snapshot.realized_volatility or 0
        atr_pct = (snapshot.atr_pct or 0) * 100
        ret_1h = snapshot.return_1h or 0
        ret_4h = snapshot.return_4h or 0
        volume_z = snapshot.volume_zscore or 0
        funding_pct = snapshot.funding_percentile
        vwap_z = snapshot.vwap_zscore or 0

        if funding_pct is not None and (funding_pct >= 0.95 or funding_pct <= 0.05):
            return MarketRegime.EXTREME_FUNDING.value
        if abs(ret_1h) > max(0.025, (snapshot.atr_pct or 0) * 3) and volume_z > 2:
            return MarketRegime.LIQUIDATION_STYLE_MOVE.value
        if rv > 100 or atr_pct > 2.5:
            return MarketRegime.HIGH_VOL.value
        if abs(vwap_z) > 2 and volume_z > 1.5:
            return MarketRegime.VOLATILITY_EXPANSION.value
        if ret_1h > 0.006 and ret_4h > 0:
            return MarketRegime.TREND_UP.value
        if ret_1h < -0.006 and ret_4h < 0:
            return MarketRegime.TREND_DOWN.value
        if rv < 35 and atr_pct < 0.8:
            return MarketRegime.LOW_VOL_RANGE.value
        return MarketRegime.NORMAL_RANGE.value

