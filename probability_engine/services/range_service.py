from probability_engine.config import HORIZON_MINUTES


class RangeService:
    def expected_distribution(self, snapshot, horizon: str, analogue_returns=None) -> dict:
        spot = float(snapshot.spot_price or 0)
        if spot <= 0:
            return {}
        horizon_minutes = HORIZON_MINUTES.get(horizon.upper(), 60)
        atr = snapshot.atr or spot * 0.008
        scale = max(1.0, (horizon_minutes / 60) ** 0.5)
        trend_bias = ((snapshot.return_1h or 0) * 0.35 + (snapshot.return_4h or 0) * 0.15)
        equilibrium_pull = 0.0
        if snapshot.vwap is not None:
            equilibrium_pull = (snapshot.vwap - spot) * 0.35
        expected_price = spot * (1 + trend_bias) + equilibrium_pull
        expected_equilibrium = snapshot.vwap if snapshot.vwap is not None else expected_price

        if analogue_returns:
            prices = sorted([spot * (1 + item) for item in analogue_returns])
            return self._quantile_ranges(prices, expected_price, expected_equilibrium)

        downside_multiplier = 1.1 if (snapshot.return_1h or 0) < 0 else 0.95
        upside_multiplier = 1.1 if (snapshot.return_1h or 0) > 0 else 0.95
        base = atr * scale
        return {
            "expected_price": round(expected_price, 2),
            "median_price": round((expected_price + spot) / 2, 2),
            "expected_equilibrium": round(expected_equilibrium, 2),
            "range_50_lower": round(spot - base * 0.67 * downside_multiplier, 2),
            "range_50_upper": round(spot + base * 0.67 * upside_multiplier, 2),
            "range_70_lower": round(spot - base * 1.04 * downside_multiplier, 2),
            "range_70_upper": round(spot + base * 1.04 * upside_multiplier, 2),
            "range_90_lower": round(spot - base * 1.64 * downside_multiplier, 2),
            "range_90_upper": round(spot + base * 1.64 * upside_multiplier, 2),
        }

    def _quantile_ranges(self, prices, expected_price, expected_equilibrium):
        def q(p):
            idx = min(len(prices) - 1, max(0, round((len(prices) - 1) * p)))
            return round(prices[idx], 2)

        return {
            "expected_price": round(expected_price, 2),
            "median_price": q(0.5),
            "expected_equilibrium": round(expected_equilibrium, 2),
            "range_50_lower": q(0.25),
            "range_50_upper": q(0.75),
            "range_70_lower": q(0.15),
            "range_70_upper": q(0.85),
            "range_90_lower": q(0.05),
            "range_90_upper": q(0.95),
        }

