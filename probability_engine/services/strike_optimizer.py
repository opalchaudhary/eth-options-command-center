import math

from probability_engine.models.option_strike import OptionStrikeRecommendation
from probability_engine.services.math_utils import clamp


class StrikeOptimizer:
    TIERS = {
        "CONSERVATIVE": {"max_touch": 0.16, "min_buffer": 0.04},
        "BALANCED": {"max_touch": 0.28, "min_buffer": 0.025},
        "RETURN": {"max_touch": 0.45, "min_buffer": 0.012},
    }

    def optimize(self, prediction, option_rows, expiry=None):
        rows = [row for row in option_rows or [] if row.get("strike") is not None and row.get("mark_price") is not None]
        if not rows or not prediction or not prediction.expected_price:
            return self._no_trade(prediction, expiry)
        recommendations = []
        for option_type in ["put_options", "call_options"]:
            candidates = [row for row in rows if row.get("type") == option_type and (expiry is None or str(row.get("expiry")) == str(expiry))]
            for tier, rules in self.TIERS.items():
                ranked = self._rank(candidates, prediction, option_type, rules)
                recommendations.append(ranked[0] if ranked else self._no_trade(prediction, expiry, option_type, tier))
        return recommendations

    def _rank(self, rows, prediction, option_type, rules):
        spot = prediction.expected_price
        ranked = []
        for row in rows:
            strike = float(row["strike"])
            premium = float(row.get("mark_price") or 0)
            distance = (spot - strike) / spot if option_type == "put_options" else (strike - spot) / spot
            if distance <= 0:
                continue
            touch = self.touch_probability(prediction, strike, option_type)
            itm = clamp(touch * 0.48)
            if touch > rules["max_touch"] or distance < rules["min_buffer"]:
                continue
            efficiency = premium / max(distance * spot, 1)
            risk_score = clamp(touch * 0.65 + itm * 0.35)
            ranked.append(OptionStrikeRecommendation(
                symbol=prediction.symbol,
                expiry=row.get("expiry"),
                option_type=option_type,
                strike=strike,
                risk_tier=next(key for key, value in self.TIERS.items() if value == rules),
                recommendation_status="CANDIDATE",
                touch_probability=round(touch, 4),
                itm_probability=round(itm, 4),
                premium=premium,
                premium_efficiency=round(efficiency, 6),
                range_buffer_pct=round(distance, 4),
                risk_score=round(risk_score, 4),
                model_version=prediction.model_version,
            ))
        return sorted(ranked, key=lambda item: (item.risk_score or 1, -(item.premium_efficiency or 0)))

    def touch_probability(self, prediction, strike, option_type):
        if option_type == "put_options":
            lower = prediction.range_90_lower or prediction.range_70_lower or prediction.range_50_lower
            upper = prediction.expected_price
            if strike <= lower:
                return 0.05
            if strike >= upper:
                return 0.95
            return clamp(0.05 + 0.9 * (strike - lower) / (upper - lower))
        upper = prediction.range_90_upper or prediction.range_70_upper or prediction.range_50_upper
        lower = prediction.expected_price
        if strike >= upper:
            return 0.05
        if strike <= lower:
            return 0.95
        return clamp(0.95 - 0.9 * (strike - lower) / (upper - lower))

    def _no_trade(self, prediction, expiry=None, option_type="put_options", tier="BALANCED"):
        return OptionStrikeRecommendation(
            symbol=getattr(prediction, "symbol", "ETHUSD"),
            expiry=expiry,
            option_type=option_type,
            risk_tier=tier,
            recommendation_status="NO_ATTRACTIVE_NAKED_SELL",
            metadata_json={"reason": "No strike passed V1 risk filters."},
        )

