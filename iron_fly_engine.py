import logging
from datetime import datetime, timezone

import pandas as pd

from analytics import calculate_atm_and_expected_move
from strategy_decision_config import DEFAULT_STRATEGY_CONFIG
from strategy_market_context import build_strategy_market_context
from strategy_risk_calculator import aggregate_greeks, iron_fly_payoff, safe_float
from strategy_scoring import clamp, confidence_label, status_from_score


logger = logging.getLogger(__name__)


def _dte(expiry):
    expiry_ts = pd.to_datetime(expiry, utc=True, errors="coerce")
    if pd.isna(expiry_ts):
        return None
    return max(0, (expiry_ts.to_pydatetime() - datetime.now(timezone.utc)).total_seconds() / 86400)


def _expiry_df(option_df, expiry):
    if option_df is None or option_df.empty:
        return pd.DataFrame()
    return option_df[option_df["expiry"].astype(str) == str(expiry)].copy()


def _row_for(df, option_type, strike):
    rows = df[(df["type"] == option_type) & (df["strike"] == strike)]
    if rows.empty:
        return {}
    return rows.iloc[0].to_dict()


def _candidate_centers(df, spot, count):
    strikes = sorted(pd.to_numeric(df["strike"], errors="coerce").dropna().unique())
    if not strikes or spot is None:
        return []
    ranked = sorted(strikes, key=lambda strike: abs(strike - spot))
    return sorted(ranked[:count])


def _liquidity_score(legs, config):
    scores = []
    reasons = []
    for leg in legs:
        oi = safe_float(leg.get("oi"), 0)
        volume = safe_float(leg.get("volume"), 0)
        if oi < config.min_option_oi or volume < config.min_option_volume:
            reasons.append(f"{leg.get('option_type')} {leg.get('strike')} is thinly traded.")
        scores.append(clamp((oi / max(config.min_option_oi, 1)) * 45 + (volume / max(config.min_option_volume, 1)) * 25, 0, 100))
    if not scores:
        return 0, ["No complete legs."]
    return round(sum(scores) / len(scores), 2), reasons


def _build_leg(row, action, quantity=1):
    return {
        "action": action,
        "option_type": row.get("type"),
        "strike": safe_float(row.get("strike")),
        "quantity": quantity,
        "mark_price": safe_float(row.get("mark_price"), 0),
        "oi": safe_float(row.get("oi"), 0),
        "volume": safe_float(row.get("volume"), 0),
        "iv": safe_float(row.get("iv")),
        "delta": safe_float(row.get("delta"), 0),
        "theta": safe_float(row.get("theta"), 0),
        "gamma": safe_float(row.get("gamma"), 0),
        "vega": safe_float(row.get("vega"), 0),
    }


def _score_candidate(candidate, context, config):
    payoff = candidate["payoff"]
    liquidity_score = candidate["liquidity_score"]
    return_on_risk = safe_float(payoff.get("return_on_risk_pct"), 0)
    credit_to_width = safe_float(payoff.get("net_credit"), 0) / max(safe_float(payoff.get("wing_width"), 1), 1)
    greeks = candidate["net_greeks"]
    trend = context.get("trend") or {}
    spot = safe_float(context.get("spot_price"))
    expected_move = safe_float(candidate.get("expected_move"))
    center = safe_float(candidate.get("center_strike"))

    pinning_score = 50
    if spot and center and expected_move:
        distance = abs(spot - center)
        pinning_score = clamp(100 - (distance / max(expected_move, 1) * 100))
    if trend.get("direction") in ["BULLISH", "BEARISH"]:
        pinning_score -= 12

    components = {
        "credit": clamp(credit_to_width / config.min_credit_to_width * 70),
        "return_on_risk": clamp(return_on_risk / max(config.min_return_on_risk_pct, 1) * 60),
        "pinning": clamp(pinning_score),
        "liquidity": liquidity_score,
        "volatility": 65 if candidate.get("iv_rv_spread") and candidate["iv_rv_spread"] > 8 else 45,
        "greeks": clamp(100 - abs(greeks.get("delta", 0)) * 100),
    }
    total = 0
    for key, weight in config.weights.items():
        total += components[key] * weight
    return round(total, 2), {key: round(value, 2) for key, value in components.items()}


def _candidate_for(df, expiry, center, width, context, config):
    lower = center - width
    upper = center + width
    rows = {
        "short_call": _row_for(df, "call_options", center),
        "short_put": _row_for(df, "put_options", center),
        "long_call": _row_for(df, "call_options", upper),
        "long_put": _row_for(df, "put_options", lower),
    }
    if not all(rows.values()):
        return None, ["Missing one or more required legs."]

    short_call = _build_leg(rows["short_call"], "sell")
    short_put = _build_leg(rows["short_put"], "sell")
    long_call = _build_leg(rows["long_call"], "buy")
    long_put = _build_leg(rows["long_put"], "buy")
    legs = [short_call, short_put, long_call, long_put]
    payoff = iron_fly_payoff(short_call, short_put, long_call, long_put)
    liquidity_score, rejection_reasons = _liquidity_score(legs, config)

    if not payoff:
        rejection_reasons.append("Could not calculate payoff.")
    if payoff.get("net_credit", 0) <= 0:
        rejection_reasons.append("Net credit is not positive.")
    if payoff.get("wing_width") and payoff.get("net_credit", 0) / payoff["wing_width"] < config.min_credit_to_width:
        rejection_reasons.append("Credit is too small relative to wing width.")
    if payoff.get("return_on_risk_pct") is not None and payoff["return_on_risk_pct"] < config.min_return_on_risk_pct:
        rejection_reasons.append("Return on risk is below threshold.")
    if liquidity_score < 45:
        rejection_reasons.append("Liquidity score is below threshold.")

    expiry_iv = (
        pd.to_numeric(df["iv"], errors="coerce").dropna()
        if "iv" in df.columns
        else pd.Series(dtype="float64")
    )
    median_iv = float(expiry_iv.median()) if not expiry_iv.empty else None
    realized_vol = safe_float(context.get("trend", {}).get("realized_vol_pct"))
    iv_rv_spread = median_iv - realized_vol if median_iv is not None and realized_vol is not None else None
    _, expected_move, _, _ = calculate_atm_and_expected_move(df, context.get("spot_price"))

    candidate = {
        "expiry": str(expiry),
        "dte": round(_dte(expiry), 2) if _dte(expiry) is not None else None,
        "center_strike": center,
        "wing_width": width,
        "legs": legs,
        "payoff": payoff,
        "net_greeks": aggregate_greeks(legs),
        "liquidity_score": liquidity_score,
        "bid_ask_quality": "Estimated from OI/volume; true bid/ask unavailable in current chain.",
        "expected_move": round(expected_move, 2) if expected_move is not None else None,
        "median_iv": median_iv,
        "realized_vol_pct": realized_vol,
        "iv_rv_spread": round(iv_rv_spread, 2) if iv_rv_spread is not None else None,
        "rejection_reasons": rejection_reasons,
    }
    score, components = _score_candidate(candidate, context, config)
    candidate["score"] = score
    candidate["component_scores"] = components
    candidate["status"] = "VALID" if not rejection_reasons else "REJECTED"
    candidate["ranking_reason"] = _ranking_reason(candidate)
    return candidate, rejection_reasons


def _ranking_reason(candidate):
    return (
        f"Score {candidate.get('score')} with return on risk "
        f"{candidate.get('payoff', {}).get('return_on_risk_pct')}%, liquidity "
        f"{candidate.get('liquidity_score')}, and center {candidate.get('center_strike')}."
    )


def _evaluate_expiry(option_df, expiry, context, config):
    df = _expiry_df(option_df, expiry)
    dte = _dte(expiry)
    if df.empty or dte is None or dte > config.max_dte:
        return [], [{"expiry": str(expiry), "reason": "Expiry unavailable or outside configured DTE limits."}]

    spot = safe_float(context.get("spot_price"))
    candidates = []
    rejections = []
    for center in _candidate_centers(df, spot, config.center_strike_count):
        for width in config.wing_widths:
            candidate, reasons = _candidate_for(df, expiry, center, width, context, config)
            if candidate:
                candidates.append(candidate)
            if reasons:
                rejections.append({"expiry": str(expiry), "center": center, "width": width, "reasons": reasons})
    return candidates, rejections


def build_iron_fly_recommendation(context=None, config=DEFAULT_STRATEGY_CONFIG):
    logger.info("iron_fly calculation started")
    context = context or build_strategy_market_context()
    option_df = context.get("option_df")
    expiries = (context.get("expiries") or [])[: config.iron_fly.max_expiries]
    all_candidates = []
    all_rejections = []

    for expiry in expiries:
        candidates, rejections = _evaluate_expiry(option_df, expiry, context, config.iron_fly)
        all_candidates.extend(candidates)
        all_rejections.extend(rejections)

    valid = [candidate for candidate in all_candidates if candidate.get("status") == "VALID"]
    ranked = sorted(valid, key=lambda item: item.get("score", 0), reverse=True)
    best = ranked[0] if ranked else None
    best_score = best.get("score", 0) if best else 0
    recommendation = status_from_score(
        best_score,
        config.iron_fly.min_score_recommended,
        config.iron_fly.min_score_watchlist,
    )

    result = {
        "ok": True,
        "engine_name": "iron_fly_research",
        "generated_at": context.get("generated_at"),
        "symbol": context.get("symbol", "ETHUSD"),
        "recommendation": recommendation if best else "NOT_RECOMMENDED",
        "iron_fly_score": round(best_score, 2),
        "confidence": confidence_label(best_score),
        "selected": best,
        "top_alternatives": ranked[1:3],
        "expiry_comparison": _expiry_summary(all_candidates),
        "rejection_reasons": all_rejections[:40],
        "entry_conditions": ["Enter only if all four legs are liquid and net credit is available near modeled credit."],
        "adjustment_triggers": ["Spot moves beyond a breakeven.", "Net delta becomes materially directional.", "Liquidity deteriorates."],
        "stop_loss_logic": "Consider exit if loss reaches 1.5x initial credit or thesis shifts to expansion.",
        "profit_booking_logic": "Consider booking 50-70% of max profit.",
        "time_based_exit": "Avoid holding unmanaged through final expiry hours.",
        "expiry_management_warning": "Expiry risk can be abrupt; this is research only, not an execution instruction.",
        "supporting_factors": best.get("ranking_reason") if best else None,
        "risk_factors": _risk_factors(context, best),
        "research_only": True,
    }
    logger.info("iron_fly calculation finished recommendation=%s score=%s", result["recommendation"], best_score)
    return result


def _expiry_summary(candidates):
    rows = []
    grouped = {}
    for candidate in candidates:
        grouped.setdefault(candidate["expiry"], []).append(candidate)
    for expiry, items in grouped.items():
        best = sorted(items, key=lambda item: item.get("score", 0), reverse=True)[0]
        rows.append(
            {
                "expiry": expiry,
                "best_score": best.get("score"),
                "dte": best.get("dte"),
                "expected_move": best.get("expected_move"),
                "net_credit": best.get("payoff", {}).get("net_credit"),
                "max_profit": best.get("payoff", {}).get("max_profit"),
                "max_loss": best.get("payoff", {}).get("max_loss"),
                "return_on_risk_pct": best.get("payoff", {}).get("return_on_risk_pct"),
                "liquidity_score": best.get("liquidity_score"),
                "reason": best.get("ranking_reason"),
            }
        )
    return sorted(rows, key=lambda row: row.get("best_score") or 0, reverse=True)


def _risk_factors(context, selected):
    factors = []
    trend = context.get("trend") or {}
    if trend.get("direction") in ["BULLISH", "BEARISH"]:
        factors.append("Directional trend can hurt an Iron Fly.")
    if selected and selected.get("iv_rv_spread") is None:
        factors.append("IV-RV spread is unavailable, reducing confidence.")
    if selected and selected.get("bid_ask_quality"):
        factors.append(selected["bid_ask_quality"])
    if context.get("unavailable_inputs"):
        factors.append("Unavailable inputs: " + ", ".join(context["unavailable_inputs"]) + ".")
    return factors
