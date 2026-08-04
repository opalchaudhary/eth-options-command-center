import logging
from datetime import datetime, timezone

import pandas as pd

from analytics import calculate_atm_and_expected_move
from strategy_decision_config import DEFAULT_STRATEGY_CONFIG
from strategy_market_context import build_strategy_market_context
from strategy_risk_calculator import futures_levels, pct_distance, safe_float
from strategy_scoring import confidence_label, data_quality_score, missing_inputs, weighted_score


logger = logging.getLogger(__name__)


def _latest_expiry_df(option_df, preferred_expiry=None):
    if option_df is None or option_df.empty or "expiry" not in option_df.columns:
        return pd.DataFrame(), None
    expiry = preferred_expiry or sorted(option_df["expiry"].dropna().unique())[0]
    return option_df[option_df["expiry"].astype(str) == str(expiry)].copy(), expiry


def _median_iv(option_df):
    if option_df is None or option_df.empty or "iv" not in option_df.columns:
        return None
    iv = pd.to_numeric(option_df["iv"], errors="coerce").dropna()
    return float(iv.median()) if not iv.empty else None


def _nearest_option(df, option_type, target_delta=None, strike_side=None, spot_price=None):
    if df is None or df.empty:
        return {}
    options = df[df["type"] == option_type].copy()
    if options.empty:
        return {}
    if strike_side == "above" and spot_price:
        options = options[options["strike"] >= spot_price]
    if strike_side == "below" and spot_price:
        options = options[options["strike"] <= spot_price]
    if options.empty:
        return {}
    options["delta_abs_distance"] = (options["delta"].abs() - abs(target_delta or 0.25)).abs()
    options["liquidity_rank"] = options["oi"].fillna(0) + options["volume"].fillna(0)
    selected = options.sort_values(["delta_abs_distance", "liquidity_rank"], ascending=[True, False]).head(1)
    return selected.iloc[0].to_dict() if not selected.empty else {}


def _trend_component(trend):
    direction = trend.get("direction")
    rsi = safe_float(trend.get("rsi"))
    if direction == "BULLISH":
        return 75 if rsi is None or rsi < 75 else 58
    if direction == "BEARISH":
        return 25 if rsi is None or rsi > 25 else 42
    return 50


def _directional_scores(context):
    trend = context.get("trend") or {}
    orderbook = context.get("orderbook_insights") or {}
    zones_df = context.get("smc_zones_df")
    spot = safe_float(context.get("spot_price"))

    trend_component = _trend_component(trend)
    momentum_component = 50
    if trend.get("last_close") and trend.get("vwap"):
        momentum_component = 65 if trend["last_close"] > trend["vwap"] else 35

    orderbook_component = 50
    bias = orderbook.get("bias")
    if bias == "Mild Bullish":
        orderbook_component = 62
    elif bias == "Mild Bearish":
        orderbook_component = 38

    structure_component = 50
    if zones_df is not None and not zones_df.empty and spot:
        active = zones_df.copy()
        active["price_low"] = pd.to_numeric(active["price_low"], errors="coerce")
        active["price_high"] = pd.to_numeric(active["price_high"], errors="coerce")
        inside = active[(active["price_low"] <= spot) & (active["price_high"] >= spot)]
        if not inside.empty:
            if "bullish" in " ".join(inside["direction"].dropna().astype(str).tolist()):
                structure_component = 62
            if "bearish" in " ".join(inside["direction"].dropna().astype(str).tolist()):
                structure_component = 38

    option_component = 50
    expiry_df, _ = _latest_expiry_df(context.get("option_df"))
    if not expiry_df.empty and spot:
        _, expected_move, _, _ = calculate_atm_and_expected_move(expiry_df, spot)
        median_iv = _median_iv(expiry_df)
        rv = safe_float(trend.get("realized_vol_pct"))
        if expected_move and expected_move / spot < 0.025:
            option_component += 5
        if median_iv is not None and rv is not None and median_iv - rv > 15:
            option_component -= 5

    components = {
        "trend": trend_component,
        "momentum": momentum_component,
        "structure": structure_component,
        "orderbook": orderbook_component,
        "volatility": 52 if safe_float(trend.get("atr")) else None,
        "options": option_component,
    }
    long_score, component_scores = weighted_score(components, DEFAULT_STRATEGY_CONFIG.futures.weights)
    short_score = round(100 - long_score, 2)
    return long_score, short_score, component_scores


def _futures_recommendation(context, config):
    spot = safe_float(context.get("spot_price"))
    trend = context.get("trend") or {}
    orderbook = context.get("orderbook_insights") or {}
    long_score, short_score, component_scores = _directional_scores(context)

    required = {
        "spot_price": spot,
        "ohlcv": context.get("ohlcv_df") is not None and len(context.get("ohlcv_df")) >= config.min_candles,
        "atr": trend.get("atr"),
        "orderbook": orderbook.get("status") == "ok",
    }
    optional = {
        "option_chain": context.get("option_df") is not None and not context.get("option_df").empty,
        "smc_zones": context.get("smc_zones_df") is not None and not context.get("smc_zones_df").empty,
        "volume_profile": context.get("volume_profile_df") is not None and not context.get("volume_profile_df").empty,
    }
    quality = data_quality_score(required, optional)

    direction = "NO_TRADE"
    score = max(long_score, short_score)
    if long_score >= config.min_score_for_trade and long_score > short_score + 8:
        direction = "LONG"
    elif short_score >= config.min_score_for_trade and short_score > long_score + 8:
        direction = "SHORT"

    factors_for = []
    factors_against = []
    if trend.get("direction") == "BULLISH":
        factors_for.append("EMA trend is bullish.")
    elif trend.get("direction") == "BEARISH":
        factors_for.append("EMA trend is bearish.")
    else:
        factors_against.append("EMA trend is neutral.")
    if orderbook.get("bias"):
        factors_for.append(f"Orderbook bias is {orderbook.get('bias')}.")
    if context.get("unavailable_inputs"):
        factors_against.append("Unavailable inputs: " + ", ".join(context["unavailable_inputs"]) + ".")

    levels = futures_levels(direction, spot, trend.get("atr"), config)
    if direction != "NO_TRADE" and quality < config.min_data_quality_for_trade:
        direction = "NO_TRADE"
        factors_against.append("Data quality is below the minimum validation threshold.")
    rr_values = [
        value
        for value in [levels.get("rr_tp1"), levels.get("rr_tp2"), levels.get("rr_tp3")]
        if value is not None
    ]
    if direction != "NO_TRADE" and (not rr_values or max(rr_values) < config.min_rr):
        direction = "NO_TRADE"
        factors_against.append("Risk/reward is below the minimum validation threshold.")
    if orderbook.get("spread_pct") is not None and safe_float(orderbook.get("spread_pct")) > config.max_spread_pct:
        direction = "NO_TRADE"
        factors_against.append("Orderbook spread is too wide.")

    risk_pct = config.suggested_risk_pct_low
    if score >= 72 and quality >= 75:
        risk_pct = config.suggested_risk_pct_high
    elif score >= 58:
        risk_pct = config.suggested_risk_pct_medium

    return {
        "recommendation": direction,
        "overall_score": round(score, 2) if direction != "NO_TRADE" else round(score, 2),
        "confidence": confidence_label(min(score, quality)),
        "market_regime": trend.get("direction", "NEUTRAL"),
        "suggested_entry_zone": levels.get("entry"),
        "invalidation_level": levels.get("stop_loss"),
        "stop_loss_price": levels.get("stop_loss"),
        "stop_loss_percentage": levels.get("stop_loss_pct"),
        "tp1": levels.get("tp1"),
        "tp2": levels.get("tp2"),
        "tp3": levels.get("tp3"),
        "risk_to_reward": {
            "tp1": levels.get("rr_tp1"),
            "tp2": levels.get("rr_tp2"),
            "tp3": levels.get("rr_tp3"),
        },
        "suggested_holding_horizon": "Intraday to 1-2 sessions",
        "suggested_position_risk_pct": risk_pct if direction != "NO_TRADE" else 0,
        "supporting_factors": factors_for,
        "contradictory_factors": factors_against,
        "component_scores": component_scores,
        "data_quality_score": quality,
        "missing_required_inputs": missing_inputs(required),
        "timestamp": context.get("generated_at"),
        "contract_used": context.get("symbol", "ETHUSD"),
        "explanation": (
            "Directional evidence is strong enough for a research recommendation."
            if direction != "NO_TRADE"
            else "No trade is preferred because validation filters or directional evidence are insufficient."
        ),
    }


def _covered_analysis(context, side, futures_recommendation, config):
    spot = safe_float(context.get("spot_price"))
    expiry_df, expiry = _latest_expiry_df(context.get("option_df"))
    option_type = "call_options" if side == "call" else "put_options"
    strike_side = "above" if side == "call" else "below"
    option = _nearest_option(
        expiry_df,
        option_type,
        target_delta=0.25,
        strike_side=strike_side,
        spot_price=spot,
    )
    required_position = "Long ETH exposure" if side == "call" else "Short ETH exposure"
    label = "Covered Call" if side == "call" else "Covered Put"

    reasons_for = []
    reasons_against = []
    status = "NOT_RECOMMENDED"
    if not option:
        reasons_against.append("No suitable short option was found for the preferred expiry.")
    else:
        premium = safe_float(option.get("mark_price"), 0)
        strike = safe_float(option.get("strike"))
        delta = safe_float(option.get("delta"))
        oi = safe_float(option.get("oi"), 0)
        volume = safe_float(option.get("volume"), 0)
        expected_yield = (premium / spot * 100) if spot else None
        buffer_pct = pct_distance(spot, strike)
        if oi >= config.min_option_oi and volume >= config.min_option_volume:
            reasons_for.append("Short option has acceptable displayed OI and volume.")
        else:
            reasons_against.append("Short option liquidity is thin.")
        if expected_yield is not None and expected_yield >= config.min_yield_pct:
            reasons_for.append("Premium yield clears the configured minimum.")
        else:
            reasons_against.append("Premium yield is too low.")
        if side == "call" and futures_recommendation.get("recommendation") == "LONG" and futures_recommendation.get("overall_score", 0) >= config.breakout_conflict_score:
            reasons_against.append("Upside breakout risk conflicts with capped upside.")
        if side == "put" and futures_recommendation.get("recommendation") == "SHORT" and futures_recommendation.get("overall_score", 0) >= config.breakout_conflict_score:
            reasons_against.append("Downside breakdown risk conflicts with capped downside.")

        if len(reasons_for) >= 2 and not any("conflicts" in reason for reason in reasons_against):
            status = "RECOMMENDED"
        elif reasons_for:
            status = "WATCHLIST"

        dte_days = None
        expiry_ts = pd.to_datetime(expiry, utc=True, errors="coerce")
        if pd.notna(expiry_ts):
            dte_days = max(0, (expiry_ts.to_pydatetime() - datetime.now(timezone.utc)).total_seconds() / 86400)
        annualized_yield = expected_yield * (365 / dte_days) if expected_yield is not None and dte_days else None

        return {
            "strategy": label,
            "status": status,
            "required_underlying_position": required_position,
            "uncovered_option_warning": "This is not an uncovered short option recommendation.",
            "preferred_expiry": str(expiry),
            "preferred_short_strike": strike,
            "option_delta": delta,
            "premium": premium,
            "expected_yield_pct": round(expected_yield, 2) if expected_yield is not None else None,
            "annualized_yield_pct": round(annualized_yield, 2) if annualized_yield is not None else None,
            "buffer_pct": round(buffer_pct, 2) if buffer_pct is not None else None,
            "breakeven": round(spot - premium, 2) if side == "call" and spot else round(spot + premium, 2) if spot else None,
            "assignment_risk": "Higher near expiry or when spot approaches strike.",
            "liquidity": {"oi": oi, "volume": volume},
            "iv_rv_context": {
                "median_iv": _median_iv(expiry_df),
                "realized_vol_pct": context.get("trend", {}).get("realized_vol_pct"),
            },
            "expected_holding_period": "Until 50-70% premium capture or risk trigger.",
            "exit_conditions": ["Buy back short option at 50-70% profit.", "Exit if spot invalidates covered thesis."],
            "roll_conditions": ["Roll if tested and thesis remains intact.", "Do not roll into uncovered exposure."],
            "reasons_for": reasons_for,
            "reasons_against": reasons_against,
        }

    return {
        "strategy": label,
        "status": status,
        "required_underlying_position": required_position,
        "uncovered_option_warning": "Never treat this as an uncovered short option recommendation.",
        "preferred_expiry": str(expiry) if expiry else None,
        "reasons_for": reasons_for,
        "reasons_against": reasons_against,
    }


def build_futures_covered_recommendation(context=None, config=DEFAULT_STRATEGY_CONFIG):
    logger.info("futures_covered calculation started")
    context = context or build_strategy_market_context()
    futures = _futures_recommendation(context, config.futures)
    covered_call = _covered_analysis(context, "call", futures, config.covered)
    covered_put = _covered_analysis(context, "put", futures, config.covered)
    result = {
        "ok": True,
        "engine_name": "futures_covered_research",
        "generated_at": context.get("generated_at"),
        "symbol": context.get("symbol", "ETHUSD"),
        "futures": futures,
        "covered_call": covered_call,
        "covered_put": covered_put,
        "data_freshness": context.get("data_freshness") or {},
        "unavailable_inputs": context.get("unavailable_inputs") or [],
        "research_only": True,
    }
    logger.info(
        "futures_covered calculation finished recommendation=%s score=%s",
        futures.get("recommendation"),
        futures.get("overall_score"),
    )
    return result
