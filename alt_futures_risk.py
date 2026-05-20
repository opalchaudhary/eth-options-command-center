import math

from validation_config import INR_PER_USDT, usdt_to_inr


ALT_FUTURES_STARTING_BALANCE_INR = 10000
ALT_FUTURES_STARTING_BALANCE_USDT = ALT_FUTURES_STARTING_BALANCE_INR / INR_PER_USDT
MIN_RR_RATIO = 1.55
MIN_LIQUIDATION_DISTANCE_PCT = 0.10
MAINTENANCE_BUFFER_PCT = 0.006
DEFAULT_RISK_PCT = 0.02
MAX_RISK_PCT = 0.03
MIN_NOTIONAL_USDT = 5


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def clamp(value, lower, upper):
    return min(max(value, lower), upper)


def estimate_liquidation_price(entry_price, direction, leverage):
    entry_price = safe_float(entry_price)
    leverage = max(safe_float(leverage), 1.0)

    if direction == "LONG":
        return round(entry_price * (1 - (1 / leverage) + MAINTENANCE_BUFFER_PCT), 6)

    if direction == "SHORT":
        return round(entry_price * (1 + (1 / leverage) - MAINTENANCE_BUFFER_PCT), 6)

    return None


def liquidation_distance_pct(entry_price, liquidation_price):
    entry_price = safe_float(entry_price)
    liquidation_price = safe_float(liquidation_price)

    if not entry_price or not liquidation_price:
        return 0.0

    return abs(entry_price - liquidation_price) / entry_price


def risk_pct_for_score(score, volatility_pct, spread_pct):
    score = safe_float(score)
    volatility_pct = safe_float(volatility_pct)
    spread_pct = safe_float(spread_pct)

    risk_pct = DEFAULT_RISK_PCT

    if score >= 84 and volatility_pct <= 3 and spread_pct <= 0.08:
        risk_pct = 0.025
    elif score < 72 or volatility_pct >= 5 or spread_pct >= 0.15:
        risk_pct = 0.015

    return min(risk_pct, MAX_RISK_PCT)


def choose_leverage(score, volatility_pct, stop_pct, spread_pct):
    score = safe_float(score)
    volatility_pct = safe_float(volatility_pct)
    stop_pct = safe_float(stop_pct)
    spread_pct = safe_float(spread_pct)

    if volatility_pct >= 5 or stop_pct >= 3 or spread_pct >= 0.15:
        return 2

    if score >= 86 and volatility_pct <= 2.5 and spread_pct <= 0.06:
        return 4

    if score >= 76 and volatility_pct <= 4:
        return 3

    return 2


def margin_cap_pct(score, volatility_pct):
    score = safe_float(score)
    volatility_pct = safe_float(volatility_pct)

    if score >= 84 and volatility_pct <= 3:
        return 0.35

    if score >= 74 and volatility_pct <= 4.5:
        return 0.30

    return 0.25


def calculate_alt_position(
    equity_usdt,
    available_balance_usdt,
    entry_price,
    stop_loss,
    take_profit_1,
    direction,
    leverage,
    risk_pct,
    max_margin_pct,
):
    equity_usdt = safe_float(equity_usdt)
    available_balance_usdt = safe_float(available_balance_usdt)
    entry_price = safe_float(entry_price)
    stop_loss = safe_float(stop_loss)
    take_profit_1 = safe_float(take_profit_1)
    leverage = max(safe_float(leverage), 1.0)
    risk_pct = clamp(safe_float(risk_pct), 0.0, MAX_RISK_PCT)
    max_margin_pct = safe_float(max_margin_pct)

    if not equity_usdt or not entry_price or not stop_loss or not take_profit_1:
        return None

    stop_distance = abs(entry_price - stop_loss)
    reward_distance = abs(take_profit_1 - entry_price)

    if stop_distance <= 0:
        return None

    rr_ratio = reward_distance / stop_distance
    risk_amount = min(equity_usdt * risk_pct, equity_usdt * MAX_RISK_PCT)
    position_size_by_risk = risk_amount / stop_distance
    max_margin_usdt = min(equity_usdt * max_margin_pct, available_balance_usdt)
    position_size_by_margin = (max_margin_usdt * leverage) / entry_price
    position_size = min(position_size_by_risk, position_size_by_margin)
    notional_usdt = position_size * entry_price

    if notional_usdt < MIN_NOTIONAL_USDT:
        return None

    precision = 3 if entry_price >= 1 else 0
    position_size = math.floor(position_size * (10**precision)) / (10**precision)

    if position_size <= 0:
        return None

    notional_usdt = position_size * entry_price
    margin_required = notional_usdt / leverage
    estimated_loss = stop_distance * position_size
    estimated_profit = reward_distance * position_size
    liquidation_price = estimate_liquidation_price(entry_price, direction, leverage)
    liq_distance = liquidation_distance_pct(entry_price, liquidation_price)

    return {
        "position_size": round(position_size, 6),
        "notional_usdt": round(notional_usdt, 4),
        "margin_required_usdt": round(margin_required, 4),
        "margin_required_inr": usdt_to_inr(margin_required),
        "risk_amount_usdt": round(estimated_loss, 4),
        "risk_amount_inr": usdt_to_inr(estimated_loss),
        "risk_pct": round((estimated_loss / equity_usdt) * 100, 2),
        "expected_reward_usdt": round(estimated_profit, 4),
        "expected_reward_inr": usdt_to_inr(estimated_profit),
        "rr_ratio": round(rr_ratio, 2),
        "liquidation_price_estimate": liquidation_price,
        "liquidation_distance_pct": round(liq_distance * 100, 2),
        "leverage": leverage,
    }


def validate_position_risk(position, entry_price, stop_loss):
    if not position:
        return ["Position size is zero after risk, notional, and margin caps."]

    reasons = []
    rr_ratio = safe_float(position.get("rr_ratio"))
    liquidation_distance = safe_float(position.get("liquidation_distance_pct")) / 100
    stop_distance_pct = abs(safe_float(entry_price) - safe_float(stop_loss)) / max(safe_float(entry_price), 1)

    if rr_ratio < MIN_RR_RATIO:
        reasons.append(f"RR {rr_ratio:.2f} is below the {MIN_RR_RATIO:.2f} minimum.")

    if liquidation_distance < MIN_LIQUIDATION_DISTANCE_PCT:
        reasons.append("Liquidation estimate is too close to entry.")

    if liquidation_distance <= stop_distance_pct * 2:
        reasons.append("Liquidation estimate does not leave enough distance beyond the stop loss.")

    if safe_float(position.get("risk_pct")) > MAX_RISK_PCT * 100:
        reasons.append("Estimated stop-loss risk exceeds the 3% hard cap.")

    return reasons
