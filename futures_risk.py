import math

from validation_config import ETH_LOT_SIZE, INR_PER_USDT, usdt_to_inr


FUTURES_STARTING_BALANCE_INR = 50000
FUTURES_STARTING_BALANCE_USDT = FUTURES_STARTING_BALANCE_INR / INR_PER_USDT
MIN_RR_RATIO = 1.5
MIN_LIQUIDATION_DISTANCE_PCT = 0.08
MAINTENANCE_BUFFER_PCT = 0.005


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def estimate_liquidation_price(entry_price, direction, leverage):
    entry_price = safe_float(entry_price)
    leverage = max(safe_float(leverage), 1.0)

    if direction == "LONG":
        return round(entry_price * (1 - (1 / leverage) + MAINTENANCE_BUFFER_PCT), 2)

    if direction == "SHORT":
        return round(entry_price * (1 + (1 / leverage) - MAINTENANCE_BUFFER_PCT), 2)

    return None


def liquidation_distance_pct(entry_price, liquidation_price):
    entry_price = safe_float(entry_price)
    liquidation_price = safe_float(liquidation_price)

    if not entry_price or not liquidation_price:
        return 0.0

    return abs(entry_price - liquidation_price) / entry_price


def choose_leverage(confidence_score, volatility_regime, signal_conflict_score):
    confidence_score = safe_float(confidence_score)
    signal_conflict_score = safe_float(signal_conflict_score)
    volatility_label = str(volatility_regime or "").lower()

    if "elevated" in volatility_label or signal_conflict_score >= 65:
        return 2

    if "expansion" in volatility_label:
        return 3 if confidence_score < 78 else 4

    if confidence_score >= 86 and signal_conflict_score <= 30:
        return 7

    if confidence_score >= 78:
        return 5

    if confidence_score >= 68:
        return 3

    return 1


def risk_pct_for_confidence(confidence_score, volatility_regime, signal_conflict_score):
    confidence_score = safe_float(confidence_score)
    signal_conflict_score = safe_float(signal_conflict_score)
    volatility_label = str(volatility_regime or "").lower()

    risk_pct = 0.05

    if confidence_score >= 88 and signal_conflict_score <= 25 and "elevated" not in volatility_label:
        risk_pct = 0.10
    elif confidence_score >= 78 and signal_conflict_score <= 40:
        risk_pct = 0.07
    elif confidence_score < 68 or signal_conflict_score >= 60:
        risk_pct = 0.03

    if "elevated" in volatility_label:
        risk_pct = min(risk_pct, 0.03)

    return risk_pct


def margin_cap_pct(confidence_score, volatility_regime, signal_conflict_score):
    confidence_score = safe_float(confidence_score)
    signal_conflict_score = safe_float(signal_conflict_score)
    volatility_label = str(volatility_regime or "").lower()

    if confidence_score >= 88 and signal_conflict_score <= 25 and "elevated" not in volatility_label:
        return 0.35

    if confidence_score >= 75 and signal_conflict_score <= 45:
        return 0.30

    return 0.20


def calculate_futures_position(
    equity_usdt,
    available_balance_usdt,
    entry_price,
    stop_loss,
    take_profit,
    direction,
    leverage,
    risk_pct,
    max_margin_pct,
):
    equity_usdt = safe_float(equity_usdt)
    available_balance_usdt = safe_float(available_balance_usdt)
    entry_price = safe_float(entry_price)
    stop_loss = safe_float(stop_loss)
    take_profit = safe_float(take_profit)
    leverage = max(safe_float(leverage), 1.0)
    risk_pct = safe_float(risk_pct)
    max_margin_pct = safe_float(max_margin_pct)

    if not equity_usdt or not entry_price or not stop_loss or not take_profit:
        return None

    stop_distance = abs(entry_price - stop_loss)
    reward_distance = abs(take_profit - entry_price)

    if stop_distance <= 0:
        return None

    rr_ratio = reward_distance / stop_distance
    risk_amount = equity_usdt * risk_pct
    position_size_eth_by_risk = risk_amount / stop_distance
    max_margin_usdt = min(equity_usdt * max_margin_pct, available_balance_usdt)
    position_size_eth_by_margin = (max_margin_usdt * leverage) / entry_price
    position_size_eth = min(position_size_eth_by_risk, position_size_eth_by_margin)
    lots = math.floor(position_size_eth / ETH_LOT_SIZE)

    if lots <= 0:
        return None

    position_size_eth = round(lots * ETH_LOT_SIZE, 4)
    notional_usdt = position_size_eth * entry_price
    margin_required = notional_usdt / leverage
    estimated_loss = stop_distance * position_size_eth
    estimated_profit = reward_distance * position_size_eth
    liquidation_price = estimate_liquidation_price(entry_price, direction, leverage)
    liq_distance = liquidation_distance_pct(entry_price, liquidation_price)

    return {
        "lots": lots,
        "position_size_eth": position_size_eth,
        "notional_usdt": round(notional_usdt, 4),
        "margin_required_usdt": round(margin_required, 4),
        "margin_required_inr": usdt_to_inr(margin_required),
        "risk_amount_usdt": round(estimated_loss, 4),
        "risk_amount_inr": usdt_to_inr(estimated_loss),
        "risk_pct": round((estimated_loss / equity_usdt) * 100, 2),
        "expected_reward_usdt": round(estimated_profit, 4),
        "expected_reward_inr": usdt_to_inr(estimated_profit),
        "reward_pct": round((estimated_profit / equity_usdt) * 100, 2),
        "rr_ratio": round(rr_ratio, 2),
        "liquidation_price_estimate": liquidation_price,
        "liquidation_distance_pct": round(liq_distance * 100, 2),
        "leverage": leverage,
    }


def validate_position_risk(position, entry_price, stop_loss):
    if not position:
        return ["Lot size is zero after risk and margin caps."]

    reasons = []
    rr_ratio = safe_float(position.get("rr_ratio"))
    liquidation_distance = safe_float(position.get("liquidation_distance_pct")) / 100
    stop_distance_pct = abs(safe_float(entry_price) - safe_float(stop_loss)) / max(safe_float(entry_price), 1)

    if rr_ratio < MIN_RR_RATIO:
        reasons.append(f"RR {rr_ratio:.2f} is below the {MIN_RR_RATIO:.2f} minimum.")

    if liquidation_distance < MIN_LIQUIDATION_DISTANCE_PCT:
        reasons.append("Liquidation estimate is too close to entry.")

    if liquidation_distance <= stop_distance_pct * 1.8:
        reasons.append("Liquidation estimate does not leave enough room beyond the stop loss.")

    if safe_float(position.get("risk_pct")) > 10:
        reasons.append("Estimated stop-loss risk exceeds the 10% hard cap.")

    return reasons
