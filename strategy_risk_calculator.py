def safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def pct_distance(price, other):
    price = safe_float(price)
    other = safe_float(other)
    if not price or other is None:
        return None
    return abs(price - other) / price * 100


def futures_levels(direction, entry, atr, config):
    entry = safe_float(entry)
    atr = safe_float(atr)
    if direction not in ["LONG", "SHORT"] or entry is None or atr is None or atr <= 0:
        return {}

    sign = 1 if direction == "LONG" else -1
    stop_loss = entry - sign * atr * config.atr_stop_multiplier
    tp1 = entry + sign * atr * config.tp1_atr_multiplier
    tp2 = entry + sign * atr * config.tp2_atr_multiplier
    tp3 = entry + sign * atr * config.tp3_atr_multiplier
    risk = abs(entry - stop_loss)

    return {
        "entry": round(entry, 2),
        "stop_loss": round(stop_loss, 2),
        "stop_loss_pct": round(risk / entry * 100, 2) if entry else None,
        "tp1": round(tp1, 2),
        "tp2": round(tp2, 2),
        "tp3": round(tp3, 2),
        "rr_tp1": round(abs(tp1 - entry) / risk, 2) if risk else None,
        "rr_tp2": round(abs(tp2 - entry) / risk, 2) if risk else None,
        "rr_tp3": round(abs(tp3 - entry) / risk, 2) if risk else None,
    }


def option_spread_pct(mark_price, bid=None, ask=None):
    mark_price = safe_float(mark_price)
    bid = safe_float(bid)
    ask = safe_float(ask)
    if bid is not None and ask is not None and mark_price:
        return abs(ask - bid) / mark_price * 100
    return None


def synthetic_leg_price(row, side):
    price = safe_float(row.get("mark_price"), 0) if isinstance(row, dict) else 0
    if side == "sell":
        return price
    return price


def iron_fly_payoff(short_call, short_put, long_call, long_put):
    sc = synthetic_leg_price(short_call, "sell")
    sp = synthetic_leg_price(short_put, "sell")
    lc = synthetic_leg_price(long_call, "buy")
    lp = synthetic_leg_price(long_put, "buy")
    center = safe_float(short_call.get("strike"))
    upper = safe_float(long_call.get("strike"))
    lower = safe_float(long_put.get("strike"))
    if None in [center, upper, lower]:
        return {}
    wing_width = min(abs(upper - center), abs(center - lower))
    net_credit = sc + sp - lc - lp
    max_profit = net_credit
    max_loss = max(0, wing_width - net_credit)
    return_on_risk = (max_profit / max_loss * 100) if max_loss else None
    return {
        "net_credit": round(net_credit, 4),
        "max_profit": round(max_profit, 4),
        "max_loss": round(max_loss, 4),
        "upper_breakeven": round(center + net_credit, 2),
        "lower_breakeven": round(center - net_credit, 2),
        "return_on_risk_pct": round(return_on_risk, 2) if return_on_risk is not None else None,
        "wing_width": wing_width,
    }


def aggregate_greeks(legs):
    totals = {"delta": 0.0, "theta": 0.0, "gamma": 0.0, "vega": 0.0}
    for leg in legs or []:
        multiplier = -1 if leg.get("action") == "sell" else 1
        qty = safe_float(leg.get("quantity"), 1) or 1
        for greek in totals:
            totals[greek] += multiplier * qty * (safe_float(leg.get(greek), 0) or 0)
    return {key: round(value, 6) for key, value in totals.items()}
