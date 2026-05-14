import pandas as pd


def _safe_float(value, default=0.0):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _normalize(value, min_value, max_value):
    value = _safe_float(value)

    if max_value == min_value:
        return 0

    score = (value - min_value) / (max_value - min_value)
    return max(0, min(score, 1))


def _delta_safety_score(delta, option_type):
    delta = abs(_safe_float(delta))

    # Ideal short-option delta zone: 0.05 to 0.20
    if delta < 0.03:
        return 0.55
    if 0.03 <= delta <= 0.08:
        return 0.85
    if 0.08 < delta <= 0.18:
        return 1.00
    if 0.18 < delta <= 0.25:
        return 0.70
    if 0.25 < delta <= 0.35:
        return 0.35

    return 0.10


def _distance_score(strike, spot_price, option_type, expected_move):
    strike = _safe_float(strike)
    spot_price = _safe_float(spot_price)
    expected_move = _safe_float(expected_move)

    if not spot_price:
        return 0

    distance = abs(strike - spot_price)
    distance_pct = distance / spot_price

    if expected_move:
        expected_move_distance = expected_move / spot_price
        if distance_pct >= expected_move_distance:
            return 1.00
        if distance_pct >= expected_move_distance * 0.75:
            return 0.80
        if distance_pct >= expected_move_distance * 0.50:
            return 0.55
        return 0.25

    if distance_pct >= 0.04:
        return 1.00
    if distance_pct >= 0.03:
        return 0.80
    if distance_pct >= 0.02:
        return 0.60
    if distance_pct >= 0.01:
        return 0.35

    return 0.10


def _premium_score(premium, max_premium):
    return _normalize(premium, 0, max_premium)


def _oi_score(oi, max_oi):
    return _normalize(oi, 0, max_oi)


def _iv_score(iv, max_iv):
    return _normalize(iv, 0, max_iv)


def _gamma_risk_penalty(gamma):
    gamma = abs(_safe_float(gamma))

    if gamma <= 0.0005:
        return 1.00
    if gamma <= 0.001:
        return 0.85
    if gamma <= 0.002:
        return 0.65
    if gamma <= 0.004:
        return 0.35

    return 0.10


def _is_otm(row, spot_price):
    strike = _safe_float(row.get("strike"))
    option_type = row.get("type")

    if option_type == "call_options":
        return strike > spot_price

    if option_type == "put_options":
        return strike < spot_price

    return False


def _find_best_hedge(expiry_df, sell_row):
    option_type = sell_row.get("type")
    sell_strike = _safe_float(sell_row.get("strike"))

    same_side = expiry_df[expiry_df["type"] == option_type].copy()

    if option_type == "call_options":
        hedge_candidates = same_side[same_side["strike"] > sell_strike].copy()
        hedge_candidates = hedge_candidates.sort_values("strike", ascending=True)

    elif option_type == "put_options":
        hedge_candidates = same_side[same_side["strike"] < sell_strike].copy()
        hedge_candidates = hedge_candidates.sort_values("strike", ascending=False)

    else:
        return None

    if hedge_candidates.empty:
        return None

    max_premium = hedge_candidates["mark_price"].max()
    max_oi = hedge_candidates["oi"].max()

    best_candidate = None
    best_score = -1

    for _, hedge in hedge_candidates.head(5).iterrows():
        hedge_premium = _safe_float(hedge.get("mark_price"))
        hedge_oi = _safe_float(hedge.get("oi"))
        hedge_gamma = _safe_float(hedge.get("gamma"))

        cheapness_score = 1 - _premium_score(hedge_premium, max_premium)
        liquidity_score = _oi_score(hedge_oi, max_oi)
        gamma_protection_score = _gamma_risk_penalty(hedge_gamma)

        strike_gap = abs(_safe_float(hedge.get("strike")) - sell_strike)

        if strike_gap <= 40:
            distance_efficiency = 1.00
        elif strike_gap <= 80:
            distance_efficiency = 0.85
        elif strike_gap <= 120:
            distance_efficiency = 0.65
        else:
            distance_efficiency = 0.40

        hedge_score = (
            cheapness_score * 0.35
            + distance_efficiency * 0.35
            + gamma_protection_score * 0.20
            + liquidity_score * 0.10
        )

        if hedge_score > best_score:
            best_score = hedge_score
            best_candidate = hedge

    if best_candidate is None:
        return None

    return {
        "hedge_strike": best_candidate.get("strike"),
        "hedge_premium": best_candidate.get("mark_price"),
        "hedge_delta": best_candidate.get("delta"),
        "hedge_score": round(best_score * 10, 2)
    }


def get_strike_recommendations(df, spot_price, expected_move_by_expiry=None, top_n=3):
    """
    Returns best risk-adjusted option selling + hedge recommendations
    across all expiries.

    expected_move_by_expiry should be a dict:
    {
        expiry: expected_move
    }
    """

    if df is None or df.empty or not spot_price:
        return pd.DataFrame()

    data = df.copy()

    numeric_cols = [
        "strike", "mark_price", "oi", "volume",
        "iv", "delta", "gamma", "theta", "vega"
    ]

    for col in numeric_cols:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    results = []

    for expiry in sorted(data["expiry"].dropna().unique()):
        expiry_df = data[data["expiry"] == expiry].copy()

        if expiry_df.empty:
            continue

        expected_move = None

        if expected_move_by_expiry:
            expected_move = expected_move_by_expiry.get(expiry)

        otm_df = expiry_df[
            expiry_df.apply(lambda row: _is_otm(row, spot_price), axis=1)
        ].copy()

        if otm_df.empty:
            continue

        max_oi = otm_df["oi"].max()
        max_premium = otm_df["mark_price"].max()
        max_iv = otm_df["iv"].max()

        for _, row in otm_df.iterrows():
            option_type = row.get("type")

            oi = _safe_float(row.get("oi"))
            premium = _safe_float(row.get("mark_price"))
            iv = _safe_float(row.get("iv"))
            delta = _safe_float(row.get("delta"))
            gamma = _safe_float(row.get("gamma"))
            strike = _safe_float(row.get("strike"))

            oi_component = _oi_score(oi, max_oi)
            distance_component = _distance_score(
                strike,
                spot_price,
                option_type,
                expected_move
            )
            delta_component = _delta_safety_score(delta, option_type)
            premium_component = _premium_score(premium, max_premium)
            iv_component = _iv_score(iv, max_iv)
            gamma_component = _gamma_risk_penalty(gamma)

            sell_score = (
                oi_component * 0.20
                + distance_component * 0.25
                + delta_component * 0.25
                + premium_component * 0.15
                + iv_component * 0.10
                + gamma_component * 0.05
            )

            hedge = _find_best_hedge(expiry_df, row)

            if hedge is None:
                continue

            side = "CALL SELL" if option_type == "call_options" else "PUT SELL"

            reason_parts = []

            if distance_component >= 0.8:
                reason_parts.append("outside/near expected move")
            if delta_component >= 0.8:
                reason_parts.append("safe delta zone")
            if oi_component >= 0.7:
                reason_parts.append("strong OI")
            if premium_component >= 0.5:
                reason_parts.append("decent premium")
            if gamma_component >= 0.8:
                reason_parts.append("low gamma risk")

            reason = " + ".join(reason_parts) if reason_parts else "balanced risk-adjusted setup"

            results.append({
                "expiry": expiry,
                "side": side,
                "sell_strike": strike,
                "hedge_strike": hedge["hedge_strike"],
                "sell_premium": round(premium, 2),
                "hedge_premium": round(_safe_float(hedge["hedge_premium"]), 2),
                "net_credit": round(premium - _safe_float(hedge["hedge_premium"]), 2),
                "delta": round(delta, 4),
                "iv": round(iv, 2),
                "oi": round(oi, 2),
                "sell_score": round(sell_score * 10, 2),
                "hedge_score": hedge["hedge_score"],
                "reason": reason
            })

    result_df = pd.DataFrame(results)

    if result_df.empty:
        return result_df

    result_df = result_df.sort_values("sell_score", ascending=False)

    return result_df.head(top_n)