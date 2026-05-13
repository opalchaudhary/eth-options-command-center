import pandas as pd


def nearest_strike(strikes, target):
    strikes = [float(x) for x in strikes if pd.notna(x)]
    return min(strikes, key=lambda x: abs(x - target))


def suggest_strategy(
    analytics,
    max_pain,
    atm_strike,
    expected_move,
    expiry_df
):
    suggestions = []

    pcr = analytics.get("pcr")
    net_gamma = analytics.get("net_gamma")
    net_delta = analytics.get("net_delta")

    expiry_df = expiry_df.copy()
    expiry_df["strike"] = pd.to_numeric(expiry_df["strike"], errors="coerce")

    strikes = sorted(expiry_df["strike"].dropna().unique())

    if not strikes or atm_strike is None or expected_move is None:
        return suggestions

    atm_strike = float(atm_strike)
    expected_move = float(expected_move)

    if max_pain is not None:
        max_pain = float(max_pain)

    lower_target = atm_strike - expected_move
    upper_target = atm_strike + expected_move

    lower_wing = nearest_strike(strikes, lower_target)
    upper_wing = nearest_strike(strikes, upper_target)

    max_pain_distance = abs(max_pain - atm_strike) if max_pain is not None else None

    if (
        net_gamma is not None
        and net_gamma > 0
        and max_pain_distance is not None
        and max_pain_distance <= expected_move * 0.35
    ):
        suggestions.append({
            "strategy": "Iron Fly",
            "reason": "Positive gamma and Max Pain close to ATM suggest controlled expiry and possible pinning.",
            "market_view": "Range-bound / theta decay",
            "sell_call": atm_strike,
            "sell_put": atm_strike,
            "buy_call": upper_wing,
            "buy_put": lower_wing
        })

    if (
        net_gamma is not None
        and net_gamma > 0
        and expected_move < atm_strike * 0.04
        and max_pain_distance is not None
        and max_pain_distance <= expected_move * 0.30
    ):
        suggestions.append({
            "strategy": "Naked Straddle",
            "reason": "Compressed expected move, positive gamma, and Max Pain close to ATM suggest possible expiry pinning. High risk strategy.",
            "market_view": "Compressed / high-risk theta selling",
            "sell_call": atm_strike,
            "sell_put": atm_strike
        })

    if net_delta is not None and net_delta > 1:
        suggestions.append({
            "strategy": "Bull Call Debit Spread",
            "reason": "Net Delta is strongly positive, showing bullish directional pressure.",
            "market_view": "Bullish",
            "buy_call": atm_strike,
            "sell_call": upper_wing
        })

    if net_delta is not None and net_delta < -1:
        suggestions.append({
            "strategy": "Bear Put Debit Spread",
            "reason": "Net Delta is strongly negative, showing bearish directional pressure.",
            "market_view": "Bearish",
            "buy_put": atm_strike,
            "sell_put": lower_wing
        })

    if pcr is not None:
        if pcr < 0.7:
            suggestions.append({
                "strategy": "Bear Call Credit Spread",
                "reason": "Low PCR shows Call OI dominance, which may create upside resistance.",
                "market_view": "Neutral to bearish",
                "sell_call": upper_wing,
                "buy_call": nearest_strike(strikes, upper_wing + expected_move * 0.5)
            })

        elif pcr > 1.3:
            suggestions.append({
                "strategy": "Bull Put Credit Spread",
                "reason": "High PCR shows Put OI dominance, which may create downside support.",
                "market_view": "Neutral to bullish",
                "sell_put": lower_wing,
                "buy_put": nearest_strike(strikes, lower_wing - expected_move * 0.5)
            })

    if net_gamma is not None and net_gamma < 0:
        mid_put = nearest_strike(strikes, atm_strike - expected_move * 0.5)
        lower_put = nearest_strike(strikes, atm_strike - expected_move)
        far_lower_put = nearest_strike(strikes, atm_strike - expected_move * 1.5)

        suggestions.append({
            "strategy": "Put Broken Wing Butterfly",
            "reason": "Negative gamma suggests unstable movement. A broken-wing structure can create asymmetric downside positioning.",
            "market_view": "Volatile / bearish-risk protection",
            "buy_put_1": atm_strike,
            "sell_put_1": mid_put,
            "sell_put_2": lower_put,
            "buy_put_2": far_lower_put
        })

    return suggestions