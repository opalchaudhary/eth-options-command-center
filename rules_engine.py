def generate_rule_based_insights(
    analytics,
    max_pain,
    atm_strike,
    expected_move,
    atm_ce_price,
    atm_pe_price
):
    insights = []

    pcr = analytics.get("pcr")
    net_delta = analytics.get("net_delta")
    net_gamma = analytics.get("net_gamma")
    net_theta = analytics.get("net_theta")
    highest_call_oi = analytics.get("highest_call_oi_strike")
    highest_put_oi = analytics.get("highest_put_oi_strike")

    if pcr is not None:
        if pcr < 0.7:
            insights.append("PCR is low. Call OI is dominating, which may create upside resistance or call-wall pressure.")
        elif pcr > 1.3:
            insights.append("PCR is high. Put OI is dominating, which may show downside fear or strong put-writing support.")
        else:
            insights.append("PCR is balanced. Option positioning is relatively neutral.")

    if highest_call_oi:
        insights.append(f"Highest Call OI is at {highest_call_oi}. This may behave as resistance / call-wall zone.")

    if highest_put_oi:
        insights.append(f"Highest Put OI is at {highest_put_oi}. This may behave as support / put-wall zone.")

    if max_pain is not None and atm_strike is not None and expected_move:
        distance = abs(max_pain - atm_strike)

        if distance <= expected_move * 0.35:
            insights.append(f"Max Pain is close to ATM. Pinning probability near {max_pain} is stronger.")
        else:
            insights.append(f"Max Pain is away from ATM. Pinning probability is weaker unless ETH moves toward {max_pain}.")

    if expected_move and atm_strike:
        lower_range = atm_strike - expected_move
        upper_range = atm_strike + expected_move

        insights.append(
            f"Expected range for this expiry is approximately {round(lower_range, 2)} to {round(upper_range, 2)}."
        )

    if net_gamma is not None:
        if net_gamma > 0:
            insights.append("Net Gamma is positive. Market may behave more controlled, range-bound, and mean-reverting.")
        elif net_gamma < 0:
            insights.append("Net Gamma is negative. Market may become unstable with sharper directional moves.")
        else:
            insights.append("Net Gamma is neutral. No clear gamma regime is visible.")

    if net_delta is not None:
        if net_delta > 1:
            insights.append("Net Delta is positive. Option chain shows bullish directional bias.")
        elif net_delta < -1:
            insights.append("Net Delta is negative. Option chain shows bearish directional pressure.")
        else:
            insights.append("Net Delta is near neutral. Directional pressure is limited.")

    if net_theta is not None:
        if net_theta < 0:
            insights.append("Net Theta is negative. Time decay is heavy; option sellers may benefit if price stays controlled.")
        elif net_theta > 0:
            insights.append("Net Theta is positive. This is unusual at chain level and should be reviewed carefully.")

    if atm_ce_price and atm_pe_price:
        if atm_pe_price > atm_ce_price * 1.2:
            insights.append("ATM Put premium is higher than ATM Call premium. Downside fear or bearish skew is present.")
        elif atm_ce_price > atm_pe_price * 1.2:
            insights.append("ATM Call premium is higher than ATM Put premium. Upside speculation or bullish skew is present.")
        else:
            insights.append("ATM Call and Put premiums are balanced. Short-term pricing is relatively neutral.")

    if (
        net_gamma is not None
        and net_gamma > 0
        and max_pain is not None
        and atm_strike is not None
        and expected_move
    ):
        if abs(max_pain - atm_strike) <= expected_move * 0.35:
            insights.append("Overall setup: Iron Fly / short premium environment looks favourable, but position size must remain controlled.")
        else:
            insights.append("Overall setup: Short premium is possible, but max pain is not strongly aligned with ATM.")

    if net_gamma is not None and net_gamma < 0:
        insights.append("Overall setup: Avoid aggressive naked short premium. Long gamma hedge may be needed.")

    return insights