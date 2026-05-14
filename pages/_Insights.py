import streamlit as st

from delta_api import get_eth_options, get_eth_spot_price

from analytics import (
    basic_expiry_analytics,
    calculate_max_pain,
    calculate_atm_and_expected_move
)

from rules_engine import generate_rule_based_insights
from strategy_engine import suggest_strategy


st.set_page_config(
    page_title="Insights | ETH Options Command Center",
    layout="wide"
)

st.title("🧠 Insights")
st.caption("Rule-based market interpretation and strategy suggestions.")

df = get_eth_options()
eth_price_data = get_eth_spot_price()

if df.empty:
    st.warning("No ETH option data found.")
    st.stop()

eth_spot_price = eth_price_data.get("spot_price")

expiry_list = sorted(df["expiry"].dropna().unique())

selected_expiry = st.sidebar.selectbox(
    "Select Expiry",
    expiry_list
)

expiry_df = df[df["expiry"] == selected_expiry].copy()

analytics = basic_expiry_analytics(expiry_df)
max_pain, pain_df = calculate_max_pain(expiry_df)

atm_strike, expected_move, atm_ce_price, atm_pe_price = calculate_atm_and_expected_move(
    expiry_df,
    eth_spot_price
)

insights = generate_rule_based_insights(
    analytics,
    max_pain,
    atm_strike,
    expected_move,
    atm_ce_price,
    atm_pe_price
)

strategy_suggestions = suggest_strategy(
    analytics,
    max_pain,
    atm_strike,
    expected_move,
    expiry_df
)

st.subheader(f"Insight Engine — {selected_expiry}")

c1, c2, c3, c4 = st.columns(4)

c1.metric("ETH Spot", f"${eth_spot_price:,.2f}" if eth_spot_price else "NA")
c2.metric("Max Pain", max_pain)
c3.metric("ATM Strike", atm_strike)
c4.metric("Expected Move", round(expected_move, 2) if expected_move else "NA")

st.divider()

st.subheader("Rule-Based Market Insights")

if insights:
    for insight in insights:
        st.info(insight)
else:
    st.warning("No rule-based insights available.")

st.divider()

st.subheader("Strategy Suggestions")

if strategy_suggestions:
    for strategy in strategy_suggestions:
        with st.container(border=True):
            st.success(f"Strategy: {strategy['strategy']}")
            st.write(f"Reason: {strategy['reason']}")
            st.write(f"Market View: {strategy['market_view']}")

            for key, value in strategy.items():
                if key not in ["strategy", "reason", "market_view"]:
                    if isinstance(value, float):
                        st.write(f"{key}: {round(value, 2)}")
                    else:
                        st.write(f"{key}: {value}")
else:
    st.warning("No strong strategy suggestion available for this expiry.")