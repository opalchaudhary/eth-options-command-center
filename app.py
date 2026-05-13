import streamlit as st

from delta_api import get_eth_options

from analytics import (
    basic_expiry_analytics,
    calculate_max_pain,
    calculate_atm_and_expected_move
)

from rules_engine import generate_rule_based_insights
from strategy_engine import suggest_strategy


st.set_page_config(
    page_title="ETH Options Command Center",
    layout="wide"
)

st.title("ETH Options Command Center")

df = get_eth_options()

if df.empty:
    st.warning("No ETH option data found.")
    st.stop()

expiry_list = sorted(df["expiry"].dropna().unique())

selected_expiry = st.sidebar.selectbox(
    "Select Expiry",
    expiry_list
)

expiry_df = df[df["expiry"] == selected_expiry].copy()

analytics = basic_expiry_analytics(expiry_df)

max_pain, pain_df = calculate_max_pain(expiry_df)

atm_strike, expected_move, atm_ce_price, atm_pe_price = calculate_atm_and_expected_move(expiry_df)

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

st.subheader(f"ETH Options Analytics: {selected_expiry}")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Put Call Ratio",
    round(analytics["pcr"], 2) if analytics["pcr"] else "NA"
)

col2.metric(
    "Highest Call OI Strike",
    analytics["highest_call_oi_strike"]
)

col3.metric(
    "Highest Put OI Strike",
    analytics["highest_put_oi_strike"]
)

col4.metric(
    "Max Pain",
    max_pain
)

col5, col6, col7, col8 = st.columns(4)

col5.metric(
    "Net Delta",
    round(analytics["net_delta"], 4)
)

col6.metric(
    "Net Gamma",
    round(analytics["net_gamma"], 6)
)

col7.metric(
    "Net Theta",
    round(analytics["net_theta"], 4)
)

col8.metric(
    "Net Vega",
    round(analytics["net_vega"], 4)
)

col9, col10, col11, col12 = st.columns(4)

col9.metric(
    "ATM Strike",
    atm_strike
)

col10.metric(
    "Expected Move",
    round(expected_move, 2) if expected_move else "NA"
)

col11.metric(
    "ATM CE Price",
    round(atm_ce_price, 2) if atm_ce_price else "NA"
)

col12.metric(
    "ATM PE Price",
    round(atm_pe_price, 2) if atm_pe_price else "NA"
)

st.subheader("Rule-Based Market Insights")

for insight in insights:
    st.info(insight)

st.subheader("Strategy Suggestions")

if strategy_suggestions:
    for strategy in strategy_suggestions:
        st.success(f"Strategy: {strategy['strategy']}")
        st.write(f"Reason: {strategy['reason']}")
        st.write(f"Market View: {strategy['market_view']}")

        for key, value in strategy.items():
            if key not in ["strategy", "reason", "market_view"]:
                if isinstance(value, float):
                    st.write(f"{key}: {round(value, 2)}")
                else:
                    st.write(f"{key}: {value}")

        st.markdown("---")
else:
    st.warning("No strong strategy suggestion available for this expiry.")

st.subheader("Option Chain")

st.dataframe(
    expiry_df,
    use_container_width=True
)

st.subheader("Open Interest by Strike")

oi_chart = (
    expiry_df
    .groupby(["strike", "type"], as_index=False)["oi"]
    .sum()
)

st.bar_chart(
    oi_chart
    .pivot(index="strike", columns="type", values="oi")
    .fillna(0)
)

st.subheader("Max Pain Curve")

if not pain_df.empty:
    st.line_chart(
        pain_df.set_index("strike")["pain"]
    )