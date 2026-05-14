import streamlit as st

from delta_api import get_eth_options, get_eth_spot_price

from analytics import (
    basic_expiry_analytics,
    calculate_max_pain,
    calculate_atm_and_expected_move
)


st.set_page_config(
    page_title="Charts | ETH Options Command Center",
    layout="wide"
)

st.title("📊 Charts")
st.caption("Visual view of ETH options positioning, OI structure, and max pain.")

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

st.subheader(f"Charts for Expiry: {selected_expiry}")

c1, c2, c3, c4 = st.columns(4)

c1.metric("ETH Spot", f"${eth_spot_price:,.2f}" if eth_spot_price else "NA")
c2.metric("Max Pain", max_pain)
c3.metric("ATM Strike", atm_strike)
c4.metric("PCR", round(analytics["pcr"], 2) if analytics["pcr"] else "NA")

st.divider()

st.subheader("Open Interest by Strike")

oi_chart = (
    expiry_df
    .groupby(["strike", "type"], as_index=False)["oi"]
    .sum()
)

oi_pivot = (
    oi_chart
    .pivot(index="strike", columns="type", values="oi")
    .fillna(0)
)

st.bar_chart(oi_pivot)

st.divider()

st.subheader("Max Pain Curve")

if not pain_df.empty:
    st.line_chart(
        pain_df.set_index("strike")["pain"]
    )
else:
    st.warning("Max pain curve unavailable.")

st.divider()

st.subheader("Premium by Strike")

premium_chart = (
    expiry_df
    .groupby(["strike", "type"], as_index=False)["mark_price"]
    .mean()
)

premium_pivot = (
    premium_chart
    .pivot(index="strike", columns="type", values="mark_price")
    .fillna(0)
)

st.line_chart(premium_pivot)

st.info(
    "Historical charts like premium decay, PCR trend, and max pain shift will be added after we create Supabase read functions."
)