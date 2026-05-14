import streamlit as st

from delta_api import get_eth_options, get_eth_spot_price


st.set_page_config(
    page_title="Option Chain | ETH Options Command Center",
    layout="wide"
)

st.title("📋 Option Chain")
st.caption("Live ETH option-chain table with expiry and strike filters.")

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

st.subheader(f"Option Chain — {selected_expiry}")

c1, c2, c3 = st.columns(3)

c1.metric("ETH Spot", f"${eth_spot_price:,.2f}" if eth_spot_price else "NA")
c2.metric("Rows", len(expiry_df))
c3.metric("Available Strikes", expiry_df["strike"].nunique())

st.divider()

min_strike = int(expiry_df["strike"].min())
max_strike = int(expiry_df["strike"].max())

strike_range = st.slider(
    "Strike Range",
    min_value=min_strike,
    max_value=max_strike,
    value=(min_strike, max_strike),
    step=20
)

option_type_filter = st.multiselect(
    "Option Type",
    options=sorted(expiry_df["type"].dropna().unique()),
    default=sorted(expiry_df["type"].dropna().unique())
)

filtered_df = expiry_df[
    (expiry_df["strike"] >= strike_range[0]) &
    (expiry_df["strike"] <= strike_range[1]) &
    (expiry_df["type"].isin(option_type_filter))
].copy()

st.dataframe(
    filtered_df,
    use_container_width=True,
    height=650
)

st.caption(
    "Use this page for full table inspection. Charts and insights are separated into their own pages."
)