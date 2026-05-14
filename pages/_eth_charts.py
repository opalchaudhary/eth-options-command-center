import streamlit as st

from database_reader import (
    get_latest_ohlcv_data,
    get_market_events,
    get_smc_zones,
    get_volume_profile,
)

from chart_engine import (
    create_eth_candlestick_chart,
    create_volume_profile_chart,
)


st.set_page_config(
    page_title="ETH Chart",
    layout="wide",
)

st.title("ETH Smart Money Chart")

st.caption("5-minute ETH candles + SMC zones from Supabase")

ohlcv_df = get_latest_ohlcv_data(
    symbol="ETHUSD",
    resolution="5m",
    limit=300,
)

events_df = get_market_events(
    symbol="ETHUSD",
    resolution="5m",
    limit=200,
)

zones_df = get_smc_zones(
    symbol="ETHUSD",
    resolution="5m",
    status="active",
    limit=200,
)

profile_df = get_volume_profile(
    symbol="ETHUSD",
    resolution="5m",
    limit=100,
)

if ohlcv_df.empty:
    st.warning("No OHLCV data available in database yet.")
else:
    chart_fig = create_eth_candlestick_chart(
        ohlcv_df,
        events_df=events_df,
        zones_df=zones_df,
        title="ETHUSD 5m Smart Money Chart",
    )

    if chart_fig:
        st.plotly_chart(chart_fig, use_container_width=True)
    else:
        st.warning("Unable to create ETH chart.")

st.divider()

st.subheader("ETH Volume Profile")

volume_fig = create_volume_profile_chart(
    profile_df,
    title="ETH Approximate Volume Profile",
)

if volume_fig:
    st.plotly_chart(volume_fig, use_container_width=True)
else:
    st.warning("No volume profile data available.")