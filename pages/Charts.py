import pandas as pd
import streamlit as st

from analytics import basic_expiry_analytics, calculate_atm_and_expected_move, calculate_max_pain
from chart_components import (
    render_iv_chart,
    render_iv_vs_rv_chart,
    render_liquidation_charts,
    render_max_pain_curve_chart,
    render_max_pain_shift_chart,
    render_oi_change_chart,
    render_open_interest_chart,
    render_pcr_trend_chart,
    render_premium_by_strike_chart,
    render_premium_decay_chart,
    render_smc_liquidity_zone_charts,
    render_spot_chart,
    render_volume_profile_chart,
    render_volume_vs_strike_chart,
)
from database_reader import (
    get_analytics_snapshots,
    get_latest_ohlcv_data,
    get_market_events,
    get_option_chain_snapshots,
    get_orderbook_insight_snapshots,
    get_premium_decay_snapshots,
    get_smc_zones,
    get_volume_profile,
)
from delta_api import get_eth_options, get_eth_spot_price
from ui_styles import load_css


st.set_page_config(
    page_title="Charts | ETH Options Command Center",
    layout="wide",
)

load_css()


@st.cache_data(ttl=60)
def load_options_data():
    return get_eth_options(), get_eth_spot_price()


@st.cache_data(ttl=60)
def load_market_structure_data(symbol, resolution, candle_limit, zone_limit):
    return (
        get_latest_ohlcv_data(symbol=symbol, resolution=resolution, limit=candle_limit),
        get_market_events(symbol=symbol, resolution=resolution, limit=zone_limit),
        get_smc_zones(symbol=symbol, resolution=resolution, status="active", limit=zone_limit),
        get_volume_profile(symbol=symbol, resolution=resolution, limit=150),
    )


@st.cache_data(ttl=60)
def load_history_data(expiry_label, symbol):
    return (
        get_analytics_snapshots(expiry_label=expiry_label, limit=500),
        get_premium_decay_snapshots(expiry_label=expiry_label, limit=500),
        get_option_chain_snapshots(expiry_label=expiry_label, limit=2500),
        get_orderbook_insight_snapshots(symbol=symbol, limit=500),
    )


def fmt_ist(value):
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")

    if pd.isna(timestamp):
        return str(value)

    return timestamp.tz_convert("Asia/Kolkata").strftime("%d %b %Y, %I:%M %p IST")


def chart_card(title, caption, fig, warning="Data not available yet for this chart."):
    with st.container(border=True):
        st.markdown(f"### {title}")

        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(warning)

        if caption:
            st.caption(caption)


def zone_summary_cards(zones_df):
    if zones_df is None or zones_df.empty:
        st.warning("Data not available yet for this chart.")
        return

    required = {"zone_type", "price_low", "price_high"}

    if not required.issubset(zones_df.columns):
        st.warning("Data not available yet for this chart.")
        return

    clean_df = zones_df.copy()
    clean_df["price_low"] = pd.to_numeric(clean_df["price_low"], errors="coerce")
    clean_df["price_high"] = pd.to_numeric(clean_df["price_high"], errors="coerce")
    clean_df = clean_df.dropna(subset=["price_low", "price_high"])

    if clean_df.empty:
        st.warning("Data not available yet for this chart.")
        return

    grouped = (
        clean_df.groupby("zone_type")
        .agg(
            count=("zone_type", "size"),
            low=("price_low", "min"),
            high=("price_high", "max"),
        )
        .reset_index()
        .sort_values("zone_type")
    )

    cols = st.columns(min(4, len(grouped)))

    for index, row in grouped.iterrows():
        with cols[index % len(cols)]:
            with st.container(border=True):
                st.markdown(f"#### {str(row['zone_type']).replace('_', ' ').title()}")
                st.metric("Zones", int(row["count"]))
                st.caption(f"{row['low']:,.2f} to {row['high']:,.2f}")


st.title("Charts")
st.caption("Unified ETH options, volatility, market structure, expiry, and liquidation dashboard.")

df, eth_price_data = load_options_data()

if df.empty:
    st.warning("No ETH option data found.")
    st.stop()

eth_spot_price = eth_price_data.get("spot_price")
symbol = st.sidebar.text_input("Symbol", value="ETHUSD")
resolution = st.sidebar.selectbox("Spot Resolution", ["5m", "15m", "1h"], index=0)
candle_limit = st.sidebar.slider("Spot Candles", min_value=100, max_value=1000, value=300, step=50)

expiry_list = sorted(df["expiry"].dropna().unique())
selected_expiry = st.sidebar.selectbox(
    "Select Expiry",
    expiry_list,
    format_func=fmt_ist,
)

expiry_df = df[df["expiry"] == selected_expiry].copy()

analytics = basic_expiry_analytics(expiry_df)
max_pain, _ = calculate_max_pain(expiry_df)
atm_strike, expected_move, atm_ce_price, atm_pe_price = calculate_atm_and_expected_move(
    expiry_df,
    eth_spot_price,
)

ohlcv_df, events_df, zones_df, profile_df = load_market_structure_data(
    symbol=symbol,
    resolution=resolution,
    candle_limit=candle_limit,
    zone_limit=250,
)
analytics_history_df, premium_decay_df, option_snapshot_df, orderbook_history_df = load_history_data(
    expiry_label=selected_expiry,
    symbol=symbol,
)

st.subheader(f"Selected Expiry: {fmt_ist(selected_expiry)}")

metric_cols = st.columns(5)
metric_cols[0].metric("ETH Spot", f"${eth_spot_price:,.2f}" if eth_spot_price else "NA")
metric_cols[1].metric("ATM Strike", atm_strike if atm_strike is not None else "NA")
metric_cols[2].metric("Max Pain", max_pain if max_pain is not None else "NA")
metric_cols[3].metric("PCR", f"{analytics['pcr']:.2f}" if analytics.get("pcr") else "NA")
metric_cols[4].metric("ATM Straddle", f"{expected_move:.2f}" if expected_move else "NA")

st.caption(
    "This page replaces the older separate Charts and ETH Smart Money Chart pages with one consolidated analytics view."
)

tabs = st.tabs(
    [
        "Options Structure",
        "Volatility",
        "Market Structure",
        "Expiry Intelligence",
        "Liquidation",
    ]
)

with tabs[0]:
    left, right = st.columns(2)

    with left:
        chart_card(
            "Open Interest",
            "CE and PE open interest by strike, with ATM and maximum OI zones highlighted when available.",
            render_open_interest_chart(expiry_df, atm_strike=atm_strike),
        )
        chart_card(
            "Volume vs Strike",
            "Active strike zones often show up first through concentrated traded volume.",
            render_volume_vs_strike_chart(expiry_df, atm_strike=atm_strike),
        )
        chart_card(
            "Premium Decay",
            "ATM CE, PE, and straddle premium decay from saved premium snapshots.",
            render_premium_decay_chart(premium_decay_df),
        )

    with right:
        chart_card(
            "Open Interest Change",
            "Positive bars show fresh positioning from the earliest to latest stored snapshot for this expiry.",
            render_oi_change_chart(
                expiry_df,
                snapshot_df=option_snapshot_df,
                atm_strike=atm_strike,
            ),
        )
        chart_card(
            "Premium by Strike",
            "CE and PE mark prices across strikes for the selected expiry.",
            render_premium_by_strike_chart(expiry_df, atm_strike=atm_strike),
        )

with tabs[1]:
    left, right = st.columns(2)

    with left:
        chart_card(
            "IV",
            "CE and PE implied volatility by strike to make skew easier to scan.",
            render_iv_chart(expiry_df, atm_strike=atm_strike),
        )

    with right:
        chart_card(
            "IV vs RV and IV-RV",
            "Realized volatility is estimated from stored OHLCV returns; the spread highlights rich or cheap implied volatility.",
            render_iv_vs_rv_chart(expiry_df, ohlcv_df),
        )

with tabs[2]:
    chart_card(
        "Spot Chart",
        "Clean ETH spot chart using stored OHLCV data. SMC zones are separated below to keep price action readable.",
        render_spot_chart(ohlcv_df),
    )

    left, right = st.columns([0.52, 0.48])

    with left:
        chart_card(
            "Volume Profile",
            "Horizontal volume distribution by price zone. The highest volume node is marked when available.",
            render_volume_profile_chart(profile_df),
        )

    with right:
        chart_card(
            "Liquidity and SMC Zones",
            "Separate zone visualization for liquidity, order blocks, fair value gaps, and related SMC levels.",
            render_smc_liquidity_zone_charts(zones_df),
        )

    with st.expander("SMC Zone Summary", expanded=False):
        zone_summary_cards(zones_df)

with tabs[3]:
    left, right = st.columns(2)

    with left:
        chart_card(
            "PCR Trend",
            "OI-based PCR from analytics snapshots, falling back to chain snapshots when needed.",
            render_pcr_trend_chart(analytics_history_df, option_snapshot_df=option_snapshot_df),
        )
        chart_card(
            "Max Pain Curve",
            "Current expiry pain curve with the calculated minimum pain strike highlighted.",
            render_max_pain_curve_chart(expiry_df),
        )

    with right:
        chart_card(
            "Max Pain Shift",
            "Historical max pain movement from saved analytics snapshots.",
            render_max_pain_shift_chart(analytics_history_df),
        )

with tabs[4]:
    chart_card(
        "Liquidation Zones",
        "Uses saved order book liquidity walls as an approximation when dedicated liquidation heatmap data is unavailable.",
        render_liquidation_charts(orderbook_history_df),
    )

    if events_df is not None and not events_df.empty:
        with st.expander("Market Event Context", expanded=False):
            st.dataframe(events_df.tail(50), use_container_width=True, hide_index=True)
