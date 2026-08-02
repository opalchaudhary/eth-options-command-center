import pandas as pd
import streamlit as st

from analytics import basic_expiry_analytics, calculate_atm_and_expected_move, calculate_max_pain
from chart_components import (
    render_composite_liquidation_heatmap,
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
from database_reader import get_latest_ohlcv_data, get_market_events, get_smc_zones, get_volume_profile
from api_client import api_get, backend_url
from liquidation_engine import build_composite_liquidation_heatmap
from ui_styles import load_css


st.set_page_config(
    page_title="Charts | ETH Options Command Center",
    layout="wide",
)

load_css()


@st.cache_data(ttl=60)
def load_options_data():
    option_chain = api_get("/option-chain", params={"limit": 500, "compact": True})
    market = api_get("/market/eth")
    return pd.DataFrame(option_chain.get("rows") or []), market


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
    response = api_get(
        "/charts",
        params={"expiry": expiry_label, "symbol": symbol, "limit": 300, "compact": True},
    )
    return (
        pd.DataFrame(response.get("analytics") or []),
        pd.DataFrame(response.get("premium_decay") or []),
        pd.DataFrame(response.get("option_chain") or []),
        pd.DataFrame(response.get("orderbook") or []),
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

    clean_df["mid_price"] = (clean_df["price_low"] + clean_df["price_high"]) / 2

    grouped = (
        clean_df.groupby("zone_type")
        .agg(
            count=("zone_type", "size"),
            low=("price_low", "min"),
            high=("price_high", "max"),
            nearest_mid=("mid_price", "median"),
        )
        .reset_index()
        .sort_values("zone_type")
    )
    grouped["label"] = grouped["zone_type"].astype(str).str.replace("_", " ").str.title()

    cols = st.columns(min(4, len(grouped)))

    for index, row in grouped.iterrows():
        with cols[index % len(cols)]:
            with st.container(border=True):
                st.markdown(f"#### {row['label']}")
                st.metric("Active Zones", int(row["count"]))
                st.caption(f"Range: {row['low']:,.2f} to {row['high']:,.2f}")
                st.caption(f"Median zone: {row['nearest_mid']:,.2f}")

    summary_df = grouped.rename(
        columns={
            "label": "Zone Type",
            "count": "Active Zones",
            "low": "Lowest Price",
            "high": "Highest Price",
            "nearest_mid": "Median Zone",
        }
    )[["Zone Type", "Active Zones", "Lowest Price", "Highest Price", "Median Zone"]]

    st.dataframe(
        summary_df,
        use_container_width=True,
        hide_index=True,
    )


def _format_zone(row):
    if row is None or row.empty:
        return "NA"
    return f"{row['zone_low']:,.0f} - {row['zone_high']:,.0f}"


def _top_zone(heatmap_df, direction=None):
    if heatmap_df is None or heatmap_df.empty:
        return None

    clean_df = heatmap_df.copy()
    if direction:
        clean_df = clean_df[clean_df["direction"] == direction]

    if clean_df.empty:
        return None

    return clean_df.sort_values("liquidation_magnet_score", ascending=False).iloc[0]


def liquidation_summary_cards(heatmap_df):
    if heatmap_df is None or heatmap_df.empty:
        return

    upside = _top_zone(heatmap_df, "upside_short_liquidation")
    downside = _top_zone(heatmap_df, "downside_long_liquidation")
    highest = _top_zone(heatmap_df)

    upside_score = upside["liquidation_magnet_score"] if upside is not None else 0
    downside_score = downside["liquidation_magnet_score"] if downside is not None else 0

    if upside_score > downside_score * 1.1:
        bias = "Upside squeeze risk"
    elif downside_score > upside_score * 1.1:
        bias = "Downside cascade risk"
    else:
        bias = "Two-way liquidation risk"

    cards = st.columns(4)
    card_data = [
        ("Top Upside Short-Liquidation Zone", upside),
        ("Top Downside Long-Liquidation Zone", downside),
        ("Highest Overall Magnet Zone", highest),
    ]

    for index, (title, row) in enumerate(card_data):
        with cards[index]:
            with st.container(border=True):
                st.markdown(f"#### {title}")
                if row is None:
                    st.metric("Zone", "NA")
                    st.caption("No usable data.")
                else:
                    st.metric("Zone", _format_zone(row), f"{row['liquidation_magnet_score']:.1f}")
                    st.caption(f"{row['confidence_level']} - {row['primary_reason']}")

    with cards[3]:
        with st.container(border=True):
            st.markdown("#### Current Market Liquidation Bias")
            st.metric("Bias", bias)
            st.caption(f"Upside {upside_score:.1f} / Downside {downside_score:.1f}")


def liquidation_breakdown_table(heatmap_df):
    if heatmap_df is None or heatmap_df.empty:
        return pd.DataFrame()

    display_cols = [
        "zone",
        "direction",
        "total score",
        "orderbook score",
        "OI score",
        "volatility score",
        "SMC score",
        "volume score",
        "gamma score",
        "funding/context score",
        "confidence",
        "primary reason",
    ]

    table_df = heatmap_df.copy()
    table_df["zone"] = table_df.apply(
        lambda row: f"{row['zone_low']:,.0f} - {row['zone_high']:,.0f}",
        axis=1,
    )
    table_df = table_df.rename(
        columns={
            "liquidation_magnet_score": "total score",
            "orderbook_score": "orderbook score",
            "oi_cluster_score": "OI score",
            "volatility_score": "volatility score",
            "smc_trap_score": "SMC score",
            "volume_imbalance_score": "volume score",
            "gamma_pressure_score": "gamma score",
            "funding_bias_score": "funding/context score",
            "confidence_level": "confidence",
            "primary_reason": "primary reason",
        }
    )

    return table_df[[col for col in display_cols if col in table_df.columns]].sort_values(
        "total score",
        ascending=False,
    )


def top_zone_warning_cards(heatmap_df):
    if heatmap_df is None or heatmap_df.empty:
        return

    top_rows = heatmap_df.sort_values("liquidation_magnet_score", ascending=False).head(3)
    cols = st.columns(len(top_rows))

    for index, (_, row) in enumerate(top_rows.iterrows()):
        with cols[index]:
            with st.container(border=True):
                st.markdown(f"#### {_format_zone(row)}")
                st.caption(row["trading_warning"])
                st.markdown(f"**Strategy Bias:** {row['strategy_bias']}")


st.title("Charts")
st.caption("Unified ETH options, volatility, market structure, expiry, and liquidation dashboard.")

try:
    df, eth_price_data = load_options_data()
except Exception as exc:
    st.error(f"FastAPI backend unavailable at {backend_url()}: {exc}")
    st.stop()

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
    composite_heatmap_df = build_composite_liquidation_heatmap(
        spot_price=eth_spot_price,
        orderbook_df=orderbook_history_df,
        ohlcv_df=ohlcv_df,
        expiry_df=expiry_df,
        option_snapshot_df=option_snapshot_df,
        analytics_history_df=analytics_history_df,
        smc_zones_df=zones_df,
        events_df=events_df,
        profile_df=profile_df,
        max_pain=max_pain,
    )

    composite_fig = render_composite_liquidation_heatmap(composite_heatmap_df)

    if composite_fig is not None:
        st.markdown("### Composite Liquidation Heatmap")
        liquidation_summary_cards(composite_heatmap_df)
        st.plotly_chart(composite_fig, use_container_width=True)

        st.caption(
            "This is a probabilistic liquidation pressure model derived from order book, options, OHLCV, volume, and SMC data. It is not exchange-provided liquidation data."
        )

        st.markdown("### Component Breakdown")
        st.dataframe(
            liquidation_breakdown_table(composite_heatmap_df),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("### Trading Warnings and Strategy Bias")
        top_zone_warning_cards(composite_heatmap_df)
    else:
        chart_card(
            "Composite Liquidation Heatmap",
            "Composite data is unavailable, so this falls back to saved order book liquidity walls only.",
            render_liquidation_charts(orderbook_history_df),
        )

    if events_df is not None and not events_df.empty:
        with st.expander("Market Event Context", expanded=False):
            st.dataframe(events_df.tail(50), use_container_width=True, hide_index=True)
