import streamlit as st
from streamlit_autorefresh import st_autorefresh
from strike_engine import get_strike_recommendations

from delta_api import get_eth_options, get_eth_spot_price

from analytics import (
    basic_expiry_analytics,
    calculate_max_pain,
    calculate_atm_and_expected_move
)

from storage import (
    save_analytics_snapshot,
    save_premium_decay_snapshot,
    save_option_chain_snapshot
)


st.set_page_config(
    page_title="ETH Options Command Center",
    layout="wide"
)


st.title("ETH Options Command Center")
st.caption("Clean ETH options dashboard powered by Delta Exchange + Supabase")

st_autorefresh(interval=60 * 1000, key="eth_options_refresh")

df = get_eth_options()
eth_price_data = get_eth_spot_price()

if df.empty:
    st.warning("No ETH option data found.")
    st.stop()

eth_spot_price = eth_price_data.get("spot_price")
eth_mark_price = eth_price_data.get("mark_price")

expiry_list = sorted(df["expiry"].dropna().unique())

selected_expiry = st.sidebar.selectbox(
    "Select Expiry",
    expiry_list
)

st.sidebar.caption("Auto-refresh: every 60 seconds")

expiry_df = df[df["expiry"] == selected_expiry].copy()

analytics = basic_expiry_analytics(expiry_df)
max_pain, pain_df = calculate_max_pain(expiry_df)

atm_strike, expected_move, atm_ce_price, atm_pe_price = calculate_atm_and_expected_move(
    expiry_df,
    eth_spot_price
)

expected_move_pct = None
expected_move_upper = None
expected_move_lower = None

if eth_spot_price and expected_move:
    expected_move_pct = (expected_move / eth_spot_price) * 100
    expected_move_upper = eth_spot_price + expected_move
    expected_move_lower = eth_spot_price - expected_move

snapshot_analytics = {
    "spot_price": eth_spot_price,
    "max_pain": max_pain,
    "atm_strike": atm_strike,
    "pcr": analytics.get("pcr"),
    "atm_straddle_price": expected_move,
    "expected_move_pct": expected_move_pct,
    "expected_move_upper": expected_move_upper,
    "expected_move_lower": expected_move_lower
}

try:
    save_analytics_snapshot(snapshot_analytics, selected_expiry)

    save_premium_decay_snapshot(
        selected_expiry,
        atm_strike,
        atm_ce_price,
        atm_pe_price,
        expected_move
    )

    save_option_chain_snapshot(
        expiry_df,
        selected_expiry
    )

    st.sidebar.success("Database updated")

except Exception as e:
    st.sidebar.warning(f"Supabase snapshot not saved: {e}")


st.subheader(f"Market Overview — {selected_expiry}")

price_col1, price_col2, price_col3 = st.columns(3)

with price_col1:
    st.metric(
        "ETH Spot Price",
        f"${eth_spot_price:,.2f}" if eth_spot_price else "NA"
    )

with price_col2:
    st.metric(
        "ETH Mark Price",
        f"${eth_mark_price:,.2f}" if eth_mark_price else "NA"
    )

with price_col3:
    st.metric(
        "Price Source",
        eth_price_data.get("symbol", "ETHUSD")
    )


st.divider()

st.subheader("Core Options Structure")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Put Call Ratio",
    round(analytics["pcr"], 2) if analytics["pcr"] else "NA"
)

col2.metric(
    "Max Pain",
    max_pain
)

col3.metric(
    "ATM Strike",
    atm_strike
)

col4.metric(
    "Expected Move %",
    f"{expected_move_pct:.2f}%" if expected_move_pct else "NA"
)


col5, col6, col7, col8 = st.columns(4)

col5.metric(
    "Highest Call OI",
    analytics["highest_call_oi_strike"]
)

col6.metric(
    "Highest Put OI",
    analytics["highest_put_oi_strike"]
)

col7.metric(
    "ATM CE Price",
    round(atm_ce_price, 2) if atm_ce_price else "NA"
)

col8.metric(
    "ATM PE Price",
    round(atm_pe_price, 2) if atm_pe_price else "NA"
)


st.divider()

st.subheader("Greeks Snapshot")

g1, g2, g3, g4 = st.columns(4)

g1.metric("Net Delta", round(analytics["net_delta"], 4))
g2.metric("Net Gamma", round(analytics["net_gamma"], 6))
g3.metric("Net Theta", round(analytics["net_theta"], 4))
g4.metric("Net Vega", round(analytics["net_vega"], 4))


st.divider()

st.subheader("Expected Range")

if eth_spot_price and expected_move:
    r1, r2, r3 = st.columns(3)

    r1.metric(
        "Lower Range",
        f"${expected_move_lower:,.2f}"
    )

    r2.metric(
        "Current Spot",
        f"${eth_spot_price:,.2f}"
    )

    r3.metric(
        "Upper Range",
        f"${expected_move_upper:,.2f}"
    )

    st.caption(
        f"ATM is calculated using real ETH spot price: ${eth_spot_price:,.2f}. "
        f"Nearest available option strike selected: {atm_strike}."
    )
else:
    st.warning("Expected range unavailable for this expiry.")

st.divider()

st.divider()

st.subheader("Live Strike Recommendation Engine")
st.caption("Best risk-adjusted option selling + hedge opportunities across all expiries.")

strike_recommendations = get_strike_recommendations(
    df,
    eth_spot_price,
    top_n=3
)

if not strike_recommendations.empty:
    rec_cols = st.columns(3)

    for index, row in strike_recommendations.reset_index(drop=True).iterrows():
        with rec_cols[index % 3]:
            with st.container(border=True):
                st.markdown(f"### #{index + 1} — {row['side']}")
                st.caption(f"Expiry: {row['expiry']}")

                st.markdown("#### Position Structure")

                p1, p2 = st.columns(2)
                p1.metric("Sell Strike", row["sell_strike"])
                p2.metric("Hedge Strike", row["hedge_strike"])

                p3, p4 = st.columns(2)
                p3.metric("Sell Premium", row["sell_premium"])
                p4.metric("Hedge Premium", row["hedge_premium"])

                st.metric("Net Credit", row["net_credit"])

                st.markdown("#### Scores")

                s1, s2 = st.columns(2)
                s1.metric("Sell Score", row["sell_score"])
                s2.metric("Hedge Score", row["hedge_score"])

                st.markdown("#### Market Data")

                m1, m2 = st.columns(2)
                m1.metric("Delta", row["delta"])
                m2.metric("IV", row["iv"])

                m3, m4 = st.columns(2)
                m3.metric("Open Interest", row["oi"])
                m4.metric("Side", row["side"])

                st.markdown("#### Reasoning")
                st.info(row["reason"])

else:
    st.warning("No strike recommendations available right now.")
    
    st.info(
    "Use the sidebar navigation for Charts, Option Chain, and Insights. "
    "This home page is intentionally kept clean for quick market reading."
)