import streamlit as st
from streamlit_autorefresh import st_autorefresh

import pandas as pd

from analytics import (
    basic_expiry_analytics,
    calculate_max_pain,
    calculate_atm_and_expected_move
)

from api_client import api_get, backend_url
from ui_styles import load_css


st.set_page_config(
    page_title="ETH Options Command Center",
    layout="wide"
)

load_css()

st.title("ETH Options Command Center")
st.caption("Clean ETH options dashboard powered by Delta Exchange -> FastAPI -> Supabase")

st_autorefresh(interval=5 * 60 * 1000, key="eth_options_refresh")


# --------------------------------------------------
# FETCH OPTIONS + SPOT DATA FROM FASTAPI
# --------------------------------------------------

try:
    option_chain_response = api_get("/option-chain", params={"limit": 500, "compact": True})
    eth_price_data = api_get("/market/eth")
except Exception as exc:
    st.error(f"FastAPI backend unavailable at {backend_url()}: {exc}")
    st.stop()

df = pd.DataFrame(option_chain_response.get("rows") or [])

if df.empty:
    st.warning("No ETH option data found.")
    st.stop()

eth_spot_price = eth_price_data.get("spot_price")
eth_mark_price = eth_price_data.get("mark_price")


# --------------------------------------------------
# SIDEBAR
# --------------------------------------------------

expiry_list = sorted(df["expiry"].dropna().unique())

selected_expiry = st.sidebar.selectbox(
    "Select Expiry",
    expiry_list
)

st.sidebar.caption("Auto-refresh: every 5 minutes")
st.sidebar.caption(f"Backend: {backend_url()}")


# --------------------------------------------------
# EXPIRY ANALYTICS
# --------------------------------------------------

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


# --------------------------------------------------
# MARKET OVERVIEW
# --------------------------------------------------

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


# --------------------------------------------------
# CORE OPTIONS STRUCTURE
# --------------------------------------------------

st.subheader("Core Options Structure")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Put Call Ratio",
    round(analytics["pcr"], 2) if analytics["pcr"] else "NA"
)

col2.metric("Max Pain", max_pain)

col3.metric("ATM Strike", atm_strike)

col4.metric(
    "Expected Move %",
    f"{expected_move_pct:.2f}%" if expected_move_pct else "NA"
)


col5, col6, col7, col8 = st.columns(4)

col5.metric("Highest Call OI", analytics["highest_call_oi_strike"])
col6.metric("Highest Put OI", analytics["highest_put_oi_strike"])

col7.metric(
    "ATM CE Price",
    round(atm_ce_price, 2) if atm_ce_price else "NA"
)

col8.metric(
    "ATM PE Price",
    round(atm_pe_price, 2) if atm_pe_price else "NA"
)


st.divider()


# --------------------------------------------------
# GREEKS SNAPSHOT
# --------------------------------------------------

st.subheader("Greeks Snapshot")

g1, g2, g3, g4 = st.columns(4)

g1.metric("Net Delta", round(analytics["net_delta"], 4))
g2.metric("Net Gamma", round(analytics["net_gamma"], 6))
g3.metric("Net Theta", round(analytics["net_theta"], 4))
g4.metric("Net Vega", round(analytics["net_vega"], 4))


st.divider()


# --------------------------------------------------
# EXPECTED RANGE
# --------------------------------------------------

st.subheader("Expected Range")

if eth_spot_price and expected_move:
    r1, r2, r3 = st.columns(3)

    r1.metric("Lower Range", f"${expected_move_lower:,.2f}")
    r2.metric("Current Spot", f"${eth_spot_price:,.2f}")
    r3.metric("Upper Range", f"${expected_move_upper:,.2f}")

    st.caption(
        f"ATM is calculated using real ETH spot price: ${eth_spot_price:,.2f}. "
        f"Nearest available option strike selected: {atm_strike}."
    )
else:
    st.warning("Expected range unavailable for this expiry.")


st.divider()


# --------------------------------------------------
# ETH PERPETUAL ORDER BOOK INTELLIGENCE
# --------------------------------------------------

st.subheader("ETH Perpetual Order Book Intelligence")
st.caption("Execution confirmation layer for strike selection, liquidity walls, spread quality, and trap risk.")

try:
    orderbook = eth_price_data.get("orderbook") or {}
    orderbook_insights = eth_price_data.get("orderbook_insights") or {}
    text_insights = eth_price_data.get("text_insights") or []

    if orderbook_insights.get("status") == "ok":

        ob1, ob2, ob3, ob4 = st.columns(4)

        ob1.metric(
            "Order Book Mid Price",
            f"${orderbook_insights['mid_price']:,.2f}"
        )

        ob2.metric(
            "Order Book Bias",
            orderbook_insights["bias"]
        )

        ob3.metric(
            "Imbalance Ratio",
            orderbook_insights["imbalance_ratio"]
        )

        ob4.metric(
            "Spread Quality",
            orderbook_insights["spread_quality"]
        )

        ob5, ob6, ob7, ob8 = st.columns(4)

        ob5.metric(
            "Best Bid",
            f"${orderbook_insights['best_bid']:,.2f}"
        )

        ob6.metric(
            "Best Ask",
            f"${orderbook_insights['best_ask']:,.2f}"
        )

        ob7.metric(
            "Spread %",
            f"{orderbook_insights['spread_pct']}%"
        )

        ob8.metric(
            "Trap Risk",
            orderbook_insights["trap_risk"]
        )

        st.markdown("#### Liquidity Walls")

        wall1, wall2 = st.columns(2)

        with wall1:
            with st.container(border=True):
                st.markdown("### Bid Wall / Support Zone")
                st.metric(
                    "Nearest Bid Wall Price",
                    f"${orderbook_insights['nearest_bid_wall_price']:,.2f}"
                )
                st.metric(
                    "Bid Wall Size",
                    orderbook_insights["nearest_bid_wall_size"]
                )

        with wall2:
            with st.container(border=True):
                st.markdown("### Ask Wall / Resistance Zone")
                st.metric(
                    "Nearest Ask Wall Price",
                    f"${orderbook_insights['nearest_ask_wall_price']:,.2f}"
                )
                st.metric(
                    "Ask Wall Size",
                    orderbook_insights["nearest_ask_wall_size"]
                )

        st.markdown("#### Execution Signal")

        st.info(orderbook_insights["execution_signal"])

        st.markdown("#### Text-Based Order Book Insights")

        for insight in text_insights:
            st.write(f"• {insight}")

        with st.expander("View Raw ETH Perpetual Order Book"):
            bid_col, ask_col = st.columns(2)

            with bid_col:
                st.markdown("#### Top Bids")
                bids_df = pd.DataFrame(orderbook.get("bids") or [])
                if not bids_df.empty:
                    st.dataframe(
                        bids_df[["price", "size"]].head(10),
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.warning("No bid data available.")

            with ask_col:
                st.markdown("#### Top Asks")
                asks_df = pd.DataFrame(orderbook.get("asks") or [])
                if not asks_df.empty:
                    st.dataframe(
                        asks_df[["price", "size"]].head(10),
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.warning("No ask data available.")

    else:
        st.warning(orderbook_insights.get("message", "Order book data unavailable."))

except Exception as e:
    st.warning(f"Order book intelligence unavailable: {e}")


st.divider()

st.info(
    "Use the sidebar navigation for Charts, Option Chain, Insights, Paper Trading, Futures Trading, and Alt Futures Scanner. "
    "This home page is intentionally kept clean for quick market reading."
)

