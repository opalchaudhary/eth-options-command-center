import streamlit as st

from data_refresh import refresh_market_structure_sources
from rule_insights import (
    build_rule_based_insights,
    get_available_expiries,
)
from validation_engine import record_validation_cycle, validation_dashboard_data


st.set_page_config(
    page_title="Rule Based Insights | ETH Options Command Center",
    layout="wide",
)

st.title("Rule Based Insights")
st.caption("Deterministic ETH market read from Supabase snapshots")


expiry_list = get_available_expiries()

if not expiry_list:
    st.warning("No analytics snapshots found in Supabase yet.")
    st.stop()

selected_expiry = st.sidebar.selectbox(
    "Select Expiry",
    expiry_list,
    index=0,
)

if st.sidebar.button("Refresh Market Sources"):
    with st.spinner("Refreshing orderbook, OHLCV, SMC zones, events, and volume profile..."):
        refresh_result = refresh_market_structure_sources()

    if (
        refresh_result.get("orderbook_saved")
        and refresh_result.get("ohlcv_saved")
        and refresh_result.get("smc_saved")
    ):
        st.sidebar.success("Market sources refreshed")
        st.rerun()
    elif refresh_result.get("ohlcv_saved"):
        st.sidebar.warning("OHLCV saved, but one or more downstream sources did not refresh")
    else:
        st.sidebar.warning("OHLCV refresh failed")

insights = build_rule_based_insights(selected_expiry)

try:
    record_validation_cycle(insights)
except Exception as e:
    st.sidebar.warning(f"Validation layer not updated: {e}")


st.subheader(f"Rule Engine Output - {selected_expiry}")

top_cols = st.columns(5)

top_cols[0].metric("Market Regime", insights["market_regime"])
top_cols[1].metric("Volatility", insights["volatility_regime"])
top_cols[2].metric("Directional Bias", insights["directional_bias"])
top_cols[3].metric("Confidence", f"{insights['confidence_score']}/100")
top_cols[4].metric("Conflict", f"{insights['signal_conflict_score']}/100")

score_cols = st.columns(5)

score_cols[0].metric("Pinning Score", f"{insights['pinning_score']}/100")
score_cols[1].metric("Trap Risk", insights["trap_risk"])
score_cols[2].metric(
    "Expected Move",
    round(insights["expected_move"], 2) if insights["expected_move"] else "NA",
)
score_cols[3].metric("Option Selling", insights["option_selling_environment"])
score_cols[4].metric("Expiry Bucket", insights["expiry_profile"]["bucket"])

st.divider()

st.subheader("Strategy Recommendation")

st.markdown(f"**Recommended Strategy - {insights['best_strategy']}**")

if insights.get("strategy_text"):
    for leg in insights["strategy_text"]:
        st.write(leg)
else:
    st.warning("No executable strikes found for this strategy from the latest option-chain snapshot.")

strategy_cols = st.columns(2)

strategy_cols[0].metric(
    "Call Sell Strike",
    insights["best_call_sell_strike"] if insights["best_call_sell_strike"] else "NA",
)
strategy_cols[1].metric(
    "Put Sell Strike",
    insights["best_put_sell_strike"] if insights["best_put_sell_strike"] else "NA",
)

st.write(
    f"For this expiry, the rule engine prefers **{insights['best_strategy']}** "
    f"with a **{insights['directional_bias']}** bias and **{insights['trap_risk']}** trap risk."
)

st.divider()

st.subheader("Market Context")

context_cols = st.columns(3)

context_cols[0].metric(
    "ETH Spot",
    f"${insights['spot_price']:,.2f}" if insights["spot_price"] else "NA",
)
context_cols[1].metric(
    "ATM Strike",
    f"{insights['atm_strike']:,.0f}" if insights["atm_strike"] else "NA",
)
context_cols[2].metric(
    "Max Pain",
    f"{insights['max_pain']:,.0f}" if insights["max_pain"] else "NA",
)

greek_cols = st.columns(3)

greek_cols[0].metric(
    "PCR",
    round(insights["pcr"], 2) if insights["pcr"] else "NA",
)
greek_cols[1].metric(
    "Net Delta",
    round(insights["net_delta"], 4) if insights["net_delta"] is not None else "NA",
)
greek_cols[2].metric(
    "Net Gamma",
    round(insights["net_gamma"], 6) if insights["net_gamma"] is not None else "NA",
)

st.divider()

st.subheader("Key Insights")

for item in insights["key_insights"]:
    st.info(item)

st.divider()

with st.expander("Data Source Health"):
    source_rows = []
    source_labels = {
        "analytics": "analytics_snapshots",
        "option_chain": "option_chain_snapshots",
        "orderbook": "orderbook_insights",
        "premium_decay": "premium_decay_snapshots",
        "ohlcv": "eth_ohlcv",
        "market_events": "eth_market_events",
        "smc_zones": "eth_smc_zones",
        "volume_profile": "eth_volume_profile",
    }
    source_writers = {
        "analytics": "app.py saves this when the main dashboard is open",
        "option_chain": "app.py saves this when the main dashboard is open",
        "orderbook": "app.py or the sidebar refresh button saves this",
        "premium_decay": "app.py saves this when the main dashboard is open",
        "ohlcv": "ohlcv_job.py or the sidebar refresh button saves this",
        "market_events": "smc_job.py or the sidebar refresh button saves this after OHLCV exists",
        "smc_zones": "smc_job.py or the sidebar refresh button saves this after OHLCV exists",
        "volume_profile": "smc_job.py or the sidebar refresh button saves this after OHLCV exists",
    }

    for source, available in insights["data_flags"].items():
        source_rows.append(
            {
                "Source": source_labels.get(source, source),
                "Status": "Available" if available else "Missing / Empty",
                "How It Is Populated": source_writers.get(source, ""),
            }
        )

    st.dataframe(source_rows, use_container_width=True, hide_index=True)

    if insights.get("missing_sources"):
        st.warning("Missing or empty: " + ", ".join(insights["missing_sources"]))
    else:
        st.success("All rule-engine source tables are available.")

st.divider()

st.subheader("Risk Warnings")

for warning in insights["risk_warnings"]:
    st.warning(warning)

st.divider()

st.subheader("Validation & Paper Trading")
st.caption("Validation-ready tracking using 1 USDT = ₹85 and Delta ETH lot size = 0.01 ETH.")

try:
    validation_data = validation_dashboard_data()
    performance = validation_data["performance"]

    perf_cols = st.columns(5)
    perf_cols[0].metric("Open Trades", performance.get("open_count", 0))
    perf_cols[1].metric("Closed Trades", performance.get("closed_count", 0))
    perf_cols[2].metric("Win Rate", f"{performance.get('win_rate', 0)}%")
    perf_cols[3].metric("Avg P&L", f"{performance.get('average_pnl_usdt', 0)} USDT")
    perf_cols[4].metric("Avg P&L INR", f"₹{performance.get('average_pnl_inr', 0)}")

    tab_recs, tab_open, tab_closed, tab_perf = st.tabs(
        [
            "Latest Recommendations",
            "Open Paper Trades",
            "Closed Paper Trades",
            "Performance",
        ]
    )

    with tab_recs:
        recs = validation_data["latest_recommendations"]
        if recs.empty:
            st.info("No recommendation journal rows found yet. Run the SQL migration first if tables are missing.")
        else:
            cols = [
                "created_at",
                "expiry_label",
                "spot_price",
                "market_regime",
                "directional_bias",
                "suggested_strategy",
                "confidence_score",
                "signal_conflict_score",
            ]
            st.dataframe(recs[[c for c in cols if c in recs.columns]], use_container_width=True)

    with tab_open:
        open_trades = validation_data["open_trades"]
        if open_trades.empty:
            st.info("No open paper trades.")
        else:
            cols = [
                "created_at",
                "strategy",
                "side",
                "entry_spot",
                "current_spot",
                "lots",
                "eth_quantity",
                "margin_used_usdt",
                "margin_used_inr",
                "unrealized_pnl_usdt",
                "unrealized_pnl_inr",
            ]
            st.dataframe(open_trades[[c for c in cols if c in open_trades.columns]], use_container_width=True)

    with tab_closed:
        closed_trades = validation_data["closed_trades"]
        if closed_trades.empty:
            st.info("No closed paper trades yet.")
        else:
            cols = [
                "closed_at",
                "strategy",
                "side",
                "lots",
                "exit_reason",
                "realized_pnl_usdt",
                "realized_pnl_inr",
            ]
            st.dataframe(closed_trades[[c for c in cols if c in closed_trades.columns]], use_container_width=True)

    with tab_perf:
        st.markdown("#### Strategy-wise Performance")
        strategy_perf = performance.get("strategy_performance")
        if strategy_perf is not None and not strategy_perf.empty:
            st.dataframe(strategy_perf, use_container_width=True)
        else:
            st.info("Strategy-wise performance will appear after trades close.")

        st.markdown("#### Confidence Bucket Performance")
        confidence_perf = performance.get("confidence_bucket_performance")
        if confidence_perf is not None and not confidence_perf.empty:
            st.dataframe(confidence_perf, use_container_width=True)
        else:
            st.info("Confidence bucket performance will appear after closed trades exist.")

        st.markdown("#### Conflict-Score Bucket Performance")
        conflict_perf = performance.get("conflict_bucket_performance")
        if conflict_perf is not None and not conflict_perf.empty:
            st.dataframe(conflict_perf, use_container_width=True)
        else:
            st.info("Conflict-score bucket performance will appear after closed trades exist.")

except Exception as e:
    st.warning(f"Validation & Paper Trading section unavailable: {e}")
