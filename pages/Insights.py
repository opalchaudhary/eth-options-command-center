import pandas as pd
import streamlit as st

from data_refresh import refresh_market_structure_sources, refresh_options_sources
from rule_insights import build_rule_based_insights, get_available_expiries


st.set_page_config(
    page_title="Insights | ETH Options Command Center",
    layout="wide",
)

st.title("Insights")
st.caption("Single rule-based market read, strategy selection, risk/reward, and data-source health.")


@st.cache_data(ttl=60, show_spinner=False)
def _cached_available_expiries():
    return get_available_expiries()


@st.cache_data(ttl=30, show_spinner=False)
def _cached_rule_insights(expiry):
    return build_rule_based_insights(expiry)


def _fmt_price(value, digits=2):
    if value is None:
        return "NA"

    return f"{float(value):,.{digits}f}"


def _fmt_money(value):
    if value is None:
        return "NA"

    return f"${float(value):,.2f}"


def _fmt_ist(value):
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")

    if pd.isna(timestamp):
        return str(value)

    return timestamp.tz_convert("Asia/Kolkata").strftime("%d %b %Y, %I:%M %p IST")


def _show_strategy_legs(legs):
    if not legs:
        st.info("No executable trade legs. Wait for cleaner pricing or stronger confirmation.")
        return

    rows = []
    for leg in legs:
        rows.append(
            {
                "Action": leg.get("action"),
                "Strike": _fmt_price(leg.get("strike"), 0),
                "Option": leg.get("option"),
                "Mark": _fmt_money(leg.get("mark_price")),
            }
        )

    st.dataframe(rows, use_container_width=True, hide_index=True)


def _refresh_options(expiry=None):
    with st.spinner("Refreshing option chain, analytics, and premium snapshots..."):
        result = refresh_options_sources(expiry_label=expiry)
        st.cache_data.clear()

    return result


def _refresh_error(result):
    results = result.get("results") or []

    for item in results:
        if item.get("error"):
            return item.get("error")

    sample = result.get("available_expiry_sample")
    if sample:
        return "Available Delta expiries include: " + ", ".join(sample)

    return "No detailed refresh error was returned."


expiry_list = _cached_available_expiries()

if st.sidebar.button("Refresh Options Chain"):
    refresh_result = _refresh_options()

    if refresh_result.get("ok"):
        st.sidebar.success(
            f"Options refreshed: {refresh_result.get('expiry_count')} expiries, "
            f"{refresh_result.get('row_count')} rows"
        )
        st.rerun()
    else:
        st.sidebar.warning("Options refresh did not save a complete snapshot")
        st.sidebar.caption(_refresh_error(refresh_result))

if not expiry_list:
    st.info("No option snapshots found yet. Pulling fresh Delta option data for Insights...")
    refresh_result = _refresh_options()

    if refresh_result.get("ok"):
        st.rerun()

    st.warning("No analytics snapshots found in Supabase yet, and automatic options refresh failed.")
    st.stop()

selected_expiry = st.sidebar.selectbox("Select Expiry", expiry_list, index=0, format_func=_fmt_ist)

if st.sidebar.button("Refresh Selected Expiry"):
    refresh_result = _refresh_options(selected_expiry)

    if refresh_result.get("ok"):
        st.sidebar.success(
            f"{_fmt_ist(selected_expiry)} refreshed with {refresh_result.get('row_count')} option rows"
        )
        st.rerun()
    else:
        st.sidebar.warning("Selected expiry refresh failed")
        st.sidebar.caption(_refresh_error(refresh_result))

if st.sidebar.button("Refresh Market Sources"):
    with st.spinner("Refreshing orderbook, OHLCV, SMC zones, events, and volume profile..."):
        refresh_result = refresh_market_structure_sources()

    if (
        refresh_result.get("orderbook_saved")
        and refresh_result.get("ohlcv_saved")
        and refresh_result.get("smc_saved")
    ):
        st.cache_data.clear()
        st.sidebar.success("Market sources refreshed")
        st.rerun()
    elif refresh_result.get("ohlcv_saved"):
        st.sidebar.warning("OHLCV saved, but one or more downstream sources did not refresh")
    else:
        st.sidebar.warning("OHLCV refresh failed")


with st.spinner("Building insights..."):
    insights = _cached_rule_insights(selected_expiry)

missing_option_chain = not insights.get("data_flags", {}).get("option_chain")
auto_refresh_key = f"auto_option_refresh_{selected_expiry}"

if missing_option_chain and not st.session_state.get(auto_refresh_key):
    st.session_state[auto_refresh_key] = True
    st.warning("Option-chain snapshot is missing for this expiry. Refreshing it now...")
    refresh_result = _refresh_options(selected_expiry)

    if refresh_result.get("ok"):
        st.rerun()

    st.warning("Option-chain refresh failed; recommendation quality may be limited.")

st.subheader(f"Market Read - {_fmt_ist(selected_expiry)}")

summary_cols = st.columns(5)
summary_cols[0].metric("Regime", insights["market_regime"])
summary_cols[1].metric("Direction", insights["directional_bias"])
summary_cols[2].metric("Volatility", insights["volatility_regime"])
summary_cols[3].metric("Confidence", f"{insights['confidence_score']}/100")
summary_cols[4].metric("Conflict", f"{insights['signal_conflict_score']}/100")

market_cols = st.columns(5)
market_cols[0].metric("ETH Spot", _fmt_money(insights.get("spot_price")))
market_cols[1].metric("ATM Strike", _fmt_price(insights.get("atm_strike"), 0))
market_cols[2].metric("Max Pain", _fmt_price(insights.get("max_pain"), 0))
market_cols[3].metric("Expected Move", _fmt_money(insights.get("expected_move")))
market_cols[4].metric("Expiry Bucket", insights["expiry_profile"]["bucket"])

st.divider()

st.subheader("Recommended Strategy")
st.markdown(f"**{insights['best_strategy']}**")

pricing = insights.get("strategy_pricing") or {}
risk_reward = insights.get("strategy_risk_reward") or {}

strategy_cols = st.columns(5)
strategy_cols[0].metric("Quality", risk_reward.get("quality", "Unknown"))
strategy_cols[1].metric("Net Credit", _fmt_money(pricing.get("net_credit_usdt")))
strategy_cols[2].metric("Net Debit", _fmt_money(pricing.get("net_debit_usdt")))
strategy_cols[3].metric(
    "Reward / Risk",
    risk_reward.get("reward_risk") if risk_reward.get("reward_risk") is not None else "NA",
)
strategy_cols[4].metric(
    "Effective Return",
    f"{risk_reward.get('effective_return_pct')}%"
    if risk_reward.get("effective_return_pct") is not None
    else "NA",
)

_show_strategy_legs(pricing.get("legs") or insights.get("strategy_legs") or [])

if risk_reward.get("max_profit_usdt") is not None or risk_reward.get("max_loss_usdt") is not None:
    payoff_cols = st.columns(2)
    payoff_cols[0].metric("Max Profit", _fmt_money(risk_reward.get("max_profit_usdt")))
    payoff_cols[1].metric("Max Loss", _fmt_money(risk_reward.get("max_loss_usdt")))

with st.expander("Strategy Candidate Scores"):
    candidates = insights.get("strategy_candidates") or []
    if candidates:
        st.dataframe(candidates, use_container_width=True, hide_index=True)
    else:
        st.info("Candidate scoring will appear after the strategy engine evaluates executable spreads.")

st.write(
    f"The engine prefers **{insights['best_strategy']}** with a "
    f"**{insights['directional_bias']}** bias, **{insights['trap_risk']}** trap risk, "
    f"and **{insights['option_selling_environment']}** option-selling conditions."
)

st.divider()

tab_insights, tab_risk, tab_sources = st.tabs(["Key Insights", "Risk Warnings", "Data Health"])

with tab_insights:
    for item in insights["key_insights"]:
        st.info(item)

with tab_risk:
    for warning in insights["risk_warnings"]:
        st.warning(warning)

with tab_sources:
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

    for source, available in insights["data_flags"].items():
        status = "Available" if available else "Missing / Empty"

        if source == "option_chain" and insights.get("option_chain_source") == "live_delta":
            status = "Live Delta fallback"

        source_rows.append(
            {
                "Source": source_labels.get(source, source),
                "Status": status,
            }
        )

    st.dataframe(source_rows, use_container_width=True, hide_index=True)

    if insights.get("missing_sources"):
        st.warning("Missing or empty: " + ", ".join(insights["missing_sources"]))
    else:
        st.success("All rule-engine source tables are available.")
