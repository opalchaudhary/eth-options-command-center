import pandas as pd
import streamlit as st

from api_client import api_get, backend_url
from ui_styles import load_css


st.set_page_config(
    page_title="Insights | ETH Options Command Center",
    layout="wide",
)

load_css()

st.title("Insights")
st.caption("Live-first rule-based market read, strategy selection, risk/reward, and data-source health.")


@st.cache_data(ttl=60, show_spinner=False)
def _cached_available_expiries():
    response = api_get("/insights")
    return {
        "ok": response.get("ok"),
        "error": response.get("error"),
        "expiries": response.get("expiries") or [],
    }


@st.cache_data(ttl=30, show_spinner=False)
def _cached_rule_insights(expiry):
    response = api_get("/insights", params={"expiry": expiry})
    if not response.get("ok"):
        return {
            "ok": False,
            "error": response.get("error") or "Insights unavailable.",
            "insights": response.get("insights") or {},
        }
    return {
        "ok": True,
        "insights": response.get("insights") or {},
    }


def _fmt_price(value, digits=2):
    if value is None:
        return "NA"

    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return "NA"


def _fmt_money(value):
    if value is None:
        return "NA"

    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return "NA"


def _fmt_ist(value):
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")

    if pd.isna(timestamp):
        return str(value)

    return timestamp.tz_convert("Asia/Kolkata").strftime("%d %b %Y, %I:%M %p IST")


def _expiry_rank_labels(expiries):
    labels = ["D1", "D2", "D3", "W1"]
    return {
        expiry: labels[index]
        for index, expiry in enumerate(expiries[: len(labels)])
    }


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


def _recommendation_summary(expiry, label, response):
    insights = response.get("insights") or {}
    risk_reward = insights.get("strategy_risk_reward") or {}

    return {
        "Bucket": label,
        "Expiry": _fmt_ist(expiry),
        "Strategy": insights.get("best_strategy", "NA"),
        "Direction": insights.get("directional_bias", "NA"),
        "Volatility": insights.get("volatility_regime", "NA"),
        "Confidence": insights.get("confidence_score", "NA"),
        "Conflict": insights.get("signal_conflict_score", "NA"),
        "Quality": risk_reward.get("quality", "Unknown"),
        "Data": "OK" if response.get("ok") else response.get("error", "Unavailable"),
    }


expiry_response = _cached_available_expiries()
expiry_list = expiry_response.get("expiries") or []
expiry_labels = _expiry_rank_labels(expiry_list)
st.sidebar.caption(f"Backend: {backend_url()}")

if not expiry_response.get("ok") and expiry_response.get("error"):
    st.sidebar.warning(expiry_response.get("error"))

if st.sidebar.button("Reload Insights"):
    st.cache_data.clear()
    st.rerun()

if not expiry_list:
    st.warning("No live or saved option expiries are available yet. Check the backend data connection and retry shortly.")
    st.stop()

selected_expiry = st.sidebar.selectbox(
    "Select Expiry",
    expiry_list,
    index=0,
    format_func=lambda value: f"{expiry_labels.get(value, '')} - {_fmt_ist(value)}".strip(" -"),
)


target_expiries = expiry_list[:4]
if target_expiries:
    with st.container(key="expiry_recommendations"):
        st.subheader("Expiry Recommendations")
        rows = []

        with st.spinner("Loading D1, D2, D3, and W1 recommendations..."):
            for expiry in target_expiries:
                label = expiry_labels.get(expiry, "Extra")
                response = _cached_rule_insights(expiry)
                rows.append(_recommendation_summary(expiry, label, response))

        st.dataframe(rows, use_container_width=True, hide_index=True)
        st.divider()


with st.spinner("Loading insights..."):
    insights_response = _cached_rule_insights(selected_expiry)

if not insights_response.get("ok"):
    st.warning(insights_response.get("error") or "Insights are not available yet.")
    st.stop()

insights = insights_response.get("insights") or {}
expiry_profile = insights.get("expiry_profile") or {}
data_flags = insights.get("data_flags") or {}

missing_option_chain = not data_flags.get("option_chain")
if missing_option_chain:
    st.warning("Option-chain data is unavailable from both live Delta and saved snapshots.")

with st.container(key="insights_market_read"):
    st.subheader(f"Market Read - {_fmt_ist(selected_expiry)}")

    summary_cols = st.columns(5)
    summary_cols[0].metric("Regime", insights.get("market_regime", "NA"))
    summary_cols[1].metric("Direction", insights.get("directional_bias", "NA"))
    summary_cols[2].metric("Volatility", insights.get("volatility_regime", "NA"))
    summary_cols[3].metric("Confidence", f"{insights.get('confidence_score', 'NA')}/100")
    summary_cols[4].metric("Conflict", f"{insights.get('signal_conflict_score', 'NA')}/100")

    market_cols = st.columns(5)
    market_cols[0].metric("ETH Spot", _fmt_money(insights.get("spot_price")))
    market_cols[1].metric("ATM Strike", _fmt_price(insights.get("atm_strike"), 0))
    market_cols[2].metric("Max Pain", _fmt_price(insights.get("max_pain"), 0))
    market_cols[3].metric("Expected Move", _fmt_money(insights.get("expected_move")))
    market_cols[4].metric("Expiry Bucket", expiry_labels.get(selected_expiry) or expiry_profile.get("bucket", "NA"))

    vol_cols = st.columns(3)
    vol_cols[0].metric("Median IV", _fmt_price(insights.get("median_iv"), 2))
    vol_cols[1].metric("Realized Vol", _fmt_price(insights.get("realized_vol_pct"), 2))
    vol_cols[2].metric("IV - RV", _fmt_price(insights.get("iv_rv_spread"), 2))

st.divider()

with st.container(key="insights_strategy"):
    st.subheader("Recommended Strategy")
    st.markdown(f"**{insights.get('best_strategy', 'NA')}**")

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
        f"The engine prefers **{insights.get('best_strategy', 'NA')}** with a "
        f"**{insights.get('directional_bias', 'NA')}** bias, **{insights.get('trap_risk', 'NA')}** trap risk, "
        f"and **{insights.get('option_selling_environment', 'NA')}** option-selling conditions."
    )

st.divider()

tab_insights, tab_risk, tab_sources = st.tabs(["Key Insights", "Risk Warnings", "Data Health"])

with tab_insights:
    key_insights = insights.get("key_insights") or []
    if key_insights:
        for item in key_insights:
            st.info(item)
    else:
        st.info("No key insights are available for this expiry yet.")

with tab_risk:
    risk_warnings = insights.get("risk_warnings") or insights.get("key_warnings") or []
    if risk_warnings:
        for warning in risk_warnings:
            st.warning(warning)
    else:
        st.success("No risk warnings for this expiry.")

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

    for source, available in data_flags.items():
        status = "Available" if available else "Missing / Empty"

        if source == "option_chain" and insights.get("option_chain_source") == "live_delta":
            status = "Live Delta"
        elif source == "analytics" and insights.get("analytics_source") == "live_delta":
            status = "Live Delta"
        elif source == "orderbook" and insights.get("orderbook_source") == "live_delta":
            status = "Live Delta"
        elif source == "ohlcv" and insights.get("ohlcv_source") == "live_delta":
            status = "Live Delta"

        source_rows.append(
            {
                "Source": source_labels.get(source, source),
                "Status": status,
            }
        )

    if source_rows:
        st.dataframe(source_rows, use_container_width=True, hide_index=True)
    else:
        st.info("Data health details are not available for this response.")

    missing_sources = insights.get("missing_sources") or []
    if isinstance(missing_sources, str):
        missing_sources = [missing_sources]

    if missing_sources:
        st.warning("Missing or empty: " + ", ".join(str(source) for source in missing_sources))
    else:
        st.success("All required rule-engine sources are available.")
