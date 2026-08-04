import pandas as pd
import streamlit as st

from api_client import api_get, backend_url
from ui_styles import load_css


st.set_page_config(page_title="Iron Fly Optimizer", layout="wide")
load_css()

st.title("Iron Fly Optimizer")
st.caption("Research-only Iron Fly feasibility, expiry comparison, and leg optimization.")
st.sidebar.caption(f"Backend: {backend_url()}")


@st.cache_data(ttl=30, show_spinner=False)
def _load_iron_fly():
    return api_get("/strategy/iron-fly/latest", params={"persist": False}, timeout=15)


def _fmt_money(value):
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return "NA"


def _fmt_pct(value):
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return "NA"


if st.sidebar.button("Reload"):
    st.cache_data.clear()
    st.rerun()

try:
    result = _load_iron_fly()
except Exception as exc:
    st.error(f"Strategy API unavailable: {exc}")
    st.stop()

selected = result.get("selected") or {}
payoff = selected.get("payoff") or {}
greeks = selected.get("net_greeks") or {}

summary = st.columns(5)
summary[0].metric("Recommendation", result.get("recommendation", "NOT_RECOMMENDED"))
summary[1].metric("Score", result.get("iron_fly_score", "NA"))
summary[2].metric("Confidence", result.get("confidence", "LOW"))
summary[3].metric("Expiry", selected.get("expiry", "NA"))
summary[4].metric("Center", selected.get("center_strike", "NA"))

risk = st.columns(5)
risk[0].metric("Net Credit", _fmt_money(payoff.get("net_credit")))
risk[1].metric("Max Profit", _fmt_money(payoff.get("max_profit")))
risk[2].metric("Max Loss", _fmt_money(payoff.get("max_loss")))
risk[3].metric("Return/Risk", _fmt_pct(payoff.get("return_on_risk_pct")))
risk[4].metric("Liquidity", selected.get("liquidity_score", "NA"))

breakevens = st.columns(4)
breakevens[0].metric("Lower BE", _fmt_money(payoff.get("lower_breakeven")))
breakevens[1].metric("Upper BE", _fmt_money(payoff.get("upper_breakeven")))
breakevens[2].metric("Expected Move", _fmt_money(selected.get("expected_move")))
breakevens[3].metric("DTE", selected.get("dte", "NA"))

tab_legs, tab_expiries, tab_alternatives, tab_rules = st.tabs(
    ["Selected Legs", "Expiry Comparison", "Alternatives & Rejections", "Rules"]
)

with tab_legs:
    legs = selected.get("legs") or []
    if legs:
        st.dataframe(legs, use_container_width=True, hide_index=True)
    else:
        st.warning("No valid Iron Fly structure selected.")
    st.dataframe(
        pd.DataFrame([{"Greek": key, "Net": value} for key, value in greeks.items()]),
        use_container_width=True,
        hide_index=True,
    )
    st.info(selected.get("ranking_reason") or "No ranking reason available.")

with tab_expiries:
    rows = result.get("expiry_comparison") or []
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("No expiry comparison is available.")

with tab_alternatives:
    alternatives = result.get("top_alternatives") or []
    if alternatives:
        st.dataframe(
            [
                {
                    "Expiry": item.get("expiry"),
                    "Center": item.get("center_strike"),
                    "Wing": item.get("wing_width"),
                    "Score": item.get("score"),
                    "Credit": item.get("payoff", {}).get("net_credit"),
                    "Return/Risk": item.get("payoff", {}).get("return_on_risk_pct"),
                    "Reason": item.get("ranking_reason"),
                }
                for item in alternatives
            ],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No valid alternatives were ranked.")

    with st.expander("Rejected Structures"):
        rejections = result.get("rejection_reasons") or []
        if rejections:
            st.dataframe(rejections, use_container_width=True, hide_index=True)
        else:
            st.success("No rejection details returned.")

with tab_rules:
    st.write("Entry conditions:", result.get("entry_conditions") or [])
    st.write("Adjustment triggers:", result.get("adjustment_triggers") or [])
    st.write("Stop-loss logic:", result.get("stop_loss_logic"))
    st.write("Profit booking:", result.get("profit_booking_logic"))
    st.write("Time exit:", result.get("time_based_exit"))
    st.warning(result.get("expiry_management_warning", "Manage expiry risk carefully."))
    for factor in result.get("risk_factors") or []:
        st.warning(factor)
    st.caption("Research only. No orders are placed, modified, or closed.")
