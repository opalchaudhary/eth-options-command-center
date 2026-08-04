import pandas as pd
import streamlit as st

from api_client import api_get, backend_url
from ui_styles import load_css


st.set_page_config(page_title="Futures & Covered Strategies", layout="wide")
load_css()

st.title("Futures & Covered Strategies")
st.caption("Research-only futures direction and covered option suitability.")
st.sidebar.caption(f"Backend: {backend_url()}")


@st.cache_data(ttl=30, show_spinner=False)
def _load_recommendation():
    return api_get("/strategy/futures/latest", params={"persist": False})


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
    result = _load_recommendation()
except Exception as exc:
    st.error(f"Strategy API unavailable: {exc}")
    st.stop()

futures = result.get("futures") or {}
covered_call = result.get("covered_call") or {}
covered_put = result.get("covered_put") or {}

summary = st.columns(5)
summary[0].metric("Recommendation", futures.get("recommendation", "NO_TRADE"))
summary[1].metric("Score", futures.get("overall_score", "NA"))
summary[2].metric("Confidence", futures.get("confidence", "LOW"))
summary[3].metric("Data Quality", futures.get("data_quality_score", "NA"))
summary[4].metric("Risk %", _fmt_pct(futures.get("suggested_position_risk_pct")))

levels = st.columns(5)
levels[0].metric("Entry", _fmt_money(futures.get("suggested_entry_zone")))
levels[1].metric("Stop", _fmt_money(futures.get("stop_loss_price")))
levels[2].metric("TP1", _fmt_money(futures.get("tp1")))
levels[3].metric("TP2", _fmt_money(futures.get("tp2")))
levels[4].metric("TP3", _fmt_money(futures.get("tp3")))

st.info(futures.get("explanation", "No explanation available."))

tab_factors, tab_covered, tab_data = st.tabs(["Evidence", "Covered Strategies", "Data"])

with tab_factors:
    left, right = st.columns(2)
    with left:
        st.subheader("Supporting Factors")
        for item in futures.get("supporting_factors") or ["No supporting factors available."]:
            st.success(item)
    with right:
        st.subheader("Contradictory Factors")
        for item in futures.get("contradictory_factors") or ["No contradictory factors available."]:
            st.warning(item)
    st.dataframe(
        pd.DataFrame(
            [{"Component": key, "Score": value} for key, value in (futures.get("component_scores") or {}).items()]
        ),
        use_container_width=True,
        hide_index=True,
    )

with tab_covered:
    rows = []
    for setup in [covered_call, covered_put]:
        rows.append(
            {
                "Strategy": setup.get("strategy"),
                "Status": setup.get("status"),
                "Required Exposure": setup.get("required_underlying_position"),
                "Expiry": setup.get("preferred_expiry"),
                "Short Strike": setup.get("preferred_short_strike"),
                "Delta": setup.get("option_delta"),
                "Premium": setup.get("premium"),
                "Yield": _fmt_pct(setup.get("expected_yield_pct")),
                "Annualized Yield": _fmt_pct(setup.get("annualized_yield_pct")),
                "Buffer": _fmt_pct(setup.get("buffer_pct")),
                "Breakeven": _fmt_money(setup.get("breakeven")),
            }
        )
    st.dataframe(rows, use_container_width=True, hide_index=True)

    for setup in [covered_call, covered_put]:
        with st.expander(setup.get("strategy", "Covered Strategy")):
            st.write(setup.get("uncovered_option_warning"))
            for reason in setup.get("reasons_for") or []:
                st.success(reason)
            for reason in setup.get("reasons_against") or []:
                st.warning(reason)
            st.write("Exit conditions:", setup.get("exit_conditions") or [])
            st.write("Roll conditions:", setup.get("roll_conditions") or [])

with tab_data:
    st.write("Generated at:", result.get("generated_at"))
    st.write("Data freshness:", result.get("data_freshness") or {})
    st.write("Unavailable inputs:", result.get("unavailable_inputs") or [])
    st.write("Persistence:", result.get("persistence") or {"persisted": False})
    st.caption("Research only. No orders are placed, modified, or closed.")
