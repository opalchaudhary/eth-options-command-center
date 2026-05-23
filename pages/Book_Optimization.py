import pandas as pd
import streamlit as st

from book_optimizer import (
    build_final_verdict,
    classify_book_greek_health,
    compare_book_with_ideal,
    derive_margin_metrics,
    get_available_option_chain,
    get_current_market_regime,
    get_ideal_greeks_for_regime,
    get_recent_book_optimization_snapshots,
    save_book_optimization_snapshot,
    suggest_book_adjustments,
)
from api_client import api_get, backend_url
from rule_insights import get_available_expiries
from ui_styles import load_css


st.set_page_config(
    page_title="Book Optimization | ETH Options Command Center",
    layout="wide",
)

load_css()

st.title("Book Optimization")
st.caption("Greek-aware optimizer for your current ETH options and futures book.")
st.sidebar.caption(f"Backend: {backend_url()}")


@st.cache_data(ttl=60, show_spinner=False)
def _cached_expiries():
    expiries = list(get_available_expiries(limit=50) or [])

    if len(expiries) >= 5:
        return expiries, "analytics_snapshots"

    try:
        live_response = api_get("/option-chain")
        live_options = pd.DataFrame(live_response.get("rows") or [])
        live_expiries = sorted(live_options["expiry"].dropna().unique()) if not live_options.empty else []
    except Exception:
        live_expiries = []

    for expiry in live_expiries:
        if expiry not in expiries:
            expiries.append(expiry)

    return expiries, "analytics_snapshots + live Delta fallback" if live_expiries else "analytics_snapshots"


@st.cache_data(ttl=45, show_spinner=False)
def _cached_insights(expiry):
    if not expiry:
        return {}
    response = api_get("/insights", params={"expiry": expiry})
    return response.get("insights") or {}


@st.cache_data(ttl=45, show_spinner=False)
def _cached_option_chain(expiries):
    return get_available_option_chain(list(expiries or []))


def _fmt_num(value, digits=2):
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return "NA"


def _fmt_range(values, digits=4):
    low, high = values
    return f"{low:,.{digits}f} to {high:,.{digits}f}"


def _fmt_ist(value):
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return str(value)
    return timestamp.tz_convert("Asia/Kolkata").strftime("%d %b %Y, %I:%M %p IST")


def _auto_scan_expiries(expiry_list):
    today = pd.Timestamp.now(tz="UTC").normalize()
    buckets = {
        "D0": None,
        "D1": None,
        "D2": None,
        "D3": None,
        "W1": None,
    }
    parsed = []

    for expiry in expiry_list or []:
        timestamp = pd.to_datetime(expiry, utc=True, errors="coerce")
        if pd.isna(timestamp):
            continue
        days = int((timestamp.normalize() - today).days)
        if days < 0:
            continue
        parsed.append((expiry, timestamp, days))

    parsed = sorted(parsed, key=lambda item: item[1])

    for label, target_day in [("D0", 0), ("D1", 1), ("D2", 2), ("D3", 3)]:
        for expiry, _, days in parsed:
            if days == target_day and not buckets[label]:
                buckets[label] = expiry
                break

    weekly_candidates = [
        (expiry, timestamp, days)
        for expiry, timestamp, days in parsed
        if 4 <= days <= 10
    ]
    if weekly_candidates:
        buckets["W1"] = weekly_candidates[0][0]

    selected = []
    for expiry, _, _ in parsed:
        if len(selected) >= 5:
            break
        if expiry in buckets.values() and expiry not in selected:
            selected.append(expiry)

    for expiry, _, _ in parsed:
        if len(selected) >= 5:
            break
        if expiry not in selected:
            selected.append(expiry)

    return selected, buckets


def _badge(label):
    colors = {
        "Healthy": ("#067647", "#ECFDF3"),
        "Caution": ("#B54708", "#FFFAEB"),
        "Risky": ("#C2410C", "#FFF7ED"),
        "Dangerous": ("#B42318", "#FEF3F2"),
    }
    fg, bg = colors.get(label, ("#344054", "#F2F4F7"))
    st.markdown(
        f"""
        <span style="
            display:inline-flex;
            align-items:center;
            padding:0.24rem 0.62rem;
            border-radius:999px;
            color:{fg};
            background:{bg};
            border:1px solid rgba(16,42,67,0.08);
            font-weight:760;
            font-size:0.88rem;">
            {label}
        </span>
        """,
        unsafe_allow_html=True,
    )


def _info_tile(label, value):
    st.markdown(
        f"""
        <div style="
            min-height: 92px;
            padding: 0.95rem 1rem;
            border: 1px solid rgba(16,42,67,0.12);
            border-radius: 12px;
            background: #FFFFFF;
            box-shadow: 0 6px 18px rgba(16,42,67,0.05);
            overflow-wrap: anywhere;">
            <div style="
                color: #627D98;
                font-size: 0.82rem;
                font-weight: 700;
                line-height: 1.15;
                margin-bottom: 0.45rem;">
                {label}
            </div>
            <div style="
                color: #102A43;
                font-size: 1.12rem;
                font-weight: 780;
                line-height: 1.18;
                letter-spacing: 0;">
                {value}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _candidate_rows(candidates):
    rows = []
    for item in candidates:
        impact = item.get("expected_greek_impact") or {}
        rows.append(
            {
                "Profile": item.get("profile"),
                "Expiry": item.get("expiry"),
                "Bucket": item.get("expiry_bucket"),
                "Type": item.get("instrument_type"),
                "Action": item.get("action"),
                "Strike": item.get("strike"),
                "Lots": item.get("lots"),
                "Aggressive Lots": item.get("aggressive_lots_if_safe") or "NA",
                "Delta Impact": impact.get("delta"),
                "Gamma Impact": impact.get("gamma"),
                "Theta Impact": impact.get("theta"),
                "Vega Impact": impact.get("vega"),
                "Reason": item.get("reason"),
                "Risk Warning": item.get("risk_warning"),
            }
        )
    return rows


expiry_list, expiry_source = _cached_expiries()
auto_expiries, auto_expiry_buckets = _auto_scan_expiries(expiry_list)
default_expiries = auto_expiries

with st.sidebar:
    st.subheader("Market Data")
    st.caption(f"Expiry source: {expiry_source}")
    if expiry_list:
        selected_expiry = st.selectbox(
            "Primary regime expiry",
            expiry_list,
            index=0,
            format_func=_fmt_ist,
        )
        selected_expiries = default_expiries
        st.caption("Auto-scanning D0, D1, D2, D3, and W1 when those expiries are available.")
        auto_rows = [
            {"Bucket": label, "Expiry": _fmt_ist(expiry) if expiry else "Not available"}
            for label, expiry in auto_expiry_buckets.items()
        ]
        st.dataframe(auto_rows, use_container_width=True, hide_index=True)
    else:
        selected_expiry = None
        selected_expiries = []
        st.info("No expiry snapshots found yet. The page will still classify manual Greeks.")
    if st.button("Refresh Optimizer Data"):
        st.cache_data.clear()
        st.rerun()


spot_price = None
try:
    spot_data = api_get("/market/eth")
    spot_price = spot_data.get("spot_price") or spot_data.get("mark_price")
except Exception:
    spot_data = {}

insights = _cached_insights(selected_expiry) if selected_expiry else {}
market_context = get_current_market_regime(selected_expiry=selected_expiry, insights=insights)
option_chain_df = _cached_option_chain(tuple(selected_expiries or default_expiries))
chain_missing = option_chain_df.empty


with st.container(border=True, key="book_current_input"):
    st.subheader("A. Current Book Input")
    st.caption("Enter your live book Greeks manually. Values can include options and futures exposure together.")

    with st.form("book_optimization_form"):
        row1 = st.columns(4)
        input_delta = row1[0].number_input("Delta", value=0.18, step=0.01, format="%.4f")
        input_gamma = row1[1].number_input("Gamma", value=-0.003, step=0.001, format="%.6f")
        input_theta = row1[2].number_input("Theta", value=4.0, step=0.25, format="%.4f")
        input_vega = row1[3].number_input("Vega", value=-1.2, step=0.10, format="%.4f")

        row2 = st.columns(3)
        wallet_size = row2[0].number_input("Wallet size / capital", value=588.2353, min_value=1.0, step=50.0, format="%.4f")
        unrealized_pnl = row2[1].number_input("Current unrealized P&L", value=0.0, step=10.0, format="%.2f")
        margin_available_to_withdraw = row2[2].number_input(
            "Margin available to withdraw",
            value=588.2353,
            min_value=0.0,
            step=50.0,
            format="%.4f",
        )

        row3 = st.columns([2, 1])
        strategy_type = row3[0].text_input("Current strategy type (optional)", placeholder="Short strangle, iron fly, futures hedge...")
        manual_spot = row3[1].number_input(
            "ETH spot override",
            value=float(spot_price or 0),
            min_value=0.0,
            step=10.0,
            format="%.2f",
        )

        submitted = st.form_submit_button("Optimize Book", type="primary")


book_greeks = {
    "delta": input_delta,
    "gamma": input_gamma,
    "theta": input_theta,
    "vega": input_vega,
    "wallet_size": wallet_size,
    "unrealized_pnl": unrealized_pnl,
    "margin_available_to_withdraw": margin_available_to_withdraw,
    "strategy_type": strategy_type,
    "spot_price": manual_spot or spot_price,
    "expiry_context": selected_expiries or default_expiries,
    "market_regime": market_context.get("book_optimizer_regime"),
}
margin_metrics = derive_margin_metrics(book_greeks)

with st.container(border=True, key="book_margin_derived"):
    st.subheader("Portfolio Margin Estimate")
    st.caption("Derived from wallet size + unrealized P&L - margin available to withdraw.")
    margin_cols = st.columns(4)
    margin_cols[0].metric("Portfolio Equity", _fmt_num(margin_metrics.get("equity"), 2))
    margin_cols[1].metric("Available to Withdraw", _fmt_num(margin_metrics.get("margin_available_to_withdraw"), 2))
    margin_cols[2].metric("Estimated Margin Utilized", _fmt_num(margin_metrics.get("margin_used"), 2))
    margin_cols[3].metric("Margin Usage", f"{_fmt_num(margin_metrics.get('margin_usage_pct'), 2)}%")

ideal_greeks = get_ideal_greeks_for_regime(
    market_context.get("book_optimizer_regime"),
    margin_metrics.get("equity") or wallet_size,
    margin_metrics.get("margin_used"),
)
health = classify_book_greek_health(book_greeks, market_context)
comparison_df = compare_book_with_ideal(book_greeks, ideal_greeks)
adjustments = suggest_book_adjustments(
    book_greeks,
    ideal_greeks,
    option_chain_df,
    expiries=selected_expiries or default_expiries,
)
verdict = build_final_verdict(health, book_greeks, ideal_greeks, market_context, adjustments)

if submitted:
    try:
        saved = save_book_optimization_snapshot(
            book_greeks,
            market_context,
            health,
            ideal_greeks,
            adjustments,
            comparison_df,
        )
        if saved:
            st.success("Book optimization snapshot saved.")
        else:
            st.warning("Snapshot was calculated, but database logging did not confirm a save. Check the migration/table if this is the first run.")
    except Exception as exc:
        st.warning(f"Snapshot was calculated, but database logging failed: {exc}")

if chain_missing:
    st.warning("Option chain data is missing. Manual Greek health analysis is active, but strike suggestions need live option-chain data.")

for warning in market_context.get("warnings") or []:
    st.caption(f"Market context warning: {warning}")


health_col, regime_col = st.columns([1, 1])

with health_col:
    with st.container(border=True, key="book_health"):
        st.subheader("B. Book Greek Health")
        c1, c2 = st.columns([1, 1])
        with c1:
            _badge(health["health_status"])
            st.metric("Risk Score", f"{health['risk_score']}/100")
        with c2:
            component_df = pd.DataFrame(
                [
                    {"Component": key.replace("_", " ").title(), "Score": value}
                    for key, value in health["component_scores"].items()
                ]
            )
            st.dataframe(component_df, use_container_width=True, hide_index=True)
        for reason in health["reasoning"]:
            st.write(f"- {reason}")

with regime_col:
    with st.container(border=True, key="book_regime"):
        st.subheader("C. Current Market Regime")
        _info_tile("Book Optimizer Regime", market_context.get("book_optimizer_regime", "Unknown"))
        st.write("")
        regime_metrics = st.columns(2)
        with regime_metrics[0]:
            _info_tile("Insights Regime", market_context.get("market_regime", "NA"))
            _info_tile("Volatility", market_context.get("volatility_regime", "NA"))
        with regime_metrics[1]:
            _info_tile("Direction", market_context.get("directional_bias", "NA"))
            _info_tile("Pinning Score", f"{_fmt_num(market_context.get('pinning_score'), 0)}/100")
        st.caption(
            "Regime inference uses the existing Insights stack when available: IV/RV, PCR, max pain, OI walls, SMC, volume profile, trend, and liquidity context."
        )


with st.container(border=True, key="book_ideal"):
    st.subheader("D. Ideal Greeks for Current Regime")
    ideal_cols = st.columns(5)
    ideal_cols[0].metric("Delta", _fmt_range(ideal_greeks["ideal_delta_range"], 2))
    ideal_cols[1].metric("Gamma", _fmt_range(ideal_greeks["ideal_gamma_range"], 4))
    ideal_cols[2].metric("Theta", _fmt_range(ideal_greeks["ideal_theta_range"], 2))
    ideal_cols[3].metric("Vega", _fmt_range(ideal_greeks["ideal_vega_range"], 2))
    ideal_cols[4].metric("Max Margin", f"{ideal_greeks['max_margin_usage'] * 100:.0f}%")
    st.write(ideal_greeks["explanation"])


with st.container(border=True, key="book_gap"):
    st.subheader("E. Book vs Ideal Greek Gap")
    st.dataframe(comparison_df, use_container_width=True, hide_index=True)


with st.container(border=True, key="book_adjustments"):
    st.subheader("F. Suggested Adjustments")
    candidates = adjustments.get("candidates") or []
    if not candidates:
        st.info("No strike-level candidates are available yet. Load option-chain data or select active expiries from the sidebar.")
    else:
        st.dataframe(_candidate_rows(candidates), use_container_width=True, hide_index=True)
        for item in candidates:
            with st.expander(f"{item.get('profile')} - {item.get('action')} {item.get('instrument_type')} {item.get('strike')}"):
                st.write(f"**Why:** {item.get('reason')}")
                st.write(f"**Lots:** {item.get('lots')} conservative lot(s). Aggressive lot size if safe: {item.get('aggressive_lots_if_safe') or 'NA'}.")
                st.write(f"**Risk remaining:** {item.get('risk_warning')}")

    for warning in adjustments.get("warnings") or []:
        st.warning(warning)


with st.container(border=True, key="book_final_plan"):
    st.subheader("G. Final Action Plan")
    st.markdown("**Final Book Optimization Verdict:**")
    st.write(verdict)


with st.expander("Recent Book Optimization Snapshots"):
    recent = get_recent_book_optimization_snapshots(limit=10)
    if recent.empty:
        st.caption("No saved snapshots found yet.")
    else:
        st.dataframe(recent, use_container_width=True, hide_index=True)
