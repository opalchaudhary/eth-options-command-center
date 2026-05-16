import pandas as pd
import streamlit as st
import importlib

import paper_trading as paper_engine


paper_engine = importlib.reload(paper_engine)
INR_PER_USDT = paper_engine.INR_PER_USDT
PAPER_WALLET_CAPITAL_INR = paper_engine.PAPER_WALLET_CAPITAL_INR
PAPER_WALLET_CAPITAL_USDT = paper_engine.PAPER_WALLET_CAPITAL_USDT
ETH_LOT_SIZE = paper_engine.ETH_LOT_SIZE
classify_greek_health = paper_engine.classify_greek_health
manual_close_trade = paper_engine.manual_close_trade
paper_trading_dashboard_data = paper_engine.paper_trading_dashboard_data


st.set_page_config(
    page_title="Paper Trading | ETH Options Command Center",
    layout="wide",
)

st.title("Paper Trading")
st.caption("Automated paper wallet, strategy selection, running book risk, and trade journal.")


def _fmt_usdt(value):
    return f"{float(value or 0):,.2f}"


def _fmt_inr(value):
    return f"Rs {float(value or 0):,.0f}"


def _fmt_pct(value):
    return f"{float(value or 0):,.2f}%"


def _fmt_num(value, digits=4):
    return f"{float(value or 0):,.{digits}f}"


def _fmt_ist(value):
    if value in [None, ""]:
        return "NA"

    timestamp = pd.to_datetime(value, utc=True, errors="coerce")

    if pd.isna(timestamp):
        return str(value)

    return timestamp.tz_convert("Asia/Kolkata").strftime("%d %b %Y, %I:%M:%S %p IST")


def _json_value(row, key, default=None):
    data = row.get("trade_json")
    return data.get(key, default) if isinstance(data, dict) else default


def _trade_value(row, column, json_key=None, default=None):
    value = row.get(column)

    if value not in [None, ""]:
        return value

    return _json_value(row, json_key or column, default)


def _exit_reason_display(row):
    detail_column = row.get("exit_reason_detail")

    if detail_column:
        return detail_column

    exit_signal = _json_value(row, "exit_signal", {}) or {}
    detail = _json_value(row, "exit_reason_detail")

    if detail:
        return detail

    if exit_signal.get("detail"):
        return exit_signal.get("detail")

    code = row.get("exit_reason")
    fallback = {
        "TP": "Target profit hit.",
        "SL": "Max loss hit.",
        "EXPIRY": "Expiry reached.",
        "MANUAL": "Position was closed manually.",
        "ENGINE_EXIT": "Engine safety exit; detail was not recorded for this older trade.",
    }

    return fallback.get(code, code)


def _greek_rows(trades):
    rows = []

    if trades.empty:
        return rows

    for _, trade in trades.iterrows():
        greeks = _json_value(trade, "current_greeks") or _json_value(trade, "entry_greeks") or {}
        rows.append(
            {
                "Strategy": trade.get("strategy"),
                "Expiry": _fmt_ist(trade.get("expiry_label")),
                "Lots": trade.get("lots"),
                "Delta": greeks.get("delta"),
                "Gamma": greeks.get("gamma"),
                "Theta": greeks.get("theta"),
                "Vega": greeks.get("vega"),
            }
        )

    return rows


def _leg_key(leg):
    return (
        str(leg.get("action") or ""),
        str(leg.get("option") or ""),
        float(leg.get("strike") or 0),
    )


def _leg_mark(leg):
    for key in ["mark_price", "mark", "mid_price", "last_price"]:
        value = leg.get(key)
        if value not in [None, ""]:
            return float(value or 0)

    return 0.0


def _position_leg_rows(trade):
    trade_json = trade.get("trade_json") if isinstance(trade.get("trade_json"), dict) else {}
    recommendation = trade_json.get("recommendation") or {}
    rec_json = recommendation.get("recommendation_json") or {}
    entry_legs = rec_json.get("legs") or trade_json.get("entry_legs") or []
    current_legs = trade_json.get("current_legs") or entry_legs
    current_by_key = {_leg_key(leg): leg for leg in current_legs}
    lots = int(trade.get("lots") or 0)
    eth_qty = lots * ETH_LOT_SIZE
    rows = []

    for entry_leg in entry_legs:
        current_leg = current_by_key.get(_leg_key(entry_leg), entry_leg)
        action = str(entry_leg.get("action") or "")
        sign = 1 if action.lower().startswith("sell") else -1
        current_mark = _leg_mark(current_leg)
        entry_mark = _leg_mark(entry_leg)
        signed_greeks = current_leg.get("signed_greeks") or {}

        rows.append(
            {
                "Action": action,
                "Strike": entry_leg.get("strike"),
                "Type": entry_leg.get("option"),
                "Entry Mark": _fmt_usdt(entry_mark),
                "Current Mark": _fmt_usdt(current_mark),
                "Lots": lots,
                "ETH Qty": _fmt_num(eth_qty, 4),
                "Leg Value": _fmt_usdt(sign * current_mark * eth_qty),
                "OI": current_leg.get("oi"),
                "Volume": current_leg.get("volume"),
                "Delta": signed_greeks.get("delta"),
                "Gamma": signed_greeks.get("gamma"),
                "Theta": signed_greeks.get("theta"),
                "Vega": signed_greeks.get("vega"),
            }
        )

    return rows


auto_enabled = st.sidebar.toggle("Auto Trading Enabled", value=False)
limit_expiries = st.sidebar.slider("Expiries To Evaluate", 3, 12, 6)

if st.sidebar.button("Refresh Paper Trading"):
    st.cache_data.clear()
    st.session_state["run_paper_evaluation"] = True

run_evaluation = st.session_state.pop("run_paper_evaluation", False)

with st.spinner("Loading paper trading book..."):
    try:
        dashboard = paper_trading_dashboard_data(
            auto_enabled=auto_enabled,
            run_evaluation=run_evaluation,
            limit_expiries=limit_expiries,
        )
    except TypeError as exc:
        if "run_evaluation" not in str(exc):
            raise

        st.warning("Reloading paper trading engine; please click Refresh Paper Trading again.")
        dashboard = paper_trading_dashboard_data(auto_enabled=False)

wallet = dashboard["wallet"]
open_trades = dashboard["open_trades"]
closed_trades = dashboard["closed_trades"]
candidates = dashboard.get("candidates") or []
selected = dashboard.get("selected")

st.subheader("Wallet Overview")

top_wallet_cols = st.columns(4)
top_wallet_cols[0].metric("Current Equity", _fmt_inr(wallet["current_equity_inr"]))
top_wallet_cols[1].metric("Available Margin", f"{_fmt_usdt(wallet['available_margin_usdt'])} USDT")
top_wallet_cols[2].metric("Used Margin", f"{_fmt_usdt(wallet['used_margin_usdt'])} USDT")
top_wallet_cols[3].metric("Margin Health", _fmt_pct(wallet["margin_health_pct"]))

wallet_rows = [
    {"Metric": "Starting capital", "INR": _fmt_inr(PAPER_WALLET_CAPITAL_INR), "USDT": _fmt_usdt(PAPER_WALLET_CAPITAL_USDT)},
    {"Metric": "Current equity", "INR": _fmt_inr(wallet["current_equity_inr"]), "USDT": _fmt_usdt(wallet["current_equity_usdt"])},
    {"Metric": "Available margin", "INR": _fmt_inr(wallet["available_margin_inr"]), "USDT": _fmt_usdt(wallet["available_margin_usdt"])},
    {"Metric": "Used margin", "INR": _fmt_inr(wallet["used_margin_inr"]), "USDT": _fmt_usdt(wallet["used_margin_usdt"])},
    {"Metric": "Realized P&L", "INR": _fmt_inr(wallet["realized_pnl_inr"]), "USDT": _fmt_usdt(wallet["realized_pnl_usdt"])},
    {"Metric": "Unrealized P&L", "INR": _fmt_inr(wallet["unrealized_pnl_inr"]), "USDT": _fmt_usdt(wallet["unrealized_pnl_usdt"])},
    {"Metric": "Assumptions", "INR": f"1 USDT = Rs {INR_PER_USDT}", "USDT": f"1 lot = {ETH_LOT_SIZE} ETH"},
]

st.dataframe(wallet_rows, use_container_width=True, hide_index=True)

st.divider()

st.subheader("Auto Trading Status")

status_cols = st.columns(4)
status_cols[0].metric("Status", "Enabled" if auto_enabled else "Disabled")
status_cols[1].metric("Last Evaluation", _fmt_ist(dashboard.get("last_evaluation_time")) if dashboard.get("last_evaluation_time") else "Idle")
status_cols[2].metric(
    "Selected Strategy",
    selected.get("strategy") if selected else "No Trade",
)
status_cols[3].metric(
    "Selection Score",
    selected.get("selection_score") if selected else "NA",
)

if selected:
    st.success(selected.get("entry_reason", "Candidate passed paper trading filters."))
else:
    st.warning(dashboard.get("action") or "No candidate passed the current selection rules.")

st.divider()

st.subheader("Running Book Greeks")

book_greeks = wallet.get("book_greeks") or {}
greek_health = wallet.get("greek_health") or classify_greek_health(book_greeks)
greek_cols = st.columns(5)
greek_cols[0].metric("Net Delta", _fmt_num(book_greeks.get("delta", 0), 4))
greek_cols[1].metric("Net Gamma", _fmt_num(book_greeks.get("gamma", 0), 6))
greek_cols[2].metric("Net Theta", _fmt_num(book_greeks.get("theta", 0), 4))
greek_cols[3].metric("Net Vega", _fmt_num(book_greeks.get("vega", 0), 4))
greek_cols[4].metric("Greek Health", greek_health)

per_position_greeks = _greek_rows(open_trades)

if per_position_greeks:
    st.dataframe(per_position_greeks, use_container_width=True, hide_index=True)
else:
    st.info("No open position Greeks yet.")

st.divider()

st.subheader("Open Paper Positions")

if open_trades.empty:
    st.info("No open paper positions.")
else:
    open_rows = []

    for _, trade in open_trades.iterrows():
        selection = _json_value(trade, "selection", {}) or {}
        open_rows.append(
            {
                "Trade ID": str(trade.get("id"))[:8],
                "Strategy": trade.get("strategy"),
                "Expiry": _fmt_ist(trade.get("expiry_label")),
                "Lots": trade.get("lots"),
                "Entry Value": _fmt_usdt(trade.get("entry_premium_usdt")),
                "Unrealized P&L": _fmt_usdt(trade.get("unrealized_pnl_usdt")),
                "Margin Used": _fmt_usdt(trade.get("margin_used_usdt")),
                "Max Risk": _fmt_usdt(trade.get("max_risk_usdt")),
                "Selection Score": selection.get("selection_score"),
            }
        )

    st.dataframe(open_rows, use_container_width=True, hide_index=True)

    st.markdown("#### Strategy Legs")
    for _, trade in open_trades.iterrows():
        label = (
            f"{trade.get('strategy')} | {_fmt_ist(trade.get('expiry_label'))} | "
            f"{trade.get('lots')} lots | P&L {_fmt_usdt(trade.get('unrealized_pnl_usdt'))} USDT"
        )
        with st.expander(label, expanded=False):
            leg_rows = _position_leg_rows(trade)

            if leg_rows:
                st.dataframe(leg_rows, use_container_width=True, hide_index=True)
            else:
                st.info("Leg details are not recorded for this position.")

    st.markdown("#### Manual Close")
    trade_options = {
        f"{row.get('strategy')} | {_fmt_ist(row.get('expiry_label'))} | {row.get('id')}": row.get("id")
        for _, row in open_trades.iterrows()
    }

    selected_trade_label = st.selectbox("Open position", list(trade_options.keys()))

    if st.button("Close Selected Position"):
        result = manual_close_trade(trade_options[selected_trade_label])
        if result:
            st.success("Paper position closed.")
            st.rerun()
        else:
            st.warning("Unable to close selected paper position.")

st.divider()

tab_closed, tab_rejected = st.tabs(["Closed Paper Trades / Journal", "Rejected Recommendations"])

with tab_closed:
    if closed_trades.empty:
        st.info("No closed paper trades yet.")
    else:
        rows = []
        for _, trade in closed_trades.iterrows():
            selection = _json_value(trade, "selection", {}) or {}
            entry_reason = _trade_value(trade, "entry_reason", default="Not recorded")
            selection_score = _trade_value(
                trade,
                "selection_score",
                default=selection.get("selection_score"),
            )
            rows.append(
                {
                    "Entry Time": _fmt_ist(trade.get("created_at")),
                    "Exit Time": _fmt_ist(trade.get("closed_at")),
                    "Strategy": trade.get("strategy"),
                    "Expiry": _fmt_ist(trade.get("expiry_label")),
                    "P&L": _fmt_usdt(trade.get("realized_pnl_usdt")),
                    "P&L %": round(
                        (float(trade.get("realized_pnl_usdt") or 0) / max(float(trade.get("max_risk_usdt") or 1), 1)) * 100,
                        2,
                    ),
                    "Entry Reason": entry_reason,
                    "Exit Reason": _exit_reason_display(trade),
                    "Selection Score": selection_score,
                }
            )
        st.dataframe(rows, use_container_width=True, hide_index=True)

with tab_rejected:
    rejected_rows = []

    for candidate in candidates:
        if candidate.get("status") != "Rejected":
            continue

        rejected_rows.append(
            {
                "Expiry": _fmt_ist(candidate.get("expiry_label")),
                "Strategy": candidate.get("strategy"),
                "Score": candidate.get("selection_score"),
                "Reward/Risk": candidate.get("reward_risk"),
                "Margin": _fmt_usdt(candidate.get("margin_used_usdt")),
                "Post-Trade Greeks": candidate.get("post_trade_greek_health"),
                "Reason": ", ".join(candidate.get("rejection_reasons") or []),
            }
        )

    if rejected_rows:
        st.dataframe(rejected_rows, use_container_width=True, hide_index=True)
    else:
        st.success("No rejected candidates in the latest evaluation.")
