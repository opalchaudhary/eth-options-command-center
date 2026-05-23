import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from api_client import api_get, backend_url
import paper_trading as paper_engine
from ui_styles import load_css


INR_PER_USDT = paper_engine.INR_PER_USDT
PAPER_WALLET_CAPITAL_INR = paper_engine.PAPER_WALLET_CAPITAL_INR
PAPER_WALLET_CAPITAL_USDT = paper_engine.PAPER_WALLET_CAPITAL_USDT
ETH_LOT_SIZE = paper_engine.ETH_LOT_SIZE
classify_greek_health = paper_engine.classify_greek_health


st.set_page_config(
    page_title="Paper Trading | ETH Options Command Center",
    layout="wide",
)

load_css()
st_autorefresh(interval=60 * 1000, key="paper_trading_observer_refresh")

st.title("Paper Trading")
st.caption("Autonomous paper wallet, strategy selection, running book risk, and trade journal.")


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


def _engine_display(engine_status):
    if not engine_status:
        return "Not connected"

    status = engine_status.get("status") or "Unknown"
    created_at = pd.to_datetime(engine_status.get("created_at"), utc=True, errors="coerce")
    interval_seconds = float(engine_status.get("interval_seconds") or 60)

    if not pd.isna(created_at):
        age_seconds = (pd.Timestamp.utcnow() - created_at).total_seconds()
        if age_seconds > max(interval_seconds * 3, 180):
            return "Stale"

    return status


def _engine_interval_display(engine_status):
    value = engine_status.get("interval_seconds") if engine_status else None
    return f"{value}s" if value else "Waiting"


def _engine_limit_display(engine_status):
    return engine_status.get("limit_expiries") if engine_status else "Waiting"


def _engine_message(engine_status, dashboard_action):
    if not engine_status:
        return (
            "No paper trading worker heartbeat has been recorded yet. Apply the paper_trading_engine_runs "
            "migration. The Streamlit-hosted worker will write a heartbeat while the app is awake."
        )

    if _engine_display(engine_status) == "Stale":
        return (
            "The last paper trading worker heartbeat is stale. The Streamlit app may have slept, restarted, "
            "or unable to reach Supabase."
        )

    if engine_status.get("error"):
        return f"Latest engine cycle failed: {engine_status.get('error')}"

    return engine_status.get("action") or dashboard_action or "Paper trading worker heartbeat received."


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


with st.spinner("Loading paper trading book..."):
    try:
        status_response = api_get("/paper-trading/status")
        dashboard = status_response.get("dashboard") or {}
    except Exception as exc:
        st.error(f"FastAPI backend unavailable at {backend_url()}: {exc}")
        st.stop()

wallet = dashboard["wallet"]
open_trades = pd.DataFrame(dashboard.get("open_trades") or [])
closed_trades = pd.DataFrame(dashboard.get("closed_trades") or [])
candidates = dashboard.get("candidates") or []
selected = dashboard.get("selected")
engine_status = dashboard.get("engine_status") or {}
engine_display = _engine_display(engine_status)

st.sidebar.caption(f"Backend: {backend_url()}")
st.sidebar.metric("Engine", engine_display)
st.sidebar.metric("Cycle Interval", _engine_interval_display(engine_status))
st.sidebar.metric("Expiries Evaluated", _engine_limit_display(engine_status))

with st.container(key="paper_wallet"):
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

with st.container(key="paper_status"):
    st.subheader("Autonomous Trading Status")

    status_cols = st.columns(4)
    status_cols[0].metric("Daemon", engine_display)
    status_cols[1].metric(
        "Last Evaluation",
        _fmt_ist(dashboard.get("last_evaluation_time") or engine_status.get("created_at"))
        if dashboard.get("last_evaluation_time") or engine_status.get("created_at")
        else "Pending",
    )
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
    elif engine_status.get("error"):
        st.error(_engine_message(engine_status, dashboard.get("action")))
    elif not engine_status or engine_display == "Stale":
        st.warning(_engine_message(engine_status, dashboard.get("action")))
    else:
        st.info(_engine_message(engine_status, dashboard.get("action")))

st.divider()

with st.container(key="paper_greeks"):
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

with st.container(key="paper_open_positions"):
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

st.divider()

with st.container(key="paper_journal"):
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
