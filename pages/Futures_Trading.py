import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from api_client import api_get, backend_url
from ui_styles import load_css
from validation_config import INR_PER_USDT, ETH_LOT_SIZE


st.set_page_config(
    page_title="Futures Trading | ETH Options Command Center",
    layout="wide",
)

load_css()
st_autorefresh(interval=60 * 1000, key="futures_trading_refresh")

st.title("Futures Trading")
st.caption("Autonomous ETH futures paper trading with capital-first risk controls.")


def _fmt_usdt(value):
    return f"{float(value or 0):,.2f}"


def _fmt_inr(value):
    return f"Rs {float(value or 0):,.0f}"


def _fmt_pct(value):
    return f"{float(value or 0):,.2f}%"


def _fmt_price(value):
    return f"{float(value or 0):,.2f}"


def _fmt_optional_price(value):
    try:
        if value is None or pd.isna(value):
            return "NA"
        return f"{float(value):,.2f}"
    except Exception:
        return "NA"


def _fmt_optional_num(value, digits=2):
    try:
        if value is None or pd.isna(value):
            return "NA"
        return f"{float(value):,.{digits}f}"
    except Exception:
        return "NA"


def _fmt_ist(value):
    if value in [None, ""]:
        return "NA"

    timestamp = pd.to_datetime(value, utc=True, errors="coerce")

    if pd.isna(timestamp):
        return str(value)

    return timestamp.tz_convert("Asia/Kolkata").strftime("%d %b %Y, %I:%M:%S %p IST")


def _json_text(value, fallback="NA"):
    def _is_empty(item):
        if item is None:
            return True
        if isinstance(item, str):
            return item == ""
        if isinstance(item, (list, tuple, set, dict)):
            return len(item) == 0
        if isinstance(item, pd.DataFrame):
            return item.empty
        try:
            return bool(pd.isna(item))
        except Exception:
            return False

    def _compact_value(item):
        if isinstance(item, pd.DataFrame):
            return f"{len(item)} rows"
        if isinstance(item, dict):
            return {
                key: _compact_value(nested)
                for key, nested in item.items()
                if not _is_empty(nested)
            }
        if isinstance(item, list):
            return [_compact_value(nested) for nested in item[:3]]
        return item

    if isinstance(value, dict):
        notes = value.get("notes") or value.get("warnings") or []
        if notes:
            return "; ".join(str(item) for item in notes[:3])
        compact = {
            key: _compact_value(item)
            for key, item in value.items()
            if not _is_empty(item)
        }
        return str(compact)[:180] if compact else fallback
    if isinstance(value, list):
        return "; ".join(str(item) for item in value[:3]) if value else fallback
    return value or fallback


def _yes_no(value):
    return "Yes" if bool(value) else "No"


def _join_parts(parts):
    clean_parts = []

    for part in parts:
        if not part:
            continue

        if str(part).split(": ")[-1] == "NA":
            continue

        clean_parts.append(part)

    return " | ".join(clean_parts) if clean_parts else "NA"


def _smc_summary(smc):
    smc = smc if isinstance(smc, dict) else {}
    notes = smc.get("notes") or []
    warnings = smc.get("warnings") or []

    parts = [
        f"Inside demand: {_yes_no(smc.get('inside_demand'))}",
        f"Inside supply: {_yes_no(smc.get('inside_supply'))}",
        f"Zone break risk: {_yes_no(smc.get('zone_break_risk'))}",
        f"Call sell confidence: {smc.get('call_sell_confidence') or 'NA'}",
        f"Put sell confidence: {smc.get('put_sell_confidence') or 'NA'}",
    ]

    if warnings:
        parts.append("Warning: " + "; ".join(str(item) for item in warnings[:2]))
    elif notes:
        parts.append("Note: " + "; ".join(str(item) for item in notes[:2]))

    return _join_parts(parts)


def _volume_summary(volume):
    volume = volume if isinstance(volume, dict) else {}
    notes = volume.get("notes") or []
    warnings = volume.get("warnings") or []

    parts = [
        f"POC: {_fmt_optional_price(volume.get('poc_price'))}",
        f"Near HVN: {_yes_no(volume.get('near_hvn'))}",
        f"Near LVN: {_yes_no(volume.get('near_lvn'))}",
        f"Expiry magnet: {_yes_no(volume.get('expiry_magnet'))}",
        f"Breakout risk: {_yes_no(volume.get('breakout_risk'))}",
    ]

    if warnings:
        parts.append("Warning: " + "; ".join(str(item) for item in warnings[:2]))
    elif notes:
        parts.append("Note: " + "; ".join(str(item) for item in notes[:2]))

    return _join_parts(parts)


def _options_summary(options):
    options = options if isinstance(options, dict) else {}
    parts = [
        f"PCR: {_fmt_optional_num(options.get('pcr'), 2)}",
        f"Max pain: {_fmt_optional_price(options.get('max_pain'))}",
        f"Call wall: {_fmt_optional_price(options.get('call_wall'))}",
        f"Put wall: {_fmt_optional_price(options.get('put_wall'))}",
        f"Median IV: {_fmt_optional_num(options.get('median_iv'), 2)}",
        f"Realized vol: {_fmt_optional_num(options.get('realized_vol_pct'), 2)}",
        f"IV-RV: {_fmt_optional_num(options.get('iv_rv_spread'), 2)}",
        f"Net delta: {_fmt_optional_num(options.get('net_delta'), 2)}",
        f"Net gamma: {_fmt_optional_num(options.get('net_gamma'), 4)}",
    ]

    environment = options.get("option_selling_environment")
    if environment:
        parts.append(f"Option selling: {environment}")

    return _join_parts(parts)


st.sidebar.caption(f"Backend: {backend_url()}")
st.sidebar.metric("Contract Size", f"{ETH_LOT_SIZE} ETH")
st.sidebar.metric("USDT/INR", f"Rs {INR_PER_USDT}")

@st.cache_data(ttl=30, show_spinner=False)
def _cached_futures_status():
    return api_get("/futures-trading/status", params={"limit": 50, "compact": True})


with st.spinner("Loading futures paper trading book..."):
    try:
        status_response = _cached_futures_status()
        dashboard = status_response.get("dashboard") or {}
    except Exception as exc:
        st.error(f"FastAPI backend unavailable at {backend_url()}: {exc}")
        st.stop()

wallet = dashboard.get("wallet") or {}
context = dashboard.get("context") or {}
decision = dashboard.get("decision") or {}
open_positions = pd.DataFrame(dashboard.get("open_positions") or [])
closed_trades = pd.DataFrame(dashboard.get("closed_trades") or [])
journal = pd.DataFrame(dashboard.get("journal") or [])
training_dataset = pd.DataFrame(dashboard.get("training_dataset") or [])
engine_status = dashboard.get("engine_status") or {}


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


def _engine_message(engine_status):
    if not engine_status:
        return (
            "No futures trading worker heartbeat has been recorded yet. Apply the futures migration; "
            "the Streamlit-hosted worker will write heartbeats while the app is awake."
        )

    if _engine_display(engine_status) == "Stale":
        return "The latest futures worker heartbeat is stale. The Streamlit app may have slept or restarted."

    if engine_status.get("error"):
        return f"Latest futures worker cycle failed: {engine_status.get('error')}"

    return engine_status.get("action") or "Futures trading worker heartbeat received."


engine_display = _engine_display(engine_status)
st.sidebar.metric("Engine", engine_display)
st.sidebar.metric("Cycle Interval", f"{engine_status.get('interval_seconds')}s" if engine_status else "Waiting")
st.sidebar.metric("Latest Action", engine_status.get("action") or dashboard.get("action", "Waiting"))

with st.container(key="futures_wallet"):
    st.subheader("Futures Wallet Summary")

    wallet_cols = st.columns(5)
    wallet_cols[0].metric("Equity", f"{_fmt_usdt(wallet.get('equity_usdt'))} USDT", _fmt_inr(wallet.get("equity_inr")))
    wallet_cols[1].metric("Available", f"{_fmt_usdt(wallet.get('available_balance_usdt'))} USDT")
    wallet_cols[2].metric("Used Margin", f"{_fmt_usdt(wallet.get('used_margin_usdt'))} USDT")
    wallet_cols[3].metric("Unrealized P&L", f"{_fmt_usdt(wallet.get('unrealized_pnl_usdt'))} USDT")
    wallet_cols[4].metric("Win Rate", _fmt_pct(wallet.get("win_rate")))

    wallet_rows = [
        {"Metric": "Starting balance", "USDT": _fmt_usdt(wallet.get("starting_balance_usdt")), "INR": _fmt_inr(wallet.get("starting_balance_inr"))},
        {"Metric": "Current balance", "USDT": _fmt_usdt(wallet.get("current_balance_usdt")), "INR": _fmt_inr(wallet.get("current_balance_inr"))},
        {"Metric": "Realized P&L", "USDT": _fmt_usdt(wallet.get("realized_pnl_usdt")), "INR": _fmt_inr(wallet.get("realized_pnl_inr"))},
        {"Metric": "Max drawdown", "USDT": _fmt_pct(wallet.get("max_drawdown_pct")), "INR": "Risk metric"},
        {"Metric": "Open positions", "USDT": wallet.get("open_positions"), "INR": "One ETH futures position max"},
    ]
    st.dataframe(wallet_rows, use_container_width=True, hide_index=True)

st.divider()

with st.container(key="futures_context"):
    st.subheader("Current Market Context")

    context_cols = st.columns(5)
    context_cols[0].metric("Mark Price", _fmt_price(context.get("mark_price")))
    context_cols[1].metric("Market Regime", context.get("market_regime") or "NA")
    context_cols[2].metric("Trend", context.get("directional_bias") or "NA")
    context_cols[3].metric("Volatility", context.get("volatility_regime") or "NA")
    context_cols[4].metric("Conflict", _fmt_pct(context.get("signal_conflict_score")))

    context_rows = [
        {"Context": "SMC", "Value": _smc_summary(context.get("smc_context"))},
        {"Context": "Volume", "Value": _volume_summary(context.get("volume_context"))},
        {"Context": "Options", "Value": _options_summary(context.get("options_context"))},
        {"Context": "Key insight", "Value": _json_text(context.get("key_insights"))},
        {"Context": "Risk warning", "Value": _json_text(context.get("risk_warnings"))},
    ]
    st.dataframe(context_rows, use_container_width=True, hide_index=True)

st.divider()

with st.container(key="futures_decision"):
    st.subheader("Current Engine Decision")

    risk = decision.get("risk") or {}
    decision_cols = st.columns(6)
    decision_cols[0].metric("Decision", decision.get("direction") or "NO_TRADE")
    decision_cols[1].metric("Confidence", _fmt_pct(decision.get("confidence_score")))
    decision_cols[2].metric("Entry", _fmt_price(decision.get("entry_price")))
    decision_cols[3].metric("Leverage", f"{risk.get('leverage') or 0}x")
    decision_cols[4].metric("Lots", risk.get("lots") or 0)
    decision_cols[5].metric("RR", risk.get("rr_ratio") or "NA")

    if decision.get("direction") == "NO_TRADE":
        st.info(decision.get("reason") or "No trade is valid under the current market and risk state.")
    else:
        st.success(decision.get("reason"))

    if engine_status.get("error"):
        st.error(_engine_message(engine_status))
    elif not engine_status or engine_display == "Stale":
        st.warning(_engine_message(engine_status))
    else:
        st.caption(_engine_message(engine_status))

st.divider()

with st.container(key="futures_open_position"):
    st.subheader("Open Futures Position")

    if open_positions is None or open_positions.empty:
        st.info("No open ETH futures paper position.")
    else:
        rows = []
        for _, trade in open_positions.iterrows():
            rows.append(
                {
                    "Trade ID": trade.get("trade_id"),
                    "Direction": trade.get("direction"),
                    "Entry": _fmt_price(trade.get("entry_price")),
                    "Mark": _fmt_price(trade.get("mark_price")),
                    "SL": _fmt_price(trade.get("stop_loss")),
                    "TP": _fmt_price(trade.get("take_profit")),
                    "Trailing SL": _fmt_price(trade.get("trailing_stop")),
                    "Liq Est": _fmt_price(trade.get("liquidation_price_estimate")),
                    "Lev": f"{trade.get('leverage')}x",
                    "Lots": trade.get("lots"),
                    "P&L USDT": _fmt_usdt(trade.get("unrealized_pnl_usdt")),
                    "Reason": trade.get("entry_reason"),
                }
            )
        st.dataframe(rows, use_container_width=True, hide_index=True)

st.divider()

with st.container(key="futures_risk"):
    st.subheader("Risk Management Panel")

    risk_rows = [
        {"Metric": "Risk amount", "Value": f"{_fmt_usdt(risk.get('risk_amount_usdt'))} USDT / {_fmt_inr(risk.get('risk_amount_inr'))}"},
        {"Metric": "Risk percentage", "Value": _fmt_pct(risk.get("risk_pct"))},
        {"Metric": "Expected reward", "Value": f"{_fmt_usdt(risk.get('expected_reward_usdt'))} USDT / {_fmt_inr(risk.get('expected_reward_inr'))}"},
        {"Metric": "Margin required", "Value": f"{_fmt_usdt(risk.get('margin_required_usdt'))} USDT / {_fmt_inr(risk.get('margin_required_inr'))}"},
        {"Metric": "Liquidation estimate", "Value": _fmt_price(risk.get("liquidation_price_estimate"))},
        {"Metric": "Liquidation distance", "Value": _fmt_pct(risk.get("liquidation_distance_pct"))},
        {"Metric": "Position size", "Value": f"{risk.get('position_size_eth') or 0} ETH"},
        {"Metric": "Safety posture", "Value": "Capital preservation first; no trade when RR, confidence, or liquidation distance fails."},
    ]
    st.dataframe(risk_rows, use_container_width=True, hide_index=True)

st.divider()

tab_journal, tab_closed, tab_performance, tab_training = st.tabs(
    ["Trade Journal", "Closed Trades", "Performance Metrics", "AI Training Dataset Status"]
)

with tab_journal:
    if journal is None or journal.empty:
        st.info("No futures journal events yet.")
    else:
        rows = []
        for _, event in journal.iterrows():
            rows.append(
                {
                    "Time": _fmt_ist(event.get("created_at")),
                    "Trade ID": event.get("trade_id"),
                    "Event": event.get("event_type"),
                    "Price": _fmt_price(event.get("price")),
                    "P&L": _fmt_usdt(event.get("pnl_usdt")),
                    "Action": event.get("action_taken"),
                    "Reason": event.get("reason"),
                }
            )
        st.dataframe(rows, use_container_width=True, hide_index=True)

with tab_closed:
    if closed_trades is None or closed_trades.empty:
        st.info("No closed futures trades yet.")
    else:
        rows = []
        for _, trade in closed_trades.iterrows():
            rows.append(
                {
                    "Entry Time": _fmt_ist(trade.get("created_at")),
                    "Exit Time": _fmt_ist(trade.get("updated_at")),
                    "Trade ID": trade.get("trade_id"),
                    "Direction": trade.get("direction"),
                    "Entry": _fmt_price(trade.get("entry_price")),
                    "Exit": _fmt_price(trade.get("exit_price")),
                    "P&L USDT": _fmt_usdt(trade.get("realized_pnl_usdt")),
                    "P&L INR": _fmt_inr(trade.get("realized_pnl_inr")),
                    "Exit Reason": trade.get("exit_reason"),
                }
            )
        st.dataframe(rows, use_container_width=True, hide_index=True)

with tab_performance:
    perf_cols = st.columns(5)
    perf_cols[0].metric("Total Trades", wallet.get("total_trades"))
    perf_cols[1].metric("Winning", wallet.get("winning_trades"))
    perf_cols[2].metric("Losing", wallet.get("losing_trades"))
    perf_cols[3].metric("Win Rate", _fmt_pct(wallet.get("win_rate")))
    perf_cols[4].metric("Max Drawdown", _fmt_pct(wallet.get("max_drawdown_pct")))

with tab_training:
    if training_dataset is None or training_dataset.empty:
        st.info("No AI training rows yet. The first opened trade will seed the dataset.")
    else:
        ready_count = int(training_dataset.get("model_ready", pd.Series(dtype=bool)).fillna(False).sum())
        status_cols = st.columns(3)
        status_cols[0].metric("Rows", len(training_dataset))
        status_cols[1].metric("Model Ready", ready_count)
        status_cols[2].metric("Pending Labels", len(training_dataset) - ready_count)

        rows = []
        for _, row in training_dataset.iterrows():
            rows.append(
                {
                    "Created": _fmt_ist(row.get("created_at")),
                    "Trade ID": row.get("trade_id"),
                    "Label": row.get("label"),
                    "Final Outcome": row.get("final_outcome"),
                    "MFE": row.get("max_favorable_excursion"),
                    "MAE": row.get("max_adverse_excursion"),
                    "Ready": row.get("model_ready"),
                }
            )
        st.dataframe(rows, use_container_width=True, hide_index=True)
