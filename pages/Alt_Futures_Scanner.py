import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

import alt_futures_engine
from alt_futures_risk import DEFAULT_RISK_PCT, MAX_RISK_PCT
from alt_futures_scanner import ALT_FUTURES_SYMBOLS
from ui_styles import load_css
from validation_config import INR_PER_USDT


st.set_page_config(
    page_title="Alt Futures Scanner | ETH Options Command Center",
    layout="wide",
)

load_css()
st_autorefresh(interval=60 * 1000, key="alt_futures_scanner_refresh")

st.title("Alt Futures Scanner")
st.caption("Autonomous altcoin futures scanner and paper-trading engine with a dedicated Rs 10,000 wallet.")


def _fmt_usdt(value):
    return f"{float(value or 0):,.2f}"


def _fmt_inr(value):
    return f"Rs {float(value or 0):,.0f}"


def _fmt_pct(value):
    return f"{float(value or 0):,.2f}%"


def _fmt_price(value):
    try:
        return f"{float(value):,.6f}".rstrip("0").rstrip(".")
    except Exception:
        return "NA"


def _fmt_ist(value):
    if value in [None, ""]:
        return "NA"

    timestamp = pd.to_datetime(value, utc=True, errors="coerce")

    if pd.isna(timestamp):
        return str(value)

    return timestamp.tz_convert("Asia/Kolkata").strftime("%d %b %Y, %I:%M:%S %p IST")


def _engine_display(engine_status):
    if not engine_status:
        return "Waiting"

    status = engine_status.get("status") or "Unknown"
    created_at = pd.to_datetime(engine_status.get("created_at"), utc=True, errors="coerce")
    interval_seconds = float(engine_status.get("interval_seconds") or 90)

    if not pd.isna(created_at):
        age_seconds = (pd.Timestamp.utcnow() - created_at).total_seconds()
        if age_seconds > max(interval_seconds * 3, 240):
            return "Stale"

    return status


def _candidate_rows(candidates):
    rows = []
    for candidate in candidates or []:
        scores = candidate.get("scores") or {}
        indicators = candidate.get("indicators") or {}
        rows.append(
            {
                "Symbol": candidate.get("symbol"),
                "Class": candidate.get("classification"),
                "Direction": candidate.get("direction"),
                "Score": candidate.get("score"),
                "Price": _fmt_price(candidate.get("price")),
                "Spread": _fmt_pct(candidate.get("spread_pct")),
                "Volume Chg": _fmt_pct(candidate.get("volume_change_pct")),
                "ATR": _fmt_pct(indicators.get("atr_pct")),
                "RSI": indicators.get("rsi"),
                "Liquidity": scores.get("liquidity"),
                "Trend": scores.get("trend"),
                "Reason": candidate.get("reason"),
            }
        )
    return rows


def _trade_rows(trades, active=False):
    rows = []
    if trades is None or trades.empty:
        return rows

    for _, trade in trades.iterrows():
        rows.append(
            {
                "Time": _fmt_ist(trade.get("created_at")),
                "Trade ID": trade.get("trade_id"),
                "Symbol": trade.get("symbol"),
                "Direction": trade.get("direction"),
                "Status": trade.get("status"),
                "Entry": _fmt_price(trade.get("entry_price")),
                "Exit": _fmt_price(trade.get("exit_price")) if not active else "Open",
                "SL": _fmt_price(trade.get("stop_loss")),
                "TP1": _fmt_price(trade.get("take_profit_1")),
                "TP2": _fmt_price(trade.get("take_profit_2")),
                "Lev": f"{trade.get('leverage') or 0}x",
                "Size": trade.get("position_size"),
                "Margin": _fmt_usdt(trade.get("margin_used_usdt")),
                "P&L USDT": _fmt_usdt(trade.get("unrealized_pnl_usdt") if active else trade.get("pnl_usdt")),
                "Reason": trade.get("reason_for_entry") if active else trade.get("reason_for_exit") or trade.get("reason_for_entry"),
            }
        )
    return rows


st.sidebar.caption("Read-only scanner state. Run execution cycles from the backend process.")
st.sidebar.metric("Wallet", "Rs 10,000")
st.sidebar.metric("USDT/INR", f"Rs {INR_PER_USDT}")
st.sidebar.metric("Universe", f"{len(ALT_FUTURES_SYMBOLS)} symbols")

with st.spinner("Loading alt futures scanner state..."):
    dashboard = alt_futures_engine.alt_futures_dashboard_data(run_cycle=False)

wallet = dashboard.get("wallet") or {}
candidates = dashboard.get("candidates") or []
decision = dashboard.get("decision") or {}
open_trades = dashboard.get("open_trades")
closed_trades = dashboard.get("closed_trades")
events = dashboard.get("events")
scanner_history = dashboard.get("scanner_history")
engine_status = dashboard.get("engine_status") or {}
engine_display = _engine_display(engine_status)

st.sidebar.metric("Engine", engine_display)
st.sidebar.metric("Latest Action", engine_status.get("action") or dashboard.get("action", "Waiting"))

with st.container(key="alt_wallet_summary"):
    st.subheader("Wallet Summary")
    cols = st.columns(5)
    cols[0].metric("Equity", f"{_fmt_usdt(wallet.get('equity_usdt'))} USDT", _fmt_inr(wallet.get("equity_inr")))
    cols[1].metric("Available", f"{_fmt_usdt(wallet.get('available_balance_usdt'))} USDT")
    cols[2].metric("Used Margin", f"{_fmt_usdt(wallet.get('used_margin_usdt'))} USDT")
    cols[3].metric("Unrealized P&L", f"{_fmt_usdt(wallet.get('unrealized_pnl_usdt'))} USDT")
    cols[4].metric("Win Rate", _fmt_pct(wallet.get("win_rate")))

st.divider()

with st.container(key="alt_best_candidate"):
    st.subheader("Current Best Candidate")
    best = candidates[0] if candidates else {}
    risk = decision.get("risk") or {}
    cols = st.columns(6)
    cols[0].metric("Symbol", decision.get("symbol") or best.get("symbol") or "NA")
    cols[1].metric("Decision", decision.get("direction") or "NO_TRADE")
    cols[2].metric("Score", decision.get("candidate_score") or best.get("score") or 0)
    cols[3].metric("Entry", _fmt_price(decision.get("entry_price")))
    cols[4].metric("RR", risk.get("rr_ratio") or "NA")
    cols[5].metric("Leverage", f"{risk.get('leverage') or 0}x")

    if decision.get("direction") == "NO_TRADE":
        st.info(decision.get("reason") or "Waiting for the scanner to produce a tradeable setup.")
    else:
        st.success(decision.get("reason"))

    if engine_status.get("error"):
        st.error(f"Latest worker cycle failed: {engine_status.get('error')}")
    elif engine_display == "Stale":
        st.warning("The latest alt futures worker heartbeat is stale. The Streamlit process may have slept.")
    elif not engine_status:
        st.warning("No alt futures worker heartbeat has been recorded yet. Apply the migration and keep the app awake.")
    else:
        st.caption(engine_status.get("action") or "Alt futures scanner heartbeat received.")

st.divider()

long_candidates = [
    item for item in candidates
    if item.get("classification") in ["STRONG_LONG", "LONG"]
]
short_candidates = [
    item for item in candidates
    if item.get("classification") in ["STRONG_SHORT", "SHORT"]
]
watchlist = [
    item for item in candidates
    if item.get("classification") in ["WATCHLIST", "AVOID", "NO_TRADE"]
]

tab_long, tab_short, tab_watch = st.tabs(["Top Long Candidates", "Top Short Candidates", "Watchlist / Avoid"])

with tab_long:
    rows = _candidate_rows(long_candidates)
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("No long candidate currently passes the scanner filter.")

with tab_short:
    rows = _candidate_rows(short_candidates)
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("No short candidate currently passes the scanner filter.")

with tab_watch:
    rows = _candidate_rows(watchlist)
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("No watchlist or avoid rows yet.")

st.divider()

with st.container(key="alt_active_trade"):
    st.subheader("Active Trade Card")
    rows = _trade_rows(open_trades, active=True)
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("No open alt futures paper trade. The engine starts with one active trade maximum.")

st.divider()

with st.container(key="alt_risk_settings"):
    st.subheader("Risk Settings Display")
    risk_rows = [
        {"Setting": "Dedicated wallet", "Value": "Rs 10,000"},
        {"Setting": "USDT conversion", "Value": f"1 USDT = Rs {INR_PER_USDT}"},
        {"Setting": "Default risk per trade", "Value": _fmt_pct(DEFAULT_RISK_PCT * 100)},
        {"Setting": "Hard max risk per trade", "Value": _fmt_pct(MAX_RISK_PCT * 100)},
        {"Setting": "Maximum margin usage", "Value": "25% to 35% of wallet"},
        {"Setting": "Active trade limit", "Value": "One open alt futures trade"},
        {"Setting": "Execution posture", "Value": "Skip when score, spread, liquidity, RR, or liquidation safety fails."},
    ]
    st.dataframe(risk_rows, use_container_width=True, hide_index=True)

st.divider()

tab_trades, tab_events, tab_history = st.tabs(["Trade Journal", "Trade Events", "Scanner History"])

with tab_trades:
    rows = _trade_rows(closed_trades)
    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
    else:
        st.info("No closed, cancelled, or skipped alt futures trades yet.")

with tab_events:
    if events is None or events.empty:
        st.info("No alt futures lifecycle events yet.")
    else:
        rows = []
        for _, event in events.iterrows():
            rows.append(
                {
                    "Time": _fmt_ist(event.get("created_at")),
                    "Trade ID": event.get("trade_id"),
                    "Event": event.get("event_type"),
                    "Price": _fmt_price(event.get("price")),
                    "P&L": _fmt_usdt(event.get("pnl_usdt")),
                    "Reason": event.get("reason"),
                }
            )
        st.dataframe(rows, use_container_width=True, hide_index=True)

with tab_history:
    if scanner_history is None or scanner_history.empty:
        st.info("No scanner snapshots yet.")
    else:
        rows = []
        for _, row in scanner_history.head(140).iterrows():
            rows.append(
                {
                    "Time": _fmt_ist(row.get("created_at")),
                    "Symbol": row.get("symbol"),
                    "Score": row.get("score"),
                    "Class": row.get("classification"),
                    "Selected": row.get("selected"),
                    "Price": _fmt_price(row.get("price")),
                    "Spread": _fmt_pct(row.get("spread_pct")),
                    "Liquidity": row.get("liquidity_score"),
                    "Trend": row.get("trend_score"),
                    "Reason": row.get("final_reason"),
                }
            )
        st.dataframe(rows, use_container_width=True, hide_index=True)
