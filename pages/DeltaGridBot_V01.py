from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd
import requests
import streamlit as st
try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

from api_client import api_get, api_post, backend_url
from ui_styles import load_css


st.set_page_config(page_title="DeltaGridBot V0.1", layout="wide")
load_css()


STATUS_COLORS = {
    "TESTNET": ("#1d4ed8", "#dbeafe"),
    "REST_FALLBACK": ("#92400e", "#fef3c7"),
    "PRIVATE_WS_BLOCKED": ("#991b1b", "#fee2e2"),
    "RUNNING": ("#166534", "#dcfce7"),
    "PAUSED": ("#92400e", "#fef3c7"),
    "STOPPED": ("#374151", "#f3f4f6"),
    "GREEN": ("#166534", "#dcfce7"),
    "YELLOW": ("#854d0e", "#fef9c3"),
    "ORANGE": ("#9a3412", "#ffedd5"),
    "RED": ("#991b1b", "#fee2e2"),
    "CRITICAL": ("#7f1d1d", "#fee2e2"),
    "UNKNOWN": ("#374151", "#f3f4f6"),
}


st.markdown(
    """
    <style>
    .gridbot-header {
        padding: 1.2rem 1.3rem;
        border: 1px solid rgba(148, 163, 184, 0.28);
        border-radius: 8px;
        background: rgba(255, 255, 255, 0.72);
        box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
        margin-bottom: 1rem;
    }
    .gridbot-title { font-size: 1.8rem; font-weight: 760; margin: 0; letter-spacing: 0; }
    .gridbot-subtitle { color: #64748b; font-size: 0.98rem; margin-top: 0.15rem; }
    .df-card {
        border: 1px solid rgba(148, 163, 184, 0.28);
        border-radius: 8px;
        padding: 1rem;
        background: rgba(255, 255, 255, 0.72);
        box-shadow: 0 8px 18px rgba(15, 23, 42, 0.04);
        min-height: 112px;
    }
    .df-card h3 { margin: 0 0 0.35rem 0; font-size: 0.82rem; color: #64748b; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; }
    .df-value { font-size: 1.45rem; font-weight: 760; color: #0f172a; line-height: 1.25; }
    .df-muted { color: #64748b; font-size: 0.86rem; margin-top: 0.15rem; }
    .df-section-title { font-size: 1.1rem; font-weight: 760; margin: 1.1rem 0 0.65rem 0; }
    .df-hero {
        border: 1px solid rgba(34, 197, 94, 0.28);
        border-radius: 8px;
        padding: 1rem;
        background: rgba(240, 253, 244, 0.72);
        margin: 0.7rem 0 1rem 0;
    }
    .badge {
        display: inline-block;
        padding: 0.24rem 0.55rem;
        border-radius: 999px;
        font-size: 0.73rem;
        font-weight: 800;
        margin-right: 0.35rem;
        margin-top: 0.25rem;
        letter-spacing: 0.02em;
    }
    .summary-row {
        display: flex;
        justify-content: space-between;
        gap: 1rem;
        border-bottom: 1px solid rgba(148, 163, 184, 0.20);
        padding: 0.42rem 0;
        font-size: 0.92rem;
    }
    .summary-row span:first-child { color: #64748b; }
    .summary-row span:last-child { font-weight: 650; color: #0f172a; }
    @media (prefers-color-scheme: dark) {
        .gridbot-header, .df-card { background: rgba(15, 23, 42, 0.45); }
        .df-value, .gridbot-title, .summary-row span:last-child { color: #e5e7eb; }
        .df-muted, .gridbot-subtitle, .df-card h3, .summary-row span:first-child { color: #94a3b8; }
        .df-hero { background: rgba(20, 83, 45, 0.28); }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def badge(label):
    fg, bg = STATUS_COLORS.get(str(label).upper(), ("#374151", "#f3f4f6"))
    return f"<span class='badge' style='color:{fg};background:{bg}'>{label}</span>"


def card(title, value, subtext=""):
    st.markdown(
        f"<div class='df-card'><h3>{title}</h3><div class='df-value'>{value}</div><div class='df-muted'>{subtext}</div></div>",
        unsafe_allow_html=True,
    )


def fmt_money(value):
    if value in [None, "", "N/A"]:
        return "N/A"
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def fmt_pct(value):
    if value in [None, "", "N/A"]:
        return "N/A"
    try:
        return f"{float(value) * 100:.1f}%"
    except Exception:
        return str(value)


def short_id(value):
    text = str(value or "")
    return text if len(text) <= 18 else f"{text[:8]}...{text[-6:]}"


def runtime(started_at):
    if not started_at:
        return "N/A"
    try:
        start = datetime.fromisoformat(str(started_at).replace("Z", "+00:00"))
        seconds = max(0, int((datetime.now(timezone.utc) - start).total_seconds()))
        hours, remainder = divmod(seconds, 3600)
        minutes, _ = divmod(remainder, 60)
        return f"{hours}h {minutes}m" if hours else f"{minutes}m"
    except Exception:
        return "N/A"


def rows_by_side(levels):
    buys = [row for row in levels if row.get("side") == "buy"]
    sells = [row for row in levels if row.get("side") == "sell"]
    max_len = max(len(buys), len(sells), 1)
    rows = []
    for index in range(max_len):
        rows.append(
            {
                "BUY LEVELS": buys[index].get("price") if index < len(buys) else "",
                "SELL LEVELS": sells[index].get("price") if index < len(sells) else "",
            }
        )
    return pd.DataFrame(rows)


def summary_line(label, value):
    st.markdown(f"<div class='summary-row'><span>{label}</span><span>{value}</span></div>", unsafe_allow_html=True)


def refresh_live_status():
    status = api_get("/api/grid/v01/live/status")
    st.session_state["gridbot_live_status"] = status
    return status


def safe_post(path, payload=None, timeout=15):
    try:
        return api_post(path, payload, timeout=timeout)
    except requests.Timeout:
        if path == "/api/grid/v01/live/start":
            st.warning("Grid start is taking longer than expected. Checking current run status...")
            status = refresh_live_status()
            active_run = status.get("active_run")
            if active_run:
                return {"ok": True, "run": active_run, "attached_after_timeout": True}
        st.error("GridBot request timed out.")
        st.stop()
    except requests.ConnectionError as exc:
        st.error(f"GridBot API connection failed: {exc}")
        st.stop()
    except requests.HTTPError as exc:
        detail = ""
        try:
            detail = exc.response.json().get("detail")
        except Exception:
            detail = str(exc)
        st.error(f"GridBot API request failed: {detail}")
        st.stop()


with st.sidebar:
    st.caption(f"Backend: {backend_url()}")
    if st.button("Refresh"):
        st.session_state.pop("gridbot_history_runs", None)
        st.session_state.pop("gridbot_history_summaries", None)
        st.rerun()

try:
    live_status = refresh_live_status()
except Exception as exc:
    st.error(f"GridBot API unavailable: {exc}")
    st.stop()

active = live_status.get("active_run") or {}
active_status = active.get("status") or "STOPPED"
if active_status == "STARTING" and st_autorefresh:
    st_autorefresh(interval=2000, key="gridbot_starting_refresh")
elif active_status == "RUNNING" and st_autorefresh:
    st_autorefresh(interval=10000, key="gridbot_running_refresh")

try:
    health = api_get("/api/grid/v01/live/market-account", {"product_symbol": "ETHUSD"}, timeout=15)
except Exception as exc:
    health = {"ok": False, "error": str(exc), "product": {}, "market": {}, "account": {}}

market = health.get("market") or {}
product = health.get("product") or {}
account = health.get("account") or {}
greeks = account.get("portfolio_greeks") or {}

st.markdown(
    "<div class='gridbot-header'>"
    "<div class='gridbot-title'>DeltaGridBot V0.1</div>"
    "<div class='gridbot-subtitle'>ETHUSD Grid Execution</div>"
    f"{badge('TESTNET')}{badge('REST_FALLBACK')}{badge('PRIVATE_WS_BLOCKED')}{badge(active_status)}"
    "</div>",
    unsafe_allow_html=True,
)

top = st.columns(5)
with top[0]:
    card("ETH Price", fmt_money(market.get("mark_price")), f"Bid {fmt_money(market.get('best_bid'))} / Ask {fmt_money(market.get('best_ask'))}")
with top[1]:
    card("Account Equity", fmt_money(account.get("account_equity")), "Delta Testnet")
with top[2]:
    card("Available Margin", fmt_money(account.get("available_margin")), f"Used {fmt_money(account.get('margin_used'))}")
with top[3]:
    card("Margin Utilisation", fmt_pct(account.get("margin_utilisation")), f"Position {account.get('current_position') or '0'} lots")
with top[4]:
    risk_label = (st.session_state.get("gridbot_preview") or {}).get("risk", {}).get("risk_state") or "UNKNOWN"
    card("Risk State", risk_label, f"Open GridBot orders {account.get('open_gridbot_orders', 'N/A')}")

greek_cols = st.columns(4)
for col, key in zip(greek_cols, ["delta", "gamma", "vega", "theta"]):
    value = greeks.get(key)
    with col:
        card(key.title(), "N/A" if value in [None, ""] else value, "Portfolio Greek")

with st.expander("Advanced / Product Details", expanded=False):
    product_rows = {
        "Product ID": product.get("product_id"),
        "Contract Type": product.get("contract_type"),
        "Contract Multiplier": product.get("contract_multiplier"),
        "Lot Size": product.get("lot_size"),
        "Minimum Quantity": product.get("min_quantity"),
        "Tick Size": product.get("tick_size"),
        "Price Precision": product.get("price_precision"),
        "Quantity Precision": product.get("quantity_precision"),
        "Last Sync": market.get("updated_at"),
    }
    st.dataframe(pd.DataFrame(product_rows.items(), columns=["Field", "Value"]), use_container_width=True, hide_index=True)

if active:
    cfg = active.get("config") or {}
    st.markdown(
        "<div class='df-hero'>"
        f"{badge(active_status)}"
        f"<div class='df-value'>{short_id(active.get('run_id'))}</div>"
        f"<div class='df-muted'>{cfg.get('grid_type', 'neutral')} | {cfg.get('spacing_type', 'arithmetic')} | "
        f"{cfg.get('lower_price')} - {cfg.get('upper_price')} | {cfg.get('grid_count')} levels | Runtime {runtime(active.get('started_at'))}</div>"
        "</div>",
        unsafe_allow_html=True,
    )
    orders = list((active.get("orders") or {}).values())
    open_orders = [row for row in orders if row.get("status") not in ["cancelled", "closed", "filled", "not_open", "manual_cancelled"]]
    fills = list((active.get("fills") or {}).values())
    summary = active.get("summary") or {}
    startup = active.get("startup") or {}
    metric_cols = st.columns(6)
    with metric_cols[0]:
        card("Net Inventory", (active.get("risk_snapshots") or [{}])[-1].get("position", account.get("current_position") or "0"), "lots")
    with metric_cols[1]:
        card("Open Orders", len(open_orders), f"{len(orders)} total")
    with metric_cols[2]:
        card("Completed Cycles", "0", "V0.1")
    with metric_cols[3]:
        card("Gross Grid P&L", summary.get("gross_pnl", "0"), "USD")
    with metric_cols[4]:
        card("Exchange Fees", summary.get("delta_fees", "0"), "USD")
    with metric_cols[5]:
        card("Net Trading P&L", summary.get("NET_TRADING_PNL_BEFORE_INCOME_TAX", "0"), "Before tax")

    if active_status == "STARTING":
        st.info(
            f"Startup: {startup.get('start_stage') or active.get('start_stage') or 'STARTING'} | "
            f"orders {startup.get('orders_submitted', 0)}/{startup.get('orders_expected', len(active.get('levels') or []))}"
        )

    bcols = st.columns([1, 1, 1, 1, 5])
    if active_status == "RUNNING":
        if bcols[0].button("Pause"):
            safe_post("/api/grid/v01/live/pause", timeout=15)
            st.rerun()
        if bcols[1].button("Edit Grid"):
            st.session_state["show_edit_grid"] = True
        if bcols[2].button("Stop"):
            st.session_state["confirm_stop"] = True
    elif active_status == "PAUSED":
        if bcols[0].button("Resume"):
            safe_post("/api/grid/v01/live/resume", timeout=15)
            st.rerun()
        if bcols[1].button("Edit Grid"):
            st.session_state["show_edit_grid"] = True
        if bcols[2].button("Stop"):
            st.session_state["confirm_stop"] = True

    if st.session_state.get("confirm_stop"):
        with st.container(border=True):
            st.subheader("Stop this Grid Run?")
            st.write("This cancels GridBot-owned resting orders, reconciles final exchange state, and generates the immutable Grid Run Summary.")
            st.caption("Existing inventory will not be market-closed automatically.")
            scol1, scol2 = st.columns(2)
            if scol1.button("Cancel"):
                st.session_state["confirm_stop"] = False
                st.rerun()
            if scol2.button("Confirm Stop", type="primary"):
                safe_post("/api/grid/v01/live/stop", {"reason": "dashboard_operator"}, timeout=15)
                st.session_state["confirm_stop"] = False
                st.session_state.pop("gridbot_history_runs", None)
                st.session_state.pop("gridbot_history_summaries", None)
                st.rerun()

    if st.session_state.get("show_edit_grid"):
        with st.container(border=True):
            st.subheader("Edit Grid")
            ecol1, ecol2, ecol3 = st.columns(3)
            with ecol1:
                proposed_lower = st.text_input("Lower Range", value=str(cfg.get("lower_price") or ""))
                proposed_upper = st.text_input("Upper Range", value=str(cfg.get("upper_price") or ""))
            with ecol2:
                proposed_count = st.number_input("Grid Count", min_value=2, max_value=200, value=int(cfg.get("grid_count") or 2), step=1)
                proposed_spacing = st.selectbox(
                    "Spacing",
                    ["arithmetic", "geometric"],
                    index=0 if str(cfg.get("spacing_type") or "arithmetic") == "arithmetic" else 1,
                )
            with ecol3:
                proposed_lot = st.text_input("Lot Size", value=str(cfg.get("lot_size") or ""))
                proposed_max = st.text_input("Max Inventory", value=str(cfg.get("max_inventory_lots") or ""))

            edit_payload = {
                "lower_price": proposed_lower,
                "upper_price": proposed_upper,
                "grid_count": int(proposed_count),
                "spacing_type": proposed_spacing,
                "lot_size": proposed_lot,
                "max_inventory_lots": proposed_max,
                "reason": "dashboard_operator",
            }
            current = pd.DataFrame(
                [
                    {"Field": "Lower Range", "Current": cfg.get("lower_price"), "Proposed": proposed_lower},
                    {"Field": "Upper Range", "Current": cfg.get("upper_price"), "Proposed": proposed_upper},
                    {"Field": "Grid Count", "Current": cfg.get("grid_count"), "Proposed": proposed_count},
                    {"Field": "Spacing", "Current": cfg.get("spacing_type"), "Proposed": proposed_spacing},
                    {"Field": "Lot Size", "Current": cfg.get("lot_size"), "Proposed": proposed_lot},
                    {"Field": "Max Inventory", "Current": cfg.get("max_inventory_lots"), "Proposed": proposed_max},
                ]
            )
            st.dataframe(current, use_container_width=True, hide_index=True)
            preview = st.session_state.get("gridbot_edit_preview")
            if preview:
                vcols = st.columns(4)
                with vcols[0]:
                    card("New Version", preview.get("proposed_config_version", "N/A"), "pending")
                with vcols[1]:
                    card("Cancel", preview.get("order_plan", {}).get("cancel_count", 0), "orders")
                with vcols[2]:
                    card("Create", preview.get("order_plan", {}).get("create_count", 0), "orders")
                with vcols[3]:
                    card("Defer", preview.get("order_plan", {}).get("defer_count", 0), "orders")
                validation = preview.get("validation") or {}
                for warning in validation.get("warnings") or []:
                    st.warning(warning)
                for error in validation.get("errors") or []:
                    st.error(error)
            rcol1, rcol2, rcol3 = st.columns(3)
            if rcol1.button("Cancel Edit"):
                st.session_state["show_edit_grid"] = False
                st.session_state.pop("gridbot_edit_preview", None)
                st.rerun()
            if rcol2.button("Preview Edit"):
                st.session_state["gridbot_edit_preview"] = safe_post("/api/grid/v01/live/edit/preview", edit_payload, timeout=15)
                st.rerun()
            if rcol3.button("Apply Edit", type="primary"):
                safe_post("/api/grid/v01/live/edit", edit_payload, timeout=20)
                st.session_state["show_edit_grid"] = False
                st.session_state.pop("gridbot_edit_preview", None)
                st.rerun()

    tabs = st.tabs(["Orders", "Risk / Account", "Execution Health"])
    with tabs[0]:
        order_df = pd.DataFrame(orders)
        if order_df.empty:
            st.info("No GridBot orders.")
        else:
            cols = ["client_order_id", "exchange_order_id", "level_id", "side", "price", "requested_quantity", "status", "config_version"]
            st.dataframe(order_df[[col for col in cols if col in order_df.columns]], use_container_width=True, hide_index=True)
    with tabs[1]:
        rcols = st.columns(4)
        with rcols[0]:
            card("Max Potential Long", cfg.get("max_inventory_lots", "N/A"), "lots")
        with rcols[1]:
            card("Max Potential Short", cfg.get("max_inventory_lots", "N/A"), "lots")
        with rcols[2]:
            card("GRR", (st.session_state.get("gridbot_preview") or {}).get("risk", {}).get("grr", "N/A"), "v0.1a")
        with rcols[3]:
            card("Drawdown", "N/A", "Unavailable")
    with tabs[2]:
        last_risk = (active.get("risk_snapshots") or [{}])[-1]
        health_rows = {
            "Execution Mode": active.get("execution_event_mode") or "REST_FALLBACK",
            "Private WS": active.get("private_ws_status") or "BLOCKED_403",
            "Last REST Poll": "N/A",
            "Last Reconciliation": active.get("last_reconciled_at") or "N/A",
            "Average Fill Detection Latency": "N/A",
            "REST Errors": "0",
            "429 Count": "0",
            "Duplicate Fills Ignored": "0",
            "Position Mismatches": "0",
            "Open Orders At Last Reconcile": last_risk.get("open_gridbot_orders", "N/A"),
        }
        st.dataframe(pd.DataFrame(health_rows.items(), columns=["Metric", "Value"]), use_container_width=True, hide_index=True)

st.markdown("<div class='df-section-title'>Create Grid</div>", unsafe_allow_html=True)
with st.form("gridbot_create_grid_form", border=True):
    c1, c2 = st.columns([1, 1])
    with c1:
        bot_name = st.text_input("Bot Name", value="ETH Testnet Grid")
        product_symbol = st.text_input("Product", value="ETHUSD")
        grid_type = st.selectbox("Grid Type", ["neutral", "long_bias", "short_bias"], format_func=lambda item: item.replace("_", " ").title())
        spacing_type = st.selectbox("Spacing Type", ["arithmetic", "geometric"], format_func=str.title)
    with c2:
        reference = float(market.get("reference_price") or market.get("mark_price") or 2450)
        lower_price = st.number_input("Lower Range", min_value=1.0, value=max(1.0, reference - 40), step=5.0)
        upper_price = st.number_input("Upper Range", min_value=1.0, value=reference + 40, step=5.0)
        grid_count = st.number_input("Grid Count", min_value=2, max_value=100, value=4, step=1)
        lot_size = st.number_input("Lot Size", min_value=1.0, value=1.0, step=1.0)
        max_inventory = st.number_input("Maximum Inventory", min_value=1.0, value=2.0, step=1.0)

    preview_payload = {
        "bot_name": bot_name,
        "product_symbol": product_symbol,
        "grid_type": grid_type,
        "lower_price": str(Decimal(str(lower_price))),
        "upper_price": str(Decimal(str(upper_price))),
        "grid_count": int(grid_count),
        "spacing_type": spacing_type,
        "lot_size": str(Decimal(str(lot_size))),
        "max_inventory_lots": str(Decimal(str(max_inventory))),
    }
    preview_submitted = st.form_submit_button("Preview Grid", type="primary")
    if preview_submitted:
        st.session_state["gridbot_preview"] = safe_post("/api/grid/v01/live/preview", preview_payload, timeout=15)
        st.session_state["gridbot_preview_payload"] = preview_payload
        st.rerun()

preview_state = st.session_state.get("gridbot_preview")
if preview_state:
    preview = preview_state.get("preview") or {}
    risk = preview_state.get("risk") or {}
    levels = preview.get("levels") or []
    st.markdown("<div class='df-section-title'>Preview</div>", unsafe_allow_html=True)
    pcols = st.columns(4)
    with pcols[0]:
        card("Current ETH", fmt_money(preview.get("reference_price")), "Auto-fetched")
    with pcols[1]:
        card("Grid", f"{preview.get('grid_type')} / {preview_state.get('config', {}).get('spacing_type')}", f"{preview.get('lower_price')} - {preview.get('upper_price')}")
    with pcols[2]:
        card("Potential Inventory", f"{preview.get('max_potential_long_inventory')} / {preview.get('max_potential_short_inventory')}", "Long / Short")
    with pcols[3]:
        card("Risk State", risk.get("risk_state", "UNKNOWN"), f"GRR {risk.get('grr') or 'N/A'}")

    left, right = st.columns([1, 1])
    with left:
        st.dataframe(rows_by_side(levels), use_container_width=True, hide_index=True)
    with right:
        st.subheader("Account Health")
        summary_line("Equity", fmt_money(preview_state.get("account", {}).get("account_equity")))
        summary_line("Available Margin", fmt_money(preview_state.get("account", {}).get("available_margin")))
        summary_line("Current Margin", fmt_pct(preview_state.get("account", {}).get("margin_utilisation")))
        summary_line("Current ETH Position", f"{preview_state.get('account', {}).get('current_position') or '0'} lots")
        summary_line("Projected Exposure", fmt_money(risk.get("projected_grid_exposure")))
        if risk.get("warnings"):
            for warning in risk["warnings"]:
                st.warning(warning)
        start_disabled = bool(active) or not st.session_state.get("gridbot_preview_payload")
        if st.button("Start Grid", type="primary", disabled=start_disabled):
            result = safe_post("/api/grid/v01/live/start", st.session_state["gridbot_preview_payload"], timeout=15)
            st.session_state["gridbot_start_result"] = result
            st.rerun()

st.markdown("<div class='df-section-title'>Historical Runs</div>", unsafe_allow_html=True)
try:
    if "gridbot_history_runs" not in st.session_state:
        st.session_state["gridbot_history_runs"] = api_get("/api/grid/v01/history/grid_runs", {"limit": 25}).get("rows") or []
    if "gridbot_history_summaries" not in st.session_state:
        st.session_state["gridbot_history_summaries"] = api_get("/api/grid/v01/history/grid_run_summaries", {"limit": 25}).get("rows") or []
    if st.button("Refresh History"):
        st.session_state["gridbot_history_runs"] = api_get("/api/grid/v01/history/grid_runs", {"limit": 25}).get("rows") or []
        st.session_state["gridbot_history_summaries"] = api_get("/api/grid/v01/history/grid_run_summaries", {"limit": 25}).get("rows") or []
        st.rerun()
    runs = st.session_state["gridbot_history_runs"]
    summaries = st.session_state["gridbot_history_summaries"]
except Exception as exc:
    runs, summaries = [], []
    st.warning(f"Historical runs unavailable: {exc}")

if runs:
    runs_df = pd.DataFrame(runs)
    show_cols = ["run_id", "status", "started_at", "stopped_at", "active_config_version", "execution_event_mode", "stop_reason"]
    st.dataframe(runs_df[[col for col in show_cols if col in runs_df.columns]], use_container_width=True, hide_index=True)
else:
    st.info("No historical GridBot runs.")

if summaries:
    selected = st.selectbox("Grid Run Summary", [row.get("run_id") for row in summaries])
    summary_row = next((row for row in summaries if row.get("run_id") == selected), None)
    if summary_row:
        report = summary_row.get("summary") or {}
        with st.container(border=True):
            st.subheader(f"Grid Run {short_id(selected)}")
            scol = st.columns(4)
            with scol[0]:
                card("Gross Grid P&L", report.get("gross_pnl", "0"), "USD")
            with scol[1]:
                card("Exchange Fees", report.get("delta_fees", "0"), "USD")
            with scol[2]:
                card("Net Trading P&L", report.get("NET_TRADING_PNL_BEFORE_INCOME_TAX", "0"), "Before tax")
            with scol[3]:
                card("Final Position", report.get("final_position", "0"), "lots")
            summary_line("Started", report.get("started_at", "N/A"))
            summary_line("Stopped", report.get("stopped_at", "N/A"))
            summary_line("Orders", report.get("orders_total", "0"))
            summary_line("Fills", report.get("fills_total", "0"))
            summary_line("Execution Mode", report.get("execution_event_mode", "N/A"))
            summary_line("Private WS", report.get("private_ws_status", "N/A"))

with st.expander("Advanced / Developer Tools", expanded=False):
    dcols = st.columns(3)
    if dcols[0].button("Preview Tiny"):
        st.session_state["durable_preview_tiny"] = api_get("/api/grid/v01/live/preview-tiny")
    if dcols[1].button("Start Tiny"):
        st.session_state["durable_start_tiny"] = safe_post("/api/grid/v01/live/start-tiny", timeout=15)
        st.rerun()
    if dcols[2].button("Reconcile"):
        st.session_state["durable_reconcile"] = safe_post("/api/grid/v01/live/reconcile", timeout=15)
        st.rerun()
    if st.checkbox("Show diagnostics"):
        st.write("Live status")
        st.dataframe(pd.DataFrame(live_status.get("runs") or []), use_container_width=True, hide_index=True)
        if st.session_state.get("durable_preview_tiny"):
            st.write("Tiny preview")
            st.dataframe(pd.DataFrame(st.session_state["durable_preview_tiny"].get("preview", {}).get("levels") or []), use_container_width=True, hide_index=True)
