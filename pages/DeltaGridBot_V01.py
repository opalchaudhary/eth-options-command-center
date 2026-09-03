from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

import pandas as pd
import requests
import streamlit as st

from api_client import api_get, api_post, backend_url
from grid_bot.operator_dashboard import (
    active_orders,
    fmt_lots,
    fmt_money,
    fmt_pct,
    health_plain_text,
    human_operator_reason,
    human_grid_type,
    human_lifecycle,
    human_spacing,
    inventory_summary,
    lifecycle_progress_summary,
    live_config,
    orders_are_updating,
    pnl_values,
    preview_edit_summary,
    recent_activity,
    split_pending_orders,
)
from ui_styles import load_css


st.set_page_config(page_title="Delta Grid Bot", layout="wide")
load_css()


st.markdown(
    """
    <style>
    .operator-top {
        border: 1px solid rgba(148, 163, 184, 0.28);
        border-radius: 8px;
        padding: 0.9rem 1rem;
        background: rgba(255, 255, 255, 0.82);
        margin-bottom: 0.75rem;
    }
    .operator-title {
        font-size: 1.35rem;
        font-weight: 760;
        color: #0f172a;
        margin: 0;
        letter-spacing: 0;
    }
    .operator-status {
        font-size: 0.98rem;
        font-weight: 760;
        text-align: right;
        color: #166534;
    }
    .operator-status.warn { color: #92400e; }
    .operator-status.bad { color: #991b1b; }
    .operator-sub {
        color: #64748b;
        font-size: 0.92rem;
        margin-top: 0.2rem;
    }
    .mini-card {
        border: 1px solid rgba(148, 163, 184, 0.24);
        border-radius: 8px;
        padding: 0.75rem 0.85rem;
        background: rgba(255, 255, 255, 0.78);
        min-height: 86px;
    }
    .mini-card h3 {
        margin: 0 0 0.25rem 0;
        color: #64748b;
        font-size: 0.76rem;
        font-weight: 740;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    .mini-value {
        color: #0f172a;
        font-size: 1.25rem;
        font-weight: 780;
        line-height: 1.25;
    }
    .mini-note {
        color: #64748b;
        font-size: 0.82rem;
        margin-top: 0.14rem;
    }
    .section-label {
        margin: 1rem 0 0.45rem 0;
        font-size: 0.94rem;
        font-weight: 780;
        color: #0f172a;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    .plain-warning {
        border: 1px solid rgba(245, 158, 11, 0.38);
        border-radius: 8px;
        padding: 0.75rem 0.85rem;
        background: #fffbeb;
        color: #78350f;
        margin: 0.5rem 0;
    }
    .plain-critical {
        border: 1px solid rgba(239, 68, 68, 0.38);
        border-radius: 8px;
        padding: 0.75rem 0.85rem;
        background: #fef2f2;
        color: #7f1d1d;
        margin: 0.5rem 0;
    }
    .activity-line {
        border-bottom: 1px solid rgba(148, 163, 184, 0.18);
        padding: 0.34rem 0;
        color: #334155;
        font-size: 0.9rem;
    }
    div[data-testid="stMetricValue"] { font-size: 1.25rem; }
    @media (prefers-color-scheme: dark) {
        .operator-top, .mini-card { background: rgba(15, 23, 42, 0.48); }
        .operator-title, .mini-value, .section-label { color: #e5e7eb; }
        .operator-sub, .mini-card h3, .mini-note, .activity-line { color: #94a3b8; }
        .plain-warning { background: rgba(120, 53, 15, 0.22); color: #fde68a; }
        .plain-critical { background: rgba(127, 29, 29, 0.24); color: #fecaca; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def fragment(run_every: str | None = None) -> Callable:
    native = getattr(st, "fragment", None)
    if native:
        return native(run_every=run_every)

    def decorator(fn: Callable) -> Callable:
        return fn

    return decorator


def safe_get(path: str, params: dict | None = None, timeout: int = 15) -> dict:
    try:
        return api_get(path, params=params, timeout=timeout)
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def safe_post(path: str, payload: dict | None = None, timeout: int = 15) -> dict:
    try:
        return api_post(path, payload=payload, timeout=timeout)
    except requests.Timeout:
        st.error("The request is still taking longer than expected. The bot worker will continue independently.")
        return {"ok": False, "error": "timeout"}
    except requests.HTTPError as exc:
        detail = str(exc)
        try:
            detail = exc.response.json().get("detail") or detail
        except Exception:
            pass
        if isinstance(detail, dict):
            message = detail.get("message") or detail.get("error") or str(detail)
            st.error(message)
            return {"ok": False, "error": message, "neutral_range": detail}
        st.error(detail)
        return {"ok": False, "error": detail}
    except Exception as exc:
        st.error(str(exc))
        return {"ok": False, "error": str(exc)}


def neutral_range_payload(response: dict | None) -> dict:
    response = response or {}
    return response.get("neutral_range") or {}


def render_neutral_range_suggestion(response: dict | None, *, lower_key: str, upper_key: str, button_key: str) -> None:
    suggestion = neutral_range_payload(response)
    if not suggestion:
        return
    st.warning("Selected range is not balanced for a Neutral grid at the current ETH price.")
    c1, c2, c3 = st.columns(3)
    with c1:
        render_card("Current ETH", fmt_money(suggestion.get("current_reference_price")))
    with c2:
        render_card("Your Grid", f"{suggestion.get('entered_buy_count', 0)} BUY / {suggestion.get('entered_sell_count', 0)} SELL")
    with c3:
        render_card("Suggested Grid", f"{suggestion.get('suggested_buy_count', 0)} BUY / {suggestion.get('suggested_sell_count', 0)} SELL")
    lower = suggestion.get("suggested_lower")
    upper = suggestion.get("suggested_upper")
    if lower in [None, ""] or upper in [None, ""]:
        return
    st.info(f"Nearest valid Neutral range: {fmt_money(lower)} - {fmt_money(upper)}")
    if st.button("Use Suggested Range", key=button_key, use_container_width=True):
        st.session_state[f"{button_key}_pending"] = {"lower_key": lower_key, "upper_key": upper_key, "lower": float(lower), "upper": float(upper)}
        st.rerun()


def apply_pending_suggested_range(button_key: str) -> None:
    pending = st.session_state.pop(f"{button_key}_pending", None)
    if not pending:
        return
    st.session_state[pending["lower_key"]] = pending["lower"]
    st.session_state[pending["upper_key"]] = pending["upper"]


def fetch_live_state() -> dict:
    return safe_get("/api/grid/v01/live/state", timeout=15)


def render_card(title: str, value: Any, note: str = "") -> None:
    st.markdown(
        f"<div class='mini-card'><h3>{title}</h3><div class='mini-value'>{value}</div><div class='mini-note'>{note}</div></div>",
        unsafe_allow_html=True,
    )


def status_class(status: str) -> str:
    if status == "HEALTHY":
        return ""
    if status == "CRITICAL":
        return "bad"
    return "warn"


def lifecycle_label(live: dict) -> str:
    lifecycle = live.get("lifecycle_state")
    if lifecycle:
        return human_lifecycle(lifecycle)
    if live.get("run_id"):
        return "Active"
    return "No active grid"


def refresh_history() -> None:
    st.session_state["gridbot_history_runs"] = safe_get("/api/grid/v01/history/grid_runs", {"limit": 25}).get("rows") or []
    st.session_state["gridbot_history_summaries"] = safe_get("/api/grid/v01/history/grid_run_summaries", {"limit": 25}).get("rows") or []
    st.session_state["gridbot_history_loaded_at"] = datetime.now(timezone.utc).isoformat()


def clear_preview() -> None:
    st.session_state.pop("gridbot_preview", None)
    st.session_state.pop("gridbot_preview_payload", None)


def render_health(health: dict) -> None:
    status = health.get("overall_status") or "HEALTHY"
    message, details = health_plain_text(health)
    if status == "HEALTHY":
        st.success(f"[OK] {message}")
        return
    css = "plain-critical" if status == "CRITICAL" else "plain-warning"
    st.markdown(f"<div class='{css}'><strong>{message}</strong></div>", unsafe_allow_html=True)
    for detail in details:
        st.write(f"- {detail}")
    st.caption(
        "New orders restricted: "
        + ("No" if health.get("safe_for_risk_increase") else "Yes")
        + " | Can reduce risk: "
        + ("Yes" if health.get("safe_for_risk_reduce") else "No")
        + " | Operator needed: "
        + ("Yes" if health.get("operator_attention_required") else "No")
    )


def grid_recommendation_action_label(action: str | None) -> str:
    labels = {
        "KEEP_CURRENT": "Keep Current",
        "CONSIDER_EDIT": "Consider Edit",
        "REGRID": "Rebuild Grid",
        "NO_GRID": "No Grid",
    }
    return labels.get(str(action or ""), str(action or "-").replace("_", " ").title())


def render_grid_recommendation_payload(payload: dict) -> None:
    recommendation = payload.get("recommendation") or {}
    inputs = recommendation.get("inputs_summary") or {}
    if not payload.get("ok", True):
        st.warning(payload.get("error") or "Recommendation unavailable.")
        return

    stale = "Yes" if recommendation.get("stale") else "No"
    v2_status = []
    if recommendation.get("v2_abstained"):
        v2_status.append("Abstained")
    if recommendation.get("v2_ood"):
        v2_status.append("OOD")
    if recommendation.get("stale"):
        v2_status.append("Stale")
    if not v2_status:
        v2_status.append("Usable")

    top = st.columns(4)
    with top[0]:
        render_card("Action", grid_recommendation_action_label(recommendation.get("action")))
    with top[1]:
        render_card("Type", human_grid_type(recommendation.get("grid_type")))
    with top[2]:
        render_card("Confidence", f"{float(recommendation.get('confidence') or 0):.2f}", recommendation.get("confidence_type") or "Recommender")
    with top[3]:
        render_card("Horizon", recommendation.get("operating_horizon") or "-", f"Stale: {stale}")

    params = st.columns(4)
    with params[0]:
        render_card("Range", f"{fmt_money(recommendation.get('lower_price'))} - {fmt_money(recommendation.get('upper_price'))}")
    with params[1]:
        render_card("Levels", recommendation.get("grid_count") or "-")
    with params[2]:
        render_card("Spacing", human_spacing(recommendation.get("spacing_type")))
    with params[3]:
        render_card("Step", fmt_money(recommendation.get("grid_step")), ", ".join(v2_status))

    signals = st.columns(4)
    with signals[0]:
        render_card("Path Inside 70", fmt_pct(inputs.get("path_inside_70")))
    with signals[1]:
        render_card("Expansion", fmt_pct(inputs.get("range_expansion")))
    with signals[2]:
        render_card("Upside", fmt_pct(inputs.get("upside_probability")))
    with signals[3]:
        render_card("Downside", fmt_pct(inputs.get("downside_probability")))

    reasons = recommendation.get("reasons") or []
    if reasons:
        st.caption(" | ".join(str(reason) for reason in reasons[:3]))
    st.caption(
        "Prediction "
        + str(payload.get("prediction_timestamp") or "-")
        + " | Spot "
        + fmt_money(payload.get("spot_price"))
    )


def render_grid_recommendation() -> None:
    st.markdown("<div class='section-label'>Grid Recommendation</div>", unsafe_allow_html=True)
    controls = st.columns([1, 3])
    with controls[0]:
        if st.button("Get Recommendation", use_container_width=True, key="gridbot_get_recommendation"):
            st.session_state["gridbot_recommendation"] = safe_post("/api/grid/v01/recommendation", timeout=15)
            st.session_state["gridbot_recommendation_loaded_at"] = datetime.now(timezone.utc).isoformat()
    loaded_at = st.session_state.get("gridbot_recommendation_loaded_at")
    with controls[1]:
        st.caption(f"Loaded {loaded_at}" if loaded_at else "Recommendation loads only when requested.")

    payload = st.session_state.get("gridbot_recommendation")
    if payload:
        render_grid_recommendation_payload(payload)


def render_orders(live: dict) -> None:
    buys, sells = split_pending_orders(live)
    updating = orders_are_updating(live)
    left, right = st.columns(2)
    with left:
        st.caption(f"{len(buys)} Buy Orders")
        placeholder = [{"Price": "Updating...", "Lots": "-", "Status": "Waiting for Delta"}] if updating else [{"Price": "-", "Lots": "-", "Status": "-"}]
        st.dataframe(pd.DataFrame(buys or placeholder), use_container_width=True, hide_index=True)
    with right:
        st.caption(f"{len(sells)} Sell Orders")
        placeholder = [{"Price": "Updating...", "Lots": "-", "Status": "Waiting for Delta"}] if updating else [{"Price": "-", "Lots": "-", "Status": "-"}]
        st.dataframe(pd.DataFrame(sells or placeholder), use_container_width=True, hide_index=True)


def render_lifecycle_progress(live: dict) -> None:
    lines = lifecycle_progress_summary(live)
    if not lines:
        return
    st.markdown("<div class='section-label'>Lifecycle Progress</div>", unsafe_allow_html=True)
    for line in lines:
        st.write(line)


def render_live_dashboard(live: dict) -> None:
    health = live.get("health") or {}
    status = health.get("overall_status") or "HEALTHY"
    telemetry = live.get("account_risk_state") or {}
    cfg = live_config(live)
    lifecycle = live.get("lifecycle_state")
    grid_name = human_grid_type(cfg.get("grid_type") or live.get("grid_nature"))
    spacing = human_spacing(cfg.get("spacing_type"))
    inv = inventory_summary(live)
    pnl = pnl_values(live)
    order_rows = active_orders(live)

    head_left, head_right = st.columns([3, 1])
    with head_left:
        st.markdown(
            "<div class='operator-top'>"
            "<div class='operator-title'>DELTA GRID BOT</div>"
            f"<div class='operator-sub'>ETH {fmt_money(telemetry.get('mark_price'))} | {lifecycle_label(live)} | {grid_name}</div>"
            "</div>",
            unsafe_allow_html=True,
        )
    with head_right:
        st.markdown(
            f"<div class='operator-top'><div class='operator-status {status_class(status)}'>[{status.replace('_', ' ')}]</div>"
            f"<div class='operator-sub'>{health_plain_text(health)[0]}</div></div>",
            unsafe_allow_html=True,
        )

    summary_cols = st.columns(3)
    with summary_cols[0]:
        render_card("GridBot Inventory", inv["label"], f"Maximum {inv['max']} lots | {inv['remaining']} lots capacity remaining")
    with summary_cols[1]:
        render_card("Net P&L", fmt_money(pnl["net"]), f"Realized {fmt_money(pnl['realized'])} | Unrealized {fmt_money(pnl['unrealized'])}")
    with summary_cols[2]:
        render_card("Account", fmt_money(telemetry.get("account_equity")), f"Available {fmt_money(telemetry.get('available_margin'))} | Margin {fmt_pct(telemetry.get('margin_utilisation_pct'))}")
    position_cols = st.columns(2)
    with position_cols[0]:
        render_card("Delta Position", inv["delta_label"], f"Difference {inv['difference']} lots")
    with position_cols[1]:
        render_card("Trading Fees", fmt_money(pnl["fees"]), f"Completed cycles {pnl['cycles']}")
    if not inv["matches"]:
        st.warning("Delta position does not match the bot's records.")
    if pnl.get("incomplete"):
        st.warning("Profit information is incomplete.")

    st.markdown("<div class='section-label'>Current Grid</div>", unsafe_allow_html=True)
    grid_cols = st.columns(6)
    with grid_cols[0]:
        render_card("Type", grid_name)
    with grid_cols[1]:
        render_card("Spacing", spacing)
    with grid_cols[2]:
        render_card("Range", f"{fmt_money(cfg.get('lower_price'))} - {fmt_money(cfg.get('upper_price'))}")
    with grid_cols[3]:
        render_card("Levels", cfg.get("grid_count") or "-")
    with grid_cols[4]:
        render_card("Order Size", fmt_lots(cfg.get("lot_size")))
    with grid_cols[5]:
        render_card("Maximum", fmt_lots(cfg.get("max_inventory_lots")))

    render_lifecycle_progress(live)

    render_grid_recommendation()

    st.markdown("<div class='section-label'>Health</div>", unsafe_allow_html=True)
    render_health(health)

    st.markdown("<div class='section-label'>Orders Waiting To Be Filled</div>", unsafe_allow_html=True)
    render_orders(live)

    st.markdown("<div class='section-label'>Trading Activity</div>", unsafe_allow_html=True)
    activity_cols = st.columns(4)
    with activity_cols[0]:
        render_card("Orders Filled", live.get("known_fill_count") or 0)
    with activity_cols[1]:
        render_card("Completed Cycles", pnl["cycles"])
    with activity_cols[2]:
        render_card("Open Orders", len(order_rows))
    with activity_cols[3]:
        render_card("Recent Activity Time", "Asia/Kolkata")
    for line in recent_activity(live):
        st.markdown(f"<div class='activity-line'>{line}</div>", unsafe_allow_html=True)

    st.markdown("<div class='section-label'>Actions</div>", unsafe_allow_html=True)
    render_actions(live)


def render_actions(live: dict) -> None:
    lifecycle = live.get("lifecycle_state")
    c1, c2, c3 = st.columns([1, 1, 1])
    if lifecycle == "RUNNING":
        if c1.button("Pause", use_container_width=True):
            safe_post("/api/grid/v01/live/pause", timeout=60)
            st.rerun()
    elif lifecycle == "PAUSED":
        if c1.button("Resume", use_container_width=True):
            safe_post("/api/grid/v01/live/resume", timeout=60)
            st.rerun()
    else:
        c1.button("Pause / Resume", disabled=True, use_container_width=True)

    if c2.button("Edit Grid", disabled=lifecycle not in {"RUNNING", "PAUSED"}, use_container_width=True):
        st.session_state["gridbot_edit_open"] = True
        st.rerun()
    can_stop = bool(live.get("run_id")) or lifecycle in {"RUNNING", "PAUSED", "EDITING", "RESUMING", "PAUSING", "STARTING"}
    if c3.button("Stop & Close", disabled=not can_stop, type="primary", use_container_width=True):
        st.session_state["gridbot_confirm_stop"] = True
        st.rerun()


def render_pending_operator_forms(live: dict) -> None:
    if st.session_state.get("gridbot_confirm_stop"):
        with st.container(border=True):
            st.subheader("Stop & Close")
            st.write("This will cancel the bot's resting orders, verify the final position, and close the run.")
            confirm = st.checkbox("I understand this will stop the active grid.", key="gridbot_stop_confirm_checkbox")
            s1, s2 = st.columns(2)
            if s1.button("Cancel", use_container_width=True):
                st.session_state["gridbot_confirm_stop"] = False
                st.rerun()
            if s2.button("Confirm Stop & Close", disabled=not confirm, type="primary", use_container_width=True):
                safe_post("/api/grid/v01/live/stop", {"reason": "dashboard_operator_stop_close"}, timeout=90)
                st.session_state["gridbot_confirm_stop"] = False
                st.session_state["gridbot_history_needs_refresh"] = True
                st.rerun()

    if st.session_state.get("gridbot_edit_open"):
        render_edit_grid(live)


def render_edit_grid(live: dict) -> None:
    apply_pending_suggested_range("edit_use_suggested_range")
    cfg = live_config(live)
    grid_types = ["neutral", "long_bias", "short_bias"]
    current_type = str(cfg.get("grid_type") or "neutral")
    with st.container(border=True):
        st.subheader("Edit Grid")
        e1, e2, e3 = st.columns(3)
        with e1:
            grid_type = st.radio(
                "Grid Type",
                grid_types,
                index=grid_types.index(current_type) if current_type in grid_types else 0,
                format_func=lambda value: {"neutral": "Neutral", "long_bias": "Long", "short_bias": "Short"}[value],
                horizontal=True,
            )
            lower = st.number_input("Lower Range", min_value=1.0, value=float(cfg.get("lower_price") or 2400), step=5.0, key="edit_lower")
            upper = st.number_input("Upper Range", min_value=1.0, value=float(cfg.get("upper_price") or 2600), step=5.0, key="edit_upper")
        with e2:
            grid_count = st.number_input("Grid Levels", min_value=2, max_value=200, value=int(cfg.get("grid_count") or 4), step=1, key="edit_count")
            spacing = st.radio("Spacing", ["arithmetic", "geometric"], index=0 if str(cfg.get("spacing_type") or "arithmetic") == "arithmetic" else 1, format_func=str.title, horizontal=True)
        with e3:
            lot_size = st.number_input("Order Size", min_value=1.0, value=float(cfg.get("lot_size") or 1), step=1.0, key="edit_lot")
            max_inventory = st.number_input("Maximum Inventory", min_value=1.0, value=float(cfg.get("max_inventory_lots") or 2), step=1.0, key="edit_max")

        payload = {
            "reason": "dashboard_operator_edit",
            "grid_type": grid_type,
            "lower_price": str(Decimal(str(lower))),
            "upper_price": str(Decimal(str(upper))),
            "grid_count": int(grid_count),
            "spacing_type": spacing,
            "lot_size": str(Decimal(str(lot_size))),
            "max_inventory_lots": str(Decimal(str(max_inventory))),
        }
        preview = st.session_state.get("gridbot_edit_preview")
        if preview:
            render_neutral_range_suggestion(preview, lower_key="edit_lower", upper_key="edit_upper", button_key="edit_use_suggested_range")
            if not neutral_range_payload(preview):
                for line in preview_edit_summary(preview):
                    st.write(line)
            validation = preview.get("validation") or {}
            for warning in validation.get("warnings") or []:
                st.warning(human_operator_reason(warning))
            for error in validation.get("errors") or []:
                st.error(human_operator_reason(error))

        b1, b2, b3 = st.columns([1, 1, 1])
        if b1.button("Cancel", use_container_width=True):
            st.session_state["gridbot_edit_open"] = False
            st.session_state.pop("gridbot_edit_preview", None)
            st.rerun()
        if b2.button("Preview Changes", use_container_width=True):
            st.session_state["gridbot_edit_preview"] = safe_post("/api/grid/v01/live/edit/preview", payload, timeout=30)
        if b3.button("Apply Changes", type="primary", disabled=not preview, use_container_width=True):
            safe_post("/api/grid/v01/live/edit", payload, timeout=90)
            st.session_state["gridbot_edit_open"] = False
            st.session_state.pop("gridbot_edit_preview", None)
            st.rerun()


@fragment(run_every="5s")
def live_fragment() -> None:
    live = fetch_live_state()
    st.session_state["gridbot_last_live_state"] = live
    if not live.get("ok", True):
        st.error(live.get("error") or "GridBot API unavailable.")
        return
    if live.get("run_id") or live.get("lifecycle_state"):
        render_live_dashboard(live)
    else:
        render_idle(live)


def render_idle(live: dict) -> None:
    health = live.get("health") or {}
    telemetry = live.get("account_risk_state") or {}
    top = st.columns([3, 1])
    with top[0]:
        st.markdown(
            "<div class='operator-top'><div class='operator-title'>DELTA GRID BOT</div>"
            f"<div class='operator-sub'>ETH {fmt_money(telemetry.get('mark_price'))} | No active grid</div></div>",
            unsafe_allow_html=True,
        )
    with top[1]:
        status = health.get("overall_status") or "HEALTHY"
        st.markdown(
            f"<div class='operator-top'><div class='operator-status {status_class(status)}'>[{status.replace('_', ' ')}]</div>"
            f"<div class='operator-sub'>{health_plain_text(health)[0]}</div></div>",
            unsafe_allow_html=True,
        )
    c1, c2, c3 = st.columns(3)
    with c1:
        render_card("Position", "Flat")
    with c2:
        render_card("Account", fmt_money(telemetry.get("account_equity")), f"Available {fmt_money(telemetry.get('available_margin'))}")
    with c3:
        render_card("Margin", fmt_pct(telemetry.get("margin_utilisation_pct")))
    render_health(health)


def render_create_grid(live: dict) -> None:
    apply_pending_suggested_range("create_use_suggested_range")
    telemetry = live.get("account_risk_state") or {}
    reference = float(telemetry.get("mark_price") or 2450)
    st.markdown("<div class='section-label'>Create Grid</div>", unsafe_allow_html=True)
    with st.form("gridbot_create_grid_form", border=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            bot_name = st.text_input("Bot Name", value="ETH Testnet Grid", key="create_bot_name")
            product_symbol = st.text_input("Product", value="ETHUSD", key="create_product_symbol")
            grid_type = st.radio(
                "Grid Type",
                ["neutral", "long_bias", "short_bias"],
                format_func=lambda value: {"neutral": "Neutral", "long_bias": "Long", "short_bias": "Short"}[value],
                horizontal=True,
                key="create_grid_type",
            )
        with c2:
            lower = st.number_input("Lower Range", min_value=1.0, value=max(1.0, reference - 40), step=5.0, key="create_lower")
            upper = st.number_input("Upper Range", min_value=1.0, value=reference + 40, step=5.0, key="create_upper")
            grid_count = st.number_input("Grid Levels", min_value=2, max_value=100, value=4, step=1, key="create_grid_count")
        with c3:
            spacing = st.radio("Spacing", ["arithmetic", "geometric"], format_func=str.title, horizontal=True, key="create_spacing")
            lot_size = st.number_input("Order Size", min_value=1.0, value=1.0, step=1.0, key="create_lot_size")
            max_inventory = st.number_input("Maximum Inventory", min_value=1.0, value=2.0, step=1.0, key="create_max_inventory")

        payload = {
            "bot_name": bot_name,
            "product_symbol": product_symbol,
            "grid_type": grid_type,
            "lower_price": str(Decimal(str(lower))),
            "upper_price": str(Decimal(str(upper))),
            "grid_count": int(grid_count),
            "spacing_type": spacing,
            "lot_size": str(Decimal(str(lot_size))),
            "max_inventory_lots": str(Decimal(str(max_inventory))),
        }
        p1, p2 = st.columns(2)
        preview_clicked = p1.form_submit_button("Preview Grid", use_container_width=True)
        start_clicked = p2.form_submit_button("Start Grid", type="primary", use_container_width=True)

    if preview_clicked:
        st.session_state["gridbot_preview"] = safe_post("/api/grid/v01/live/preview", payload, timeout=30)
        st.session_state["gridbot_preview_payload"] = payload
    if start_clicked:
        if st.session_state.get("gridbot_preview_payload") != payload:
            st.warning("Preview the current grid settings before starting.")
        else:
            safe_post("/api/grid/v01/live/start", payload, timeout=90)
            clear_preview()
            st.rerun()

    preview = st.session_state.get("gridbot_preview")
    if preview:
        render_create_preview(preview)


def render_create_preview(preview: dict) -> None:
    if not preview.get("ok", True):
        render_neutral_range_suggestion(preview, lower_key="create_lower", upper_key="create_upper", button_key="create_use_suggested_range")
        return
    data = preview.get("preview") or {}
    risk = preview.get("risk") or {}
    levels = data.get("levels") or []
    st.markdown("<div class='section-label'>Preview</div>", unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_card("ETH Price", fmt_money(data.get("reference_price")))
    with c2:
        render_card("Range", f"{fmt_money(data.get('lower_price'))} - {fmt_money(data.get('upper_price'))}")
    with c3:
        render_card("Levels", len(levels))
    with c4:
        render_card("Projected Exposure", fmt_money(risk.get("projected_grid_exposure")))
    d1, d2, d3, d4 = st.columns(4)
    with d1:
        render_card("Spacing", str(data.get("spacing_type") or "").title() or "-")
    with d2:
        render_card("Order Size", fmt_lots(data.get("lot_size")))
    with d3:
        render_card("Maximum Inventory", fmt_lots(data.get("max_inventory")))
    with d4:
        render_card("Waiting", len(data.get("deferred_levels") or []))
    e1, e2 = st.columns(2)
    with e1:
        render_card("Initial Buy Orders", data.get("opening_buy_orders_eligible", 0))
    with e2:
        render_card("Initial Sell Orders", data.get("opening_sell_orders_eligible", 0))
    buys = [{"Price": fmt_money(row.get("price")), "Lots": row.get("quantity"), "Status": "Will Open"} for row in levels if row.get("side") == "buy"]
    sells = [{"Price": fmt_money(row.get("price")), "Lots": row.get("quantity"), "Status": "Will Open"} for row in levels if row.get("side") == "sell"]
    left, right = st.columns(2)
    with left:
        st.caption(f"{len(buys)} Buy Orders")
        st.dataframe(pd.DataFrame(buys or [{"Price": "-", "Lots": "-", "Status": "-"}]), use_container_width=True, hide_index=True)
    with right:
        st.caption(f"{len(sells)} Sell Orders")
        st.dataframe(pd.DataFrame(sells or [{"Price": "-", "Lots": "-", "Status": "-"}]), use_container_width=True, hide_index=True)
    for warning in risk.get("warnings") or []:
        st.warning(warning)


def render_history() -> None:
    st.markdown("<div class='section-label'>Historical Runs</div>", unsafe_allow_html=True)
    h1, h2 = st.columns([1, 4])
    if h1.button("Refresh History", use_container_width=True) or st.session_state.pop("gridbot_history_needs_refresh", False):
        refresh_history()
    loaded_at = st.session_state.get("gridbot_history_loaded_at")
    h2.caption(f"Loaded {loaded_at}" if loaded_at else "History loads only when requested.")

    runs = st.session_state.get("gridbot_history_runs") or []
    summaries = st.session_state.get("gridbot_history_summaries") or []
    summary_by_run = {row.get("run_id"): row.get("summary") or {} for row in summaries}
    if not runs:
        st.info("No history loaded.")
        return
    def summary_net_pnl(report: dict) -> str | None:
        return report.get("net_run_pnl") if str(report.get("accounting_status") or "UNAVAILABLE") == "COMPLETE" else None

    rows = []
    for run in runs:
        report = summary_by_run.get(run.get("run_id"), {})
        rows.append(
            {
                "Date": (run.get("started_at") or run.get("created_at") or "")[:16].replace("T", " "),
                "Grid Type": human_grid_type(report.get("grid_type") or run.get("grid_type")),
                "Status": human_lifecycle(run.get("status")),
                "Cycles": report.get("cycles_total") or report.get("cycles_completed") or "-",
                "Net P&L": fmt_money(summary_net_pnl(report)),
                "Fees": fmt_money(report.get("delta_fees") or report.get("trading_fees")),
                "Accounting": report.get("accounting_status") or "-",
                "Result": run.get("stop_reason") or "-",
            }
        )
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    choices = [row.get("run_id") for row in summaries if row.get("run_id")]
    if choices:
        selected = st.selectbox("View completed run", choices, format_func=lambda value: f"Run ending {str(summary_by_run.get(value, {}).get('stopped_at') or value)[:16]}")
        report = summary_by_run.get(selected, {})
        cols = st.columns(4)
        with cols[0]:
            render_card("Net P&L", fmt_money(summary_net_pnl(report)))
        with cols[1]:
            render_card("Fees", fmt_money(report.get("delta_fees") or report.get("trading_fees")))
        with cols[2]:
            render_card("Cycles", report.get("cycles_total") or "-")
        with cols[3]:
            render_card("Final Position", fmt_lots(report.get("final_position") or 0))


with st.sidebar:
    st.caption(f"Backend: {backend_url()}")
    if st.button("Refresh Live Now", use_container_width=True):
        st.rerun()

live_fragment()
last_live = st.session_state.get("gridbot_last_live_state") or {}
if last_live.get("run_id") or last_live.get("lifecycle_state"):
    render_pending_operator_forms(last_live)
else:
    render_create_grid(last_live)
render_history()
