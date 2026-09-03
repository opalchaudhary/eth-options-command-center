from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - old Python fallback
    ZoneInfo = None


TERMINAL_ORDER_STATUSES = {
    "filled",
    "cancelled",
    "canceled",
    "closed",
    "manual_cancelled",
    "not_open",
    "rejected",
    "abandoned_by_stop",
    "cancelled_before_submission",
    "superseded",
}

HEALTH_MESSAGES = {
    "POSITION_MISMATCH": "Delta position does not match the bot's records.",
    "EXTERNAL_POSITION_CHANGE": "Delta position changed outside the GridBot. Trading has been paused until the position is reconciled.",
    "FORCED_LIQUIDATION": "Delta forcibly reduced or liquidated the position. Grid trading has been paused.",
    "POSITION_ATTRIBUTION_UNSAFE": "Position problem: account exposure cannot be safely matched to this bot.",
    "TELEMETRY_STALE": "Account information is delayed.",
    "CRITICAL_TELEMETRY_UNAVAILABLE": "Important account information is unavailable.",
    "WORKER_DEAD_RUNNING": "The bot worker is not running.",
    "WORKER_STALLED": "The bot has not completed a recent live check.",
    "RECONCILIATION_STALE": "Exchange verification is delayed.",
    "DELTA_AUTH_FAILURE": "Delta Exchange connection is having problems.",
    "DELTA_RATE_LIMIT": "Delta Exchange is rate-limiting requests.",
    "DELTA_REPEATED_429": "Delta rate limits are repeating.",
    "DELTA_TIMEOUT": "Delta is taking too long to answer.",
    "DELTA_5XX": "Delta is returning temporary server errors.",
    "GRID_ORDER_ORPHAN": "An exchange order appears to belong to the bot but is not in bot records.",
    "GRID_ORDER_MISSING_UNEXPECTEDLY": "A bot order expected on Delta is missing.",
    "GRID_ORDER_UNRESOLVED": "A bot order cannot be verified on Delta.",
    "GRID_DEPLOYMENT_INCOMPLETE": "The deployed grid is incomplete.",
    "LIFECYCLE_RECOVERY_REQUIRED": "The bot needs lifecycle recovery before normal trading can continue.",
    "DUPLICATE_ORDER": "Duplicate bot order identity detected.",
    "DUPLICATE_FILL_IGNORED": "A duplicate fill was safely ignored.",
    "MISSING_REPLACEMENT": "A filled order has not been replaced yet.",
    "MAX_INVENTORY_VIOLATION": "Inventory is above the configured maximum.",
    "GRID_NATURE_INVENTORY_VIOLATION": "Inventory direction does not match this grid type.",
    "PAUSED_WITH_RESTING_ORDERS": "The bot is paused but still has resting orders.",
    "STOPPED_WITH_EXPOSURE": "The bot is stopped but exposure or orders remain.",
    "STOP_REQUIRES_ATTENTION": "Stop and close needs operator attention.",
    "LIFECYCLE_STUCK": "The bot could not complete the requested action.",
    "ACCOUNTING_INCOMPLETE": "Profit information is incomplete.",
    "SUPABASE_FAILURE": "Trading records are temporarily unavailable.",
}

IST = ZoneInfo("Asia/Kolkata") if ZoneInfo else timezone(timedelta(hours=5, minutes=30))


def decimal_value(value: Any, default: str = "0") -> Decimal:
    try:
        if value in [None, ""]:
            return Decimal(default)
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def compact_decimal(value: Any) -> str:
    normalized = decimal_value(value).normalize()
    if normalized == normalized.to_integral():
        return format(normalized, "f")
    return format(normalized, "f").rstrip("0").rstrip(".")


def fmt_money(value: Any, empty: str = "-") -> str:
    if value in [None, "", "N/A"]:
        return empty
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def fmt_lots(value: Any, empty: str = "-") -> str:
    if value in [None, "", "N/A"]:
        return empty
    try:
        return f"{compact_decimal(value)} lots"
    except Exception:
        return f"{value} lots"


def fmt_pct(value: Any, empty: str = "-") -> str:
    if value in [None, "", "N/A"]:
        return empty
    try:
        number = Decimal(str(value))
        if abs(number) <= 1:
            number *= Decimal("100")
        return f"{float(number):.1f}%"
    except Exception:
        return str(value)


def human_grid_type(value: Any) -> str:
    mapping = {"neutral": "Neutral Grid", "long_bias": "Long Grid", "short_bias": "Short Grid"}
    return mapping.get(str(value or "").lower(), "Grid")


def human_spacing(value: Any) -> str:
    if value in [None, "", "N/A"]:
        return "-"
    text = str(value).replace("_", " ")
    return text[:1].upper() + text[1:]


def human_lifecycle(value: Any) -> str:
    mapping = {
        "RUNNING": "Running",
        "PAUSED": "Paused",
        "PAUSING": "Pausing",
        "RESUMING": "Resuming",
        "EDITING": "Editing",
        "STOPPING": "Stopping",
        "STOPPED": "Stopped",
        "STARTING": "Starting",
        "STOP_REQUIRES_ATTENTION": "Needs Attention",
    }
    return mapping.get(str(value or "STOPPED").upper(), str(value or "Stopped").title())


def position_label(lots: Any) -> str:
    value = decimal_value(lots)
    if value > 0:
        return f"Long {fmt_lots(value)}"
    if value < 0:
        return f"Short {fmt_lots(abs(value))}"
    return "Flat"


def health_plain_text(health: dict | None) -> tuple[str, list[str]]:
    health = health or {}
    issues = health.get("active_issues") or []
    if not issues:
        return "Everything is working normally.", []
    if len(issues) == 1 and issues[0].get("code") == "ACCOUNTING_INCOMPLETE":
        return "Profit information is incomplete.", []
    return "The bot needs attention.", [health_issue_text(item) for item in issues[:5]]


def health_issue_source(code: Any) -> str:
    code = str(code or "")
    if code.startswith("DELTA_") or code in {"GRID_ORDER_ORPHAN", "GRID_ORDER_MISSING_UNEXPECTEDLY", "GRID_ORDER_UNRESOLVED"}:
        return "Delta / Exchange"
    if code.startswith("SUPABASE") or code == "ACTIVE_LOCK_CONTRADICTION":
        return "Supabase / Database"
    if code.startswith("WORKER"):
        return "Worker"
    if "TELEMETRY" in code:
        return "Telemetry"
    if code.startswith("ACCOUNTING") or code == "DUPLICATE_EXCHANGE_COST_PROTECTION":
        return "Accounting"
    if code.startswith("POSITION") or code in {"EXTERNAL_POSITION_CHANGE", "FORCED_LIQUIDATION"} or "INVENTORY" in code or code == "FILL_LEDGER_MISMATCH":
        return "Position / Inventory"
    if code.startswith("LIFECYCLE") or code.startswith("STOP"):
        return "Lifecycle"
    return "Lifecycle"


def health_issue_text(issue: dict) -> str:
    code = issue.get("code")
    text = HEALTH_MESSAGES.get(code, issue.get("message") or "Something needs attention.")
    return f"{health_issue_source(code)}: {text}"


def active_orders(live: dict | None) -> list[dict]:
    rows = live_order_rows(live)
    return [row for row in rows if str(row.get("raw_status") or "").lower() not in TERMINAL_ORDER_STATUSES]


def live_order_rows(live: dict | None) -> list[dict]:
    live = live or {}
    rows = live.get("known_gridbot_orders") or list(((live.get("active_run") or {}).get("orders") or {}).values())
    normalized = []
    for row in rows:
        status = str(row.get("status") or "").lower()
        remaining = decimal_value(row.get("remaining_quantity"), str(row.get("requested_quantity") or "0"))
        if status in TERMINAL_ORDER_STATUSES or remaining <= 0:
            continue
        normalized.append(
            {
                "Price": fmt_money(row.get("price")),
                "Lots": compact_decimal(remaining),
                "Status": human_order_status(status),
                "side": str(row.get("side") or "").lower(),
                "price_value": float(decimal_value(row.get("price"))),
                "raw_status": status,
            }
        )
    return normalized


def split_pending_orders(live: dict | None) -> tuple[list[dict], list[dict]]:
    rows = live_order_rows(live)
    buys = sorted([display_order(row) for row in rows if row.get("side") == "buy"], key=lambda row: row["_price"], reverse=True)
    sells = sorted([display_order(row) for row in rows if row.get("side") == "sell"], key=lambda row: row["_price"])
    return strip_private_keys(buys), strip_private_keys(sells)


def display_order(row: dict) -> dict:
    return {"Price": row["Price"], "Lots": row["Lots"], "Status": row["Status"], "_price": row["price_value"]}


def strip_private_keys(rows: list[dict]) -> list[dict]:
    return [{key: value for key, value in row.items() if not key.startswith("_")} for row in rows]


def human_order_status(status: str) -> str:
    status = str(status or "").lower()
    if status in {"open", "live"}:
        return "Open"
    if status in {"partially_filled", "partial"}:
        return "Partially Filled"
    if status in {"deferred", "blocked", "not_submitted"}:
        return "Waiting"
    if status in {"submitted", "pending", "proposed"}:
        return "Pending"
    return status.replace("_", " ").title() or "-"


def pnl_values(live: dict | None) -> dict:
    accounting = (live or {}).get("accounting") or {}
    status = str(accounting.get("accounting_status") or "UNAVAILABLE")
    incomplete = status != "COMPLETE"
    net = accounting.get("live_net_pnl")
    if net in [None, "", "N/A"]:
        net = accounting.get("net_run_pnl")
    if net in [None, "", "N/A"]:
        net = accounting.get("net_realized_pnl")
    net_value = None if net in [None, "", "N/A"] else net
    unavailable = status == "UNAVAILABLE"
    return {
        "net": None if incomplete else net_value,
        "realized": None if unavailable else accounting.get("net_realized_pnl"),
        "unrealized": None if unavailable else accounting.get("unrealized_pnl"),
        "fees": None if unavailable else accounting.get("trading_fees"),
        "cycles": accounting.get("cycles_completed") or 0,
        "incomplete": incomplete,
    }


def inventory_summary(live: dict | None) -> dict:
    live = live or {}
    cfg = live_config(live)
    agreement = (live.get("health") or {}).get("position_inventory_agreement") or {}
    inventory = decimal_value(agreement.get("gridbot_inventory") or live.get("fill_derived_inventory"))
    delta_position = decimal_value(agreement.get("delta_position") or live.get("delta_position"))
    max_inventory = abs(decimal_value(cfg.get("max_inventory_lots")))
    remaining = max(Decimal("0"), max_inventory - abs(inventory)) if max_inventory else Decimal("0")
    return {
        "label": position_label(inventory),
        "delta_label": position_label(delta_position),
        "inventory": compact_decimal(inventory),
        "delta_position": compact_decimal(delta_position),
        "matches": inventory == delta_position,
        "difference": compact_decimal(delta_position - inventory),
        "max": compact_decimal(max_inventory) if max_inventory else "-",
        "remaining": compact_decimal(remaining) if max_inventory else "-",
    }


def live_config(live: dict | None) -> dict:
    live = live or {}
    if live.get("active_run") and isinstance(live["active_run"], dict):
        return live["active_run"].get("config") or {}
    if live.get("config") and isinstance(live["config"], dict):
        return live["config"]
    levels = live.get("grid_levels") or []
    prices = [decimal_value(level.get("price")) for level in levels if level.get("price") not in [None, ""]]
    fallback = {
        "grid_type": live.get("grid_nature"),
        "lower_price": compact_decimal(min(prices)) if prices else None,
        "upper_price": compact_decimal(max(prices)) if prices else None,
        "grid_count": len(levels) or None,
        "lot_size": compact_decimal(levels[0].get("quantity")) if levels else None,
        "levels": live.get("grid_levels"),
    }
    return fallback


def recent_activity(live: dict | None, limit: int = 8) -> list[str]:
    live = live or {}
    activity = []
    last_fill = live.get("last_fill") or {}
    fill = last_fill.get("raw") or {}
    if fill:
        side = str(fill.get("side") or "").upper()
        activity.append(f"{time_label(fill.get('created_at') or live.get('last_successful_poll_at'))}  {side} {fill.get('size') or fill.get('quantity') or '-'} filled @ {fmt_money(fill.get('price') or fill.get('fill_price'))}")
    last_replacement = live.get("last_replacement") or {}
    if last_replacement:
        activity.append(f"{time_label(live.get('last_successful_poll_at'))}  Replacement order {last_replacement.get('state', 'updated')}")
    for issue in (live.get("health") or {}).get("recent_resolved_issues") or []:
        activity.append(f"{time_label(issue.get('resolved_at'))}  Health issue resolved: {HEALTH_MESSAGES.get(issue.get('code'), issue.get('code'))}")
    if live.get("last_successful_reconcile"):
        activity.append(f"{time_label(live.get('last_successful_reconcile'))}  Exchange check completed")
    return activity[:limit] or ["No recent activity yet"]


def time_label(timestamp: Any) -> str:
    if not timestamp:
        return "--:--"
    try:
        parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(IST).strftime("%H:%M IST")
    except Exception:
        return "--:--"


def preview_edit_summary(preview: dict | None) -> list[str]:
    preview = preview or {}
    plan = preview.get("order_plan") or {}
    lines = ["Your current position will not be closed."]
    remain = plan.get("remain_count", plan.get("remain", 0))
    cancel = plan.get("cancel_count", plan.get("cancel", 0))
    create = plan.get("create_count", plan.get("create", 0))
    defer = plan.get("defer_count", plan.get("defer", 0))
    lines.append(f"{remain} existing orders will remain")
    lines.append(f"{cancel} orders will be cancelled")
    lines.append(f"{create} new orders will be placed")
    lines.append(f"{defer} orders will wait because of limits or market conditions")
    return lines
