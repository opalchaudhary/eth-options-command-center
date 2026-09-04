from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import time
from typing import Any, Callable

from .models import GridType, Side, utc_now
from .semantics import evaluate_order_semantics


GRIDBOT_ORDER_PREFIX = "DGB01-"
TERMINAL_STATES = {
    "FILLED",
    "CANCELLED",
    "MANUAL_CANCELLED",
    "REJECTED",
    "UNKNOWN",
    "DEFERRED",
    "BLOCKED",
    "ABANDONED_BY_STOP",
    "CANCELLED_BEFORE_SUBMISSION",
    "SUPERSEDED",
    "NOT_SUBMITTED",
    "NEVER_SUBMITTED",
}


@dataclass
class ExchangeTruthResult:
    orders_checked: int = 0
    exchange_open_orders: int = 0
    orders_resolved: int = 0
    new_fills: int = 0
    duplicate_fills_ignored: int = 0
    partial_fills: int = 0
    filled_orders: int = 0
    cancelled_orders: int = 0
    manual_cancelled_orders: int = 0
    unresolved_orders: int = 0
    fill_ledger_mismatches: int = 0
    position_mismatches: int = 0
    gridbot_inventory: Decimal = Decimal("0")
    delta_position: Decimal = Decimal("0")
    request_count: int = 0
    errors: list[str] = field(default_factory=list)
    events: list[dict] = field(default_factory=list)
    timings: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "orders_checked": self.orders_checked,
            "exchange_open_orders": self.exchange_open_orders,
            "orders_resolved": self.orders_resolved,
            "new_fills": self.new_fills,
            "duplicate_fills_ignored": self.duplicate_fills_ignored,
            "partial_fills": self.partial_fills,
            "filled_orders": self.filled_orders,
            "cancelled_orders": self.cancelled_orders,
            "manual_cancelled_orders": self.manual_cancelled_orders,
            "unresolved_orders": self.unresolved_orders,
            "fill_ledger_mismatches": self.fill_ledger_mismatches,
            "position_mismatches": self.position_mismatches,
            "gridbot_inventory": str(self.gridbot_inventory),
            "delta_position": str(self.delta_position),
            "request_count": self.request_count,
            "timings": self.timings,
            "errors": self.errors,
            "events": self.events,
            "last_successful_reconcile": utc_now() if not self.errors else None,
        }


def decimal_value(value: Any, default: str = "0") -> Decimal:
    try:
        if value in [None, ""]:
            return Decimal(default)
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def result_rows(payload: dict | None) -> list[dict]:
    result = (payload or {}).get("result") or []
    if isinstance(result, dict):
        for key in ["rows", "data", "orders", "fills"]:
            if isinstance(result.get(key), list):
                return result[key]
        return [result]
    return result if isinstance(result, list) else []


def next_cursor(payload: dict | None) -> str | None:
    payload = payload or {}
    candidates = [
        payload.get("after"),
        payload.get("next"),
        payload.get("next_cursor"),
        (payload.get("meta") or {}).get("after") if isinstance(payload.get("meta"), dict) else None,
        (payload.get("pagination") or {}).get("after") if isinstance(payload.get("pagination"), dict) else None,
        ((payload.get("result") or {}).get("after") if isinstance(payload.get("result"), dict) else None),
    ]
    return next((str(item) for item in candidates if item not in [None, ""]), None)


def _record_timing(result: ExchangeTruthResult, label: str, started: float, *, count: int | None = None) -> None:
    elapsed = round(time.perf_counter() - started, 4)
    calls = result.timings.setdefault("calls", [])
    item = {"label": label, "elapsed_seconds": elapsed}
    if count is not None:
        item["rows"] = count
    calls.append(item)
    totals = result.timings.setdefault("totals", {})
    bucket = totals.setdefault(label, {"calls": 0, "elapsed_seconds": 0.0, "rows": 0})
    bucket["calls"] += 1
    bucket["elapsed_seconds"] = round(float(bucket.get("elapsed_seconds") or 0.0) + elapsed, 4)
    if count is not None:
        bucket["rows"] = int(bucket.get("rows") or 0) + count


def fetch_paginated(fetch_page: Callable[..., dict], result: ExchangeTruthResult, label: str, page_size: int = 50, max_pages: int = 10) -> list[dict]:
    rows: list[dict] = []
    after = None
    seen: set[str] = set()
    for _ in range(max_pages):
        started = time.perf_counter()
        payload = fetch_page(after=after, page_size=page_size)
        result.request_count += 1
        page_rows = result_rows(payload)
        rows.extend(page_rows)
        _record_timing(result, label, started, count=len(page_rows))
        after = next_cursor(payload)
        if not after or after in seen:
            break
        seen.add(after)
    return rows


def exchange_order_id(row: dict) -> str:
    return str(row.get("exchange_order_id") or row.get("order_id") or row.get("id") or "")


def client_order_id(row: dict) -> str:
    return str(row.get("client_order_id") or "")


def fill_identity(fill: dict) -> str:
    stable = fill.get("id") or fill.get("fill_id") or fill.get("trade_id") or fill.get("execution_id")
    if stable not in [None, ""]:
        return str(stable)
    parts = [
        fill.get("order_id") or fill.get("exchange_order_id"),
        fill.get("client_order_id"),
        fill.get("created_at") or fill.get("timestamp"),
        fill.get("side"),
        fill.get("price") or fill.get("fill_price"),
        fill.get("size") or fill.get("quantity"),
    ]
    return ":".join(str(part) for part in parts)


def fill_quantity(fill: dict) -> Decimal:
    return decimal_value(fill.get("size") or fill.get("quantity") or fill.get("fill_size"))


def order_quantity(order: dict) -> Decimal:
    return decimal_value(order.get("requested_quantity") or order.get("size") or order.get("quantity"))


def exchange_remaining(order: dict, fallback: Decimal) -> Decimal:
    return decimal_value(order.get("unfilled_size") or order.get("remaining_quantity") or order.get("remaining_size"), str(fallback))


def normalize_order_state(row: dict | None, executed: Decimal, requested: Decimal, remaining: Decimal | None = None) -> str:
    raw = str((row or {}).get("state") or (row or {}).get("status") or "").lower()
    if executed >= requested and requested > 0:
        return "FILLED"
    if raw in {"open", "pending", "live"}:
        return "PARTIALLY_FILLED" if executed > 0 else "OPEN"
    if raw in {"filled"}:
        return "FILLED"
    if raw == "closed":
        if remaining == 0:
            return "FILLED"
        return "PARTIALLY_FILLED" if executed > 0 else "CANCELLED"
    if raw in {"cancelled", "canceled", "user_cancelled"}:
        return "MANUAL_CANCELLED" if executed == 0 else "CANCELLED"
    if raw in {"rejected", "failed"}:
        return "REJECTED"
    if remaining is not None and remaining > 0 and executed > 0:
        return "PARTIALLY_FILLED"
    return "UNRESOLVED"


def fill_matches_order(fill: dict, order: dict) -> bool:
    fill_client_id = client_order_id(fill)
    fill_exchange_id = str(fill.get("order_id") or fill.get("exchange_order_id") or "")
    return bool(
        (fill_client_id and fill_client_id == order.get("client_order_id"))
        or (fill_exchange_id and fill_exchange_id == str(order.get("exchange_order_id") or ""))
        or (fill_exchange_id and fill_exchange_id == str(order.get("order_key") or ""))
    )


def inventory_from_fills(fills: list[dict]) -> Decimal:
    inventory = Decimal("0")
    for fill in fills:
        side_value = str(fill.get("side") or "").lower()
        quantity = fill_quantity(fill)
        if side_value == Side.BUY.value:
            inventory += quantity
        elif side_value == Side.SELL.value:
            inventory -= quantity
    return inventory


def directional_open_order_violations(run: dict, current_inventory: Decimal) -> list[dict]:
    config = run.get("config") or {}
    try:
        grid_type = GridType(config.get("grid_type") or GridType.NEUTRAL.value)
    except ValueError:
        return []
    if grid_type not in {GridType.LONG_BIAS, GridType.SHORT_BIAS}:
        return []
    max_inventory = decimal_value(config.get("max_inventory_lots"))
    simulated_open: list[dict[str, Any]] = []
    violations: list[dict] = []
    for order in (run.get("orders") or {}).values():
        if str(order.get("status") or "").lower() not in {"open", "partially_filled", "pending", "submitted"}:
            continue
        try:
            side = Side(str(order.get("side")).lower())
        except ValueError:
            continue
        remaining = decimal_value(order.get("remaining_quantity") or order.get("requested_quantity"))
        if remaining <= 0:
            continue
        decision = evaluate_order_semantics(grid_type, current_inventory, max_inventory, side, remaining, simulated_open)
        nature_reasons = [
            reason
            for reason in decision.reason_codes
            if reason in {"LONG_BIAS_CANNOT_OPEN_NET_SHORT", "SHORT_BIAS_CANNOT_OPEN_NET_LONG"}
        ]
        if nature_reasons:
            violations.append(
                {
                    "grid_type": grid_type.value,
                    "inventory": str(current_inventory),
                    "client_order_id": order.get("client_order_id"),
                    "side": side.value,
                    "remaining_quantity": str(remaining),
                    "reason_codes": nature_reasons,
                }
            )
            continue
        simulated_open.append(
            {
                "side": side.value,
                "remaining_quantity": str(remaining),
                "requested_quantity": str(order.get("requested_quantity") or remaining),
                "status": "open",
                "opens_inventory": decision.opens_inventory,
            }
        )
    return violations


def position_size(positions_response: dict, product_id: int, product_symbol: str) -> Decimal:
    rows = result_rows(positions_response)
    for row in rows:
        if str(row.get("product_id")) == str(product_id) or row.get("product_symbol") == product_symbol or row.get("symbol") == product_symbol:
            return decimal_value(row.get("size"))
    return Decimal("0")


def _event(result: ExchangeTruthResult, event_type: str, payload: dict) -> None:
    result.events.append({"event_type": event_type, "payload": payload})


def reconcile_exchange_truth(
    run: dict,
    client: Any,
    db: Any | None = None,
    *,
    suppress_replacements: bool = True,
    persist_order_updates: bool = True,
) -> dict:
    del suppress_replacements
    result = ExchangeTruthResult()
    product = run.get("product") or {}
    config = run.get("config") or {}
    product_id = int(product.get("product_id") or 0)
    product_symbol = product.get("symbol") or config.get("product_symbol") or "ETHUSD"

    try:
        started = time.perf_counter()
        open_rows = result_rows(client.open_orders(product_id))
        result.request_count += 1
        _record_timing(result, "open_orders", started, count=len(open_rows))
        started = time.perf_counter()
        positions = client.positions("ETH")
        result.request_count += 1
        _record_timing(result, "positions", started, count=len(result_rows(positions)))
        order_history = fetch_paginated(lambda after=None, page_size=50: client.order_history(product_id, after=after, page_size=page_size), result, "order_history")
        fill_history = fetch_paginated(lambda after=None, page_size=50: client.fills(product_id, after=after, page_size=page_size), result, "fills")
    except Exception as exc:
        result.errors.append(str(exc))
        return result.as_dict()

    open_gridbot = [row for row in open_rows if client_order_id(row).startswith(GRIDBOT_ORDER_PREFIX)]
    result.exchange_open_orders = len(open_gridbot)
    open_by_client = {client_order_id(row): row for row in open_rows if client_order_id(row)}
    history_by_client = {client_order_id(row): row for row in order_history if client_order_id(row)}
    history_by_exchange = {exchange_order_id(row): row for row in order_history if exchange_order_id(row)}
    direct_by_exchange: dict[str, dict] = {}

    orders = run.setdefault("orders", {})
    persisted_fills = run.setdefault("fills", {})
    known_fill_ids = set(persisted_fills)
    for fill in persisted_fills.values():
        known_fill_ids.add(fill_identity(fill.get("raw") or fill))

    matched_fills = [fill for fill in fill_history if any(fill_matches_order(fill, order) for order in orders.values())]
    cycles_need_persist = False
    for fill in matched_fills:
        fid = fill_identity(fill)
        if fid in known_fill_ids:
            result.duplicate_fills_ignored += 1
            continue
        persisted_fills[fid] = fill
        known_fill_ids.add(fid)
        result.new_fills += 1
        if db and getattr(db, "enabled", False):
            inserted = db.persist_fill(run, fid, fill, persist_cycles=False)
            cycles_need_persist = cycles_need_persist or bool(inserted)
    if cycles_need_persist and db and getattr(db, "enabled", False):
        db.persist_cycles(run)

    all_fills = list(persisted_fills.values())
    result.gridbot_inventory = inventory_from_fills([fill.get("raw") or fill for fill in all_fills])
    result.delta_position = position_size(positions, product_id, product_symbol)

    result.orders_checked = len(orders)
    for cid, order in orders.items():
        local_status = str(order.get("status") or "").upper()
        if not order.get("exchange_order_id") and local_status in TERMINAL_STATES:
            continue
        if local_status in TERMINAL_STATES and local_status not in {"UNRESOLVED", "ABANDONED_BY_STOP"} and order.get("status") != "open":
            continue
        exchange = open_by_client.get(cid)
        local_exchange_id = str(order.get("exchange_order_id") or "")
        history = history_by_client.get(cid) or history_by_exchange.get(local_exchange_id)
        direct_lookup_used = False
        if not exchange and not history and local_exchange_id and hasattr(client, "get_order"):
            try:
                direct_order = direct_by_exchange.get(local_exchange_id)
                if direct_order is None:
                    started = time.perf_counter()
                    direct_rows = result_rows(client.get_order(local_exchange_id))
                    result.request_count += 1
                    _record_timing(result, "direct_order_lookup", started, count=len(direct_rows))
                    direct_order = direct_rows[0] if direct_rows else {}
                    direct_by_exchange[local_exchange_id] = direct_order
                if direct_order:
                    history = direct_order
                    direct_lookup_used = True
            except Exception as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                if status_code != 404:
                    result.errors.append(str(exc))
                    return result.as_dict()
        order_fills = [fill.get("raw") or fill for fill in all_fills if fill_matches_order(fill.get("raw") or fill, order)]
        executed = sum((fill_quantity(fill) for fill in order_fills), Decimal("0"))
        requested = order_quantity(order)
        evidence = exchange or history
        remaining = exchange_remaining(evidence, max(Decimal("0"), requested - executed)) if evidence else max(Decimal("0"), requested - executed)

        state = normalize_order_state(evidence, executed, requested, remaining)
        if not exchange and not history and executed == 0:
            if not order.get("exchange_order_id"):
                state = "AMBIGUOUS_SUBMISSION" if local_status in {"SUBMITTED", "PENDING", "PROPOSED", "OPEN", "AMBIGUOUS_SUBMISSION"} else "NEVER_SUBMITTED"
                if state == "AMBIGUOUS_SUBMISSION":
                    _event(result, "ORDER_UNRESOLVED", {"client_order_id": cid, "exchange_order_id": order.get("exchange_order_id"), "reason": "ambiguous_submission"})
            else:
                state = "UNRESOLVED"
                _event(result, "ORDER_UNRESOLVED", {"client_order_id": cid, "exchange_order_id": order.get("exchange_order_id")})
        elif state in {"FILLED", "REJECTED"} or (state in {"CANCELLED", "MANUAL_CANCELLED"} and executed == 0):
            remaining = Decimal("0")

        if executed > requested:
            result.fill_ledger_mismatches += 1
            _event(result, "FILL_LEDGER_MISMATCH", {"client_order_id": cid, "executed": str(executed), "requested": str(requested)})

        previous_status = str(order.get("status") or "").lower()
        previous_filled = str(order.get("filled_quantity") or "0")
        previous_remaining = str(order.get("remaining_quantity") or "")
        order["filled_quantity"] = str(executed)
        order["remaining_quantity"] = str(remaining)
        order["status"] = state.lower()
        order["last_exchange_state"] = (evidence or {}).get("state") or (evidence or {}).get("status")
        order["last_reconciled_at"] = utc_now()
        if evidence:
            order["raw"] = evidence
            adopted_exchange_id = exchange_order_id(evidence)
            if adopted_exchange_id:
                order["exchange_order_id"] = adopted_exchange_id
        changed = (
            previous_status != order["status"]
            or previous_filled != order["filled_quantity"]
            or previous_remaining != order["remaining_quantity"]
        )
        if db and getattr(db, "enabled", False) and (persist_order_updates or changed):
            db.persist_order(run, order)

        result.orders_resolved += 1
        if state == "PARTIALLY_FILLED":
            result.partial_fills += 1
        elif state == "FILLED":
            result.filled_orders += 1
        elif state == "CANCELLED":
            result.cancelled_orders += 1
        elif state == "MANUAL_CANCELLED":
            result.manual_cancelled_orders += 1
        elif state == "UNRESOLVED":
            result.unresolved_orders += 1
        elif state == "AMBIGUOUS_SUBMISSION":
            result.unresolved_orders += 1
        if direct_lookup_used and state in {"OPEN", "PARTIALLY_FILLED"}:
            result.exchange_open_orders += 1

    if result.gridbot_inventory != result.delta_position:
        result.position_mismatches += 1
        _event(
            result,
            "POSITION_MISMATCH",
            {
                "run_id": run.get("run_id"),
                "gridbot_inventory": str(result.gridbot_inventory),
                "delta_position": str(result.delta_position),
                "difference": str(result.delta_position - result.gridbot_inventory),
                "orders_checked": result.orders_checked,
                "fills": len(all_fills),
            },
        )

    for violation in directional_open_order_violations(run, result.gridbot_inventory):
        _event(result, "GRID_NATURE_INVENTORY_VIOLATION", violation)

    run["last_reconciled_at"] = utc_now()
    return result.as_dict()
