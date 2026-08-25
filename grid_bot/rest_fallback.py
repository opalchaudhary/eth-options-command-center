import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import requests

from .delta_testnet_client import DeltaTestnetClient
from .models import ExecutionEventMode, OperationalState, Side, utc_now
from .repository import InMemoryGridRepository, repository


DEFAULT_FILL_LOOKBACK_SECONDS = 120
DEFAULT_FAST_POLL_INTERVAL_SECONDS = 2.0


@dataclass
class RestFallbackMetrics:
    rest_poll_count: int = 0
    rest_errors: int = 0
    rest_retries: int = 0
    rate_limit_429_count: int = 0
    fills_detected_through_rest: int = 0
    duplicate_fills_ignored: int = 0
    order_reconciliations: int = 0
    position_reconciliations: int = 0
    position_mismatches: int = 0
    reconciliation_mismatches: int = 0
    fill_detection_latencies: list[float] = field(default_factory=list)
    poll_intervals: list[float] = field(default_factory=list)
    last_poll_started_at: float | None = None
    backoff_seconds: float = 0.0

    def as_dict(self) -> dict:
        average_latency = None
        max_latency = None
        if self.fill_detection_latencies:
            average_latency = sum(self.fill_detection_latencies) / len(self.fill_detection_latencies)
            max_latency = max(self.fill_detection_latencies)
        average_poll_interval = None
        if self.poll_intervals:
            average_poll_interval = sum(self.poll_intervals) / len(self.poll_intervals)
        return {
            "rest_poll_count": self.rest_poll_count,
            "average_poll_interval": average_poll_interval,
            "average_fill_detection_latency": average_latency,
            "max_fill_detection_latency": max_latency,
            "REST_errors": self.rest_errors,
            "REST_retries": self.rest_retries,
            "429_count": self.rate_limit_429_count,
            "fills_detected_through_REST": self.fills_detected_through_rest,
            "duplicate_fills_ignored": self.duplicate_fills_ignored,
            "REST_order_reconciliations": self.order_reconciliations,
            "REST_position_reconciliations": self.position_reconciliations,
            "reconciliation_mismatches": self.reconciliation_mismatches,
            "position_mismatches": self.position_mismatches,
        }


@dataclass
class RestFallbackState:
    run_id: str
    product_id: int
    product_symbol: str
    execution_event_mode: ExecutionEventMode = ExecutionEventMode.REST_FALLBACK
    operational_state: OperationalState = OperationalState.DEGRADED
    private_ws_status: str = "BLOCKED_403"
    fill_lookback_seconds: int = DEFAULT_FILL_LOOKBACK_SECONDS
    last_confirmed_fill_created_at_us: int | None = None
    seen_fill_ids: set[str] = field(default_factory=set)
    local_inventory: Decimal = Decimal("0")
    metrics: RestFallbackMetrics = field(default_factory=RestFallbackMetrics)


def _safe_decimal(value: Any) -> Decimal:
    try:
        if value in [None, ""]:
            return Decimal("0")
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _created_at_us(item: dict) -> int | None:
    value = item.get("created_at") or item.get("timestamp")
    try:
        return int(value) if value not in [None, ""] else None
    except Exception:
        return None


def _fill_id(fill: dict) -> str:
    return str(fill.get("id") or fill.get("fill_id") or f"{fill.get('order_id')}:{fill.get('created_at')}:{fill.get('size')}")


def _position_size(positions_response: dict, product_id: int, product_symbol: str) -> Decimal:
    rows = positions_response.get("result") or []
    if isinstance(rows, dict):
        rows = [rows]
    for row in rows:
        if str(row.get("product_id")) == str(product_id) or row.get("product_symbol") == product_symbol or row.get("symbol") == product_symbol:
            return _safe_decimal(row.get("size"))
    return Decimal("0")


class RestFallbackPoller:
    """REST fallback v0.1.

    Fill cursor method: poll recent fills with a small overlapping lookback from
    the last confirmed exchange fill timestamp. Exchange fill IDs are stored as
    the idempotency barrier, so overlapping polls and restarts process each fill
    at most once.
    """

    def __init__(self, client: DeltaTestnetClient, repo: InMemoryGridRepository | None = None):
        self.client = client
        self.repo = repo or repository

    def activate(self, run_id: str, product_id: int, product_symbol: str) -> RestFallbackState:
        state = RestFallbackState(run_id=run_id, product_id=product_id, product_symbol=product_symbol)
        self.repo.set_rest_fallback_state(run_id, self.serialise_state(state))
        self.repo.log_event(None, run_id, "PRIVATE_WS_UNAVAILABLE", {"private_ws_status": state.private_ws_status})
        self.repo.log_event(None, run_id, "REST_FALLBACK_ENABLED", {"execution_event_mode": state.execution_event_mode.value})
        self.repo.log_event(None, run_id, "EXECUTION_MODE_CHANGED", self.serialise_state(state))
        return state

    def poll_once(self, state: RestFallbackState) -> dict:
        now = time.monotonic()
        if state.metrics.last_poll_started_at is not None:
            state.metrics.poll_intervals.append(now - state.metrics.last_poll_started_at)
        state.metrics.last_poll_started_at = now
        state.metrics.rest_poll_count += 1

        try:
            fills = self._fetch_recent_fills(state)
            orders = self.client.open_orders(state.product_id)
            positions = self.client.positions("ETH")
            margin = self.client.account_margin()
        except requests.HTTPError as exc:
            self._record_poll_error(state, exc, getattr(getattr(exc, "response", None), "status_code", None))
            return {"ok": False, "error": str(exc), "state": self.serialise_state(state)}
        except Exception as exc:
            self._record_poll_error(state, exc, None)
            return {"ok": False, "error": str(exc), "state": self.serialise_state(state)}

        state.metrics.backoff_seconds = 0
        fill_result = self.process_fills(state, fills.get("result") or [])
        order_result = self.reconcile_orders(state, orders.get("result") or [])
        position_result = self.reconcile_position(state, positions)
        self.repo.log_event(None, state.run_id, "REST_ORDER_RECONCILED", order_result)
        self.repo.log_event(None, state.run_id, "REST_POSITION_RECONCILED", position_result)
        self.repo.set_rest_fallback_state(state.run_id, self.serialise_state(state))
        return {
            "ok": True,
            "fills": fill_result,
            "orders": order_result,
            "position": position_result,
            "margin_keys": list(margin.keys())[:10] if isinstance(margin, dict) else [],
            "state": self.serialise_state(state),
        }

    def _record_poll_error(self, state: RestFallbackState, exc: Exception, status_code: int | None) -> None:
        state.metrics.rest_errors += 1
        state.metrics.rest_retries += 1
        if status_code == 429:
            state.metrics.rate_limit_429_count += 1
            state.metrics.backoff_seconds = min(max(state.metrics.backoff_seconds * 2, 2), 30)
            event = "REST_RATE_LIMITED"
        else:
            state.metrics.backoff_seconds = min(max(state.metrics.backoff_seconds * 2, 1), 15)
            event = "REST_POLL_ERROR"
        self.repo.log_event(None, state.run_id, event, {"error": str(exc), "backoff_seconds": state.metrics.backoff_seconds})
        self.repo.set_rest_fallback_state(state.run_id, self.serialise_state(state))

    def _fetch_recent_fills(self, state: RestFallbackState) -> dict:
        start_time = None
        if state.last_confirmed_fill_created_at_us:
            start_time = max(0, state.last_confirmed_fill_created_at_us - state.fill_lookback_seconds * 1_000_000)
        return self.client.fills(state.product_id, start_time=start_time, page_size=50)

    def process_fills(self, state: RestFallbackState, fills: list[dict]) -> dict:
        processed = 0
        duplicates = 0
        for fill in sorted(fills, key=lambda item: _created_at_us(item) or 0):
            exchange_fill_id = _fill_id(fill)
            created_us = _created_at_us(fill)
            inserted = self.repo.insert_fill_once(exchange_fill_id, {"run_id": state.run_id, "fill": fill, "detected_at": time.time()})
            if not inserted or exchange_fill_id in state.seen_fill_ids:
                duplicates += 1
                state.metrics.duplicate_fills_ignored += 1
                continue
            state.seen_fill_ids.add(exchange_fill_id)
            side = Side(fill.get("side")) if fill.get("side") in {"buy", "sell"} else None
            size = _safe_decimal(fill.get("size"))
            if side == Side.BUY:
                state.local_inventory += size
            elif side == Side.SELL:
                state.local_inventory -= size
            if created_us:
                state.last_confirmed_fill_created_at_us = max(state.last_confirmed_fill_created_at_us or 0, created_us)
                state.metrics.fill_detection_latencies.append(max(0.0, (time.time() * 1_000_000 - created_us) / 1_000_000))
            processed += 1
            state.metrics.fills_detected_through_rest += 1
            self.repo.log_event(None, state.run_id, "REST_FILL_DETECTED", {"exchange_fill_id": exchange_fill_id, "side": fill.get("side"), "size": str(size)})
        return {"processed": processed, "duplicates": duplicates}

    def reconcile_orders(self, state: RestFallbackState, open_orders: list[dict]) -> dict:
        state.metrics.order_reconciliations += 1
        gridbot_orders = [order for order in open_orders if str(order.get("client_order_id", "")).startswith("DGB")]
        for order in gridbot_orders:
            key = str(order.get("client_order_id") or order.get("id"))
            self.repo.upsert_order(key, {"run_id": state.run_id, "exchange_order": order, "reconciled_at": time.time()})
        return {"open_gridbot_orders": len(gridbot_orders), "open_exchange_orders": len(open_orders)}

    def reconcile_position(self, state: RestFallbackState, positions_response: dict) -> dict:
        state.metrics.position_reconciliations += 1
        exchange_position = _position_size(positions_response, state.product_id, state.product_symbol)
        mismatch = exchange_position != state.local_inventory
        if mismatch:
            state.operational_state = OperationalState.DEGRADED_RECONCILIATION
            state.metrics.position_mismatches += 1
            state.metrics.reconciliation_mismatches += 1
            self.repo.log_event(None, state.run_id, "POSITION_MISMATCH", {"local_inventory": str(state.local_inventory), "exchange_position": str(exchange_position)})
        return {"local_inventory": str(state.local_inventory), "exchange_position": str(exchange_position), "mismatch": mismatch}

    def serialise_state(self, state: RestFallbackState) -> dict:
        return {
            "run_id": state.run_id,
            "product_id": state.product_id,
            "product_symbol": state.product_symbol,
            "execution_event_mode": state.execution_event_mode.value,
            "operational_state": state.operational_state.value,
            "private_ws_status": state.private_ws_status,
            "fill_lookback_seconds": state.fill_lookback_seconds,
            "last_confirmed_fill_created_at_us": state.last_confirmed_fill_created_at_us,
            **state.metrics.as_dict(),
        }

