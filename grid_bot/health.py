from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from .accounting import build_run_accounting
from .models import GridStatus, GridType, utc_now


class HealthStatus(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    ATTENTION_REQUIRED = "ATTENTION_REQUIRED"
    CRITICAL = "CRITICAL"


class HealthSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


ATTENTION_CODES = {
    "ACTIVE_LOCK_CONTRADICTION",
    "ACCOUNTING_MISMATCH",
    "FILL_LEDGER_MISMATCH",
    "GRID_NATURE_INVENTORY_VIOLATION",
    "GRID_ORDER_ORPHAN",
    "GRID_ORDER_MISSING_UNEXPECTEDLY",
    "GRID_ORDER_UNRESOLVED",
    "LIFECYCLE_STUCK",
    "MISSING_REPLACEMENT",
    "POSITION_MISMATCH",
    "RESERVATION_MISMATCH",
    "STOP_REQUIRES_ATTENTION",
    "SUBMITTED_ORDER_UNRESOLVED",
    "SUPABASE_FAILURE",
}
CRITICAL_CODES = {
    "ACTIVE_LOCK_CONTRADICTION",
    "GRID_NATURE_INVENTORY_VIOLATION",
    "MAX_INVENTORY_VIOLATION",
    "POSITION_ATTRIBUTION_UNSAFE",
    "STOPPED_WITH_EXPOSURE",
    "WORKER_DEAD_RUNNING",
}
RUNNING_STATES = {GridStatus.RUNNING.value}
ACTIVE_STATES = {
    GridStatus.STARTING.value,
    GridStatus.RUNNING.value,
    GridStatus.PAUSING.value,
    GridStatus.PAUSED.value,
    GridStatus.RESUMING.value,
    GridStatus.EDITING.value,
    GridStatus.REGRID_PENDING.value,
    GridStatus.STOPPING.value,
    GridStatus.STOP_REQUIRES_ATTENTION.value,
}
STUCK_STATES = {
    GridStatus.PAUSING.value,
    GridStatus.RESUMING.value,
    GridStatus.EDITING.value,
    GridStatus.STOPPING.value,
}
OPEN_ORDER_STATUSES = {"open", "partially_filled", "pending", "submitted", "proposed"}


@dataclass
class HealthIssue:
    code: str
    severity: str
    message: str
    run_id: str | None = None
    context: dict[str, Any] = field(default_factory=dict)
    first_seen: str = field(default_factory=utc_now)
    last_seen: str = field(default_factory=utc_now)
    occurrence_count: int = 1
    active: bool = True
    resolved_at: str | None = None

    @property
    def key(self) -> str:
        context_key = self.context.get("context_key") or self.context.get("client_order_id") or self.context.get("exchange_order_id") or ""
        return f"{self.run_id or 'global'}:{self.code}:{context_key}"

    def as_dict(self) -> dict:
        return asdict(self)


def _decimal(value: Any, default: str = "0") -> Decimal:
    try:
        if value in [None, ""]:
            return Decimal(default)
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _age_seconds(timestamp: str | None, now: datetime | None = None) -> float | None:
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, ((now or datetime.now(timezone.utc)) - parsed).total_seconds())
    except Exception:
        return None


def _issue(code: str, severity: HealthSeverity, message: str, run: dict | None = None, **context: Any) -> HealthIssue:
    return HealthIssue(code=code, severity=severity.value, message=message, run_id=(run or {}).get("run_id"), context={k: v for k, v in context.items() if v not in [None, ""]})


def _open_orders(run: dict | None) -> list[dict]:
    return [
        order
        for order in ((run or {}).get("orders") or {}).values()
        if str(order.get("status") or "").lower() in OPEN_ORDER_STATUSES and _decimal(order.get("remaining_quantity"), str(order.get("requested_quantity") or "0")) > 0
    ]


def _replacement_missing(run: dict | None) -> list[str]:
    fills = set(((run or {}).get("fills") or {}).keys())
    replaced = {
        str(order.get("source_fill_id") or ((order.get("raw") or {}).get("gridbot") or {}).get("source_fill_id"))
        for order in ((run or {}).get("orders") or {}).values()
        if order.get("order_kind") == "replacement"
    }
    deferred = set(((run or {}).get("deferred_orders") or {}).keys())
    return sorted(fill_id for fill_id in fills if fill_id and fill_id not in replaced and fill_id not in deferred)


def _reservation_mismatches(run: dict | None) -> list[str]:
    bad = []
    for key, order in ((run or {}).get("orders") or {}).items():
        if order.get("opens_inventory") is True and order.get("reserved_long_after") is None and order.get("reserved_short_after") is None:
            bad.append(str(order.get("client_order_id") or key))
    return bad


def _classify_delta_error(message: str, status: Any = None) -> tuple[str, HealthSeverity, str]:
    raw = f"{status or ''} {message}".lower()
    if "401" in raw or "unauthorized" in raw or "auth" in raw:
        return "DELTA_AUTH_FAILURE", HealthSeverity.CRITICAL, "Delta authentication failed."
    if "429" in raw or "rate limit" in raw or "too many" in raw:
        return "DELTA_RATE_LIMIT", HealthSeverity.WARNING, "Delta rate limit encountered."
    if "timeout" in raw or "timed out" in raw:
        return "DELTA_TIMEOUT", HealthSeverity.WARNING, "Delta request timed out."
    if any(code in raw for code in ["500", "502", "503", "504", "5xx"]):
        return "DELTA_5XX", HealthSeverity.WARNING, "Delta server error encountered."
    if "malformed" in raw or "json" in raw or "schema" in raw:
        return "DELTA_MALFORMED_RESPONSE", HealthSeverity.WARNING, "Delta response could not be normalized."
    return "DELTA_API_ERROR", HealthSeverity.WARNING, "Delta API error encountered."


def evaluate_gridbot_health(
    worker_state: dict | None = None,
    run: dict | None = None,
    reconciliation: dict | None = None,
    accounting: dict | None = None,
    *,
    now: datetime | None = None,
) -> dict:
    state = dict(worker_state or {})
    run = run or {}
    reconciliation = reconciliation or {}
    accounting = accounting or state.get("accounting") or build_run_accounting(run).as_dict()
    now = now or datetime.now(timezone.utc)
    status = run.get("status") or state.get("lifecycle_state")
    run_id = run.get("run_id") or state.get("run_id")
    run_for_issue = {**run, "run_id": run_id} if run_id else run
    poll_interval = float(state.get("poll_interval_seconds") or 2)
    stale_after = max(180.0, poll_interval * 60)
    issues: list[HealthIssue] = []

    if status == GridStatus.RUNNING.value:
        if not state.get("thread_alive") or state.get("running") is False:
            issues.append(_issue("WORKER_DEAD_RUNNING", HealthSeverity.CRITICAL, "RUNNING run does not have a healthy worker thread.", run_for_issue, worker_status=state.get("status")))
        poll_age = _age_seconds(state.get("last_successful_poll_at"), now)
        if poll_age is not None and poll_age > stale_after:
            issues.append(_issue("WORKER_STALLED", HealthSeverity.CRITICAL, "GridBot worker has not completed a recent poll.", run_for_issue, age_seconds=round(poll_age, 3)))
        reconcile_age = _age_seconds(state.get("last_successful_reconcile") or run.get("last_reconciled_at"), now)
        if reconcile_age is None and int(state.get("successful_polls") or 0) > 0:
            issues.append(_issue("RECONCILIATION_STALE", HealthSeverity.WARNING, "No successful reconciliation timestamp is available for the running run.", run_for_issue))
        elif reconcile_age is not None and reconcile_age > stale_after:
            issues.append(_issue("RECONCILIATION_STALE", HealthSeverity.WARNING, "Exchange reconciliation is stale.", run_for_issue, age_seconds=round(reconcile_age, 3)))

    last_error = str(state.get("last_error") or "")
    if last_error:
        code, severity, message = _classify_delta_error(last_error)
        issues.append(_issue(code, severity, message, run_for_issue, error=last_error[:300]))
    if int(state.get("rate_limit_429s") or 0) >= 3:
        issues.append(_issue("DELTA_REPEATED_429", HealthSeverity.WARNING, "Delta rate limits are repeating.", run_for_issue, count=state.get("rate_limit_429s")))
    for error in reconciliation.get("errors") or []:
        code, severity, message = _classify_delta_error(str(error))
        issues.append(_issue(code, severity, message, run_for_issue, error=str(error)[:300]))

    if int(reconciliation.get("unresolved_orders") or state.get("unresolved_orders") or 0) > 0:
        issues.append(_issue("GRID_ORDER_UNRESOLVED", HealthSeverity.CRITICAL, "One or more submitted GridBot orders cannot be resolved on exchange truth.", run_for_issue, count=reconciliation.get("unresolved_orders")))
    if int(reconciliation.get("duplicate_fills_ignored") or 0) > 0:
        issues.append(_issue("DUPLICATE_FILL_IGNORED", HealthSeverity.INFO, "Duplicate exchange fill was ignored by the fill ledger.", run_for_issue, count=reconciliation.get("duplicate_fills_ignored")))
    if int(reconciliation.get("position_mismatches") or state.get("position_mismatches") or 0) > 0:
        issues.append(
            _issue(
                "POSITION_MISMATCH",
                HealthSeverity.CRITICAL,
                "Delta position and GridBot fill-derived inventory disagree.",
                run_for_issue,
                gridbot_inventory=reconciliation.get("gridbot_inventory") or state.get("fill_derived_inventory"),
                delta_position=reconciliation.get("delta_position") or state.get("delta_position"),
            )
        )
    if int(reconciliation.get("fill_ledger_mismatches") or state.get("fill_ledger_mismatches") or 0) > 0:
        issues.append(_issue("FILL_LEDGER_MISMATCH", HealthSeverity.CRITICAL, "Fill ledger exceeds requested order quantity.", run_for_issue))
    for event in reconciliation.get("events") or []:
        event_type = event.get("event_type")
        payload = event.get("payload") or {}
        if event_type == "GRID_NATURE_INVENTORY_VIOLATION":
            issues.append(_issue("GRID_NATURE_INVENTORY_VIOLATION", HealthSeverity.CRITICAL, "Grid inventory direction violates grid nature.", run_for_issue, **payload))
        elif event_type == "ORDER_UNRESOLVED":
            issues.append(_issue("GRID_ORDER_UNRESOLVED", HealthSeverity.CRITICAL, "GridBot order is unresolved on exchange.", run_for_issue, **payload))
        elif event_type == "FILL_LEDGER_MISMATCH":
            issues.append(_issue("FILL_LEDGER_MISMATCH", HealthSeverity.CRITICAL, "Fill ledger exceeds requested order quantity.", run_for_issue, **payload))

    inventory = _decimal(reconciliation.get("gridbot_inventory") or state.get("fill_derived_inventory"))
    position = _decimal(reconciliation.get("delta_position") or state.get("delta_position"))
    max_inventory = abs(_decimal((run.get("config") or {}).get("max_inventory_lots")))
    if max_inventory and abs(inventory) > max_inventory:
        issues.append(_issue("MAX_INVENTORY_VIOLATION", HealthSeverity.CRITICAL, "GridBot inventory exceeds max inventory.", run_for_issue, inventory=str(inventory), max_inventory=str(max_inventory)))
    grid_type = str((run.get("config") or {}).get("grid_type") or "")
    if grid_type == GridType.LONG_BIAS.value and inventory < 0:
        issues.append(_issue("GRID_NATURE_INVENTORY_VIOLATION", HealthSeverity.CRITICAL, "Long grid is unexpectedly net short.", run_for_issue, inventory=str(inventory)))
    if grid_type == GridType.SHORT_BIAS.value and inventory > 0:
        issues.append(_issue("GRID_NATURE_INVENTORY_VIOLATION", HealthSeverity.CRITICAL, "Short grid is unexpectedly net long.", run_for_issue, inventory=str(inventory)))
    if abs(position - inventory) > 0:
        issues.append(_issue("POSITION_ATTRIBUTION_UNSAFE", HealthSeverity.CRITICAL, "Account exposure cannot be safely attributed to this GridBot.", run_for_issue, delta_position=str(position), gridbot_inventory=str(inventory)))

    open_orders = _open_orders(run)
    known_gridbot_orders = state.get("known_gridbot_orders") or list(((run or {}).get("orders") or {}).values())
    known_open_count = len(open_orders) if run else len([order for order in known_gridbot_orders if str(order.get("status") or "").lower() in OPEN_ORDER_STATUSES])
    exchange_open_count = int(reconciliation.get("exchange_open_orders") or 0)
    has_fresh_exchange_order_truth = "exchange_open_orders" in reconciliation
    if has_fresh_exchange_order_truth and exchange_open_count > known_open_count:
        issues.append(_issue("GRID_ORDER_ORPHAN", HealthSeverity.CRITICAL, "Exchange has GridBot-owned open orders not matched to local run orders.", run_for_issue, exchange_open_orders=exchange_open_count, known_open_orders=known_open_count))
    if has_fresh_exchange_order_truth and status == GridStatus.RUNNING.value and known_open_count > exchange_open_count:
        issues.append(_issue("GRID_ORDER_MISSING_UNEXPECTEDLY", HealthSeverity.CRITICAL, "Local GridBot open orders are missing from exchange open-order truth.", run_for_issue, exchange_open_orders=exchange_open_count, known_open_orders=known_open_count))
    client_ids = [str(order.get("client_order_id") or "") for order in known_gridbot_orders if order.get("client_order_id")]
    exchange_ids = [str(order.get("exchange_order_id") or "") for order in known_gridbot_orders if order.get("exchange_order_id")]
    if len(client_ids) != len(set(client_ids)) or len(exchange_ids) != len(set(exchange_ids)):
        issues.append(_issue("DUPLICATE_ORDER", HealthSeverity.CRITICAL, "Duplicate GridBot order identity detected.", run_for_issue))
    unresolved_submissions = [
        str(order.get("client_order_id") or key)
        for key, order in ((run or {}).get("orders") or {}).items()
        if str(order.get("status") or "").lower() in {"submitted", "pending", "proposed"} and not order.get("exchange_order_id")
    ]
    if unresolved_submissions:
        issues.append(_issue("SUBMITTED_ORDER_UNRESOLVED", HealthSeverity.CRITICAL, "Submitted GridBot orders have no exchange order id yet.", run_for_issue, orders=unresolved_submissions[:10]))
    if status == GridStatus.PAUSED.value and open_orders:
        issues.append(_issue("PAUSED_WITH_RESTING_ORDERS", HealthSeverity.CRITICAL, "PAUSED run still has GridBot resting orders.", run_for_issue, open_orders=len(open_orders)))
    if status == GridStatus.STOPPED.value and (open_orders or position != 0 or inventory != 0):
        issues.append(_issue("STOPPED_WITH_EXPOSURE", HealthSeverity.CRITICAL, "STOPPED run still has open orders or attributable inventory.", run_for_issue, open_orders=len(open_orders), delta_position=str(position), gridbot_inventory=str(inventory)))
    if status == GridStatus.STOP_REQUIRES_ATTENTION.value:
        issues.append(_issue("STOP_REQUIRES_ATTENTION", HealthSeverity.CRITICAL, "Stop and close requires operator attention.", run_for_issue, diagnostics=run.get("stop_diagnostics")))
    if status in STUCK_STATES:
        status_age = _age_seconds(run.get("status_updated_at") or run.get("updated_at"), now)
        if status_age is not None and status_age > 120:
            issues.append(_issue("LIFECYCLE_STUCK", HealthSeverity.CRITICAL, f"{status} lifecycle state appears stuck.", run_for_issue, lifecycle_state=status, age_seconds=status_age))

    missing_replacements = _replacement_missing(run)
    if status == GridStatus.RUNNING.value and missing_replacements:
        issues.append(_issue("MISSING_REPLACEMENT", HealthSeverity.CRITICAL, "Filled orders have no active, existing, or deferred replacement.", run_for_issue, missing_fill_ids=missing_replacements[:10]))
    reservation_mismatches = _reservation_mismatches(run)
    if reservation_mismatches:
        issues.append(_issue("RESERVATION_MISMATCH", HealthSeverity.CRITICAL, "Opening orders are missing reservation metadata.", run_for_issue, orders=reservation_mismatches[:10]))

    telemetry = state.get("account_risk_state") or {}
    telemetry_freshness = {
        "status": telemetry.get("telemetry_status"),
        "account_age_seconds": telemetry.get("account_age_seconds"),
        "position_age_seconds": telemetry.get("position_age_seconds"),
        "order_age_seconds": telemetry.get("order_age_seconds"),
        "market_age_seconds": telemetry.get("market_age_seconds"),
        "unavailable_fields": telemetry.get("unavailable_fields") or [],
        "errors": telemetry.get("errors") or [],
    }
    telemetry_status = telemetry.get("telemetry_status")
    if telemetry_status == "STALE":
        issues.append(_issue("TELEMETRY_STALE", HealthSeverity.WARNING, "Critical account, position, order, or market telemetry is stale.", run_for_issue, **telemetry_freshness))
    elif telemetry_status == "UNAVAILABLE":
        issues.append(_issue("CRITICAL_TELEMETRY_UNAVAILABLE", HealthSeverity.CRITICAL, "Critical account, position, order, or market telemetry is unavailable.", run_for_issue, **telemetry_freshness))
    elif telemetry_status == "DEGRADED":
        issues.append(_issue("TELEMETRY_DEGRADED", HealthSeverity.WARNING, "Optional or account telemetry is degraded.", run_for_issue, **telemetry_freshness))

    has_accounting_activity = bool((run or {}).get("fills") or (run or {}).get("exchange_costs") or accounting.get("warnings") or accounting.get("accounting_warnings"))
    if run_id and has_accounting_activity and str(accounting.get("accounting_status") or "COMPLETE") != "COMPLETE":
        severity = HealthSeverity.WARNING
        issues.append(_issue("ACCOUNTING_INCOMPLETE", severity, "Run accounting is partial or incomplete.", run_for_issue, warnings=accounting.get("warnings") or accounting.get("accounting_warnings") or []))
    if run_id and (accounting.get("pnl_mismatch") or accounting.get("cycle_mismatch")):
        issues.append(_issue("ACCOUNTING_MISMATCH", HealthSeverity.CRITICAL, "Accounting reconciliation mismatch detected.", run_for_issue, accounting_status=accounting.get("accounting_status")))
    if run_id and "DUPLICATE_EXCHANGE_COST" in (accounting.get("warnings") or []):
        issues.append(_issue("DUPLICATE_EXCHANGE_COST_PROTECTION", HealthSeverity.CRITICAL, "Duplicate exchange-cost protection fired.", run_for_issue))

    db_counts = state.get("supabase_request_counts") or {}
    if db_counts.get("last_error") or state.get("supabase_error"):
        issues.append(_issue("SUPABASE_FAILURE", HealthSeverity.CRITICAL, "Supabase read/write failed.", run_for_issue, error=db_counts.get("last_error") or state.get("supabase_error")))
    if state.get("active_lock_contradiction"):
        issues.append(_issue("ACTIVE_LOCK_CONTRADICTION", HealthSeverity.CRITICAL, "Active-run lock contradicts recovered run state.", run_for_issue))

    deduped: dict[str, HealthIssue] = {}
    for item in issues:
        deduped.setdefault(item.key, item)
    issues = list(deduped.values())
    severities = {issue.severity for issue in issues}
    codes = {issue.code for issue in issues}
    if codes & CRITICAL_CODES or HealthSeverity.CRITICAL.value in severities and any(code in codes for code in CRITICAL_CODES | {"CRITICAL_TELEMETRY_UNAVAILABLE", "DELTA_AUTH_FAILURE"}):
        overall = HealthStatus.CRITICAL.value
    elif codes & ATTENTION_CODES or HealthSeverity.CRITICAL.value in severities:
        overall = HealthStatus.ATTENTION_REQUIRED.value
    elif HealthSeverity.WARNING.value in severities:
        overall = HealthStatus.DEGRADED.value
    else:
        overall = HealthStatus.HEALTHY.value

    safe_for_risk_increase = overall == HealthStatus.HEALTHY.value
    safe_for_risk_reduce = not any(issue.code in {"WORKER_DEAD_RUNNING", "CRITICAL_TELEMETRY_UNAVAILABLE", "POSITION_ATTRIBUTION_UNSAFE", "ACTIVE_LOCK_CONTRADICTION"} for issue in issues)
    operator_attention_required = overall in {HealthStatus.ATTENTION_REQUIRED.value, HealthStatus.CRITICAL.value} or bool(codes & ATTENTION_CODES)
    agreement = {
        "gridbot_inventory": str(inventory),
        "delta_position": str(position),
        "matches": inventory == position,
        "difference": str(position - inventory),
    }
    return {
        "overall_status": overall,
        "safe_for_risk_increase": safe_for_risk_increase,
        "safe_for_risk_reduce": safe_for_risk_reduce,
        "operator_attention_required": operator_attention_required,
        "worker_health": {
            "running": bool(state.get("running")),
            "thread_alive": bool(state.get("thread_alive")),
            "status": state.get("status"),
            "poll_count": state.get("poll_count") or 0,
            "successful_polls": state.get("successful_polls") or 0,
            "last_poll_at": state.get("last_poll_at"),
            "last_successful_poll_at": state.get("last_successful_poll_at"),
            "last_loop_duration_seconds": state.get("last_loop_duration_seconds"),
            "average_loop_duration_seconds": state.get("average_loop_duration_seconds"),
        },
        "last_successful_poll": state.get("last_successful_poll_at"),
        "last_successful_reconcile": state.get("last_successful_reconcile") or run.get("last_reconciled_at") or reconciliation.get("last_successful_reconcile"),
        "telemetry_freshness": telemetry_freshness,
        "active_issues": [issue.as_dict() for issue in issues],
        "recent_resolved_issues": state.get("recent_resolved_health_issues") or [],
        "api_error_counters": {
            "rest_errors": state.get("rest_errors") or 0,
            "rate_limit_429s": state.get("rate_limit_429s") or 0,
            "delta_account_telemetry_request_counts": state.get("delta_account_telemetry_request_counts") or {},
        },
        "429_count": state.get("rate_limit_429s") or 0,
        "position_inventory_agreement": agreement,
        "accounting_status": {
            "status": accounting.get("accounting_status") or "COMPLETE",
            "warnings": accounting.get("warnings") or accounting.get("accounting_warnings") or [],
            "funding_attribution_status": accounting.get("funding_attribution_status"),
        },
        "auto_recovery": {
            "conservative_only": True,
            "supported": [
                "refresh_stale_telemetry",
                "retry_reconciliation",
                "recover_worker_after_restart",
                "recognize_existing_exchange_order",
                "rebuild_reservations",
                "resolve_transient_api_failure",
            ],
            "forbidden": [
                "flatten_ambiguous_unrelated_exposure",
                "clear_active_locks_blindly",
                "delete_unresolved_records",
                "restart_trading_when_safety_unknown",
            ],
        },
    }


class HealthIssueTracker:
    def __init__(self):
        self._active: dict[str, HealthIssue] = {}
        self._recent_resolved: list[dict] = []
        self._last_persisted_signature: tuple[tuple[str, str, str], ...] = ()

    @property
    def recent_resolved(self) -> list[dict]:
        return list(self._recent_resolved)

    def update(self, health: dict, db: Any | None = None) -> dict:
        now = utc_now()
        current: dict[str, HealthIssue] = {}
        for payload in health.get("active_issues") or []:
            issue = HealthIssue(**{key: payload.get(key) for key in HealthIssue.__dataclass_fields__})
            existing = self._active.get(issue.key)
            if existing:
                issue.first_seen = existing.first_seen
                issue.occurrence_count = existing.occurrence_count + 1
            issue.last_seen = now
            current[issue.key] = issue
        resolved = []
        for key, issue in self._active.items():
            if key not in current:
                issue.active = False
                issue.resolved_at = now
                issue.last_seen = now
                resolved.append(issue)
        self._active = current
        self._recent_resolved = [issue.as_dict() for issue in resolved] + self._recent_resolved[:20]
        health["active_issues"] = [issue.as_dict() for issue in current.values()]
        health["recent_resolved_issues"] = self.recent_resolved
        signature = tuple(sorted((issue.key, issue.severity, issue.message) for issue in current.values()))
        if db and getattr(db, "enabled", False) and (signature != self._last_persisted_signature or resolved):
            db.sync_health_issues(list(current.values()), resolved)
            self._last_persisted_signature = signature
        return health
