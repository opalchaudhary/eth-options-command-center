from __future__ import annotations

import logging
import os
import threading
import time
from copy import deepcopy
from decimal import Decimal
from typing import Any

from .account_telemetry import AccountTelemetryCache, account_telemetry_cache
from .accounting import build_run_accounting
from .delta_testnet_client import DeltaTestnetClient
from .durable_lifecycle import DurableGridBotLifecycle
from .exchange_truth import inventory_from_fills, reconcile_exchange_truth
from .health import HealthIssueTracker, evaluate_gridbot_health
from .models import GridStatus, utc_now
from .supabase_repository import SupabaseGridRepository


logger = logging.getLogger(__name__)

EXECUTABLE_STATUSES = {GridStatus.STARTING.value, GridStatus.RUNNING.value}
POLL_INTERVAL_SECONDS = float(os.getenv("GRIDBOT_V01_WORKER_POLL_SECONDS", "2"))
SNAPSHOT_INTERVAL_SECONDS = float(os.getenv("GRIDBOT_V01_WORKER_SNAPSHOT_SECONDS", "300"))
ACTIVE_RUN_REFRESH_SECONDS = float(os.getenv("GRIDBOT_V01_WORKER_ACTIVE_REFRESH_SECONDS", "10"))
ACCOUNT_TELEMETRY_SECONDS = float(os.getenv("GRIDBOT_V01_ACCOUNT_TELEMETRY_SECONDS", "30"))
MATERIAL_INVENTORY_DELTA = Decimal(os.getenv("GRIDBOT_V01_SNAPSHOT_MATERIAL_INVENTORY_DELTA", "1"))
POSITION_MISMATCH_CONFIRMATION_POLLS = int(os.getenv("GRIDBOT_V01_POSITION_MISMATCH_CONFIRMATION_POLLS", "2"))
POSITION_MISMATCH_CONFIRMATION_SECONDS = float(os.getenv("GRIDBOT_V01_POSITION_MISMATCH_CONFIRMATION_SECONDS", "30"))


def _decimal(value: Any, default: str = "0") -> Decimal:
    try:
        if value in [None, ""]:
            return Decimal(default)
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


class ContinuousGridBotWorker:
    def __init__(
        self,
        client: DeltaTestnetClient | None = None,
        db: SupabaseGridRepository | None = None,
        poll_interval_seconds: float = POLL_INTERVAL_SECONDS,
        snapshot_interval_seconds: float = SNAPSHOT_INTERVAL_SECONDS,
    ):
        self.client = client or DeltaTestnetClient()
        self.db = db or SupabaseGridRepository()
        self.poll_interval_seconds = poll_interval_seconds
        self.snapshot_interval_seconds = snapshot_interval_seconds
        self.active_run_refresh_seconds = ACTIVE_RUN_REFRESH_SECONDS
        self.account_telemetry = AccountTelemetryCache(client=self.client, refresh_interval_seconds=ACCOUNT_TELEMETRY_SECONDS)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._run: dict | None = None
        self._health_tracker = HealthIssueTracker()
        self._last_snapshot_monotonic = 0.0
        self._last_active_refresh_monotonic = 0.0
        self._last_snapshot_signature: tuple | None = None
        self._state = {
            "ok": True,
            "worker_owner": "FastAPI backend process",
            "running": False,
            "thread_alive": False,
            "run_id": None,
            "status": "idle",
            "poll_interval_seconds": self.poll_interval_seconds,
            "snapshot_interval_seconds": self.snapshot_interval_seconds,
            "active_run_refresh_seconds": self.active_run_refresh_seconds,
            "account_telemetry_refresh_seconds": ACCOUNT_TELEMETRY_SECONDS,
            "poll_count": 0,
            "successful_polls": 0,
            "rest_errors": 0,
            "rate_limit_429s": 0,
            "last_poll_at": None,
            "last_successful_poll_at": None,
            "last_successful_reconcile": None,
            "last_loop_duration_seconds": None,
            "average_loop_duration_seconds": None,
            "last_error": None,
            "last_fill": None,
            "last_replacement": None,
            "fill_derived_inventory": "0",
            "delta_position": "0",
            "open_gridbot_orders": 0,
            "known_fill_count": 0,
            "known_order_count": 0,
            "replacement_count": 0,
            "deferred_replacement_count": 0,
            "position_mismatches": 0,
            "pending_position_mismatch": None,
            "fill_ledger_mismatches": 0,
            "snapshot_writes": 0,
            "supabase_write_policy": {
                "fills_orders_replacements": "event-driven immediate",
                "health_anomalies": "event-driven immediate",
                "snapshots": f"approximately every {int(self.snapshot_interval_seconds)} seconds while running",
                "idle_heartbeat_rows": "none",
            },
            "supabase_request_counts": {},
            "account_risk_state": None,
            "account_telemetry_refresh_count": 0,
            "delta_account_telemetry_request_counts": {},
            "accounting": build_run_accounting({}).as_dict(),
            "health": evaluate_gridbot_health({}),
            "recent_resolved_health_issues": [],
        }

    def start(self) -> dict:
        with self._lock:
            if self._thread and self._thread.is_alive():
                already_running = True
            else:
                already_running = False
        if already_running:
            return self.state()
        with self._lock:
            if self._thread and self._thread.is_alive():
                return deepcopy(self._state)
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, name="gridbot-v01-continuous-worker", daemon=True)
            self._thread.start()
            self._state["running"] = True
            self._state["thread_alive"] = True
            return deepcopy(self._state)

    def stop(self) -> dict:
        self._stop.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5)
        with self._lock:
            self._state["running"] = False
            self._state["thread_alive"] = bool(thread and thread.is_alive())
        return self.state()

    def state(self) -> dict:
        with self._lock:
            state = deepcopy(self._state)
            run = self._run or {}
            if run:
                state.update(
                    {
                        "run_id": run.get("run_id"),
                        "lifecycle_state": run.get("status"),
                        "config": run.get("config") or {},
                        "config_version": (run.get("config") or {}).get("config_version"),
                        "grid_nature": (run.get("config") or {}).get("grid_type"),
                        "grid_levels": run.get("levels") or [],
                        "known_gridbot_orders": list((run.get("orders") or {}).values()),
                        "known_fill_ids": list((run.get("fills") or {}).keys()),
                        "replacement_state": run.get("replacement_keys") or {},
                    }
                )
            state["thread_alive"] = bool(self._thread and self._thread.is_alive())
            if hasattr(self.db, "stats"):
                state["supabase_request_counts"] = self.db.stats()
            cached = self.account_telemetry.snapshot() or account_telemetry_cache.snapshot()
            if cached:
                state["account_risk_state"] = cached
            if run:
                fill_inventory = inventory_from_fills([fill.get("raw") if isinstance(fill, dict) and isinstance(fill.get("raw"), dict) else fill for fill in (run.get("fills") or {}).values()])
                state["fill_derived_inventory"] = str(fill_inventory)
                state["known_fill_count"] = len(run.get("fills") or {})
                state["known_order_count"] = len(run.get("orders") or {})
                if cached and (cached or {}).get("position_lots") not in [None, ""]:
                    state["delta_position"] = str(_decimal((cached or {}).get("position_lots")))
                mark_price = _decimal((cached or {}).get("mark_price")) if cached and (cached or {}).get("mark_price") not in [None, ""] else None
                account_position = _decimal((cached or {}).get("position_lots")) if cached and (cached or {}).get("position_lots") not in [None, ""] else None
                state["accounting"] = build_run_accounting(run, mark_price=mark_price, account_position_lots=account_position).as_dict()
            state["recent_resolved_health_issues"] = self._health_tracker.recent_resolved
            state["health"] = evaluate_gridbot_health(state, run)
            return state

    def ensure_active_worker(self) -> dict:
        if self.db.enabled:
            self._recover_active_run()
        return self.start()

    def _set_state(self, **updates: Any) -> None:
        with self._lock:
            self._state.update(updates)

    def _set_idle_state(self) -> None:
        self._set_state(
            status="idle",
            run_id=None,
            open_gridbot_orders=0,
            known_order_count=0,
            known_fill_count=0,
            replacement_count=0,
            deferred_replacement_count=0,
            position_mismatches=0,
            pending_position_mismatch=None,
            fill_ledger_mismatches=0,
            fill_derived_inventory="0",
            delta_position="0",
            last_fill=None,
            last_replacement=None,
        )

    def _recover_active_run(self) -> dict | None:
        active = self.db.active_run() if self.db.enabled else None
        if not active:
            with self._lock:
                self._run = None
            self._set_idle_state()
            return None
        run = self.db.load_run_state(active["run_id"])
        with self._lock:
            self._run = run
            self._state.update(
                {
                    "run_id": run.get("run_id"),
                    "status": "running" if run.get("status") in EXECUTABLE_STATUSES else "waiting",
                    "known_fill_count": len(run.get("fills") or {}),
                    "known_order_count": len(run.get("orders") or {}),
                    "replacement_count": len(run.get("replacement_keys") or {}),
                }
            )
        return run

    def _refresh_active_run_if_due(self) -> dict | None:
        if not self.db.enabled:
            return self._run
        now = time.monotonic()
        run = self._run
        if run and run.get("status") in EXECUTABLE_STATUSES and now - self._last_active_refresh_monotonic < self.active_run_refresh_seconds:
            return run
        self._last_active_refresh_monotonic = now
        active = self.db.active_run()
        if not active:
            with self._lock:
                self._run = None
            return None
        if (
            run
            and active.get("run_id") == run.get("run_id")
            and active.get("status") == run.get("status")
            and active.get("status") in EXECUTABLE_STATUSES
        ):
            return run
        return self._recover_active_run()

    def _loop(self) -> None:
        logger.info("DeltaGridBot V0.1 continuous worker started.")
        self._set_state(running=True, status="running", last_error=None)
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                run = self._refresh_active_run_if_due()
                if not run or run.get("status") not in EXECUTABLE_STATUSES:
                    self._set_idle_state()
                    self._stop.wait(self.poll_interval_seconds)
                    continue

                if run.get("status") == GridStatus.STARTING.value:
                    recovery = DurableGridBotLifecycle(client=self.client, db=self.db, use_supabase=self.db.enabled).complete_operator_grid_start(run["run_id"])
                    recovered_run = recovery.get("run") or run
                    with self._lock:
                        self._run = recovered_run
                    if recovered_run.get("status") != GridStatus.RUNNING.value:
                        self._set_state(
                            status="waiting",
                            run_id=recovered_run.get("run_id"),
                            last_poll_at=utc_now(),
                            last_error=(recovered_run.get("startup") or {}).get("last_error"),
                            known_order_count=len(recovered_run.get("orders") or {}),
                            known_fill_count=len(recovered_run.get("fills") or {}),
                            open_gridbot_orders=len([
                                order
                                for order in (recovered_run.get("orders") or {}).values()
                                if str(order.get("status") or "").lower() in {"open", "partially_filled", "pending", "submitted", "ambiguous_submission"}
                            ]),
                        )
                        self._update_health(recovered_run, recovery.get("reconciliation") or {})
                        self._stop.wait(self.poll_interval_seconds)
                        continue
                    run = recovered_run

                result = self._poll_once(run)
                duration = time.monotonic() - started
                poll_count = self._state["poll_count"] + 1
                previous_average = _decimal(self._state.get("average_loop_duration_seconds"), "0")
                average = duration if poll_count == 1 else ((float(previous_average) * (poll_count - 1)) + duration) / poll_count
                self._set_state(
                    status="running",
                    poll_count=poll_count,
                    successful_polls=self._state["successful_polls"] + 1,
                    last_poll_at=utc_now(),
                    last_successful_poll_at=utc_now(),
                    last_successful_reconcile=run.get("last_reconciled_at"),
                    last_loop_duration_seconds=round(duration, 4),
                    average_loop_duration_seconds=round(average, 4),
                    last_error=None,
                    fill_derived_inventory=result.get("gridbot_inventory"),
                    delta_position=result.get("delta_position"),
                    open_gridbot_orders=result.get("exchange_open_orders"),
                    known_fill_count=len(run.get("fills") or {}),
                    known_order_count=len(run.get("orders") or {}),
                    replacement_count=len(run.get("replacement_keys") or {}),
                    deferred_replacement_count=len(run.get("deferred_orders") or {}),
                    position_mismatches=result.get("position_mismatches"),
                    fill_ledger_mismatches=result.get("fill_ledger_mismatches"),
                )
            except Exception as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                self._set_state(
                    status="error",
                    rest_errors=self._state["rest_errors"] + 1,
                    rate_limit_429s=self._state["rate_limit_429s"] + (1 if status == 429 else 0),
                    last_error=str(exc)[:500],
                    last_poll_at=utc_now(),
                )
                logger.exception("DeltaGridBot V0.1 continuous worker loop failed.")
                try:
                    if self.db.enabled:
                        self.db.log_event(self._run, "GRID_WORKER_ERROR", {"error": str(exc)[:500], "status": status})
                except Exception:
                    logger.exception("Failed to persist GridBot worker error event.")
                self._update_health(self._run, {})
            elapsed = time.monotonic() - started
            self._stop.wait(max(0.0, self.poll_interval_seconds - elapsed))
        self._set_state(running=False, thread_alive=False, status="stopped")
        logger.info("DeltaGridBot V0.1 continuous worker stopped.")

    def _snapshot_signature(self, run: dict, reconciliation: dict, replacement_result: dict, telemetry: Any) -> tuple:
        telemetry_payload = telemetry.as_dict() if hasattr(telemetry, "as_dict") else (telemetry or {})
        return (
            run.get("status"),
            int((run.get("config") or {}).get("config_version") or 1),
            str(reconciliation.get("gridbot_inventory")),
            str(reconciliation.get("delta_position")),
            int(reconciliation.get("exchange_open_orders") or 0),
            int(reconciliation.get("position_mismatches") or 0),
            int(reconciliation.get("fill_ledger_mismatches") or 0),
            int(reconciliation.get("unresolved_orders") or 0),
            int(replacement_result.get("created") or 0),
            int(replacement_result.get("deferred") or 0),
            telemetry_payload.get("telemetry_status"),
            telemetry_payload.get("account_equity"),
            telemetry_payload.get("available_margin"),
            telemetry_payload.get("used_margin"),
            telemetry_payload.get("margin_utilisation_pct"),
            telemetry_payload.get("mark_price"),
        )

    def _snapshot_materially_changed(self, signature: tuple) -> bool:
        if self._last_snapshot_signature is None:
            return True
        if signature == self._last_snapshot_signature:
            return False
        previous_inventory = _decimal(self._last_snapshot_signature[2])
        current_inventory = _decimal(signature[2])
        if abs(current_inventory - previous_inventory) >= MATERIAL_INVENTORY_DELTA:
            return True
        material_indexes = {0, 1, 3, 4, 5, 6, 8, 9}
        return any(signature[index] != self._last_snapshot_signature[index] for index in material_indexes)

    def _poll_once(self, run: dict) -> dict:
        before_fills = set((run.get("fills") or {}).keys())
        result = reconcile_exchange_truth(run, self.client, self.db if self.db.enabled else None, persist_order_updates=False)
        before_telemetry_counts = dict(self.account_telemetry.request_counts)
        telemetry = self.account_telemetry.get("ETHUSD")
        telemetry_refreshed = dict(self.account_telemetry.request_counts) != before_telemetry_counts
        lifecycle = DurableGridBotLifecycle(client=self.client, db=self.db, use_supabase=self.db.enabled)
        if int(result.get("position_mismatches") or 0):
            if self._position_mismatch_confirmed(run, result):
                lifecycle._safe_pause_for_external_position_change(lifecycle._load(), run, result, reason="worker_reconcile", previous_status=run.get("status"))
                replacement_result = {"created": 0, "deferred": 0, "skipped": len(run.get("fills") or {}), "items": [{"state": "skipped", "reason": "external_position_change"}], "metrics": {}}
                self._clear_pending_position_mismatch()
            else:
                replacement_result = {"created": 0, "deferred": 0, "skipped": len(run.get("fills") or {}), "items": [{"state": "skipped", "reason": "pending_position_mismatch"}], "metrics": {}}
        else:
            self._clear_pending_position_mismatch()
            replacement_result = lifecycle.process_replacements(run, result)
        new_fill_ids = [fill_id for fill_id in (run.get("fills") or {}) if fill_id not in before_fills]
        if new_fill_ids:
            self._set_state(last_fill={"fill_id": new_fill_ids[-1], "raw": (run.get("fills") or {}).get(new_fill_ids[-1])})
        if replacement_result.get("items"):
            self._set_state(last_replacement=replacement_result["items"][-1])
        signature = self._snapshot_signature(run, result, replacement_result, telemetry)
        should_snapshot = bool(new_fill_ids or replacement_result.get("created") or replacement_result.get("deferred"))
        should_snapshot = should_snapshot or self._snapshot_materially_changed(signature)
        if time.monotonic() - self._last_snapshot_monotonic >= self.snapshot_interval_seconds:
            should_snapshot = True
        if self.db.enabled and should_snapshot:
            risk = {
                "created_at": utc_now(),
                "position": result.get("delta_position"),
                "gridbot_inventory": result.get("gridbot_inventory"),
                "open_gridbot_orders": result.get("exchange_open_orders"),
                "position_mismatches": result.get("position_mismatches"),
                "fill_ledger_mismatches": result.get("fill_ledger_mismatches"),
                "reconciliation": result,
                "replacements": replacement_result,
                "account_risk_state": telemetry.as_dict(),
                "snapshot_reason": "periodic_or_material_change",
            }
            run.setdefault("risk_snapshots", []).append(risk)
            persisted = self.db.persist_snapshot(run, risk, run.get("summary"))
            self._last_snapshot_monotonic = time.monotonic()
            self._last_snapshot_signature = signature
            if persisted is not False:
                self._set_state(snapshot_writes=self._state["snapshot_writes"] + 1)
        self._set_state(
            account_risk_state=telemetry.as_dict(),
            accounting=build_run_accounting(run, mark_price=telemetry.mark_price, account_position_lots=telemetry.position_lots).as_dict(),
            account_telemetry_refresh_count=self._state["account_telemetry_refresh_count"] + (1 if telemetry_refreshed else 0),
            delta_account_telemetry_request_counts=dict(self.account_telemetry.request_counts),
            replacement_aggregation_metrics=replacement_result.get("metrics") or {},
        )
        with self._lock:
            self._run = run
        self._update_health(run, result)
        return result

    def _position_mismatch_signature(self, run: dict, reconciliation: dict) -> dict:
        return {
            "run_id": run.get("run_id"),
            "gridbot_inventory": str(reconciliation.get("gridbot_inventory")),
            "delta_position": str(reconciliation.get("delta_position")),
        }

    def _position_mismatch_confirmed(self, run: dict, reconciliation: dict) -> bool:
        now = time.monotonic()
        signature = self._position_mismatch_signature(run, reconciliation)
        seen_at = utc_now()
        with self._lock:
            pending = self._state.get("pending_position_mismatch")
            if not pending or pending.get("signature") != signature:
                self._state["pending_position_mismatch"] = {
                    "signature": signature,
                    "first_seen_monotonic": now,
                    "last_seen_monotonic": now,
                    "polls": 1,
                    "first_seen_at": seen_at,
                    "last_seen_at": seen_at,
                }
                return False
            pending = deepcopy(pending)
            pending["polls"] = int(pending.get("polls") or 0) + 1
            pending["last_seen_monotonic"] = now
            pending["last_seen_at"] = seen_at
            self._state["pending_position_mismatch"] = pending

        age_seconds = now - float(pending.get("first_seen_monotonic") or now)
        return (
            int(pending.get("polls") or 0) >= POSITION_MISMATCH_CONFIRMATION_POLLS
            and age_seconds >= POSITION_MISMATCH_CONFIRMATION_SECONDS
        )

    def _clear_pending_position_mismatch(self) -> None:
        with self._lock:
            self._state["pending_position_mismatch"] = None

    def _update_health(self, run: dict | None, reconciliation: dict | None) -> None:
        with self._lock:
            state = deepcopy(self._state)
        if run and run.get("status") == GridStatus.RUNNING.value:
            state["running"] = True
            state["thread_alive"] = True
            state["status"] = "running"
        cached = self.account_telemetry.snapshot() or account_telemetry_cache.snapshot()
        if cached:
            state["account_risk_state"] = cached
        if run:
            mark_price = _decimal((cached or {}).get("mark_price")) if cached and (cached or {}).get("mark_price") not in [None, ""] else None
            account_position = _decimal((cached or {}).get("position_lots")) if cached and (cached or {}).get("position_lots") not in [None, ""] else None
            state["accounting"] = build_run_accounting(run, mark_price=mark_price, account_position_lots=account_position).as_dict()
        health = evaluate_gridbot_health(state, run, reconciliation, state.get("accounting"))
        try:
            health = self._health_tracker.update(health, self.db if self.db.enabled else None)
        except Exception:
            logger.exception("Failed to persist GridBot health state.")
            health = self._health_tracker.update(health)
        self._set_state(health=health, recent_resolved_health_issues=health.get("recent_resolved_issues") or [])


worker = ContinuousGridBotWorker()


def start_continuous_gridbot_worker() -> dict:
    return worker.ensure_active_worker()


def stop_continuous_gridbot_worker() -> dict:
    return worker.stop()


def gridbot_live_state() -> dict:
    state = worker.state()
    try:
        if state.get("run_id"):
            telemetry = worker.account_telemetry.get("ETHUSD")
        else:
            telemetry = account_telemetry_cache.get("ETHUSD")
        state["account_risk_state"] = telemetry.as_dict()
    except Exception as exc:
        state["account_risk_state_error"] = str(exc)[:300]
    state["health"] = evaluate_gridbot_health(state)
    return state
