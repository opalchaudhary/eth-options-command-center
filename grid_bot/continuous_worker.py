from __future__ import annotations

import logging
import os
import threading
import time
from copy import deepcopy
from decimal import Decimal
from typing import Any

from .delta_testnet_client import DeltaTestnetClient
from .durable_lifecycle import DurableGridBotLifecycle
from .exchange_truth import reconcile_exchange_truth
from .models import GridStatus, utc_now
from .supabase_repository import SupabaseGridRepository


logger = logging.getLogger(__name__)

EXECUTABLE_STATUSES = {GridStatus.RUNNING.value}
POLL_INTERVAL_SECONDS = float(os.getenv("GRIDBOT_V01_WORKER_POLL_SECONDS", "2"))
SNAPSHOT_INTERVAL_SECONDS = float(os.getenv("GRIDBOT_V01_WORKER_SNAPSHOT_SECONDS", "300"))
ACTIVE_RUN_REFRESH_SECONDS = float(os.getenv("GRIDBOT_V01_WORKER_ACTIVE_REFRESH_SECONDS", "10"))


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
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._run: dict | None = None
        self._last_snapshot_monotonic = 0.0
        self._last_active_refresh_monotonic = 0.0
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
            "fill_ledger_mismatches": 0,
            "snapshot_writes": 0,
            "supabase_write_policy": {
                "fills_orders_replacements": "event-driven immediate",
                "health_anomalies": "event-driven immediate",
                "snapshots": f"approximately every {int(self.snapshot_interval_seconds)} seconds while running",
                "idle_heartbeat_rows": "none",
            },
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
                        "config_version": (run.get("config") or {}).get("config_version"),
                        "grid_nature": (run.get("config") or {}).get("grid_type"),
                        "grid_levels": run.get("levels") or [],
                        "known_gridbot_orders": list((run.get("orders") or {}).values()),
                        "known_fill_ids": list((run.get("fills") or {}).keys()),
                        "replacement_state": run.get("replacement_keys") or {},
                    }
                )
            state["thread_alive"] = bool(self._thread and self._thread.is_alive())
            return state

    def ensure_active_worker(self) -> dict:
        if self.db.enabled:
            self._recover_active_run()
        return self.start()

    def _set_state(self, **updates: Any) -> None:
        with self._lock:
            self._state.update(updates)

    def _recover_active_run(self) -> dict | None:
        active = self.db.active_run() if self.db.enabled else None
        if not active:
            with self._lock:
                self._run = None
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
        return self._recover_active_run()

    def _loop(self) -> None:
        logger.info("DeltaGridBot V0.1 continuous worker started.")
        self._set_state(running=True, status="running", last_error=None)
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                run = self._refresh_active_run_if_due()
                if not run or run.get("status") not in EXECUTABLE_STATUSES:
                    self._set_state(status="idle", run_id=None)
                    self._stop.wait(self.poll_interval_seconds)
                    continue

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
            elapsed = time.monotonic() - started
            self._stop.wait(max(0.0, self.poll_interval_seconds - elapsed))
        self._set_state(running=False, thread_alive=False, status="stopped")
        logger.info("DeltaGridBot V0.1 continuous worker stopped.")

    def _poll_once(self, run: dict) -> dict:
        before_fills = set((run.get("fills") or {}).keys())
        result = reconcile_exchange_truth(run, self.client, self.db if self.db.enabled else None, persist_order_updates=False)
        replacement_result = DurableGridBotLifecycle(client=self.client, db=self.db, use_supabase=self.db.enabled).process_replacements(run, result)
        new_fill_ids = [fill_id for fill_id in (run.get("fills") or {}) if fill_id not in before_fills]
        if new_fill_ids:
            self._set_state(last_fill={"fill_id": new_fill_ids[-1], "raw": (run.get("fills") or {}).get(new_fill_ids[-1])})
        if replacement_result.get("items"):
            self._set_state(last_replacement=replacement_result["items"][-1])
        should_snapshot = bool(new_fill_ids or replacement_result.get("created") or replacement_result.get("deferred") or result.get("events"))
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
            }
            run.setdefault("risk_snapshots", []).append(risk)
            self.db.persist_snapshot(run, risk, run.get("summary"))
            self._last_snapshot_monotonic = time.monotonic()
            self._set_state(snapshot_writes=self._state["snapshot_writes"] + 1)
        with self._lock:
            self._run = run
        return result


worker = ContinuousGridBotWorker()


def start_continuous_gridbot_worker() -> dict:
    return worker.ensure_active_worker()


def stop_continuous_gridbot_worker() -> dict:
    return worker.stop()


def gridbot_live_state() -> dict:
    return worker.state()
