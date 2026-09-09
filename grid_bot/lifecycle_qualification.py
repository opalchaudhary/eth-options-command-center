from __future__ import annotations

from dataclasses import dataclass
from threading import Thread
import time
from typing import Any, Callable


LoadRunState = Callable[[str], dict]
LifecycleCall = Callable[[], dict]


AUTHORITATIVE_TERMINAL_STATUSES = {
    "EDIT": {"RUNNING"},
    "PAUSE": {"PAUSED"},
    "RESUME": {"RUNNING", "PAUSED"},
    "STOP": {"STOPPED"},
}

TRANSITIONAL_STATUSES = {
    "EDIT": {"EDITING"},
    "PAUSE": {"PAUSING"},
    "RESUME": {"RESUMING", "PAUSING"},
    "STOP": {"STOPPING", "STOP_REQUIRES_ATTENTION"},
}


@dataclass(frozen=True)
class LifecycleCompletion:
    ok: bool
    operation: str
    run_id: str
    status: str | None
    authoritative: bool
    caller_completed: bool
    caller_error: str | None
    elapsed_seconds: float
    run: dict | None
    result: dict | None
    reason: str | None = None

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "operation": self.operation,
            "run_id": self.run_id,
            "status": self.status,
            "authoritative": self.authoritative,
            "caller_completed": self.caller_completed,
            "caller_error": self.caller_error,
            "elapsed_seconds": self.elapsed_seconds,
            "run": self.run,
            "result": self.result,
            "reason": self.reason,
        }


def authoritative_lifecycle_complete(operation: str, run: dict | None) -> bool:
    if not run:
        return False
    op = operation.upper()
    status = str(run.get("status") or "").upper()
    if status not in AUTHORITATIVE_TERMINAL_STATUSES.get(op, set()):
        return False
    if op == "EDIT":
        progress = run.get("lifecycle_progress") or {}
        edit_state = run.get("edit_state") or {}
        stage = str(progress.get("stage") or edit_state.get("stage") or status).upper()
        if edit_state and stage not in {"COMPLETE", "RUNNING"}:
            return False
    if op == "STOP" and status == "STOPPED":
        return True
    return True


def run_with_authoritative_lifecycle_wait(
    call: LifecycleCall,
    *,
    run_id: str,
    operation: str,
    load_run_state: LoadRunState,
    timeout_seconds: float = 300,
    poll_seconds: float = 2,
) -> dict:
    """Run a lifecycle caller but return on durable backend completion.

    Live qualification callers must not treat a still-open HTTP/SSH/Python call
    as authoritative. This helper starts the caller on a daemon thread and polls
    the persisted run state until the operation reaches its durable completion
    condition, returning even if the initiating caller has not unwound yet.
    """

    started = time.monotonic()
    box: dict[str, Any] = {"completed": False, "result": None, "error": None}

    def target() -> None:
        try:
            box["result"] = call()
        except Exception as exc:  # pragma: no cover - exercised by callers
            box["error"] = exc
        finally:
            box["completed"] = True

    op = operation.upper()
    initial_run: dict | None = None
    try:
        initial_run = load_run_state(run_id)
    except Exception:
        initial_run = None
    initial_status = str((initial_run or {}).get("status") or "").upper()
    initial_config_version = ((initial_run or {}).get("config") or {}).get("config_version")
    initial_updated_at = (initial_run or {}).get("updated_at")
    observed_transition = initial_status in TRANSITIONAL_STATUSES.get(op, set())

    thread = Thread(target=target, name=f"gridbot-{operation.lower()}-qualification", daemon=True)
    thread.start()
    last_run: dict | None = None
    last_status: str | None = None
    deadline = started + timeout_seconds
    while time.monotonic() < deadline:
        try:
            last_run = load_run_state(run_id)
            last_status = str(last_run.get("status") or "") if last_run else None
        except Exception as exc:
            last_run = {"status": None, "load_error": str(exc)[:500]}
            last_status = None
        status_upper = str(last_status or "").upper()
        observed_transition = observed_transition or status_upper in TRANSITIONAL_STATUSES.get(op, set())
        config_version = ((last_run or {}).get("config") or {}).get("config_version")
        updated_at = (last_run or {}).get("updated_at")
        changed_since_initial = (
            status_upper != initial_status
            or config_version != initial_config_version
            or (updated_at is not None and updated_at != initial_updated_at)
        )
        terminal_is_authoritative = authoritative_lifecycle_complete(operation, last_run)
        terminal_is_not_stale_initial = (
            not authoritative_lifecycle_complete(operation, initial_run)
            or observed_transition
            or changed_since_initial
            or bool(box["completed"])
        )
        if terminal_is_authoritative and terminal_is_not_stale_initial:
            return LifecycleCompletion(
                ok=True,
                operation=op,
                run_id=run_id,
                status=last_status,
                authoritative=True,
                caller_completed=bool(box["completed"]),
                caller_error=str(box["error"])[:500] if box["error"] else None,
                elapsed_seconds=round(time.monotonic() - started, 3),
                run=last_run,
                result=box["result"],
            ).as_dict()
        if box["completed"]:
            if box["error"]:
                return LifecycleCompletion(
                    ok=False,
                    operation=operation.upper(),
                    run_id=run_id,
                    status=last_status,
                    authoritative=False,
                    caller_completed=True,
                    caller_error=str(box["error"])[:500],
                    elapsed_seconds=round(time.monotonic() - started, 3),
                    run=last_run,
                    result=None,
                    reason="caller_error_before_authoritative_completion",
                ).as_dict()
            result_run = (box["result"] or {}).get("run") if isinstance(box["result"], dict) else None
            if authoritative_lifecycle_complete(operation, result_run):
                return LifecycleCompletion(
                    ok=True,
                    operation=operation.upper(),
                    run_id=run_id,
                    status=str(result_run.get("status") or "") if result_run else last_status,
                    authoritative=True,
                    caller_completed=True,
                    caller_error=None,
                    elapsed_seconds=round(time.monotonic() - started, 3),
                    run=result_run,
                    result=box["result"],
                ).as_dict()
        time.sleep(max(0.05, poll_seconds))
    return LifecycleCompletion(
        ok=False,
        operation=operation.upper(),
        run_id=run_id,
        status=last_status,
        authoritative=False,
        caller_completed=bool(box["completed"]),
        caller_error=str(box["error"])[:500] if box["error"] else None,
        elapsed_seconds=round(time.monotonic() - started, 3),
        run=last_run,
        result=box["result"],
        reason="timeout_waiting_for_authoritative_completion",
    ).as_dict()
