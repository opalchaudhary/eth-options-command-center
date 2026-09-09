import threading

from grid_bot.lifecycle_qualification import run_with_authoritative_lifecycle_wait


def test_pause_harness_returns_when_backend_reaches_paused_even_if_caller_waits():
    caller_release = threading.Event()
    calls = {"count": 0}

    def call():
        caller_release.wait(timeout=10)
        return {"ok": True, "run": {"run_id": "run-pause", "status": "PAUSED"}}

    def load_run_state(run_id):
        calls["count"] += 1
        return {"run_id": run_id, "status": "PAUSING" if calls["count"] == 1 else "PAUSED"}

    result = run_with_authoritative_lifecycle_wait(
        call,
        run_id="run-pause",
        operation="PAUSE",
        load_run_state=load_run_state,
        timeout_seconds=1,
        poll_seconds=0.01,
    )

    assert result["ok"] is True
    assert result["status"] == "PAUSED"
    assert result["authoritative"] is True
    assert result["caller_completed"] is False
    caller_release.set()


def test_edit_harness_returns_when_backend_reaches_running_with_complete_edit():
    caller_release = threading.Event()
    calls = {"count": 0}

    def call():
        caller_release.wait(timeout=10)
        return {"ok": True, "run": {"run_id": "run-edit", "status": "RUNNING"}}

    def load_run_state(run_id):
        calls["count"] += 1
        if calls["count"] == 1:
            return {"run_id": run_id, "status": "EDITING", "edit_state": {"stage": "PLACING_ORDERS"}}
        return {
            "run_id": run_id,
            "status": "RUNNING",
            "edit_state": {"stage": "COMPLETE"},
            "lifecycle_progress": {"stage": "COMPLETE"},
        }

    result = run_with_authoritative_lifecycle_wait(
        call,
        run_id="run-edit",
        operation="EDIT",
        load_run_state=load_run_state,
        timeout_seconds=1,
        poll_seconds=0.01,
    )

    assert result["ok"] is True
    assert result["status"] == "RUNNING"
    assert result["caller_completed"] is False
    caller_release.set()


def test_edit_harness_does_not_accept_initial_running_before_edit_progress():
    caller_release = threading.Event()
    calls = {"count": 0}

    def call():
        caller_release.wait(timeout=10)
        return {"ok": True, "run": {"run_id": "run-edit", "status": "RUNNING"}}

    def load_run_state(run_id):
        calls["count"] += 1
        if calls["count"] <= 2:
            return {"run_id": run_id, "status": "RUNNING", "config": {"config_version": 1}, "updated_at": "t0"}
        if calls["count"] == 3:
            return {"run_id": run_id, "status": "EDITING", "config": {"config_version": 1}, "updated_at": "t1"}
        return {
            "run_id": run_id,
            "status": "RUNNING",
            "config": {"config_version": 2},
            "updated_at": "t2",
            "edit_state": {"stage": "COMPLETE"},
            "lifecycle_progress": {"stage": "COMPLETE"},
        }

    result = run_with_authoritative_lifecycle_wait(
        call,
        run_id="run-edit",
        operation="EDIT",
        load_run_state=load_run_state,
        timeout_seconds=1,
        poll_seconds=0.01,
    )

    assert calls["count"] >= 4
    assert result["ok"] is True
    assert result["status"] == "RUNNING"
    assert result["run"]["config"]["config_version"] == 2
    assert result["caller_completed"] is False
    caller_release.set()


def test_resume_harness_accepts_running_and_paused_safe_fallback():
    running_calls = {"count": 0}
    paused_calls = {"count": 0}

    def running_state(run_id):
        running_calls["count"] += 1
        return {"run_id": run_id, "status": "RESUMING" if running_calls["count"] == 1 else "RUNNING"}

    def paused_state(run_id):
        paused_calls["count"] += 1
        if paused_calls["count"] == 1:
            return {"run_id": run_id, "status": "RESUMING"}
        return {"run_id": run_id, "status": "PAUSED", "resume_diagnostics": {"reason": "safe_fallback"}}

    running = run_with_authoritative_lifecycle_wait(
        lambda: threading.Event().wait(timeout=10),
        run_id="run-resume-running",
        operation="RESUME",
        load_run_state=running_state,
        timeout_seconds=1,
        poll_seconds=0.01,
    )
    paused = run_with_authoritative_lifecycle_wait(
        lambda: threading.Event().wait(timeout=10),
        run_id="run-resume-paused",
        operation="RESUME",
        load_run_state=paused_state,
        timeout_seconds=1,
        poll_seconds=0.01,
    )

    assert running["ok"] is True
    assert running["status"] == "RUNNING"
    assert paused["ok"] is True
    assert paused["status"] == "PAUSED"


def test_stop_harness_returns_when_backend_reaches_stopped_even_if_caller_waits():
    calls = {"count": 0}

    def load_run_state(run_id):
        calls["count"] += 1
        if calls["count"] == 1:
            return {"run_id": run_id, "status": "STOPPING"}
        return {"run_id": run_id, "status": "STOPPED", "summary": {"ok": True}}

    result = run_with_authoritative_lifecycle_wait(
        lambda: threading.Event().wait(timeout=10),
        run_id="run-stop",
        operation="STOP",
        load_run_state=load_run_state,
        timeout_seconds=1,
        poll_seconds=0.01,
    )

    assert result["ok"] is True
    assert result["status"] == "STOPPED"
    assert result["caller_completed"] is False
