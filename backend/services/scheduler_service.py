import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from copy import deepcopy
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler

from backend import config
from backend.services import futures_trading_service, market_data_service, paper_trading_service


logger = logging.getLogger(__name__)

MARKET_REFRESH_INTERVAL_SECONDS = 60
PAPER_TRADING_INTERVAL_SECONDS = 60
FUTURES_SIMULATION_INTERVAL_SECONDS = 60

_scheduler = None
_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="backend-cycle")
_state_lock = threading.Lock()
_job_locks = {
    "market_refresh": threading.Lock(),
    "paper_trading": threading.Lock(),
    "futures_simulation": threading.Lock(),
}
_job_state = {
    "market_refresh": {
        "status": "not_started",
        "last_started_at": None,
        "last_finished_at": None,
        "last_duration_seconds": None,
        "last_error": None,
        "last_result": None,
        "run_count": 0,
        "skipped_count": 0,
        "timeout_count": 0,
    },
    "paper_trading": {
        "status": "not_started",
        "last_started_at": None,
        "last_finished_at": None,
        "last_duration_seconds": None,
        "last_error": None,
        "last_result": None,
        "run_count": 0,
        "skipped_count": 0,
        "timeout_count": 0,
    },
    "futures_simulation": {
        "status": "not_started",
        "last_started_at": None,
        "last_finished_at": None,
        "last_duration_seconds": None,
        "last_error": None,
        "last_result": None,
        "run_count": 0,
        "skipped_count": 0,
        "timeout_count": 0,
    },
}


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _summarize_result(result):
    if not isinstance(result, dict):
        return result

    summary = {
        key: result.get(key)
        for key in ["ok", "mode", "action", "row_count", "expiry_count"]
        if key in result
    }

    evaluation = result.get("evaluation")
    if isinstance(evaluation, dict):
        summary["action"] = evaluation.get("action") or summary.get("action")
        decision = evaluation.get("decision")
        if isinstance(decision, dict):
            summary["decision"] = {
                "symbol": decision.get("symbol"),
                "direction": decision.get("direction"),
                "confidence_score": decision.get("confidence_score"),
                "reason": decision.get("reason"),
            }

    refresh = result.get("refresh")
    if isinstance(refresh, dict):
        summary["refresh"] = {
            key: refresh.get(key)
            for key in ["options", "market_sources"]
            if key in refresh
        }

    return summary or {"type": type(result).__name__}


def _set_state(job_name, **updates):
    with _state_lock:
        _job_state[job_name].update(updates)


def _increment_state(job_name, key):
    with _state_lock:
        _job_state[job_name][key] += 1


def _run_with_lock(job_name, callback):
    lock = _job_locks[job_name]

    if not lock.acquire(blocking=False):
        _increment_state(job_name, "skipped_count")
        _set_state(
            job_name,
            status="skipped",
            last_error="Previous cycle is still running.",
        )
        logger.warning("%s skipped because the previous cycle is still running.", job_name)
        return

    started_at = _now_iso()
    started = time.monotonic()
    _set_state(
        job_name,
        status="running",
        last_started_at=started_at,
        last_error=None,
    )
    logger.info("%s cycle started.", job_name)

    try:
        result = callback()
        duration = round(time.monotonic() - started, 3)
        _increment_state(job_name, "run_count")
        _set_state(
            job_name,
            status="ok",
            last_finished_at=_now_iso(),
            last_duration_seconds=duration,
            last_error=None,
            last_result=_summarize_result(result),
        )
        logger.info("%s cycle finished in %ss.", job_name, duration)
    except Exception as exc:
        duration = round(time.monotonic() - started, 3)
        _set_state(
            job_name,
            status="error",
            last_finished_at=_now_iso(),
            last_duration_seconds=duration,
            last_error=str(exc),
        )
        logger.exception("%s cycle failed.", job_name)
    finally:
        lock.release()


def _schedule_job(job_name, callback):
    future = _executor.submit(_run_with_lock, job_name, callback)

    try:
        future.result(timeout=config.BACKEND_JOB_TIMEOUT_SECONDS)
    except TimeoutError:
        _increment_state(job_name, "timeout_count")
        _set_state(
            job_name,
            status="timeout",
            last_error=f"Cycle exceeded {config.BACKEND_JOB_TIMEOUT_SECONDS}s timeout.",
        )
        logger.error("%s cycle exceeded %ss timeout.", job_name, config.BACKEND_JOB_TIMEOUT_SECONDS)


def _market_refresh_cycle():
    options = market_data_service.refresh_options()
    market_sources = market_data_service.refresh_market_sources()

    return {
        "ok": bool(options.get("ok")) and bool(market_sources.get("ohlcv_saved")),
        "row_count": options.get("row_count"),
        "expiry_count": options.get("expiry_count"),
        "refresh": {
            "options": options,
            "market_sources": market_sources,
        },
    }


def start_scheduler():
    global _scheduler

    if not config.BACKEND_SCHEDULER_ENABLED:
        logger.info("Backend scheduler is disabled by config.")
        return None

    if _scheduler and _scheduler.running:
        logger.info("Backend scheduler is already running.")
        return _scheduler

    _scheduler = BackgroundScheduler(timezone="UTC")
    _scheduler.add_job(
        lambda: _schedule_job("market_refresh", _market_refresh_cycle),
        "interval",
        seconds=MARKET_REFRESH_INTERVAL_SECONDS,
        id="market_refresh",
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc),
    )
    _scheduler.add_job(
        lambda: _schedule_job("paper_trading", paper_trading_service.run_cycle),
        "interval",
        seconds=PAPER_TRADING_INTERVAL_SECONDS,
        id="paper_trading",
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc),
    )
    _scheduler.add_job(
        lambda: _schedule_job("futures_simulation", futures_trading_service.run_cycle),
        "interval",
        seconds=FUTURES_SIMULATION_INTERVAL_SECONDS,
        id="futures_simulation",
        max_instances=1,
        coalesce=True,
        next_run_time=datetime.now(timezone.utc),
    )
    _scheduler.start()
    logger.info("Backend scheduler started with 60s market, paper, and futures jobs.")
    return _scheduler


def stop_scheduler():
    global _scheduler

    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Backend scheduler stopped.")


def scheduler_status():
    jobs = []

    if _scheduler:
        for job in _scheduler.get_jobs():
            jobs.append(
                {
                    "id": job.id,
                    "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                    "trigger": str(job.trigger),
                }
            )

    with _state_lock:
        job_state = deepcopy(_job_state)

    return {
        "ok": True,
        "scheduler_enabled": config.BACKEND_SCHEDULER_ENABLED,
        "scheduler_running": bool(_scheduler and _scheduler.running),
        "job_timeout_seconds": config.BACKEND_JOB_TIMEOUT_SECONDS,
        "jobs": jobs,
        "job_state": job_state,
    }
