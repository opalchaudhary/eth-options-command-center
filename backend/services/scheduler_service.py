import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from copy import deepcopy
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.background import BackgroundScheduler

from backend import config
from backend.services import market_data_service
from data_refresh import cleanup_retained_snapshots, refresh_smc_sources, refresh_volume_profile_sources
from probability_engine.config import get_probability_config
from probability_engine.jobs.evaluation_job import run_probability_performance_job
from probability_engine.jobs.outcome_job import run_probability_outcome_job
from probability_engine.jobs.prediction_job import run_probability_prediction_job
from probability_engine.jobs.strike_job import run_probability_strike_scan_job
from probability_engine.jobs.v2_shadow_job import run_probability_v2_shadow_job
from backend.services.rich_orderflow_ws_service import orderflow_ws_status
from rich_data.jobs import (
    run_rich_derivatives_job,
    run_rich_options_surface_job,
    run_rich_orderbook_job,
    run_rich_orderflow_job,
)


logger = logging.getLogger(__name__)

BUSY_REFRESH_ERROR = "Backend is refreshing data. Please retry shortly."

_scheduler = None
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="backend-cycle")
_state_lock = threading.Lock()
_job_locks = {
    "market_refresh": threading.Lock(),
    "option_chain_refresh": threading.Lock(),
    "smc_refresh": threading.Lock(),
    "volume_profile_refresh": threading.Lock(),
    "retention_cleanup": threading.Lock(),
    "probability_prediction_v1": threading.Lock(),
    "probability_strike_scan_v1": threading.Lock(),
    "probability_outcome_evaluator_v1": threading.Lock(),
    "probability_performance_daily_v1": threading.Lock(),
    "probability_v2_shadow_prediction_v1": threading.Lock(),
    "rich_derivatives_v1": threading.Lock(),
    "rich_orderflow_v1": threading.Lock(),
    "rich_orderbook_v1": threading.Lock(),
    "rich_options_surface_v1": threading.Lock(),
}


def _initial_job_state():
    return {
        "status": "not_started",
        "last_started_at": None,
        "last_finished_at": None,
        "last_success_at": None,
        "last_duration_seconds": None,
        "last_error": None,
        "last_result": None,
        "run_count": 0,
        "skipped_count": 0,
        "timeout_count": 0,
    }


_job_state = {job_name: _initial_job_state() for job_name in _job_locks}


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _summarize_result(result):
    if not isinstance(result, dict):
        return result

    summary = {
        key: result.get(key)
        for key in [
            "ok",
            "mode",
            "action",
            "row_count",
            "expiry_count",
            "candidate_count",
            "mature_count",
            "attempted_count",
            "created_count",
            "skipped_existing_count",
            "skipped_incomplete_count",
            "failed_count",
            "batch_limit",
            "candidate_pages_scanned",
        ]
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
        finished_at = _now_iso()
        _increment_state(job_name, "run_count")
        _set_state(
            job_name,
            status="ok",
            last_finished_at=finished_at,
            last_success_at=finished_at,
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
    market_sources = market_data_service.refresh_market_sources(include_smc=False)

    return {
        "ok": bool(market_sources.get("ohlcv_saved")) or bool(market_sources.get("orderbook_saved")),
        "refresh": {
            "market_sources": market_sources,
        },
    }


def _option_chain_refresh_cycle():
    options = market_data_service.refresh_options()

    return {
        "ok": bool(options.get("ok")),
        "row_count": options.get("row_count"),
        "expiry_count": options.get("expiry_count"),
        "refresh": {
            "options": options,
        },
    }


def _add_interval_job(job_name, callback, seconds, start_immediately=False, start_delay_seconds=None):
    job_kwargs = {}

    if start_immediately:
        job_kwargs["next_run_time"] = datetime.now(timezone.utc)
    elif start_delay_seconds is not None:
        job_kwargs["next_run_time"] = datetime.now(timezone.utc) + timedelta(seconds=start_delay_seconds)

    _scheduler.add_job(
        lambda: _schedule_job(job_name, callback),
        "interval",
        seconds=seconds,
        id=job_name,
        max_instances=1,
        coalesce=True,
        **job_kwargs,
    )


def start_scheduler():
    global _scheduler

    if not config.BACKEND_SCHEDULER_ENABLED:
        logger.info("Backend scheduler is disabled by config.")
        return None

    if _scheduler and _scheduler.running:
        logger.info("Backend scheduler is already running.")
        return _scheduler

    _scheduler = BackgroundScheduler(timezone="UTC")
    _add_interval_job("market_refresh", _market_refresh_cycle, config.MARKET_REFRESH_INTERVAL_SECONDS, True)
    _add_interval_job(
        "option_chain_refresh",
        _option_chain_refresh_cycle,
        config.OPTION_CHAIN_REFRESH_INTERVAL_SECONDS,
        True,
    )
    _add_interval_job("smc_refresh", refresh_smc_sources, config.SMC_REFRESH_INTERVAL_SECONDS)
    _add_interval_job(
        "volume_profile_refresh",
        refresh_volume_profile_sources,
        config.VOLUME_PROFILE_REFRESH_INTERVAL_SECONDS,
        start_delay_seconds=max(60, config.VOLUME_PROFILE_REFRESH_INTERVAL_SECONDS // 2),
    )
    _add_interval_job("retention_cleanup", cleanup_retained_snapshots, config.RETENTION_CLEANUP_INTERVAL_SECONDS)
    probability_config = get_probability_config()
    if probability_config.enabled:
        _add_interval_job(
            "probability_prediction_v1",
            run_probability_prediction_job,
            probability_config.prediction_interval_seconds,
        )
        _add_interval_job(
            "probability_strike_scan_v1",
            run_probability_strike_scan_job,
            probability_config.strike_scan_interval_seconds,
        )
        _add_interval_job(
            "probability_outcome_evaluator_v1",
            run_probability_outcome_job,
            probability_config.outcome_interval_seconds,
        )
        _add_interval_job(
            "probability_performance_daily_v1",
            run_probability_performance_job,
            probability_config.performance_interval_seconds,
        )
        logger.info("Probability Engine scheduler jobs registered.")
    else:
        logger.info("Probability Engine scheduler jobs are disabled by config.")
    if probability_config.v2_shadow_enabled:
        _add_interval_job(
            "probability_v2_shadow_prediction_v1",
            run_probability_v2_shadow_job,
            probability_config.v2_shadow_interval_seconds,
        )
        logger.info("Probability V2 shadow scheduler job registered.")
    else:
        logger.info("Probability V2 shadow scheduler job is disabled by config.")
    if config.RICH_DATA_COLLECTION_ENABLED:
        _add_interval_job(
            "rich_derivatives_v1",
            run_rich_derivatives_job,
            config.RICH_DERIVATIVES_INTERVAL_SECONDS,
            start_delay_seconds=30,
        )
        if config.RICH_ORDERFLOW_REST_ENABLED:
            _add_interval_job(
                "rich_orderflow_v1",
                run_rich_orderflow_job,
                config.RICH_ORDERFLOW_INTERVAL_SECONDS,
                start_delay_seconds=45,
            )
        else:
            logger.info("REST rich orderflow scheduler job is disabled by config.")
        _add_interval_job(
            "rich_orderbook_v1",
            run_rich_orderbook_job,
            config.RICH_ORDERBOOK_INTERVAL_SECONDS,
            start_delay_seconds=50,
        )
        if config.RICH_OPTIONS_SURFACE_ENABLED:
            _add_interval_job(
                "rich_options_surface_v1",
                run_rich_options_surface_job,
                config.RICH_OPTIONS_SURFACE_INTERVAL_SECONDS,
                start_delay_seconds=60,
            )
        else:
            logger.info("Rich options surface scheduler job is disabled by config.")
        logger.info("Rich data scheduler jobs registered.")
    else:
        logger.info("Rich data scheduler jobs are disabled by config.")
    _scheduler.start()
    logger.info("Backend scheduler started with production interval configuration.")
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

    running_jobs = [
        job_name
        for job_name, state in job_state.items()
        if state.get("status") == "running" or _job_locks[job_name].locked()
    ]
    skipped_cycles = {
        job_name: state.get("skipped_count", 0)
        for job_name, state in job_state.items()
    }

    return {
        "ok": True,
        "scheduler_enabled": config.BACKEND_SCHEDULER_ENABLED,
        "scheduler_running": bool(_scheduler and _scheduler.running),
        "job_timeout_seconds": config.BACKEND_JOB_TIMEOUT_SECONDS,
        "last_successful_market_refresh": job_state["market_refresh"].get("last_success_at"),
        "last_option_chain_refresh": job_state["option_chain_refresh"].get("last_success_at"),
        "skipped_cycles": skipped_cycles,
        "running_jobs": running_jobs,
        "jobs": jobs,
        "job_state": job_state,
        "rich_orderflow_ws": orderflow_ws_status(),
    }


def data_refresh_jobs_running():
    with _state_lock:
        return [
            job_name
            for job_name in [
                "market_refresh",
                "option_chain_refresh",
                "smc_refresh",
                "volume_profile_refresh",
                "probability_prediction_v1",
                "probability_strike_scan_v1",
                "probability_outcome_evaluator_v1",
                "probability_performance_daily_v1",
                "probability_v2_shadow_prediction_v1",
                "rich_derivatives_v1",
                "rich_orderflow_v1",
                "rich_orderbook_v1",
                "rich_options_surface_v1",
            ]
            if _job_state.get(job_name, {}).get("status") == "running" or _job_locks[job_name].locked()
        ]
