import os
import signal
import time
import traceback
from datetime import datetime, timezone

import futures_engine
from futures_storage import record_futures_engine_run


DEFAULT_INTERVAL_SECONDS = 60
_running = True


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _int_env(name, default):
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


def daemon_config():
    return max(15, _int_env("FUTURES_TRADING_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS))


def _handle_shutdown(signum, frame):
    global _running
    _running = False


def run_cycle(interval_seconds=None):
    interval_seconds = interval_seconds or daemon_config()
    started_at = _now_iso()

    try:
        evaluation = futures_engine.auto_trade_cycle(enabled=True, persist=True)
        record_futures_engine_run(
            "OK",
            cycle_started_at=started_at,
            interval_seconds=interval_seconds,
            evaluation=evaluation,
        )
        return evaluation
    except Exception as exc:
        error = "".join(traceback.format_exception_only(type(exc), exc)).strip()
        record_futures_engine_run(
            "ERROR",
            cycle_started_at=started_at,
            action="Futures trading daemon cycle failed.",
            error=error,
            interval_seconds=interval_seconds,
        )
        raise


def run_forever():
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)
    print("Futures trading daemon started.")

    while _running:
        interval_seconds = daemon_config()

        try:
            evaluation = run_cycle(interval_seconds)
            print(f"{_now_iso()} | {evaluation.get('action')}")
        except Exception as exc:
            print(f"{_now_iso()} | Futures trading daemon cycle failed: {exc}")

        slept = 0
        while _running and slept < interval_seconds:
            time.sleep(min(1, interval_seconds - slept))
            slept += 1

    print("Futures trading daemon stopped.")


if __name__ == "__main__":
    run_forever()
