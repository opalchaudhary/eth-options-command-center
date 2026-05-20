import os
import signal
import time
import traceback
from datetime import datetime, timezone

import alt_futures_engine
from alt_futures_journal import record_engine_run


DEFAULT_INTERVAL_SECONDS = 90
_running = True


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _int_env(name, default):
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


def daemon_config():
    return max(30, _int_env("ALT_FUTURES_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS))


def _handle_shutdown(signum, frame):
    global _running
    _running = False


def run_cycle(interval_seconds=None):
    interval_seconds = interval_seconds or daemon_config()
    started_at = _now_iso()

    try:
        evaluation = alt_futures_engine.auto_trade_cycle(enabled=True, persist=True)
        record_engine_run(
            "OK",
            cycle_started_at=started_at,
            interval_seconds=interval_seconds,
            evaluation=evaluation,
        )
        return evaluation
    except Exception as exc:
        error = "".join(traceback.format_exception_only(type(exc), exc)).strip()
        record_engine_run(
            "ERROR",
            cycle_started_at=started_at,
            action="Alt futures scanner cycle failed.",
            error=error,
            interval_seconds=interval_seconds,
        )
        raise


def run_forever():
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)
    print("Alt futures scanner daemon started.")

    while _running:
        interval_seconds = daemon_config()

        try:
            evaluation = run_cycle(interval_seconds)
            print(f"{_now_iso()} | {evaluation.get('action')}")
        except Exception as exc:
            print(f"{_now_iso()} | Alt futures scanner cycle failed: {exc}")

        slept = 0
        while _running and slept < interval_seconds:
            time.sleep(min(1, interval_seconds - slept))
            slept += 1

    print("Alt futures scanner daemon stopped.")


if __name__ == "__main__":
    run_forever()
