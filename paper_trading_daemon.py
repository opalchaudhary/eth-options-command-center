import os
import signal
import time
import traceback
from datetime import datetime, timezone

import paper_trading


DEFAULT_INTERVAL_SECONDS = 60
DEFAULT_LIMIT_EXPIRIES = 6
_running = True


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _int_env(name, default):
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


def daemon_config():
    interval = max(15, _int_env("PAPER_TRADING_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS))
    limit_expiries = max(1, _int_env("PAPER_TRADING_LIMIT_EXPIRIES", DEFAULT_LIMIT_EXPIRIES))
    return interval, limit_expiries


def _handle_shutdown(signum, frame):
    global _running
    _running = False


def run_cycle(interval_seconds=None, limit_expiries=None):
    interval_seconds = interval_seconds or daemon_config()[0]
    limit_expiries = limit_expiries or daemon_config()[1]
    started_at = _now_iso()

    try:
        evaluation = paper_trading.auto_trade_cycle(
            enabled=True,
            limit_expiries=limit_expiries,
            persist=True,
        )
        paper_trading.record_paper_engine_run(
            "OK",
            cycle_started_at=started_at,
            interval_seconds=interval_seconds,
            limit_expiries=limit_expiries,
            evaluation=evaluation,
        )
        return evaluation
    except Exception as exc:
        error = "".join(traceback.format_exception_only(type(exc), exc)).strip()
        paper_trading.record_paper_engine_run(
            "ERROR",
            cycle_started_at=started_at,
            action="Paper trading daemon cycle failed.",
            error=error,
            interval_seconds=interval_seconds,
            limit_expiries=limit_expiries,
        )
        raise


def run_forever():
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)
    print("Paper trading daemon started.")

    while _running:
        interval_seconds, limit_expiries = daemon_config()

        try:
            evaluation = run_cycle(interval_seconds, limit_expiries)
            print(f"{_now_iso()} | {evaluation.get('action')}")
        except Exception as exc:
            print(f"{_now_iso()} | Paper trading daemon cycle failed: {exc}")

        slept = 0
        while _running and slept < interval_seconds:
            time.sleep(min(1, interval_seconds - slept))
            slept += 1

    print("Paper trading daemon stopped.")


if __name__ == "__main__":
    run_forever()
