import logging

import futures_engine
import futures_trading_daemon

from .json_utils import to_jsonable


logger = logging.getLogger(__name__)


def status():
    dashboard = futures_engine.futures_dashboard_data(run_cycle=False)
    return {
        "ok": True,
        "mode": "fastapi-backend",
        "dashboard": to_jsonable(dashboard),
        "engine_status": to_jsonable(dashboard.get("engine_status")),
    }


def run_cycle():
    interval_seconds = futures_trading_daemon.daemon_config()
    evaluation = futures_trading_daemon.run_cycle(interval_seconds)
    return {
        "ok": True,
        "mode": "fastapi-backend",
        "evaluation": to_jsonable(evaluation),
    }

