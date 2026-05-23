import logging

import paper_trading
import paper_trading_daemon

from .json_utils import to_jsonable


logger = logging.getLogger(__name__)


def status():
    return {
        "ok": True,
        "mode": "fastapi-backend",
        "dashboard": to_jsonable(paper_trading.paper_trading_dashboard_data()),
        "engine_status": to_jsonable(paper_trading.get_latest_paper_engine_run()),
    }


def run_cycle():
    interval_seconds, limit_expiries = paper_trading_daemon.daemon_config()
    evaluation = paper_trading_daemon.run_cycle(interval_seconds, limit_expiries)
    return {
        "ok": True,
        "mode": "fastapi-backend",
        "evaluation": to_jsonable(evaluation),
    }

