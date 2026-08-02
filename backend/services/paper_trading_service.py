import logging

import paper_trading
import paper_trading_daemon

from .json_utils import to_jsonable


logger = logging.getLogger(__name__)


def _compact_candidates(candidates, limit=10):
    rows = []
    for item in (candidates or [])[:limit]:
        rows.append(
            {
                "expiry_label": item.get("expiry_label"),
                "strategy": item.get("strategy"),
                "selection_score": item.get("selection_score"),
                "status": item.get("status"),
                "rejection_reasons": item.get("rejection_reasons") or [],
                "entry_reason": item.get("entry_reason"),
            }
        )
    return rows


def status(limit=50, compact=True, include_raw=False):
    dashboard = paper_trading.paper_trading_dashboard_data()
    if compact and not include_raw:
        dashboard["candidates"] = _compact_candidates(dashboard.get("candidates") or [], limit=10)
        selected = dashboard.get("selected") or {}
        dashboard["selected"] = {
            "expiry_label": selected.get("expiry_label"),
            "strategy": selected.get("strategy"),
            "selection_score": selected.get("selection_score"),
            "status": selected.get("status"),
            "entry_reason": selected.get("entry_reason"),
        } if selected else None
    return {
        "ok": True,
        "mode": "fastapi-backend",
        "dashboard": to_jsonable(dashboard),
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
