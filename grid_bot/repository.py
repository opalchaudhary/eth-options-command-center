from copy import deepcopy
from dataclasses import asdict
from threading import Lock
from typing import Any

from .models import GridConfig, GridRun, GridStatus, new_id, to_record_dict, utc_now


ACTIVE_STATUSES = {GridStatus.STARTING, GridStatus.RUNNING, GridStatus.PAUSED, GridStatus.REGRID_PENDING}


class InMemoryGridRepository:
    def __init__(self):
        self._lock = Lock()
        self.bots: dict[str, GridConfig] = {}
        self.config_versions: dict[str, list[GridConfig]] = {}
        self.runs: dict[str, GridRun] = {}
        self.events: list[dict] = []
        self.orders: dict[str, dict] = {}
        self.fills: dict[str, dict] = {}
        self.risk_snapshots: list[dict] = []
        self.summaries: dict[str, dict] = {}
        self.rest_fallback_state: dict[str, dict] = {}

    def create_bot(self, config: GridConfig) -> GridConfig:
        with self._lock:
            self.bots[config.bot_id] = config
            self.config_versions.setdefault(config.bot_id, []).append(config)
            self.log_event(config.bot_id, None, "GRID_CREATED", {"config_version": config.config_version})
            return config

    def active_run(self) -> GridRun | None:
        with self._lock:
            return next((run for run in self.runs.values() if run.status in ACTIVE_STATUSES), None)

    def start_run(self, bot_id: str, reference_price=None) -> GridRun:
        with self._lock:
            active = next((run for run in self.runs.values() if run.status in ACTIVE_STATUSES), None)
            if active:
                raise RuntimeError("Another DeltaGridBot is currently active. Stop the existing grid before starting another.")
            config = self.bots[bot_id]
            run = GridRun(
                run_id=new_id("run"),
                bot_id=bot_id,
                status=GridStatus.STARTING,
                config_version=config.config_version,
                started_at=utc_now(),
                reference_price=reference_price,
            )
            self.runs[run.run_id] = run
            self.log_event(bot_id, run.run_id, "GRID_RUN_STARTED", {})
            return run

    def set_run_status(self, run_id: str, status: GridStatus, **updates) -> GridRun:
        with self._lock:
            run = self.runs[run_id]
            for key, value in updates.items():
                setattr(run, key, value)
            run.status = status
            return run

    def log_event(self, bot_id: str | None, run_id: str | None, event_type: str, payload: dict | None = None) -> None:
        self.events.append({"event_id": new_id("evt"), "bot_id": bot_id, "run_id": run_id, "event_type": event_type, "payload": payload or {}, "created_at": utc_now()})

    def create_summary(self, run_id: str, summary: dict) -> dict:
        with self._lock:
            if run_id in self.summaries:
                raise RuntimeError("Grid Run Summary is immutable and already exists.")
            payload = deepcopy(summary)
            payload["immutable"] = True
            self.summaries[run_id] = payload
            self.log_event(payload.get("bot_id"), run_id, "GRID_RUN_SUMMARY_GENERATED", {"summary_id": payload.get("summary_id")})
            return payload

    def update_summary(self, run_id: str, updates: dict) -> None:
        if run_id in self.summaries and self.summaries[run_id].get("immutable"):
            raise RuntimeError("Grid Run Summary is immutable and cannot be modified.")
        self.summaries[run_id].update(updates)

    def upsert_order(self, order_key: str, payload: dict) -> None:
        with self._lock:
            self.orders[order_key] = {**self.orders.get(order_key, {}), **payload}

    def insert_fill_once(self, exchange_fill_id: str, payload: dict) -> bool:
        with self._lock:
            if exchange_fill_id in self.fills:
                return False
            self.fills[exchange_fill_id] = payload
            return True

    def set_rest_fallback_state(self, run_id: str, payload: dict) -> None:
        with self._lock:
            self.rest_fallback_state[run_id] = {**self.rest_fallback_state.get(run_id, {}), **payload}

    def snapshot(self) -> dict[str, Any]:
        return {
            "bots": [to_record_dict(item) for item in self.bots.values()],
            "runs": [to_record_dict(item) for item in self.runs.values()],
            "events": deepcopy(self.events),
            "summaries": deepcopy(list(self.summaries.values())),
            "rest_fallback_state": deepcopy(self.rest_fallback_state),
        }


repository = InMemoryGridRepository()
