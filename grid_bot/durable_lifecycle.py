import json
import os
import subprocess
import threading
import time
from copy import deepcopy
from dataclasses import asdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from .accounting import build_run_accounting
from .config import DEFAULT_RISK_THRESHOLDS, GRIDBOT_VERSION
from .account_telemetry import AccountTelemetryCache, risk_increasing_action_allowed
from .delta_testnet_client import DeltaTestnetClient
from .execution import make_client_order_id, order_payload
from .exchange_truth import reconcile_exchange_truth
from .grid_builder import build_grid_levels, preview_grid, quantize_price, validate_grid_config, validate_neutral_grid_suitability
from .models import GridConfig, GridStatus, GridType, OrderProposal, Side, SpacingType, new_id, to_record_dict, utc_now
from .semantics import evaluate_order_semantics, round_price_for_side, validate_post_only_price
from .supabase_repository import SupabaseGridRepository, SupabasePersistenceError


ACTIVE_STATUSES = {
    GridStatus.STARTING.value,
    GridStatus.RUNNING.value,
    GridStatus.PAUSING.value,
    GridStatus.PAUSED.value,
    GridStatus.RESUMING.value,
    GridStatus.EDITING.value,
    GridStatus.REGRID_PENDING.value,
    GridStatus.STOPPING.value,
    GridStatus.STOP_REQUIRES_ATTENTION.value,
}
GRIDBOT_ORDER_PREFIX = "DGB01-"
DEFAULT_STATE_PATH = Path(os.getenv("GRIDBOT_V01_STATE_PATH", "grid_bot_state_v01.json"))
START_TERMINAL_ORDER_STATUSES = {
    "cancelled",
    "closed",
    "filled",
    "not_open",
    "manual_cancelled",
    "deferred",
    "blocked",
    "abandoned_by_stop",
    "cancelled_before_submission",
    "superseded",
}
DEFERRED_ORDER_STATUSES = {"deferred", "blocked"}
_START_WORKERS: dict[str, threading.Thread] = {}
_START_WORKERS_LOCK = threading.Lock()
STOP_ATTENTION_STATUS = GridStatus.STOP_REQUIRES_ATTENTION.value


def _decimal(value: Any, default: str = "0") -> Decimal:
    try:
        if value in [None, ""]:
            return Decimal(default)
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "__dataclass_fields__"):
        return _jsonable(asdict(value))
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    return value


def _result_rows(payload: dict | None) -> list[dict]:
    result = (payload or {}).get("result") or []
    return result if isinstance(result, list) else []


def _order_id(row: dict) -> str:
    return str(row.get("id") or row.get("order_id") or "")


def _fill_id(row: dict) -> str:
    return str(row.get("id") or row.get("fill_id") or f"{row.get('order_id')}:{row.get('created_at')}:{row.get('size')}")


def _position_size(client: DeltaTestnetClient, product_id: int) -> Decimal:
    for row in _result_rows(client.positions("ETH")):
        if str(row.get("product_id")) == str(product_id) or row.get("product_symbol") == "ETHUSD" or row.get("symbol") == "ETHUSD":
            return _decimal(row.get("size"))
    return Decimal("0")


def _gridbot_orders(rows: list[dict]) -> list[dict]:
    return [row for row in rows if str(row.get("client_order_id", "")).startswith(GRIDBOT_ORDER_PREFIX)]


def _find_exchange_order_by_client_id(client: DeltaTestnetClient, product_id: int, client_order_id: str) -> dict | None:
    for row in _result_rows(client.open_orders(product_id)):
        if str(row.get("client_order_id") or "") == client_order_id:
            return row
    for row in _result_rows(client.order_history(product_id, page_size=50)):
        if str(row.get("client_order_id") or "") == client_order_id:
            return row
    return None


def _gross_pnl(fills: list[dict], multiplier: Decimal) -> Decimal:
    total = Decimal("0")
    for fill in fills:
        sign = Decimal("1") if str(fill.get("side")).lower() == Side.SELL.value else Decimal("-1")
        total += sign * _decimal(fill.get("price")) * _decimal(fill.get("size")) * multiplier
    return total


def _fee_total(fills: list[dict]) -> Decimal:
    return sum((_decimal(fill.get("commission")) for fill in fills), Decimal("0"))


def _first_number(payload: dict, keys: list[str]) -> Decimal | None:
    for key in keys:
        value = payload.get(key)
        if value not in [None, ""]:
            return _decimal(value)
    return None


def _fill_identity(fill: dict, fallback: str = "") -> str:
    return str(fill.get("id") or fill.get("fill_id") or fill.get("trade_id") or fill.get("execution_id") or fallback)


def _replacement_client_order_id(run_id: str, level_id: str, side: Side, source_fill_id: str) -> str:
    stable = "".join(ch for ch in source_fill_id if ch.isalnum())[-8:] or "fill"
    return f"DGB01-{run_id[-8:]}-{level_id}-{side.value[0].upper()}-R{stable}"[:32]


def _flatten_client_order_id(run_id: str, side: Side, sequence: int) -> str:
    return f"DGB01-{run_id[-8:]}-STOP-{side.value[0].upper()}-{sequence}"[:32]


def _config_fingerprint(config: dict) -> tuple:
    return (
        str(config.get("grid_type")),
        str(config.get("lower_price")),
        str(config.get("upper_price")),
        int(config.get("grid_count") or 0),
        str(config.get("spacing_type")),
        str(config.get("lot_size")),
        str(config.get("max_inventory_lots")),
    )


class DurableGridBotLifecycle:
    def __init__(
        self,
        client: DeltaTestnetClient | None = None,
        state_path: str | Path = DEFAULT_STATE_PATH,
        db: SupabaseGridRepository | None = None,
        use_supabase: bool | None = None,
    ):
        self.client = client or DeltaTestnetClient()
        self.state_path = Path(state_path)
        pytest_running = bool(os.getenv("PYTEST_CURRENT_TEST"))
        if use_supabase is None:
            use_supabase = not pytest_running and os.getenv("GRIDBOT_V01_SUPABASE_ENABLED", "1") != "0"
        self.db = db or (SupabaseGridRepository() if use_supabase else None)
        self._save_lock = threading.Lock()
        self.account_telemetry = AccountTelemetryCache(client=self.client)

    def _db_enabled(self) -> bool:
        return bool(self.db and self.db.enabled)

    def _load(self) -> dict:
        if self._db_enabled():
            active = self.db.active_run()
            if active:
                run = self.db.load_run_state(active["run_id"])
                return {"runs": {run["run_id"]: run}, "active_run_id": run["run_id"], "events": []}
            return {"runs": {}, "active_run_id": None, "events": []}
        if not self.state_path.exists():
            return {"runs": {}, "active_run_id": None, "events": []}
        last_error: Exception | None = None
        for attempt in range(5):
            try:
                return json.loads(self.state_path.read_text(encoding="utf-8"))
            except (PermissionError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt == 4:
                    raise
                time.sleep(0.05)
        raise last_error or RuntimeError("Unable to load durable DeltaGridBot state.")

    def _save(self, state: dict, *, include_children: bool = True) -> None:
        with self._save_lock:
            if self._db_enabled():
                for run in (state.get("runs") or {}).values():
                    self.db.persist_run_state(run, include_children=include_children)
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
            tmp.write_text(json.dumps(_jsonable(state), indent=2, sort_keys=True), encoding="utf-8")
            for attempt in range(5):
                try:
                    tmp.replace(self.state_path)
                    return
                except PermissionError:
                    if attempt == 4:
                        raise
                    time.sleep(0.05)

    def _event(self, state: dict, run_id: str | None, event_type: str, payload: dict | None = None) -> None:
        run = (state.get("runs") or {}).get(run_id) if run_id else None
        if self._db_enabled():
            self.db.log_event(run, event_type, payload or {})
        state.setdefault("events", []).append(
            {"event_id": new_id("evt"), "run_id": run_id, "event_type": event_type, "payload": payload or {}, "created_at": utc_now()}
        )

    def _active_run(self, state: dict) -> dict | None:
        run_id = state.get("active_run_id")
        run = state.get("runs", {}).get(run_id) if run_id else None
        return run if run and run.get("status") in ACTIVE_STATUSES else None

    def status(self) -> dict:
        if self._db_enabled():
            payload = self.db.status_payload()
            payload["state_path"] = str(self.state_path)
            return payload
        state = self._load()
        run = self._active_run(state)
        return {
            "ok": True,
            "state_path": str(self.state_path),
            "active_run_id": run.get("run_id") if run else None,
            "active_run": deepcopy(run),
            "runs": list(state.get("runs", {}).values()),
            "events": state.get("events", [])[-50:],
        }

    def _startup_progress(self, run: dict, stage: str | None = None, last_error: str | None = None) -> dict:
        levels = run.get("levels") or []
        orders = run.get("orders") or {}
        expected = len(levels)
        submitted = len(orders)
        verified = len([row for row in orders.values() if row.get("exchange_order_id")])
        if not stage:
            stage = run.get("start_stage")
        return {
            "start_stage": stage,
            "orders_expected": expected,
            "orders_submitted": submitted,
            "orders_verified": verified,
            "last_error": last_error if last_error is not None else run.get("last_error"),
        }

    def _set_start_stage(self, state: dict, run: dict, stage: str, payload: dict | None = None) -> None:
        run["start_stage"] = stage
        run["startup"] = self._startup_progress(run, stage)
        self._event(state, run["run_id"], "GRID_RUN_START_STAGE", {"start_stage": stage, **(payload or {})})
        self._save(state)

    def product_account_health(self, product_symbol: str = "ETHUSD") -> dict:
        telemetry = self.account_telemetry.get(product_symbol)
        spec = self.client.product_spec(product_symbol)
        bid = spec.best_bid or telemetry.mark_price or spec.mark_price
        ask = spec.best_ask or telemetry.mark_price or spec.mark_price
        reference = quantize_price((bid + ask) / Decimal("2"), spec.tick_size)
        margin_utilisation = telemetry.margin_utilisation_pct / Decimal("100") if telemetry.margin_utilisation_pct is not None else None
        return {
            "ok": True,
            "product": to_record_dict(spec),
            "market": {
                "reference_price": str(reference),
                "mark_price": str(spec.mark_price),
                "best_bid": str(spec.best_bid) if spec.best_bid is not None else None,
                "best_ask": str(spec.best_ask) if spec.best_ask is not None else None,
                "updated_at": utc_now(),
            },
            "account": {
                "account_equity": str(telemetry.account_equity) if telemetry.account_equity is not None else None,
                "available_margin": str(telemetry.available_margin) if telemetry.available_margin is not None else None,
                "margin_used": str(telemetry.used_margin) if telemetry.used_margin is not None else None,
                "margin_utilisation": str(margin_utilisation) if margin_utilisation is not None else None,
                "current_position": str(telemetry.position_lots) if telemetry.position_lots is not None else None,
                "open_gridbot_orders": telemetry.open_order_count,
                "portfolio_greeks": {
                    "delta": str(telemetry.portfolio_delta) if telemetry.portfolio_delta is not None else None,
                    "gamma": str(telemetry.portfolio_gamma) if telemetry.portfolio_gamma is not None else None,
                    "vega": str(telemetry.portfolio_vega) if telemetry.portfolio_vega is not None else None,
                    "theta": str(telemetry.portfolio_theta) if telemetry.portfolio_theta is not None else None,
                },
                "raw_available": telemetry.telemetry_status != "UNAVAILABLE",
                "telemetry_status": telemetry.telemetry_status,
                "unavailable_fields": telemetry.unavailable_fields,
            },
            "account_risk_state": telemetry.as_dict(),
        }

    def _config_from_operator_payload(self, payload: dict, spec, reference: Decimal, health: dict) -> GridConfig:
        required = [
            "grid_type",
            "lower_price",
            "upper_price",
            "grid_count",
            "spacing_type",
            "lot_size",
            "max_inventory_lots",
        ]
        missing = [key for key in required if payload.get(key) in [None, ""]]
        if missing:
            raise ValueError(f"Missing operator grid field(s): {', '.join(missing)}.")
        lower_price = _decimal(payload.get("lower_price"))
        upper_price = _decimal(payload.get("upper_price"))
        if quantize_price(lower_price, spec.tick_size) != lower_price or quantize_price(upper_price, spec.tick_size) != upper_price:
            raise ValueError("Lower Range and Upper Range must align with the exchange tick size.")
        max_inventory = _decimal(payload.get("max_inventory_lots"))
        lot_size = _decimal(payload.get("lot_size"))
        account_equity = _decimal((health.get("account") or {}).get("account_equity"), "0")
        projected_exposure = max_inventory * reference * spec.contract_multiplier
        denominator = account_equity if account_equity > 0 else max(projected_exposure, Decimal("1"))
        config = GridConfig(
            bot_id=payload.get("bot_id") or new_id("bot"),
            config_version=int(payload.get("config_version") or 1),
            bot_name=payload.get("bot_name") or "DeltaGridBot V0.1 Operator Grid",
            product_symbol=payload.get("product_symbol") or spec.symbol,
            grid_type=GridType(payload["grid_type"]),
            lower_price=lower_price,
            upper_price=upper_price,
            grid_count=int(payload["grid_count"]),
            spacing_type=SpacingType(payload["spacing_type"]),
            lot_size=lot_size,
            max_inventory_lots=max_inventory,
            allocated_capital=denominator,
            risk_capital=denominator,
            risk_thresholds=DEFAULT_RISK_THRESHOLDS,
        )
        validate_grid_config(config, spec.min_quantity)
        validate_neutral_grid_suitability(config, reference, spec.tick_size)
        return config

    def preview_operator_grid(self, payload: dict) -> dict:
        product_symbol = payload.get("product_symbol") or "ETHUSD"
        health = self.product_account_health(product_symbol)
        spec_dict = health["product"]
        spec = self.client.product_spec(product_symbol)
        reference = _decimal(health["market"]["reference_price"])
        config = self._config_from_operator_payload(payload, spec, reference, health)
        position = _position_size(self.client, spec.product_id)
        preview = preview_grid(config, reference, spec.tick_size, spec.best_bid, spec.best_ask, position)
        reserved_inventory = _decimal(preview.get("reserved_long_exposure")) + _decimal(preview.get("reserved_short_exposure"))
        projected_exposure = reserved_inventory * reference * spec.contract_multiplier
        equity = _decimal(health["account"].get("account_equity"), "0")
        grr = projected_exposure / equity if equity > 0 else None
        risk_state = "UNKNOWN"
        warnings = []
        if grr is None:
            warnings.append("Account equity unavailable; projected exposure is informational only.")
        elif grr >= Decimal("1"):
            risk_state = "RED"
        elif grr >= Decimal("0.75"):
            risk_state = "ORANGE"
        elif grr >= Decimal("0.5"):
            risk_state = "YELLOW"
        else:
            risk_state = "GREEN"
        return {
            "ok": True,
            "product": spec_dict,
            "market": health["market"],
            "account": health["account"],
            "config": to_record_dict(config),
            "preview": to_record_dict(preview),
            "risk": {
                "version": "gridbot_v01_account_health_grr_v1",
                "formula": "projected_grid_exposure / account_equity",
                "projected_grid_exposure": str(projected_exposure),
                "account_equity": str(equity) if equity > 0 else None,
                "grr": str(grr) if grr is not None else None,
                "risk_state": risk_state,
                "warnings": warnings,
            },
        }

    def preview_tiny_grid(self) -> dict:
        spec = self.client.product_spec("ETHUSD")
        bid = spec.best_bid or spec.mark_price
        ask = spec.best_ask or spec.mark_price
        reference = quantize_price((bid + ask) / Decimal("2"), spec.tick_size)
        width = max(Decimal("40"), abs(ask - bid) * Decimal("20"))
        config = GridConfig(
            bot_id=new_id("bot"),
            config_version=1,
            bot_name="DeltaGridBot V0.1 Tiny Testnet Grid",
            product_symbol="ETHUSD",
            grid_type=GridType.NEUTRAL,
            lower_price=quantize_price(reference - width, spec.tick_size),
            upper_price=quantize_price(reference + width, spec.tick_size),
            grid_count=4,
            spacing_type=SpacingType.ARITHMETIC,
            lot_size=max(spec.min_quantity, Decimal("1")),
            max_inventory_lots=max(spec.min_quantity * Decimal("2"), Decimal("2")),
            allocated_capital=Decimal("100"),
            risk_capital=Decimal("50"),
            risk_thresholds=DEFAULT_RISK_THRESHOLDS,
        )
        position = _position_size(self.client, spec.product_id)
        preview = preview_grid(config, reference, spec.tick_size, spec.best_bid, spec.best_ask, position)
        return {"ok": True, "product": to_record_dict(spec), "config": to_record_dict(config), "preview": to_record_dict(preview)}

    def start_operator_grid(self, payload: dict) -> dict:
        state = self._load()
        if self._active_run(state):
            raise RuntimeError("Another durable DeltaGridBot V0.1 run is already active.")
        preview_payload = self.preview_operator_grid(payload)
        product = preview_payload["product"]
        spec = self.client.product_spec(product["symbol"])
        self._assert_clean_start(spec.product_id)
        config = preview_payload["config"]
        levels = preview_payload["preview"]["levels"]
        run_id = new_id("run")
        run = {
            "run_id": run_id,
            "bot_id": config["bot_id"],
            "status": GridStatus.STARTING.value,
            "gridbot_version": GRIDBOT_VERSION,
            "config": config,
            "levels": levels,
            "product": product,
            "reference_price": preview_payload["preview"]["reference_price"],
            "execution_event_mode": "REST_FALLBACK",
            "private_ws_status": "BLOCKED_403",
            "operational_state": "DEGRADED",
            "sequence": 1,
            "orders": {},
            "deferred_orders": {},
            "fills": {},
            "replacement_keys": {},
            "risk_snapshots": [],
            "started_at": utc_now(),
        }
        state.setdefault("runs", {})[run_id] = run
        state["active_run_id"] = run_id
        if self._db_enabled():
            self.db.acquire_active_run_guard(run_id)
        self._event(state, run_id, "GRID_RUN_STARTING", {"execution_event_mode": "REST_FALLBACK", "source": "operator_grid"})
        self._save(state)
        try:
            for level in levels:
                proposal = self._proposal_for_level(run_id, level, int(run["sequence"]))
                self._place_proposal(run, spec.product_id, proposal, "initial_grid")
            run["status"] = GridStatus.RUNNING.value
            self._event(state, run_id, "GRID_RUN_RUNNING", {"open_orders": len(run["orders"])})
            self._save(state)
            return {"ok": True, "run": deepcopy(run), "preview": preview_payload}
        except Exception:
            for order in list(run.get("orders", {}).values()):
                self._cancel_order_safely(spec.product_id, order)
            run["status"] = "ERROR"
            state["active_run_id"] = None
            self._event(state, run_id, "GRID_RUN_START_FAILED", {})
            if self._db_enabled():
                self.db.release_active_run_guard(run_id)
            self._save(state)
            raise

    def begin_operator_grid_start(self, payload: dict) -> dict:
        state = self._load()
        active = self._active_run(state)
        if active:
            return {"ok": True, "run": deepcopy(active), "attached": True, **self._startup_progress(active)}
        self._event(state, None, "GRID_RUN_START_STAGE", {"start_stage": "VALIDATING", "source": "operator_grid"})
        preview_payload = self.preview_operator_grid(payload)
        product = preview_payload["product"]
        spec = self.client.product_spec(product["symbol"])
        self._assert_clean_start(spec.product_id)
        config = preview_payload["config"]
        levels = preview_payload["preview"]["levels"]
        run_id = new_id("run")
        run = {
            "run_id": run_id,
            "bot_id": config["bot_id"],
            "status": GridStatus.STARTING.value,
            "start_stage": "PERSISTING",
            "gridbot_version": GRIDBOT_VERSION,
            "config": config,
            "levels": levels,
            "product": product,
            "reference_price": preview_payload["preview"]["reference_price"],
            "execution_event_mode": "REST_FALLBACK",
            "private_ws_status": "BLOCKED_403",
            "operational_state": "DEGRADED",
            "sequence": 1,
            "orders": {},
            "deferred_orders": {},
            "fills": {},
            "replacement_keys": {},
            "risk_snapshots": [],
            "startup": {},
            "started_at": utc_now(),
        }
        run["startup"] = self._startup_progress(run, "PERSISTING")
        state.setdefault("runs", {})[run_id] = run
        state["active_run_id"] = run_id
        if self._db_enabled():
            self.db.acquire_active_run_guard(run_id)
        self._event(state, run_id, "GRID_RUN_STARTING", {"execution_event_mode": "REST_FALLBACK", "source": "operator_grid"})
        self._save(state)
        return {"ok": True, "run": deepcopy(run), "preview": preview_payload, "attached": False, **self._startup_progress(run)}

    def complete_operator_grid_start(self, run_id: str) -> dict:
        state = self._load()
        run = state.get("runs", {}).get(run_id)
        if not run:
            raise RuntimeError("No durable DeltaGridBot run found.")
        if run.get("status") == GridStatus.RUNNING.value:
            return {"ok": True, "run": deepcopy(run), **self._startup_progress(run, "RUNNING")}
        if run.get("status") != GridStatus.STARTING.value:
            return {"ok": True, "run": deepcopy(run), **self._startup_progress(run)}
        product_id = int(run["product"]["product_id"])
        self._set_start_stage(state, run, "PLACING_ORDERS")
        try:
            for level in run.get("levels") or []:
                state = self._load()
                run = state.get("runs", {}).get(run_id)
                if not run or run.get("status") != GridStatus.STARTING.value:
                    return {"ok": True, "run": deepcopy(run or {}), **self._startup_progress(run or {})}
                existing = [
                    order
                    for order in run.get("orders", {}).values()
                    if order.get("level_id") == level.get("level_id") and order.get("status") not in START_TERMINAL_ORDER_STATUSES
                ]
                if existing:
                    continue
                proposal = self._proposal_for_level(run_id, level, int(run["sequence"]))
                created = self._place_proposal(run, product_id, proposal, "initial_grid")
                latest_state = self._load()
                latest_run = latest_state.get("runs", {}).get(run_id)
                if not latest_run or latest_run.get("status") != GridStatus.STARTING.value:
                    if created.get("status") not in START_TERMINAL_ORDER_STATUSES:
                        self._cancel_order_safely(product_id, created)
                    if latest_run:
                        latest_run.setdefault("orders", {})[created["client_order_id"]] = created
                        latest_run["startup"] = self._startup_progress(latest_run)
                        self._save(latest_state)
                    return {"ok": True, "run": deepcopy(latest_run or {}), **self._startup_progress(latest_run or {})}
                run["startup"] = self._startup_progress(run, "PLACING_ORDERS")
                self._save(state)
            state = self._load()
            run = state.get("runs", {}).get(run_id)
            if not run or run.get("status") != GridStatus.STARTING.value:
                return {"ok": True, "run": deepcopy(run or {}), **self._startup_progress(run or {})}
            self._set_start_stage(state, run, "VERIFYING_ORDERS")
            self.reconcile(run_id)
            state = self._load()
            run = state["runs"][run_id]
            if run.get("status") != GridStatus.STARTING.value:
                return {"ok": True, "run": deepcopy(run), **self._startup_progress(run)}
            self._set_start_stage(state, run, "RECONCILING")
            run["status"] = GridStatus.RUNNING.value
            run["start_stage"] = "RUNNING"
            run["startup"] = self._startup_progress(run, "RUNNING")
            self._event(state, run_id, "GRID_RUN_RUNNING", {"open_orders": len(run["orders"])})
            self._save(state)
            return {"ok": True, "run": deepcopy(run), **self._startup_progress(run, "RUNNING")}
        except Exception as exc:
            for order in list(run.get("orders", {}).values()):
                self._cancel_order_safely(product_id, order)
            run["status"] = GridStatus.START_FAILED.value
            run["start_stage"] = "START_FAILED"
            run["last_error"] = str(exc)[:500]
            run["startup"] = self._startup_progress(run, "START_FAILED", run["last_error"])
            state["active_run_id"] = None
            self._event(state, run_id, "GRID_RUN_START_FAILED", {"error": run["last_error"]})
            if self._db_enabled():
                self.db.release_active_run_guard(run_id)
            self._save(state)
            raise

    def start_operator_grid_background(self, payload: dict) -> dict:
        begun = self.begin_operator_grid_start(payload)
        run = begun.get("run") or {}
        run_id = run.get("run_id")
        if not run_id or run.get("status") != GridStatus.STARTING.value or begun.get("attached"):
            return begun
        with _START_WORKERS_LOCK:
            worker = _START_WORKERS.get(run_id)
            if worker and worker.is_alive():
                return begun

            def _target() -> None:
                try:
                    self.complete_operator_grid_start(run_id)
                finally:
                    with _START_WORKERS_LOCK:
                        _START_WORKERS.pop(run_id, None)

            worker = threading.Thread(target=_target, name=f"gridbot-start-{run_id}", daemon=True)
            _START_WORKERS[run_id] = worker
            worker.start()
        return begun

    def _assert_clean_start(self, product_id: int) -> None:
        position = _position_size(self.client, product_id)
        if position != 0:
            raise RuntimeError(f"Refusing to start GridBot: ETHUSD position is {position}, expected 0.")
        open_orders = _gridbot_orders(_result_rows(self.client.open_orders(product_id)))
        if open_orders:
            raise RuntimeError(f"Refusing to start GridBot: {len(open_orders)} GridBot-owned open orders already exist.")

    def _proposal_for_level(
        self,
        run_id: str,
        level: dict,
        sequence: int,
        quantity: Decimal | None = None,
        client_order_id: str | None = None,
    ) -> OrderProposal:
        side = Side(level["side"])
        return OrderProposal(
            run_id=run_id,
            level_id=level["level_id"],
            side=side,
            price=_decimal(level["price"]),
            quantity=quantity or _decimal(level["quantity"]),
            client_order_id=client_order_id or make_client_order_id(run_id, level["level_id"], side, sequence),
        )

    def _defer_proposal(
        self,
        run: dict,
        proposal: OrderProposal,
        order_kind: str,
        reason_codes: list[str],
        normalized_price: Decimal | None = None,
        source_fill_id: str | None = None,
    ) -> dict:
        record = {
            "order_key": proposal.client_order_id,
            "run_id": run["run_id"],
            "level_id": proposal.level_id,
            "side": proposal.side.value,
            "price": str(normalized_price or proposal.price),
            "requested_quantity": str(proposal.quantity),
            "filled_quantity": "0",
            "remaining_quantity": "0",
            "client_order_id": proposal.client_order_id,
            "exchange_order_id": "",
            "status": "deferred",
            "order_kind": order_kind,
            "config_version": run["config"]["config_version"],
            "rejection_reason": ",".join(reason_codes),
            "source_fill_id": source_fill_id,
            "created_at": utc_now(),
        }
        run.setdefault("deferred_orders", {})[proposal.client_order_id] = record
        if self._db_enabled():
            self.db.persist_order_proposal(run, proposal, order_kind, source_fill_id)
            self.db.persist_order(run, record)
        return record

    def _terminalize_never_submitted_orders(self, run: dict, status: str = "abandoned_by_stop", reason: str = "run_stop_before_submission") -> int:
        count = 0
        now = utc_now()
        candidates = list((run.get("orders") or {}).values()) + list((run.get("deferred_orders") or {}).values())
        seen: set[str] = set()
        for order in candidates:
            client_order_id = str(order.get("client_order_id") or order.get("order_key") or "")
            if not client_order_id or client_order_id in seen:
                continue
            seen.add(client_order_id)
            order_status = str(order.get("status") or "").lower()
            if order.get("exchange_order_id") or order_status not in DEFERRED_ORDER_STATUSES:
                continue
            order["status"] = status
            order["remaining_quantity"] = "0"
            order["terminal_reason"] = reason
            order["cancelled_at"] = now
            run.setdefault("orders", {})[client_order_id] = order
            run.setdefault("deferred_orders", {})[client_order_id] = order
            if self._db_enabled():
                self.db.persist_order(run, order)
            count += 1
        return count

    def _open_order_records(self, run: dict) -> list[dict]:
        return [
            order
            for order in (run.get("orders") or {}).values()
            if order.get("status") not in START_TERMINAL_ORDER_STATUSES
        ]

    def _set_run_status(self, state: dict, run: dict, status: GridStatus, event_type: str, payload: dict | None = None) -> None:
        now = utc_now()
        run["status"] = status.value
        run["status_updated_at"] = now
        run["updated_at"] = now
        self._event(state, run["run_id"], event_type, payload or {})
        self._save(state)

    def _terminalize_deferred_for_pause(self, run: dict) -> int:
        return self._terminalize_never_submitted_orders(
            run,
            status="cancelled_before_submission",
            reason="pause_before_submission",
        )

    def _cancel_known_gridbot_resting_orders(self, run: dict, product_id: int) -> int:
        attempted = 0
        for order in run.get("orders", {}).values():
            if order.get("status") in START_TERMINAL_ORDER_STATUSES:
                continue
            if order.get("order_kind") == "safety_flatten":
                continue
            if not order.get("exchange_order_id"):
                continue
            attempted += 1
            self._cancel_order_safely(product_id, order)
        return attempted

    def _stop_attention(self, state: dict, run: dict, reason: str, diagnostics: dict) -> dict:
        now = utc_now()
        run["status"] = STOP_ATTENTION_STATUS
        run["status_updated_at"] = now
        run["updated_at"] = now
        run["stop_reason"] = reason
        run["stop_diagnostics"] = {
            **(run.get("stop_diagnostics") or {}),
            "updated_at": now,
            **diagnostics,
        }
        self._event(state, run["run_id"], "GRID_RUN_STOP_REQUIRES_ATTENTION", run["stop_diagnostics"])
        self._save(state)
        return {"ok": False, "run": deepcopy(run), "requires_attention": True, "diagnostics": deepcopy(run["stop_diagnostics"])}

    def _flatten_orders(self, run: dict) -> list[dict]:
        return [order for order in (run.get("orders") or {}).values() if order.get("order_kind") == "safety_flatten"]

    def _find_fill_by_client_id(self, product_id: int, client_order_id: str) -> dict | None:
        for row in _result_rows(self.client.fills(product_id, page_size=50)):
            if str(row.get("client_order_id") or "") == client_order_id:
                return row
        return None

    def _flatten_order_record(
        self,
        run: dict,
        side: Side,
        quantity: Decimal,
        client_order_id: str,
        exchange_order: dict | None,
        fill: dict | None,
        price: Decimal,
    ) -> dict:
        exchange_order = exchange_order or {}
        return {
            "order_key": client_order_id,
            "run_id": run["run_id"],
            "level_id": "STOP",
            "side": side.value,
            "price": str(price),
            "requested_quantity": str(quantity),
            "filled_quantity": "0",
            "remaining_quantity": str(quantity),
            "client_order_id": client_order_id,
            "exchange_order_id": _order_id(exchange_order) or str((fill or {}).get("order_id") or ""),
            "status": (exchange_order.get("state") or exchange_order.get("status") or ("filled" if fill else "open")).lower(),
            "order_kind": "safety_flatten",
            "config_version": run["config"]["config_version"],
            "reduce_only": True,
            "post_only": False,
            "time_in_force": "ioc",
            "raw": exchange_order or {"recovered_from_fill": fill},
            "created_at": utc_now(),
            "submitted_at": utc_now(),
        }

    def _recover_flatten_order(self, run: dict, product_id: int, inventory: Decimal) -> dict | None:
        side = Side.SELL if inventory > 0 else Side.BUY
        quantity = abs(inventory)
        sequence = len(self._flatten_orders(run)) + 1
        client_order_id = _flatten_client_order_id(run["run_id"], side, sequence)
        existing = run.get("orders", {}).get(client_order_id)
        if existing:
            return existing

        exchange_order = _find_exchange_order_by_client_id(self.client, product_id, client_order_id)
        fill = None if exchange_order else self._find_fill_by_client_id(product_id, client_order_id)
        if not exchange_order and not fill:
            return None
        spec = self.client.product_spec(run.get("product", {}).get("symbol") or run.get("config", {}).get("product_symbol") or "ETHUSD")
        price = spec.best_bid if side == Side.SELL else spec.best_ask
        if not price:
            price = spec.mark_price or spec.last_price
        if not price:
            raise RuntimeError("Cannot submit safety flatten without a usable ETHUSD price.")

        record = self._flatten_order_record(run, side, quantity, client_order_id, exchange_order, fill, price)
        run.setdefault("orders", {})[client_order_id] = record
        run.setdefault("flatten", {}).setdefault("orders", []).append(
            {
                "client_order_id": client_order_id,
                "side": side.value,
                "quantity": str(quantity),
                "reduce_only": True,
                "created_at": record["created_at"],
            }
        )
        if self._db_enabled():
            self.db.persist_order(run, record)
        return record

    def _recover_or_place_flatten_order(self, run: dict, product_id: int, inventory: Decimal) -> dict:
        recovered = self._recover_flatten_order(run, product_id, inventory)
        if recovered:
            return recovered

        side = Side.SELL if inventory > 0 else Side.BUY
        quantity = abs(inventory)
        sequence = len(self._flatten_orders(run)) + 1
        client_order_id = _flatten_client_order_id(run["run_id"], side, sequence)
        spec = self.client.product_spec(run.get("product", {}).get("symbol") or run.get("config", {}).get("product_symbol") or "ETHUSD")
        price = spec.best_bid if side == Side.SELL else spec.best_ask
        if not price:
            price = spec.mark_price or spec.last_price
        if not price:
            raise RuntimeError("Cannot submit safety flatten without a usable ETHUSD price.")
        proposal = OrderProposal(
            run_id=run["run_id"],
            level_id="STOP",
            side=side,
            price=price,
            quantity=quantity,
            client_order_id=client_order_id,
            post_only=False,
            time_in_force="ioc",
            reduce_only=True,
        )
        exchange_order = self.client.place_order(order_payload(product_id, proposal)).get("result") or {}
        record = self._flatten_order_record(run, side, quantity, client_order_id, exchange_order, None, price)
        run.setdefault("orders", {})[client_order_id] = record
        run.setdefault("flatten", {}).setdefault("orders", []).append(
            {
                "client_order_id": client_order_id,
                "side": side.value,
                "quantity": str(quantity),
                "reduce_only": True,
                "created_at": record["created_at"],
            }
        )
        if self._db_enabled():
            self.db.persist_order(run, record)
        return record

    def _build_stop_summary(self, run: dict, reason: str, reconciliation: dict, position: Decimal, open_orders: list[dict]) -> dict:
        fills = list(run.get("fills", {}).values())
        telemetry = self.account_telemetry.get(run.get("product", {}).get("symbol") or "ETHUSD")
        accounting = build_run_accounting(run, mark_price=telemetry.mark_price, account_position_lots=position)
        external_resolution = run.get("external_position_resolution") or {}
        operational_gridbot_inventory = "0" if external_resolution.get("status") == "EXTERNALLY_RESOLVED" else str(reconciliation.get("gridbot_inventory") or "0")
        accounting_warnings = sorted(set(accounting.warnings + (["EXTERNAL_POSITION_CLOSE_UNATTRIBUTED"] if external_resolution else [])))
        flatten_orders = self._flatten_orders(run)
        flatten_order_ids = {order.get("client_order_id") for order in flatten_orders}
        flatten_fills = []
        for fill in fills:
            raw = fill.get("raw") if isinstance(fill, dict) and isinstance(fill.get("raw"), dict) else fill
            if isinstance(raw, dict) and str(raw.get("client_order_id") or "") in flatten_order_ids:
                flatten_fills.append(fill)
        return {
            "summary_id": new_id("summary"),
            "run_id": run["run_id"],
            "gridbot_version": GRIDBOT_VERSION,
            "execution_event_mode": run.get("execution_event_mode"),
            "private_ws_available": False,
            "private_ws_status": run.get("private_ws_status"),
            "started_at": run.get("started_at"),
            "stopped_at": utc_now(),
            "stop_reason": reason,
            "stop_mode": "STOP_AND_CLOSE",
            "orders_total": len(run.get("orders", {})),
            "fills_total": len(fills),
            "cycles_total": accounting.cycles_completed,
            "orders_cancelled": len([order for order in run.get("orders", {}).values() if str(order.get("status") or "").lower() in {"cancelled", "manual_cancelled", "abandoned_by_stop"}]),
            "late_fills": int(reconciliation.get("new_fills") or 0),
            "flatten_orders": deepcopy(flatten_orders),
            "flatten_fills": deepcopy(flatten_fills),
            "gross_pnl": str(accounting.gross_realized_pnl),
            "gross_realized_pnl": str(accounting.gross_realized_pnl),
            "gross_grid_profit": str(accounting.gross_realized_pnl),
            "delta_fees": str(accounting.trading_fees),
            "trading_fees": str(accounting.trading_fees),
            "maker_fees": str(accounting.maker_fees),
            "taker_fees": str(accounting.taker_fees),
            "unknown_role_fees": str(accounting.unknown_role_fees),
            "funding": str(accounting.funding_net),
            "funding_paid": str(accounting.funding_paid),
            "funding_received": str(accounting.funding_received),
            "funding_net": str(accounting.funding_net),
            "other_delta_costs_credits": str(accounting.other_credits - accounting.other_costs),
            "other_costs": str(accounting.other_costs),
            "other_credits": str(accounting.other_credits),
            "net_realized_pnl": str(accounting.net_realized_pnl),
            "unrealized_pnl": str(accounting.unrealized_pnl) if accounting.unrealized_pnl is not None else None,
            "live_net_pnl": str(accounting.live_net_pnl) if accounting.live_net_pnl is not None else None,
            "net_run_pnl": str(accounting.live_net_pnl) if accounting.live_net_pnl is not None else str(accounting.net_realized_pnl),
            "fee_to_gross_profit_ratio": str(accounting.fee_to_gross_ratio) if accounting.fee_to_gross_ratio is not None else None,
            "accounting_status": "PARTIAL" if external_resolution else accounting.accounting_status,
            "accounting_warnings": accounting_warnings,
            "funding_attribution_status": accounting.funding_attribution_status,
            "accounting_completeness": "PARTIAL" if external_resolution else accounting.accounting_status,
            "NET_TRADING_PNL_BEFORE_INCOME_TAX": str(accounting.net_realized_pnl),
            "final_gridbot_inventory": operational_gridbot_inventory,
            "ledger_gridbot_inventory": str(reconciliation.get("gridbot_inventory") or "0"),
            "final_delta_position": str(position),
            "final_position": str(position),
            "external_position_resolution": deepcopy(external_resolution) if external_resolution else None,
            "stray_gridbot_orders": len(open_orders),
            "stop_warnings": accounting_warnings,
            "stop_errors": [],
            "immutable": True,
            "created_at": utc_now(),
        }

    def _assert_pause_reconciled(self, reconciliation: dict) -> None:
        errors = reconciliation.get("errors") or []
        open_orders = int(reconciliation.get("exchange_open_orders") or 0)
        unresolved = int(reconciliation.get("unresolved_orders") or 0)
        mismatches = int(reconciliation.get("position_mismatches") or 0)
        if errors or open_orders or unresolved or mismatches:
            raise RuntimeError(
                "Pause could not establish exchange truth: "
                f"errors={errors}, open_gridbot_orders={open_orders}, "
                f"unresolved_orders={unresolved}, position_mismatches={mismatches}"
            )

    def _assert_resume_ready(self, run: dict, reconciliation: dict) -> Decimal:
        errors = reconciliation.get("errors") or []
        unresolved = int(reconciliation.get("unresolved_orders") or 0)
        mismatches = int(reconciliation.get("position_mismatches") or 0)
        if errors or unresolved or mismatches:
            raise RuntimeError(
                "Resume could not establish exchange truth: "
                f"errors={errors}, unresolved_orders={unresolved}, position_mismatches={mismatches}"
            )
        product_symbol = run.get("product", {}).get("symbol") or run.get("config", {}).get("product_symbol") or "ETHUSD"
        telemetry = self.account_telemetry.get(product_symbol, force=True)
        allowed, reasons = risk_increasing_action_allowed(telemetry)
        if not allowed:
            raise RuntimeError(f"Resume requires fresh critical telemetry before placement: {','.join(reasons)}")
        return _decimal(reconciliation.get("gridbot_inventory"), str(telemetry.position_lots or "0"))

    def _place_proposal(
        self,
        run: dict,
        product_id: int,
        proposal: OrderProposal,
        order_kind: str,
        *,
        current_inventory: Decimal | None = None,
        source_fill_id: str | None = None,
    ) -> dict:
        spec = self.client.product_spec(run.get("product", {}).get("symbol") or run.get("config", {}).get("product_symbol") or "ETHUSD")
        position = current_inventory if current_inventory is not None else _position_size(self.client, product_id)
        normalized_price = round_price_for_side(proposal.price, spec.tick_size, proposal.side)
        semantic = evaluate_order_semantics(
            GridType(run["config"]["grid_type"]),
            position,
            _decimal(run["config"]["max_inventory_lots"]),
            proposal.side,
            proposal.quantity,
            self._open_order_records(run),
        )
        post_only = validate_post_only_price(proposal.side, normalized_price, spec.best_bid, spec.best_ask)
        reason_codes = [*semantic.reason_codes, *post_only.reason_codes]
        lower_price = _decimal(run.get("config", {}).get("lower_price"))
        upper_price = _decimal(run.get("config", {}).get("upper_price"))
        market_price = spec.mark_price or spec.last_price
        if semantic.opens_inventory and market_price and (market_price < lower_price or market_price > upper_price):
            reason_codes.append("MARKET_OUTSIDE_CONFIGURED_GRID_RANGE")
        if reason_codes:
            return self._defer_proposal(run, proposal, order_kind, reason_codes, normalized_price, source_fill_id)
        if normalized_price != proposal.price:
            proposal = OrderProposal(
                run_id=proposal.run_id,
                level_id=proposal.level_id,
                side=proposal.side,
                price=normalized_price,
                quantity=proposal.quantity,
                client_order_id=proposal.client_order_id,
                post_only=proposal.post_only,
                time_in_force=proposal.time_in_force,
                reduce_only=proposal.reduce_only,
            )
        if self._db_enabled():
            self.db.persist_order_proposal(run, proposal, order_kind, source_fill_id)
        exchange_order = _find_exchange_order_by_client_id(self.client, product_id, proposal.client_order_id)
        if not exchange_order:
            response = self.client.place_order(order_payload(product_id, proposal))
            exchange_order = response.get("result") or {}
        record = {
            "order_key": proposal.client_order_id,
            "run_id": run["run_id"],
            "level_id": proposal.level_id,
            "side": proposal.side.value,
            "price": str(proposal.price),
            "requested_quantity": str(proposal.quantity),
            "filled_quantity": "0",
            "remaining_quantity": str(proposal.quantity),
            "client_order_id": proposal.client_order_id,
            "exchange_order_id": _order_id(exchange_order),
            "status": exchange_order.get("state") or "open",
            "order_kind": order_kind,
            "config_version": run["config"]["config_version"],
            "source_fill_id": source_fill_id,
            "opens_inventory": semantic.opens_inventory,
            "projected_inventory_if_filled": str(semantic.projected_inventory),
            "reserved_long_after": str(semantic.reserved_long_after),
            "reserved_short_after": str(semantic.reserved_short_after),
            "raw": exchange_order,
            "created_at": utc_now(),
            "submitted_at": utc_now(),
        }
        run.setdefault("orders", {})[proposal.client_order_id] = record
        if self._db_enabled():
            try:
                self.db.persist_order(run, record)
            except Exception:
                self._cancel_order_safely(product_id, record)
                raise
        run["sequence"] = int(run.get("sequence", 0)) + 1
        return record

    def start_tiny_grid(self) -> dict:
        state = self._load()
        if self._active_run(state):
            raise RuntimeError("Another durable DeltaGridBot V0.1 run is already active.")
        spec = self.client.product_spec("ETHUSD")
        self._assert_clean_start(spec.product_id)
        preview = self.preview_tiny_grid()
        config = preview["config"]
        levels = preview["preview"]["levels"]
        run_id = new_id("run")
        run = {
            "run_id": run_id,
            "bot_id": config["bot_id"],
            "status": GridStatus.STARTING.value,
            "gridbot_version": GRIDBOT_VERSION,
            "config": config,
            "levels": levels,
            "product": preview["product"],
            "reference_price": preview["preview"]["reference_price"],
            "execution_event_mode": "REST_FALLBACK",
            "private_ws_status": "BLOCKED_403",
            "operational_state": "DEGRADED",
            "sequence": 1,
            "orders": {},
            "deferred_orders": {},
            "fills": {},
            "replacement_keys": {},
            "risk_snapshots": [],
            "started_at": utc_now(),
        }
        state.setdefault("runs", {})[run_id] = run
        state["active_run_id"] = run_id
        if self._db_enabled():
            self.db.acquire_active_run_guard(run_id)
        self._event(state, run_id, "GRID_RUN_STARTING", {"execution_event_mode": "REST_FALLBACK"})
        self._save(state)
        try:
            for level in levels:
                proposal = self._proposal_for_level(run_id, level, int(run["sequence"]))
                self._place_proposal(run, spec.product_id, proposal, "initial_grid")
            run["status"] = GridStatus.RUNNING.value
            self._event(state, run_id, "GRID_RUN_RUNNING", {"open_orders": len(run["orders"])})
            self._save(state)
            return {"ok": True, "run": deepcopy(run)}
        except Exception:
            for order in list(run.get("orders", {}).values()):
                self._cancel_order_safely(spec.product_id, order)
            run["status"] = "ERROR"
            state["active_run_id"] = None
            self._event(state, run_id, "GRID_RUN_START_FAILED", {})
            if self._db_enabled():
                self.db.release_active_run_guard(run_id)
            self._save(state)
            raise

    def _cancel_order_safely(self, product_id: int, order: dict) -> bool:
        order_id = order.get("exchange_order_id")
        if not order_id:
            return False
        try:
            self.client.cancel_order(product_id, str(order_id))
            order["status"] = "cancelled"
            order["cancelled_at"] = utc_now()
            return True
        except Exception as exc:
            order["cancel_error"] = str(exc)[:300]
            return False

    def reconcile(
        self,
        run_id: str | None = None,
        *,
        process_replacements: bool = False,
        persist_snapshot: bool = True,
        log_reconcile_event: bool = True,
        persist_order_updates: bool = True,
    ) -> dict:
        state = self._load()
        run = state.get("runs", {}).get(run_id or state.get("active_run_id"))
        if not run:
            raise RuntimeError("No durable DeltaGridBot run found.")
        result = reconcile_exchange_truth(
            run,
            self.client,
            self.db if self._db_enabled() else None,
            suppress_replacements=True,
            persist_order_updates=persist_order_updates,
        )
        replacement_result = self.process_replacements(run, result) if process_replacements else {"created": 0, "deferred": 0, "skipped": 0}
        risk = {
            "created_at": utc_now(),
            "position": result.get("delta_position"),
            "gridbot_inventory": result.get("gridbot_inventory"),
            "open_gridbot_orders": result.get("exchange_open_orders"),
            "position_mismatches": result.get("position_mismatches"),
            "fill_ledger_mismatches": result.get("fill_ledger_mismatches"),
            "reconciliation": result,
            "replacements": replacement_result,
        }
        run.setdefault("risk_snapshots", []).append(risk)
        for event in result.get("events", []):
            self._event(state, run["run_id"], event["event_type"], event.get("payload") or {})
        if log_reconcile_event:
            self._event(state, run["run_id"], "REST_RECONCILED", {**result, "replacements": replacement_result})
        if self._db_enabled() and persist_snapshot:
            self.db.persist_snapshot(run, risk, run.get("summary"))
        self._save(state, include_children=False)
        return {
            "ok": not result.get("errors"),
            "run": deepcopy(run),
            "new_fills": result.get("new_fills"),
            "position": result.get("delta_position"),
            "open_gridbot_orders": result.get("exchange_open_orders"),
            "reconciliation": result,
            "replacements": replacement_result,
        }

    def process_replacements(self, run: dict, reconciliation: dict | None = None) -> dict:
        product_id = int(run["product"]["product_id"])
        outcome = {"created": 0, "deferred": 0, "skipped": 0, "existing": 0, "items": []}
        if run.get("status") and run.get("status") != GridStatus.RUNNING.value:
            outcome["skipped"] = len(run.get("fills") or {})
            outcome["items"].append({"state": "skipped", "reason": "run_not_running", "status": run.get("status")})
            return outcome
        fills = run.get("fills") or {}
        inventory = _decimal((reconciliation or {}).get("gridbot_inventory"), str(_position_size(self.client, product_id)))
        for fill_id, fill in list(fills.items()):
            raw = fill.get("raw") if isinstance(fill, dict) and isinstance(fill.get("raw"), dict) else fill
            result = self._place_replacement_for_fill(run, raw, product_id, str(fill_id), inventory)
            if result:
                outcome[result["state"]] = outcome.get(result["state"], 0) + 1
                outcome["items"].append(result)
        return outcome

    def _place_replacement_for_fill(
        self,
        run: dict,
        fill: dict,
        product_id: int,
        fill_id: str,
        current_inventory: Decimal | None = None,
    ) -> dict | None:
        key = f"{fill_id}:replacement"
        if key in run.setdefault("replacement_keys", {}):
            existing_key = run["replacement_keys"][key]
            existing_client_order_id = existing_key.get("client_order_id")
            existing_order = (run.get("orders") or {}).get(str(existing_client_order_id)) or (run.get("deferred_orders") or {}).get(str(existing_client_order_id))
            if (
                str(existing_key.get("state") or "").lower() in DEFERRED_ORDER_STATUSES
                and existing_order
                and not existing_order.get("exchange_order_id")
                and str(existing_order.get("status") or "").lower() in DEFERRED_ORDER_STATUSES
            ):
                run["replacement_keys"].pop(key, None)
            else:
                return {**run["replacement_keys"][key], "state": "existing", "source_fill_id": fill_id}
        order = run["orders"].get(str(fill.get("client_order_id")))
        if not order:
            order = next((row for row in run.get("orders", {}).values() if str(row.get("exchange_order_id")) == str(fill.get("order_id"))), None)
        if not order:
            run["replacement_keys"][key] = {"skipped": True, "reason": "source_order_not_found"}
            return {"state": "skipped", "source_fill_id": fill_id, "reason": "source_order_not_found"}
        levels = run["levels"]
        current_index = next((index for index, level in enumerate(levels) if level["level_id"] == order["level_id"]), None)
        if current_index is None:
            run["replacement_keys"][key] = {"skipped": True, "reason": "source_level_not_found"}
            return {"state": "skipped", "source_fill_id": fill_id, "reason": "source_level_not_found"}
        fill_side = Side(str(fill.get("side")).lower())
        target_index = current_index + 1 if fill_side == Side.BUY else current_index - 1
        if target_index < 0 or target_index >= len(levels):
            run["replacement_keys"][key] = {"skipped": True, "reason": "edge_level"}
            return {"state": "skipped", "source_fill_id": fill_id, "reason": "edge_level"}
        target = deepcopy(levels[target_index])
        target["side"] = Side.SELL.value if fill_side == Side.BUY else Side.BUY.value
        source_fill_id = _fill_identity(fill, fill_id)
        client_order_id = _replacement_client_order_id(run["run_id"], target["level_id"], Side(target["side"]), source_fill_id)
        existing = run.get("orders", {}).get(client_order_id) or run.get("deferred_orders", {}).get(client_order_id)
        if existing:
            existing_status = str(existing.get("status") or "").lower()
            if existing_status in DEFERRED_ORDER_STATUSES and not existing.get("exchange_order_id"):
                proposal = self._proposal_for_level(
                    run["run_id"],
                    target,
                    int(run["sequence"]),
                    quantity=_decimal(fill.get("size")),
                    client_order_id=client_order_id,
                )
                run.setdefault("orders", {}).pop(client_order_id, None)
                run.setdefault("deferred_orders", {}).pop(client_order_id, None)
                created = self._place_proposal(
                    run,
                    product_id,
                    proposal,
                    "replacement",
                    current_inventory=current_inventory,
                    source_fill_id=source_fill_id,
                )
                state = "deferred" if created.get("status") == "deferred" else "created"
                run["replacement_keys"][key] = {
                    "client_order_id": created["client_order_id"],
                    "exchange_order_id": created.get("exchange_order_id"),
                    "state": created.get("status"),
                    "source_fill_id": source_fill_id,
                    "created_at": utc_now(),
                }
                return {"state": state, "source_fill_id": source_fill_id, "client_order_id": created["client_order_id"], "level_id": target["level_id"]}
            run["replacement_keys"][key] = {
                "client_order_id": client_order_id,
                "exchange_order_id": existing.get("exchange_order_id"),
                "state": existing.get("status"),
                "source_fill_id": source_fill_id,
            }
            return {"state": "existing", "source_fill_id": source_fill_id, "client_order_id": client_order_id}
        proposal = self._proposal_for_level(
            run["run_id"],
            target,
            int(run["sequence"]),
            quantity=_decimal(fill.get("size")),
            client_order_id=client_order_id,
        )
        created = self._place_proposal(
            run,
            product_id,
            proposal,
            "replacement",
            current_inventory=current_inventory,
            source_fill_id=source_fill_id,
        )
        state = "deferred" if created.get("status") == "deferred" else "created"
        run["replacement_keys"][key] = {
            "client_order_id": created["client_order_id"],
            "exchange_order_id": created.get("exchange_order_id"),
            "state": created.get("status"),
            "source_fill_id": source_fill_id,
            "created_at": utc_now(),
        }
        return {"state": state, "source_fill_id": source_fill_id, "client_order_id": created["client_order_id"], "level_id": target["level_id"]}

    def pause(self, run_id: str | None = None) -> dict:
        state = self._load()
        run = state.get("runs", {}).get(run_id or state.get("active_run_id"))
        if not run:
            raise RuntimeError("No active durable DeltaGridBot run found.")
        product_id = int(run["product"]["product_id"])
        if run.get("status") == GridStatus.PAUSED.value:
            reconciled = self.reconcile(run["run_id"], process_replacements=False)
            self._assert_pause_reconciled(reconciled["reconciliation"])
            return {"ok": True, "run": deepcopy(reconciled["run"]), "reconciliation": reconciled["reconciliation"]}

        if run.get("status") != GridStatus.PAUSING.value:
            self._set_run_status(state, run, GridStatus.PAUSING, "GRID_RUN_PAUSING", {"previous_status": run.get("status")})
            state = self._load()
            run = state["runs"][run["run_id"]]

        cancelled_attempts = self._cancel_known_gridbot_resting_orders(run, product_id)
        deferred_terminalized = self._terminalize_deferred_for_pause(run)
        self._save(state)

        reconciled = self.reconcile(run["run_id"], process_replacements=False, persist_order_updates=False)
        if reconciled["reconciliation"].get("exchange_open_orders"):
            state = self._load()
            run = state["runs"][run["run_id"]]
            cancelled_attempts += self._cancel_known_gridbot_resting_orders(run, product_id)
            self._save(state)
            reconciled = self.reconcile(run["run_id"], process_replacements=False)

        state = self._load()
        run = state["runs"][run["run_id"]]
        try:
            self._assert_pause_reconciled(reconciled["reconciliation"])
        except RuntimeError as exc:
            self._event(
                state,
                run["run_id"],
                "GRID_RUN_PAUSE_BLOCKED",
                {"reason": str(exc), "cancel_attempts": cancelled_attempts, "deferred_terminalized": deferred_terminalized},
            )
            self._save(state)
            raise

        now = utc_now()
        run["status"] = GridStatus.PAUSED.value
        run["status_updated_at"] = now
        run["updated_at"] = now
        self._event(
            state,
            run["run_id"],
            "GRID_RUN_PAUSED",
            {
                "open_gridbot_orders": reconciled["open_gridbot_orders"],
                "gridbot_inventory": reconciled["reconciliation"].get("gridbot_inventory"),
                "delta_position": reconciled["position"],
                "cancel_attempts": cancelled_attempts,
                "deferred_terminalized": deferred_terminalized,
            },
        )
        self._save(state)
        return {"ok": True, "run": deepcopy(run), "reconciliation": reconciled["reconciliation"]}

    def resume(self, run_id: str | None = None) -> dict:
        state = self._load()
        run = state.get("runs", {}).get(run_id or state.get("active_run_id"))
        if not run:
            raise RuntimeError("No active durable DeltaGridBot run found.")
        if run.get("status") == GridStatus.RUNNING.value:
            return {"ok": True, "run": deepcopy(run), "attached": True}
        if run.get("status") == GridStatus.PAUSING.value:
            paused = self.pause(run["run_id"])
            state = self._load()
            run = state["runs"][paused["run"]["run_id"]]
        if run.get("status") != GridStatus.RESUMING.value:
            if run.get("status") not in {GridStatus.PAUSED.value, GridStatus.REGRID_PENDING.value}:
                raise RuntimeError(f"Cannot resume durable DeltaGridBot run from status {run.get('status')}.")
            self._set_run_status(state, run, GridStatus.RESUMING, "GRID_RUN_RESUMING", {"previous_status": run.get("status")})
            state = self._load()
            run = state["runs"][run["run_id"]]

        product_id = int(run["product"]["product_id"])
        reconciled = self.reconcile(run["run_id"], process_replacements=False)
        state = self._load()
        run = state["runs"][run["run_id"]]
        inventory = self._assert_resume_ready(run, reconciled["reconciliation"])

        for level in run["levels"]:
            existing_open = [
                order
                for order in run.get("orders", {}).values()
                if order["level_id"] == level["level_id"] and order.get("status") not in START_TERMINAL_ORDER_STATUSES
            ]
            if not existing_open:
                proposal = self._proposal_for_level(run["run_id"], level, int(run["sequence"]))
                self._place_proposal(run, product_id, proposal, "resume_grid", current_inventory=inventory)
        self._save(state)

        verified = self.reconcile(run["run_id"], process_replacements=False)
        state = self._load()
        run = state["runs"][run["run_id"]]
        errors = verified["reconciliation"].get("errors") or []
        unresolved = int(verified["reconciliation"].get("unresolved_orders") or 0)
        mismatches = int(verified["reconciliation"].get("position_mismatches") or 0)
        if errors or unresolved or mismatches:
            self._event(
                state,
                run["run_id"],
                "GRID_RUN_RESUME_BLOCKED",
                {"errors": errors, "unresolved_orders": unresolved, "position_mismatches": mismatches},
            )
            self._save(state)
            raise RuntimeError(
                "Resume placement could not be verified: "
                f"errors={errors}, unresolved_orders={unresolved}, position_mismatches={mismatches}"
            )
        now = utc_now()
        run["status"] = GridStatus.RUNNING.value
        run["status_updated_at"] = now
        run["updated_at"] = now
        self._event(
            state,
            run["run_id"],
            "GRID_RUN_RESUMED",
            {
                "open_gridbot_orders": verified["open_gridbot_orders"],
                "gridbot_inventory": verified["reconciliation"].get("gridbot_inventory"),
                "delta_position": verified["position"],
            },
        )
        self._save(state)
        return {"ok": True, "run": deepcopy(run), "reconciliation": verified["reconciliation"]}

    def _edit_config_from_payload(self, run: dict, payload: dict, spec, reference: Decimal, health: dict) -> GridConfig:
        current = run.get("config") or {}
        merged = {
            **current,
            **{key: value for key, value in (payload or {}).items() if value not in [None, ""]},
            "bot_id": run["bot_id"],
            "bot_name": current.get("bot_name") or "DeltaGridBot V0.1 Operator Grid",
            "product_symbol": current.get("product_symbol") or (run.get("product") or {}).get("symbol") or spec.symbol,
            "config_version": int(current.get("config_version") or 1) + 1,
        }
        return self._config_from_operator_payload(merged, spec, reference, health)

    def _edit_order_plan(self, run: dict, new_levels: list[dict], inventory: Decimal, spec) -> dict:
        existing_open = [
            order
            for order in (run.get("orders") or {}).values()
            if order.get("status") not in START_TERMINAL_ORDER_STATUSES and order.get("order_kind") != "safety_flatten"
        ]
        target_keys = {
            (str(level.get("side")), str(level.get("price")), str(level.get("quantity")))
            for level in new_levels
        }
        preserve_candidates = [
            order
            for order in existing_open
            if (str(order.get("side")), str(order.get("price")), str(order.get("requested_quantity"))) in target_keys
        ]
        create = []
        deferred = []
        simulated_open: list[dict] = []
        config = run.get("config") or {}
        for level in new_levels:
            side = Side(level["side"])
            price = _decimal(level["price"])
            quantity = _decimal(level["quantity"])
            semantic = evaluate_order_semantics(
                GridType(config["grid_type"]),
                inventory,
                _decimal(config["max_inventory_lots"]),
                side,
                quantity,
                simulated_open,
            )
            post_only = validate_post_only_price(side, round_price_for_side(price, spec.tick_size, side), spec.best_bid, spec.best_ask)
            reasons = [*semantic.reason_codes, *post_only.reason_codes]
            lower_price = _decimal(config.get("lower_price"))
            upper_price = _decimal(config.get("upper_price"))
            market_price = spec.mark_price or spec.last_price
            if semantic.opens_inventory and market_price and (market_price < lower_price or market_price > upper_price):
                reasons.append("MARKET_OUTSIDE_CONFIGURED_GRID_RANGE")
            item = {"level_id": level["level_id"], "side": side.value, "price": str(price), "quantity": str(quantity), "reason_codes": reasons}
            if reasons:
                deferred.append(item)
            else:
                create.append(item)
                simulated_open.append(
                    {
                        "side": side.value,
                        "remaining_quantity": str(quantity),
                        "requested_quantity": str(quantity),
                        "status": "open",
                        "opens_inventory": semantic.opens_inventory,
                    }
                )
        return {
            "remain": [],
            "preserve_candidates": preserve_candidates,
            "cancel": existing_open,
            "create": create,
            "defer": deferred,
        }

    def preview_edit_grid(self, run_id: str | None = None, payload: dict | None = None) -> dict:
        state = self._load()
        run = state.get("runs", {}).get(run_id or state.get("active_run_id"))
        if not run:
            raise RuntimeError("No active durable DeltaGridBot run found.")
        if run.get("status") in {GridStatus.STOPPING.value, STOP_ATTENTION_STATUS, GridStatus.STOPPED.value}:
            raise RuntimeError(f"Cannot edit durable DeltaGridBot run from status {run.get('status')}.")
        if run.get("status") not in {GridStatus.RUNNING.value, GridStatus.PAUSED.value, GridStatus.EDITING.value, GridStatus.REGRID_PENDING.value}:
            raise RuntimeError(f"Cannot edit durable DeltaGridBot run from status {run.get('status')}.")
        product_symbol = (run.get("config") or {}).get("product_symbol") or (run.get("product") or {}).get("symbol") or "ETHUSD"
        health = self.product_account_health(product_symbol)
        spec = self.client.product_spec(product_symbol)
        reference = _decimal(health["market"]["reference_price"] or run.get("reference_price"))
        current_config = deepcopy(run.get("config") or {})
        proposed = self._edit_config_from_payload(run, payload or {}, spec, reference, health)
        proposed_dict = to_record_dict(proposed)
        proposed_levels = to_record_dict(build_grid_levels(proposed, reference, spec.tick_size))
        try:
            reconciliation = reconcile_exchange_truth(
                deepcopy(run),
                self.client,
                None,
                suppress_replacements=True,
                persist_order_updates=False,
            )
        except Exception as exc:
            reconciliation = {"errors": [str(exc)], "gridbot_inventory": "0", "delta_position": "0", "exchange_open_orders": 0}
        inventory = _decimal(reconciliation.get("gridbot_inventory"))
        plan_run = {**run, "config": proposed_dict}
        plan = self._edit_order_plan(plan_run, proposed_levels, inventory, spec)
        warnings = []
        if inventory != 0:
            warnings.append("CURRENT_INVENTORY_PRESERVED")
        max_inventory = _decimal(proposed_dict.get("max_inventory_lots"))
        if abs(inventory) > max_inventory:
            warnings.append("CURRENT_INVENTORY_ABOVE_PROPOSED_MAX")
        if reconciliation.get("errors"):
            warnings.append("EXCHANGE_TRUTH_UNAVAILABLE")
        return {
            "ok": not reconciliation.get("errors"),
            "run_id": run["run_id"],
            "current_config_version": int(current_config.get("config_version") or 1),
            "proposed_config_version": int(proposed_dict.get("config_version") or 1),
            "current_config": current_config,
            "proposed_config": proposed_dict,
            "current_inventory": str(inventory),
            "delta_position": reconciliation.get("delta_position"),
            "projected_inventory_limits": {
                "max_inventory_lots": proposed_dict.get("max_inventory_lots"),
                "current_inventory": str(inventory),
                "above_new_max": abs(inventory) > max_inventory,
            },
            "orders": {
                "remain": len(plan["remain"]),
                "preserve_candidates": len(plan["preserve_candidates"]),
                "cancel": len(plan["cancel"]),
                "create": len(plan["create"]),
                "defer": len(plan["defer"]),
                "create_items": plan["create"],
                "defer_items": plan["defer"],
            },
            "validation": {"warnings": warnings, "errors": reconciliation.get("errors") or []},
        }

    def _editing_blocked(self, state: dict, run: dict, reason: str, diagnostics: dict) -> dict:
        run["status"] = GridStatus.EDITING.value
        run["edit_diagnostics"] = {"reason": reason, "updated_at": utc_now(), **diagnostics}
        self._event(state, run["run_id"], "GRID_RUN_EDIT_BLOCKED", run["edit_diagnostics"])
        self._save(state)
        return {"ok": False, "run": deepcopy(run), "requires_attention": True, "diagnostics": deepcopy(run["edit_diagnostics"])}

    def edit_grid(self, run_id: str | None = None, payload: dict | None = None, reason: str = "manual_edit") -> dict:
        payload = payload or {}
        state = self._load()
        run = state.get("runs", {}).get(run_id or state.get("active_run_id"))
        if not run:
            raise RuntimeError("No active durable DeltaGridBot run found.")
        if run.get("status") in {GridStatus.STOPPING.value, STOP_ATTENTION_STATUS, GridStatus.STOPPED.value}:
            raise RuntimeError(f"Stop state takes precedence; cannot edit run from {run.get('status')}.")
        if run.get("status") not in {GridStatus.RUNNING.value, GridStatus.PAUSED.value, GridStatus.EDITING.value, GridStatus.REGRID_PENDING.value}:
            raise RuntimeError(f"Cannot edit durable DeltaGridBot run from status {run.get('status')}.")

        product_symbol = (run.get("config") or {}).get("product_symbol") or (run.get("product") or {}).get("symbol") or "ETHUSD"
        health = self.product_account_health(product_symbol)
        spec = self.client.product_spec(product_symbol)
        reference = _decimal(health["market"]["reference_price"] or run.get("reference_price"))
        previous_status = (run.get("edit_state") or {}).get("previous_status") or run.get("status")
        old_config = deepcopy(run.get("config") or {})
        proposed = self._edit_config_from_payload(run, payload, spec, reference, health)
        new_config = to_record_dict(proposed)
        if _config_fingerprint(old_config) == _config_fingerprint(new_config) and run.get("status") != GridStatus.EDITING.value:
            return {"ok": True, "run": deepcopy(run), "idempotent": True, "preview": self.preview_edit_grid(run["run_id"], payload)}

        if run.get("status") != GridStatus.EDITING.value:
            now = utc_now()
            run["status"] = GridStatus.EDITING.value
            run["status_updated_at"] = now
            run["updated_at"] = now
            run["edit_state"] = {
                "previous_status": previous_status,
                "from_config_version": int(old_config.get("config_version") or 1),
                "to_config_version": int(new_config.get("config_version") or 1),
                "fingerprint": list(_config_fingerprint(new_config)),
                "stage": "FREEZE_PLACEMENT",
                "reason": reason,
                "started_at": utc_now(),
                "target_config": new_config,
            }
            self._event(state, run["run_id"], "GRID_RUN_EDITING", {"previous_status": previous_status, "to_config_version": new_config["config_version"], "reason": reason})
            self._save(state)
            state = self._load()
            run = state["runs"][run["run_id"]]
        else:
            new_config = (run.get("edit_state") or {}).get("target_config") or new_config

        reconciliation_result = self.reconcile(run["run_id"], process_replacements=False, persist_snapshot=False, log_reconcile_event=False)
        state = self._load()
        run = state["runs"][run["run_id"]]
        if run.get("status") == GridStatus.STOPPING.value:
            return self.stop(run["run_id"], reason="stop_preempted_edit")
        reconciliation = reconciliation_result["reconciliation"]
        if reconciliation.get("errors"):
            return self._editing_blocked(state, run, "exchange_truth_unavailable", {"errors": reconciliation.get("errors")})
        if int(reconciliation.get("unresolved_orders") or 0) or int(reconciliation.get("fill_ledger_mismatches") or 0):
            return self._editing_blocked(state, run, "reconciliation_unresolved", {"reconciliation": reconciliation})
        telemetry = self.account_telemetry.get(product_symbol, force=True)
        allowed, telemetry_reasons = risk_increasing_action_allowed(telemetry)
        if not allowed:
            return self._editing_blocked(state, run, "critical_telemetry_unavailable", {"reason_codes": telemetry_reasons})

        product_id = int(run["product"]["product_id"])
        cancelled = 0
        for order in list((run.get("orders") or {}).values()):
            if order.get("status") in START_TERMINAL_ORDER_STATUSES or order.get("order_kind") == "safety_flatten":
                continue
            self._cancel_order_safely(product_id, order)
            order["superseded_by_config_version"] = new_config["config_version"]
            cancelled += 1
        deferred_superseded = self._terminalize_never_submitted_orders(run, status="superseded", reason="edit_grid_new_config")
        self._save(state)

        state = self._load()
        latest = state.get("runs", {}).get(run["run_id"])
        if latest and latest.get("status") == GridStatus.STOPPING.value:
            return self.stop(run["run_id"], reason="stop_preempted_edit")
        run = latest or run
        old_config = deepcopy(run.get("config") or old_config)
        new_config["effective_from"] = utc_now()
        new_config_obj = GridConfig(
            bot_id=new_config["bot_id"],
            config_version=int(new_config["config_version"]),
            bot_name=new_config["bot_name"],
            product_symbol=new_config["product_symbol"],
            grid_type=GridType(new_config["grid_type"]),
            lower_price=_decimal(new_config["lower_price"]),
            upper_price=_decimal(new_config["upper_price"]),
            grid_count=int(new_config["grid_count"]),
            spacing_type=SpacingType(new_config["spacing_type"]),
            lot_size=_decimal(new_config["lot_size"]),
            max_inventory_lots=_decimal(new_config["max_inventory_lots"]),
            allocated_capital=_decimal(new_config["allocated_capital"]),
            risk_capital=_decimal(new_config["risk_capital"]),
            risk_thresholds=new_config.get("risk_thresholds") or DEFAULT_RISK_THRESHOLDS,
        )
        run["config_history"] = run.get("config_history", []) + [old_config]
        run["config"] = {**to_record_dict(new_config_obj), "effective_from": new_config["effective_from"]}
        run["levels"] = to_record_dict(build_grid_levels(new_config_obj, reference, spec.tick_size))
        run["reference_price"] = str(reference)
        if self._db_enabled():
            self.db.retire_config(run["run_id"], int(old_config["config_version"]))
            self.db.persist_config(run, reason="edit_grid")
            self.db.persist_levels(run)
            self.db.insert_once(
                "grid_parameter_changes",
                {
                    "change_id": new_id("chg"),
                    "run_id": run["run_id"],
                    "bot_id": run["bot_id"],
                    "from_config_version": int(old_config["config_version"]),
                    "to_config_version": int(run["config"]["config_version"]),
                    "reason": reason,
                    "payload": {"old_config": old_config, "new_config": run["config"], "cancelled_orders": cancelled, "deferred_superseded": deferred_superseded},
                    "created_at": utc_now(),
                },
                on_conflict="change_id",
            )

        created = deferred = 0
        inventory = _decimal(reconciliation.get("gridbot_inventory"))
        if previous_status == GridStatus.RUNNING.value:
            try:
                for level in run["levels"]:
                    proposal = self._proposal_for_level(run["run_id"], level, int(run["sequence"]))
                    order = self._place_proposal(run, product_id, proposal, "edit_grid", current_inventory=inventory)
                    if order.get("status") == "deferred":
                        deferred += 1
                    else:
                        created += 1
            except Exception as exc:
                for order in list((run.get("orders") or {}).values()):
                    if order.get("order_kind") == "edit_grid" and order.get("status") not in START_TERMINAL_ORDER_STATUSES:
                        self._cancel_order_safely(product_id, order)
                now = utc_now()
                run["status"] = GridStatus.PAUSED.value
                run["status_updated_at"] = now
                run["updated_at"] = now
                run["edit_state"] = {
                    **(run.get("edit_state") or {}),
                    "stage": "PLACEMENT_FAILED",
                    "failed_at": now,
                    "created_orders": created,
                    "deferred_orders": deferred,
                    "cancelled_orders": cancelled,
                    "deferred_superseded": deferred_superseded,
                    "error": str(exc)[:500],
                }
                run["edit_diagnostics"] = {"reason": "placement_failed", "error": str(exc)[:500], "updated_at": now}
                self._event(state, run["run_id"], "GRID_RUN_EDIT_PLACEMENT_FAILED", run["edit_diagnostics"])
                self._save(state)
                return {"ok": False, "run": deepcopy(run), "requires_attention": True, "diagnostics": deepcopy(run["edit_diagnostics"]), "edit": deepcopy(run["edit_state"])}
        self._save(state)

        verified = self.reconcile(run["run_id"], process_replacements=False, persist_snapshot=True)
        state = self._load()
        run = state["runs"][run["run_id"]]
        if verified["reconciliation"].get("errors"):
            return self._editing_blocked(state, run, "verification_failed", {"reconciliation": verified["reconciliation"]})
        now = utc_now()
        run["status"] = GridStatus.RUNNING.value if previous_status == GridStatus.RUNNING.value else GridStatus.PAUSED.value
        run["status_updated_at"] = now
        run["updated_at"] = now
        run["edit_state"] = {
            **(run.get("edit_state") or {}),
            "stage": "COMPLETE",
            "completed_at": utc_now(),
            "created_orders": created,
            "deferred_orders": deferred,
            "cancelled_orders": cancelled,
            "deferred_superseded": deferred_superseded,
        }
        self._event(
            state,
            run["run_id"],
            "GRID_RUN_EDIT_APPLIED",
            {
                "from_config_version": int(old_config["config_version"]),
                "to_config_version": int(run["config"]["config_version"]),
                "previous_status": previous_status,
                "final_status": run["status"],
                "created_orders": created,
                "deferred_orders": deferred,
                "cancelled_orders": cancelled,
                "gridbot_inventory": verified["reconciliation"].get("gridbot_inventory"),
            },
        )
        self._save(state)
        return {"ok": True, "run": deepcopy(run), "reconciliation": verified["reconciliation"], "edit": deepcopy(run["edit_state"])}

    def regrid(self, run_id: str | None = None) -> dict:
        state = self._load()
        run = state.get("runs", {}).get(run_id or state.get("active_run_id"))
        if not run:
            raise RuntimeError("No active durable DeltaGridBot run found.")
        reference = _decimal(run["reference_price"])
        old_config = deepcopy(run["config"])
        spec = self.client.product_spec("ETHUSD")
        width = max(abs(_decimal(old_config["upper_price"]) - _decimal(old_config["lower_price"])) / Decimal("2"), Decimal("45"))
        payload = {
            "lower_price": str(quantize_price(reference - width - Decimal("5"), spec.tick_size)),
            "upper_price": str(quantize_price(reference + width + Decimal("5"), spec.tick_size)),
        }
        return self.edit_grid(run["run_id"], payload, reason="legacy_regrid_compat")

    def _legacy_regrid(self, run_id: str | None = None) -> dict:
        paused = self.pause(run_id)
        state = self._load()
        run = state["runs"][paused["run"]["run_id"]]
        spec = self.client.product_spec("ETHUSD")
        old_config = deepcopy(run["config"])
        reference = _decimal(run["reference_price"])
        width = max(abs(_decimal(old_config["upper_price"]) - _decimal(old_config["lower_price"])) / Decimal("2"), Decimal("45"))
        new_config = {**old_config, "config_version": int(old_config["config_version"]) + 1}
        new_config["lower_price"] = str(quantize_price(reference - width - Decimal("5"), spec.tick_size))
        new_config["upper_price"] = str(quantize_price(reference + width + Decimal("5"), spec.tick_size))
        config_obj = GridConfig(
            bot_id=new_config["bot_id"],
            config_version=int(new_config["config_version"]),
            bot_name=new_config["bot_name"],
            product_symbol=new_config["product_symbol"],
            grid_type=GridType(new_config["grid_type"]),
            lower_price=_decimal(new_config["lower_price"]),
            upper_price=_decimal(new_config["upper_price"]),
            grid_count=int(new_config["grid_count"]),
            spacing_type=SpacingType(new_config["spacing_type"]),
            lot_size=_decimal(new_config["lot_size"]),
            max_inventory_lots=_decimal(new_config["max_inventory_lots"]),
            allocated_capital=_decimal(new_config["allocated_capital"]),
            risk_capital=_decimal(new_config["risk_capital"]),
            risk_thresholds=new_config.get("risk_thresholds") or DEFAULT_RISK_THRESHOLDS,
        )
        run["config_history"] = run.get("config_history", []) + [old_config]
        run["config"] = to_record_dict(config_obj)
        run["levels"] = to_record_dict(build_grid_levels(config_obj, reference, spec.tick_size))
        now = utc_now()
        run["status"] = GridStatus.REGRID_PENDING.value
        run["status_updated_at"] = now
        run["updated_at"] = now
        self._event(state, run["run_id"], "GRID_REGRID_APPLIED", {"config_version": new_config["config_version"]})
        if self._db_enabled():
            self.db.retire_config(run["run_id"], int(old_config["config_version"]))
            self.db.persist_config(run, reason="regrid")
            self.db.persist_levels(run)
            self.db.insert_once(
                "grid_parameter_changes",
                {
                    "change_id": new_id("chg"),
                    "run_id": run["run_id"],
                    "bot_id": run["bot_id"],
                    "from_config_version": int(old_config["config_version"]),
                    "to_config_version": int(new_config["config_version"]),
                    "reason": "manual_regrid",
                    "payload": {"old_config": old_config, "new_config": run["config"]},
                    "created_at": utc_now(),
                },
                on_conflict="change_id",
            )
        self._save(state)
        return self.resume(run["run_id"])

    def stop(self, run_id: str | None = None, reason: str = "manual") -> dict:
        state = self._load()
        run = state.get("runs", {}).get(run_id or state.get("active_run_id"))
        if not run and run_id and self._db_enabled():
            run = self.db.load_run_state(run_id)
            state.setdefault("runs", {})[run_id] = run
        if not run:
            raise RuntimeError("No active durable DeltaGridBot run found.")
        if run.get("status") == GridStatus.STOPPED.value and run.get("summary"):
            if self._db_enabled():
                self.db.resolve_health_issue_codes(
                    run["run_id"],
                    {"LIFECYCLE_STUCK", "WORKER_STALLED", "RECONCILIATION_STALE"},
                )
            return {"ok": True, "run": deepcopy(run), "summary": deepcopy(run["summary"])}
        if run.get("status") not in {GridStatus.STOPPING.value, STOP_ATTENTION_STATUS}:
            previous_status = run.get("status")
            now = utc_now()
            run["status"] = GridStatus.STOPPING.value
            run["status_updated_at"] = now
            run["updated_at"] = now
            run["stop_reason"] = reason
            if run.get("start_stage"):
                run["start_stage"] = "STOPPING"
                run["startup"] = self._startup_progress(run, "STOPPING")
            self._event(state, run["run_id"], "GRID_RUN_STOPPING", {"previous_status": previous_status, "reason": reason})
            self._save(state)
            state = self._load()
            run = state["runs"][run["run_id"]]
        elif run.get("status") == STOP_ATTENTION_STATUS:
            now = utc_now()
            run["status"] = GridStatus.STOPPING.value
            run["status_updated_at"] = now
            run["updated_at"] = now
            run["stop_reason"] = reason
            self._event(state, run["run_id"], "GRID_RUN_STOP_RETRYING", {"reason": reason, "previous_diagnostics": run.get("stop_diagnostics")})
            self._save(state)
            state = self._load()
            run = state["runs"][run["run_id"]]

        product_id = int(run["product"]["product_id"])
        cancelled_attempts = 0
        try:
            exchange_open_gridbot_orders = _gridbot_orders(_result_rows(self.client.open_orders(product_id)))
        except Exception as exc:
            return self._stop_attention(state, run, reason, {"reason": "exchange_truth_unavailable", "errors": [str(exc)]})
        for exchange_order in exchange_open_gridbot_orders:
            cid = str(exchange_order.get("client_order_id") or "")
            local = run.setdefault("orders", {}).get(cid)
            if local and local.get("order_kind") == "safety_flatten":
                continue
            if local:
                if self._cancel_order_safely(product_id, local):
                    cancelled_attempts += 1
            else:
                try:
                    self.client.cancel_order(product_id, str(exchange_order.get("id") or exchange_order.get("order_id")))
                    cancelled_attempts += 1
                except Exception:
                    pass
        self._terminalize_never_submitted_orders(run)
        self._save(state, include_children=False)

        reconciled = self.reconcile(run["run_id"], process_replacements=False)
        state = self._load()
        run = state["runs"][run["run_id"]]
        reconciliation = reconciled["reconciliation"]
        if reconciliation.get("errors"):
            return self._stop_attention(state, run, reason, {"reason": "exchange_truth_unavailable", "errors": reconciliation.get("errors"), "cancel_attempts": cancelled_attempts})

        for _ in range(5):
            gridbot_inventory = _decimal(reconciliation.get("gridbot_inventory"))
            delta_position = _decimal(reconciliation.get("delta_position"))
            open_gridbot_orders = int(reconciliation.get("exchange_open_orders") or 0)
            unresolved = int(reconciliation.get("unresolved_orders") or 0)
            fill_mismatches = int(reconciliation.get("fill_ledger_mismatches") or 0)
            if unresolved or fill_mismatches:
                return self._stop_attention(
                    state,
                    run,
                    reason,
                    {
                        "reason": "unresolved_gridbot_order",
                        "unresolved_orders": unresolved,
                        "fill_ledger_mismatches": fill_mismatches,
                        "reconciliation": reconciliation,
                    },
                )
            if open_gridbot_orders:
                for exchange_order in _gridbot_orders(_result_rows(self.client.open_orders(product_id))):
                    cid = str(exchange_order.get("client_order_id") or "")
                    local = run.setdefault("orders", {}).get(cid)
                    if local and local.get("order_kind") == "safety_flatten":
                        continue
                    if local:
                        self._cancel_order_safely(product_id, local)
                    else:
                        self.client.cancel_order(product_id, str(exchange_order.get("id") or exchange_order.get("order_id")))
                self._save(state)
                reconciled = self.reconcile(run["run_id"], process_replacements=False, persist_order_updates=False)
                state = self._load()
                run = state["runs"][run["run_id"]]
                reconciliation = reconciled["reconciliation"]
                if reconciliation.get("errors"):
                    return self._stop_attention(state, run, reason, {"reason": "exchange_truth_unavailable", "errors": reconciliation.get("errors")})
                continue
            if gridbot_inventory != delta_position:
                if gridbot_inventory != 0:
                    try:
                        recovered = self._recover_flatten_order(run, product_id, gridbot_inventory)
                    except Exception as exc:
                        return self._stop_attention(
                            state,
                            run,
                            reason,
                            {"reason": "flatten_state_ambiguous", "error": str(exc)[:500], "reconciliation": reconciliation},
                        )
                    if recovered:
                        self._event(
                            state,
                            run["run_id"],
                            "GRID_RUN_STOP_FLATTEN_RECOVERED",
                            {"client_order_id": recovered.get("client_order_id"), "inventory_before_recovery": str(gridbot_inventory)},
                        )
                        self._save(state, include_children=False)
                        reconciled = self.reconcile(run["run_id"], process_replacements=False, persist_order_updates=False)
                        state = self._load()
                        run = state["runs"][run["run_id"]]
                        reconciliation = reconciled["reconciliation"]
                        if reconciliation.get("errors"):
                            return self._stop_attention(state, run, reason, {"reason": "exchange_truth_unavailable_after_flatten_recovery", "errors": reconciliation.get("errors")})
                        continue
                    if delta_position == 0 and open_gridbot_orders == 0:
                        now = utc_now()
                        run["external_position_resolution"] = {
                            "status": "EXTERNALLY_RESOLVED",
                            "reason": "delta_flat_gridbot_ledger_stale",
                            "gridbot_inventory_before_resolution": str(gridbot_inventory),
                            "delta_position": str(delta_position),
                            "resolved_at": now,
                            "accounting_status": "PARTIAL",
                            "accounting_warning": "EXTERNAL_POSITION_CLOSE_UNATTRIBUTED",
                        }
                        run["stop_diagnostics"] = {
                            **(run.get("stop_diagnostics") or {}),
                            "external_position_resolution": deepcopy(run["external_position_resolution"]),
                            "updated_at": now,
                        }
                        self._event(state, run["run_id"], "GRID_RUN_STOP_EXTERNAL_POSITION_RESOLVED", run["external_position_resolution"])
                        self._save(state, include_children=False)
                        break
                return self._stop_attention(
                    state,
                    run,
                    reason,
                    {
                        "reason": "attribution_mismatch",
                        "gridbot_inventory": str(gridbot_inventory),
                        "delta_position": str(delta_position),
                        "unexplained_difference": str(delta_position - gridbot_inventory),
                        "reconciliation": reconciliation,
                    },
                )
            if gridbot_inventory == 0:
                break
            try:
                self._recover_or_place_flatten_order(run, product_id, gridbot_inventory)
                self._event(
                    state,
                    run["run_id"],
                    "GRID_RUN_STOP_FLATTEN_SUBMITTED",
                    {"inventory": str(gridbot_inventory), "side": (Side.SELL if gridbot_inventory > 0 else Side.BUY).value},
                )
                self._save(state)
            except Exception as exc:
                return self._stop_attention(state, run, reason, {"reason": "flatten_submission_failed", "error": str(exc)[:500], "inventory": str(gridbot_inventory)})
            reconciled = self.reconcile(run["run_id"], process_replacements=False, persist_order_updates=False)
            state = self._load()
            run = state["runs"][run["run_id"]]
            reconciliation = reconciled["reconciliation"]
            if reconciliation.get("errors"):
                return self._stop_attention(state, run, reason, {"reason": "exchange_truth_unavailable_after_flatten", "errors": reconciliation.get("errors")})
        else:
            return self._stop_attention(state, run, reason, {"reason": "flatten_not_resolved", "reconciliation": reconciliation})

        position = _decimal(reconciliation.get("delta_position"))
        final_inventory = _decimal(reconciliation.get("gridbot_inventory"))
        open_orders = _gridbot_orders(_result_rows(self.client.open_orders(product_id)))
        external_resolution = (run.get("external_position_resolution") or {}).get("status") == "EXTERNALLY_RESOLVED"
        if (final_inventory != 0 and not external_resolution) or open_orders or position != 0:
            return self._stop_attention(
                state,
                run,
                reason,
                {
                    "reason": "final_stop_gate_failed",
                    "gridbot_inventory": str(final_inventory),
                    "delta_position": str(position),
                    "open_gridbot_orders": len(open_orders),
                    "external_position_resolution": run.get("external_position_resolution"),
                },
            )

        summary = self._build_stop_summary(run, reason, reconciliation, position, open_orders)
        if run.get("summary"):
            summary = run["summary"]
        run["summary"] = summary
        now = utc_now()
        run["status"] = GridStatus.STOPPED.value
        run["status_updated_at"] = now
        run["updated_at"] = now
        run["stopped_at"] = summary["stopped_at"]
        run["stop_reason"] = reason
        if state.get("active_run_id") == run["run_id"]:
            state["active_run_id"] = None
        self._event(state, run["run_id"], "GRID_RUN_SUMMARY_GENERATED", {"summary_id": summary["summary_id"]})
        if self._db_enabled():
            self.db.persist_summary(run, summary)
            self.db.release_active_run_guard(run["run_id"])
            self.db.resolve_health_issue_codes(
                run["run_id"],
                {"LIFECYCLE_STUCK", "WORKER_STALLED", "RECONCILIATION_STALE"},
            )
        self._save(state, include_children=False)
        return {"ok": True, "run": deepcopy(run), "summary": deepcopy(summary)}


def first_real_grid_validation(state_path: str | Path = DEFAULT_STATE_PATH, restart_service: bool = True) -> dict:
    lifecycle = DurableGridBotLifecycle(state_path=state_path)
    started = lifecycle.start_tiny_grid()
    run_id = started["run"]["run_id"]
    reconciled = lifecycle.reconcile(run_id)
    restarted = False
    recovered = None
    if restart_service:
        try:
            subprocess.run(["systemctl", "restart", "eth-fastapi"], check=True, timeout=20)
            time.sleep(5)
            restarted = True
        except Exception as exc:
            recovered = {"restart_error": str(exc)[:300]}
    recovered_status = DurableGridBotLifecycle(state_path=state_path).status()
    recovered_run_id = recovered_status.get("active_run_id")
    paused = lifecycle.pause(run_id)
    resumed = lifecycle.resume(run_id)
    regridded = lifecycle.regrid(run_id)
    stopped = lifecycle.stop(run_id, "v0.1_first_real_grid_validation")
    final_status = DurableGridBotLifecycle(state_path=state_path).status()
    return {
        "ok": stopped["summary"]["stray_gridbot_orders"] == 0 and stopped["summary"]["final_position"] == "0" and recovered_run_id == run_id,
        "run_id": run_id,
        "started_orders": len(started["run"].get("orders", {})),
        "initial_reconcile": {"new_fills": reconciled["new_fills"], "position": reconciled["position"], "open_gridbot_orders": reconciled["open_gridbot_orders"]},
        "fastapi_restarted": restarted,
        "restart_recovery": recovered or {"active_run_id_after_restart": recovered_run_id, "same_run": recovered_run_id == run_id},
        "pause_status": paused["run"]["status"],
        "resume_status": resumed["run"]["status"],
        "regrid_status": regridded["run"]["status"],
        "summary": stopped["summary"],
        "final_active_run_id": final_status.get("active_run_id"),
    }
