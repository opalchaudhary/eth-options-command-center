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

from .config import DEFAULT_RISK_THRESHOLDS, GRIDBOT_VERSION
from .delta_testnet_client import DeltaTestnetClient
from .execution import make_client_order_id, order_payload
from .exchange_truth import reconcile_exchange_truth
from .grid_builder import build_grid_levels, preview_grid, quantize_price
from .models import GridConfig, GridStatus, GridType, OrderProposal, Side, SpacingType, new_id, to_record_dict, utc_now
from .semantics import evaluate_order_semantics, round_price_for_side, validate_post_only_price
from .supabase_repository import SupabaseGridRepository, SupabasePersistenceError


ACTIVE_STATUSES = {GridStatus.STARTING.value, GridStatus.RUNNING.value, GridStatus.PAUSED.value, GridStatus.REGRID_PENDING.value}
GRIDBOT_ORDER_PREFIX = "DGB01-"
DEFAULT_STATE_PATH = Path(os.getenv("GRIDBOT_V01_STATE_PATH", "grid_bot_state_v01.json"))
START_TERMINAL_ORDER_STATUSES = {"cancelled", "closed", "filled", "not_open", "manual_cancelled"}
_START_WORKERS: dict[str, threading.Thread] = {}
_START_WORKERS_LOCK = threading.Lock()


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

    def _db_enabled(self) -> bool:
        return bool(self.db and self.db.enabled)

    def _load(self) -> dict:
        if self._db_enabled():
            active = self.db.active_run()
            if active:
                run = self.db.load_run_state(active["run_id"])
                return {"runs": {run["run_id"]: run}, "active_run_id": run["run_id"], "events": []}
        if not self.state_path.exists():
            return {"runs": {}, "active_run_id": None, "events": []}
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def _save(self, state: dict) -> None:
        with self._save_lock:
            if self._db_enabled():
                for run in (state.get("runs") or {}).values():
                    self.db.persist_run_state(run)
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
        spec = self.client.product_spec(product_symbol)
        bid = spec.best_bid or spec.mark_price
        ask = spec.best_ask or spec.mark_price
        reference = quantize_price((bid + ask) / Decimal("2"), spec.tick_size)
        position = _position_size(self.client, spec.product_id)
        open_gridbot_orders = len(_gridbot_orders(_result_rows(self.client.open_orders(spec.product_id))))
        margin_payload = self.client.account_margin()
        margin_result = margin_payload.get("result") if isinstance(margin_payload.get("result"), dict) else margin_payload
        account_equity = _first_number(
            margin_result,
            ["account_equity", "equity", "net_equity", "balance", "portfolio_value", "collateral"],
        )
        available_margin = _first_number(
            margin_result,
            ["available_margin", "available_balance", "free_margin", "available_collateral"],
        )
        margin_used = _first_number(
            margin_result,
            ["margin_used", "used_margin", "blocked_margin", "initial_margin", "total_margin"],
        )
        margin_utilisation = None
        if account_equity and margin_used is not None and account_equity > 0:
            margin_utilisation = margin_used / account_equity
        elif account_equity and available_margin is not None and account_equity > 0:
            margin_utilisation = max(Decimal("0"), (account_equity - available_margin) / account_equity)
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
                "account_equity": str(account_equity) if account_equity is not None else None,
                "available_margin": str(available_margin) if available_margin is not None else None,
                "margin_used": str(margin_used) if margin_used is not None else None,
                "margin_utilisation": str(margin_utilisation) if margin_utilisation is not None else None,
                "current_position": str(position),
                "open_gridbot_orders": open_gridbot_orders,
                "portfolio_greeks": {"delta": None, "gamma": None, "vega": None, "theta": None},
                "raw_available": bool(margin_payload.get("success", True)),
            },
        }

    def _config_from_operator_payload(self, payload: dict, spec, reference: Decimal, health: dict) -> GridConfig:
        max_inventory = _decimal(payload.get("max_inventory_lots"), "1")
        lot_size = max(_decimal(payload.get("lot_size"), str(spec.min_quantity)), spec.min_quantity)
        account_equity = _decimal((health.get("account") or {}).get("account_equity"), "0")
        projected_exposure = max_inventory * reference * spec.contract_multiplier
        denominator = account_equity if account_equity > 0 else max(projected_exposure, Decimal("1"))
        return GridConfig(
            bot_id=payload.get("bot_id") or new_id("bot"),
            config_version=int(payload.get("config_version") or 1),
            bot_name=payload.get("bot_name") or "DeltaGridBot V0.1 Operator Grid",
            product_symbol=payload.get("product_symbol") or spec.symbol,
            grid_type=GridType(payload.get("grid_type") or GridType.NEUTRAL.value),
            lower_price=quantize_price(_decimal(payload.get("lower_price")), spec.tick_size),
            upper_price=quantize_price(_decimal(payload.get("upper_price")), spec.tick_size),
            grid_count=int(payload.get("grid_count") or 4),
            spacing_type=SpacingType(payload.get("spacing_type") or SpacingType.ARITHMETIC.value),
            lot_size=lot_size,
            max_inventory_lots=max_inventory,
            allocated_capital=denominator,
            risk_capital=denominator,
            risk_thresholds=DEFAULT_RISK_THRESHOLDS,
        )

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
                existing = [
                    order
                    for order in run.get("orders", {}).values()
                    if order.get("level_id") == level.get("level_id") and order.get("status") not in START_TERMINAL_ORDER_STATUSES
                ]
                if existing:
                    continue
                proposal = self._proposal_for_level(run_id, level, int(run["sequence"]))
                self._place_proposal(run, product_id, proposal, "initial_grid")
                run["startup"] = self._startup_progress(run, "PLACING_ORDERS")
                self._save(state)
            self._set_start_stage(state, run, "VERIFYING_ORDERS")
            self.reconcile(run_id)
            state = self._load()
            run = state["runs"][run_id]
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

    def _open_order_records(self, run: dict) -> list[dict]:
        return [
            order
            for order in (run.get("orders") or {}).values()
            if order.get("status") not in START_TERMINAL_ORDER_STATUSES
        ]

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
        self._save(state)
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
        for order in run.get("orders", {}).values():
            if order.get("status") not in START_TERMINAL_ORDER_STATUSES:
                self._cancel_order_safely(product_id, order)
        run["status"] = GridStatus.PAUSED.value
        self._event(state, run["run_id"], "GRID_RUN_PAUSED", {})
        self._save(state)
        return {"ok": True, "run": deepcopy(run)}

    def resume(self, run_id: str | None = None) -> dict:
        state = self._load()
        run = state.get("runs", {}).get(run_id or state.get("active_run_id"))
        if not run:
            raise RuntimeError("No active durable DeltaGridBot run found.")
        product_id = int(run["product"]["product_id"])
        for level in run["levels"]:
            existing_open = [
                order
                for order in run.get("orders", {}).values()
                if order["level_id"] == level["level_id"] and order.get("status") not in START_TERMINAL_ORDER_STATUSES
            ]
            if not existing_open:
                proposal = self._proposal_for_level(run["run_id"], level, int(run["sequence"]))
                self._place_proposal(run, product_id, proposal, "resume_grid")
        run["status"] = GridStatus.RUNNING.value
        self._event(state, run["run_id"], "GRID_RUN_RESUMED", {})
        self._save(state)
        return {"ok": True, "run": deepcopy(run)}

    def regrid(self, run_id: str | None = None) -> dict:
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
        run["status"] = GridStatus.REGRID_PENDING.value
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
        if not run:
            raise RuntimeError("No active durable DeltaGridBot run found.")
        product_id = int(run["product"]["product_id"])
        for order in run.get("orders", {}).values():
            if order.get("status") not in START_TERMINAL_ORDER_STATUSES:
                self._cancel_order_safely(product_id, order)
        self._save(state)
        self.reconcile(run["run_id"])
        state = self._load()
        run = state["runs"][run["run_id"]]
        position = _position_size(self.client, product_id)
        open_orders = _gridbot_orders(_result_rows(self.client.open_orders(product_id)))
        fills = list(run.get("fills", {}).values())
        spec_multiplier = _decimal(run["product"].get("contract_multiplier"), "1")
        gross = _gross_pnl(fills, spec_multiplier)
        fees = _fee_total(fills)
        summary = {
            "summary_id": new_id("summary"),
            "run_id": run["run_id"],
            "gridbot_version": GRIDBOT_VERSION,
            "execution_event_mode": run.get("execution_event_mode"),
            "private_ws_available": False,
            "private_ws_status": run.get("private_ws_status"),
            "started_at": run.get("started_at"),
            "stopped_at": utc_now(),
            "stop_reason": reason,
            "orders_total": len(run.get("orders", {})),
            "fills_total": len(fills),
            "gross_pnl": str(gross),
            "delta_fees": str(fees),
            "funding": "0",
            "other_delta_costs_credits": "0",
            "NET_TRADING_PNL_BEFORE_INCOME_TAX": str(gross - fees),
            "final_position": str(position),
            "stray_gridbot_orders": len(open_orders),
            "immutable": True,
            "created_at": utc_now(),
        }
        if run.get("summary"):
            summary = run["summary"]
        run["summary"] = summary
        run["status"] = GridStatus.STOPPED.value
        run["stopped_at"] = summary["stopped_at"]
        run["stop_reason"] = reason
        if state.get("active_run_id") == run["run_id"]:
            state["active_run_id"] = None
        self._event(state, run["run_id"], "GRID_RUN_SUMMARY_GENERATED", {"summary_id": summary["summary_id"]})
        if self._db_enabled():
            self.db.persist_summary(run, summary)
            self.db.release_active_run_guard(run["run_id"])
        self._save(state)
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
