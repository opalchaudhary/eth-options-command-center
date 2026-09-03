import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

import requests

import storage
from .accounting import FEE_CONFIRMED, build_run_accounting, cycle_to_row, decimal_value, extract_fee, fill_notional, normalize_maker_taker_role
from .health import HealthIssue
from .models import new_id, utc_now


ACTIVE_STATUSES = {"STARTING", "RUNNING", "PAUSING", "PAUSED", "RESUMING", "EDITING", "REGRID_PENDING", "STOPPING", "STOP_REQUIRES_ATTENTION"}
SNAPSHOT_DEDUPE_SECONDS = int(os.getenv("GRIDBOT_V01_SNAPSHOT_DEDUPE_SECONDS", "60"))
DEPLOYMENT_OPEN_STATUSES = {"open", "partially_filled"}
DEPLOYMENT_VALID_STATUSES = {"confirmed_open", "filled", "deferred"}
DEPLOYMENT_UNRESOLVED_STATUSES = {"submit_pending", "cancel_pending", "unknown", "unresolved", "ambiguous", "replace_pending"}
DEPLOYMENT_DEFERRED_STATUSES = {"deferred", "blocked"}


class SupabasePersistenceError(RuntimeError):
    pass


def _jsonable(value: Any) -> Any:
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    return value


def _decimalish(value: Any) -> str | None:
    if value in [None, ""]:
        return None
    return str(value)


def _risk_telemetry(risk: dict) -> dict:
    telemetry = risk.get("account_risk_state") or {}
    if not isinstance(telemetry, dict):
        return {}
    return telemetry


def _risk_value(risk: dict, telemetry: dict, *keys: str) -> str | None:
    for key in keys:
        if key in risk and risk.get(key) not in [None, ""]:
            return _decimalish(risk.get(key))
        if key in telemetry and telemetry.get(key) not in [None, ""]:
            return _decimalish(telemetry.get(key))
    return None


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _snapshot_dedupe_signature(row: dict | None) -> tuple:
    metrics = ((row or {}).get("margin_metrics") or (row or {}).get("margin_state") or {}) if isinstance(row, dict) else {}
    return (
        (row or {}).get("risk_state"),
        (row or {}).get("active_config_version") or metrics.get("config_version"),
        str((row or {}).get("inventory") if (row or {}).get("inventory") is not None else metrics.get("gridbot_inventory")),
        str(metrics.get("delta_position")),
        str((row or {}).get("open_orders") if (row or {}).get("open_orders") is not None else metrics.get("open_gridbot_orders")),
        ((row or {}).get("payload") or {}).get("run", {}).get("status") if isinstance((row or {}).get("payload"), dict) else None,
    )


def _reconstructed_deployment_completeness(config: dict, levels: list[dict], orders: list[dict]) -> dict:
    config_version = int(config.get("config_version") or 1)
    current_orders = [
        order
        for order in orders
        if int(order.get("config_version") or config_version) == config_version and order.get("order_kind") != "safety_flatten"
    ]
    by_level: dict[str, list[dict]] = {}
    for order in current_orders:
        by_level.setdefault(str(order.get("level_id") or ""), []).append(order)
    counts = {
        "expected": len(levels),
        "eligible": len(levels),
        "confirmed_open": 0,
        "filled": 0,
        "deferred": 0,
        "terminal": 0,
        "ambiguous": 0,
        "missing": 0,
    }
    level_states = []
    buy_valid = sell_valid = 0
    reasons = []
    for level in levels:
        level_id = str(level.get("level_id") or "")
        side = str(level.get("side") or "")
        level_orders = by_level.get(level_id) or []
        status = "missing"
        if level_orders:
            statuses = {str(order.get("status") or "").lower() for order in level_orders}
            if statuses & DEPLOYMENT_UNRESOLVED_STATUSES:
                status = "ambiguous"
            elif statuses & DEPLOYMENT_OPEN_STATUSES:
                status = "confirmed_open"
            elif "filled" in statuses:
                status = "filled"
            elif statuses & DEPLOYMENT_DEFERRED_STATUSES:
                status = "deferred"
            else:
                status = "terminal"
        counts[status] += 1
        if status in DEPLOYMENT_VALID_STATUSES:
            if side == "buy":
                buy_valid += 1
            elif side == "sell":
                sell_valid += 1
        level_states.append({"level_id": level_id, "side": side, "status": status})
    if counts["missing"]:
        reasons.append("MISSING_INTENDED_ORDERS")
    if counts["ambiguous"]:
        reasons.append("AMBIGUOUS_SUBMISSIONS")
    if counts["terminal"]:
        reasons.append("TERMINAL_INTENDED_ORDERS")
    expected_sides = {str(level.get("side") or "") for level in levels}
    if config.get("grid_type") == "neutral" and {"buy", "sell"}.issubset(expected_sides):
        if buy_valid == 0 or sell_valid == 0:
            reasons.append("NEUTRAL_DEPLOYMENT_ONE_SIDED")
    return {
        **counts,
        "complete": not reasons,
        "reasons": reasons,
        "buy_accounted": buy_valid,
        "sell_accounted": sell_valid,
        "level_states": level_states,
        "checked_at": utc_now(),
        "source": "supabase_reconstruction",
    }


class SupabaseGridRepository:
    def __init__(self, url: str | None = None, key: str | None = None, timeout: int = 15):
        self.url = (url or storage.SUPABASE_URL or "").rstrip("/")
        self.key = key or storage.SUPABASE_KEY
        self.timeout = timeout
        self.enabled = bool(self.url and self.key)
        self.request_counts = {
            "select": 0,
            "upsert_rows": 0,
            "insert_once": 0,
            "patch": 0,
            "delete": 0,
            "by_table": {},
        }

    def _count(self, operation: str, table: str, amount: int = 1) -> None:
        self.request_counts[operation] = self.request_counts.get(operation, 0) + amount
        by_table = self.request_counts.setdefault("by_table", {})
        table_counts = by_table.setdefault(table, {})
        table_counts[operation] = table_counts.get(operation, 0) + amount

    def stats(self) -> dict:
        return {
            **{key: value for key, value in self.request_counts.items() if key != "by_table"},
            "by_table": {
                table: dict(counts)
                for table, counts in self.request_counts.get("by_table", {}).items()
            },
        }

    @property
    def headers(self) -> dict:
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, table: str, *, params: dict | None = None, json: Any = None, prefer: str | None = None) -> Any:
        if not self.enabled:
            raise SupabasePersistenceError("Supabase credentials are not configured.")
        headers = self.headers
        if prefer:
            headers = {**headers, "Prefer": prefer}
        response = requests.request(
            method,
            f"{self.url}/rest/v1/{table}",
            headers=headers,
            params=params,
            json=_jsonable(json),
            timeout=self.timeout,
        )
        if response.status_code not in {200, 201, 202, 204}:
            raise SupabasePersistenceError(f"Supabase {method} {table} failed: {response.status_code} {response.text[:500]}")
        if response.text:
            try:
                return response.json()
            except ValueError:
                return None
        return None

    def select(self, table: str, params: dict | None = None) -> list[dict]:
        self._count("select", table)
        return self._request("GET", table, params=params or {"select": "*"}) or []

    def upsert(self, table: str, payload: dict | list[dict], on_conflict: str | None = None) -> None:
        self._count("upsert_rows", table, len(payload if isinstance(payload, list) else [payload]))
        params = {"on_conflict": on_conflict} if on_conflict else None
        self._request("POST", table, params=params, json=payload, prefer="resolution=merge-duplicates,return=minimal")

    def upsert_with_optional_source_fill_id(self, table: str, payload: dict, on_conflict: str | None = None) -> None:
        try:
            self.upsert(table, payload, on_conflict)
        except SupabasePersistenceError as exc:
            missing_source_column = "source_fill_id" in str(exc) and ("42703" in str(exc) or "PGRST204" in str(exc))
            if "source_fill_id" not in payload or not missing_source_column:
                raise
            fallback = dict(payload)
            source_fill_id = fallback.get("source_fill_id")
            if table == "grid_orders" and source_fill_id:
                raw = fallback.get("raw") or {}
                fallback["raw"] = {
                    **raw,
                    "gridbot": {**(raw.get("gridbot") or {}), "source_fill_id": source_fill_id},
                }
            fallback.pop("source_fill_id", None)
            self.upsert(table, fallback, on_conflict)

    def insert_once(self, table: str, payload: dict, on_conflict: str | None = None) -> bool:
        self._count("insert_once", table)
        params = {"on_conflict": on_conflict} if on_conflict else None
        try:
            self._request("POST", table, params=params, json=payload, prefer="resolution=ignore-duplicates,return=minimal")
            return True
        except SupabasePersistenceError as exc:
            if "duplicate key" in str(exc).lower() or "23505" in str(exc):
                return False
            raise

    def insert_once_with_optional_config_version(self, table: str, payload: dict, on_conflict: str | None = None) -> bool:
        try:
            return self.insert_once(table, payload, on_conflict)
        except SupabasePersistenceError as exc:
            if "config_version" not in payload or "config_version" not in str(exc) or "42703" not in str(exc):
                raise
            fallback = dict(payload)
            fallback.pop("config_version", None)
            return self.insert_once(table, fallback, on_conflict)

    def patch(self, table: str, filters: dict, payload: dict) -> None:
        self._count("patch", table)
        params = {key: f"eq.{value}" for key, value in filters.items()}
        self._request("PATCH", table, params=params, json=payload, prefer="return=minimal")

    def active_run(self) -> dict | None:
        locks = self.select(
            "grid_active_run_locks",
            {"select": "run_id", "lock_name": "eq.gridbot_v01_active_run", "limit": 1},
        )
        if not locks:
            return None
        run_id = locks[0].get("run_id")
        rows = self.select(
            "grid_runs",
            {
                "select": "*",
                "run_id": f"eq.{run_id}",
                "status": f"in.({','.join(sorted(ACTIVE_STATUSES))})",
                "order": "started_at.desc",
                "limit": 1,
            },
        )
        return rows[0] if rows else None

    def active_config_snapshot(self) -> dict | None:
        active = self.active_run()
        if not active:
            return None
        config_version = active.get("active_config_version") or active.get("config_version") or 1
        config_rows = self.select(
            "grid_config_versions",
            {
                "select": "*",
                "run_id": f"eq.{active['run_id']}",
                "config_version": f"eq.{config_version}",
                "limit": 1,
            },
        )
        config = (config_rows[0].get("config") if config_rows else None) or (config_rows[0] if config_rows else {})
        if not config:
            return None
        return {
            "run_id": active["run_id"],
            "bot_id": active.get("bot_id") or config.get("bot_id"),
            "status": active.get("status"),
            "config_version": int(config.get("config_version") or config_version),
            "product_symbol": config.get("product_symbol") or "ETHUSD",
            "grid_type": config.get("grid_type"),
            "lower_price": config.get("lower_price"),
            "upper_price": config.get("upper_price"),
            "grid_count": config.get("grid_count"),
            "spacing_type": config.get("spacing_type"),
            "lot_size": config.get("lot_size"),
            "max_inventory_lots": config.get("max_inventory_lots"),
        }

    def _locked_run_is_active(self, run_id: str | None) -> bool:
        if not run_id:
            return False
        rows = self.select(
            "grid_runs",
            {
                "select": "run_id",
                "run_id": f"eq.{run_id}",
                "status": f"in.({','.join(sorted(ACTIVE_STATUSES))})",
                "limit": 1,
            },
        )
        return bool(rows)

    def retire_unlocked_active_run_rows(self, retain_run_id: str | None = None) -> int:
        locks = self.select(
            "grid_active_run_locks",
            {"select": "run_id", "lock_name": "eq.gridbot_v01_active_run", "limit": 1},
        )
        locked_run_id = str(locks[0].get("run_id")) if locks and locks[0].get("run_id") else None
        rows = self.select(
            "grid_runs",
            {
                "select": "run_id,status",
                "status": f"in.({','.join(sorted(ACTIVE_STATUSES))})",
                "order": "started_at.desc",
            },
        )
        retired = 0
        for row in rows:
            row_run_id = str(row.get("run_id") or "")
            if not row_run_id or row_run_id in {locked_run_id, retain_run_id}:
                continue
            self.patch(
                "grid_runs",
                {"run_id": row_run_id},
                {
                    "status": "STOPPED",
                    "stop_reason": "stale_active_lock_recovery",
                    "stopped_at": utc_now(),
                    "updated_at": utc_now(),
                },
            )
            retired += 1
        return retired

    def acquire_active_run_guard(self, run_id: str) -> None:
        self.retire_unlocked_active_run_rows(retain_run_id=run_id)
        self.insert_once(
            "grid_active_run_locks",
            {"lock_name": "gridbot_v01_active_run", "run_id": run_id, "created_at": utc_now()},
            on_conflict="lock_name",
        )
        rows = self.select(
            "grid_active_run_locks",
            {"select": "run_id", "lock_name": "eq.gridbot_v01_active_run", "limit": 1},
        )
        locked_run_id = rows[0].get("run_id") if rows else None
        if locked_run_id != run_id and not self._locked_run_is_active(locked_run_id):
            self.release_active_run_guard(str(locked_run_id))
            self.retire_unlocked_active_run_rows(retain_run_id=run_id)
            self.insert_once(
                "grid_active_run_locks",
                {"lock_name": "gridbot_v01_active_run", "run_id": run_id, "created_at": utc_now()},
                on_conflict="lock_name",
            )
            rows = self.select(
                "grid_active_run_locks",
                {"select": "run_id", "lock_name": "eq.gridbot_v01_active_run", "limit": 1},
            )
        if not rows or rows[0].get("run_id") != run_id:
            raise SupabasePersistenceError("Another DeltaGridBot V0.1 run is already active.")

    def release_active_run_guard(self, run_id: str) -> None:
        self._count("delete", "grid_active_run_locks")
        self._request(
            "DELETE",
            "grid_active_run_locks",
            params={"lock_name": "eq.gridbot_v01_active_run", "run_id": f"eq.{run_id}"},
            prefer="return=minimal",
        )

    def persist_run_state(self, run: dict, status: str | None = None, include_children: bool = True) -> None:
        config = run.get("config") or {}
        product = run.get("product") or {}
        bot_id = run["bot_id"]
        run_id = run["run_id"]
        now = utc_now()
        self.upsert(
            "grid_bots",
            {
                "bot_id": bot_id,
                "bot_name": config.get("bot_name") or "DeltaGridBot V0.1",
                "product_symbol": config.get("product_symbol") or product.get("symbol") or "ETHUSD",
                "product_id": product.get("product_id"),
                "environment": "testnet",
                "status": status or run.get("status"),
                "current_status": status or run.get("status"),
                "updated_at": now,
            },
            on_conflict="bot_id",
        )
        self.upsert(
            "grid_runs",
            {
                "run_id": run_id,
                "bot_id": bot_id,
                "status": status or run.get("status"),
                "config_version": int(config.get("config_version") or run.get("config_version") or 1),
                "execution_event_mode": run.get("execution_event_mode"),
                "operational_state": run.get("operational_state"),
                "started_at": run.get("started_at"),
                "stopped_at": run.get("stopped_at"),
                "starting_config_version": 1,
                "active_config_version": int(config.get("config_version") or run.get("config_version") or 1),
                "ending_config_version": int(config.get("config_version") or run.get("config_version") or 1),
                "starting_market_price": run.get("reference_price"),
                "ending_market_price": run.get("ending_market_price"),
                "stop_reason": run.get("stop_reason"),
                "updated_at": now,
            },
            on_conflict="run_id",
        )
        if not include_children:
            return
        self.persist_config(run)
        self.persist_levels(run)
        for order in (run.get("orders") or {}).values():
            self.persist_order(run, order)
        for fill_id, fill in (run.get("fills") or {}).items():
            self.persist_fill(run, fill_id, fill)

    def persist_config(self, run: dict, reason: str = "start") -> None:
        config = deepcopy(run.get("config") or {})
        version = int(config.get("config_version") or 1)
        self.upsert(
            "grid_config_versions",
            {
                "run_id": run["run_id"],
                "bot_id": run["bot_id"],
                "config_version": version,
                "effective_from": config.get("effective_from") or run.get("started_at") or utc_now(),
                "grid_type": config.get("grid_type"),
                "lower_price": config.get("lower_price"),
                "upper_price": config.get("upper_price"),
                "grid_count": config.get("grid_count"),
                "spacing_type": config.get("spacing_type"),
                "lot_size": config.get("lot_size"),
                "max_inventory_lots": config.get("max_inventory_lots"),
                "allocated_capital": config.get("allocated_capital"),
                "risk_capital": config.get("risk_capital"),
                "risk_thresholds": config.get("risk_thresholds") or {},
                "reason": reason,
                "regrid_required": False,
                "config": config,
            },
            on_conflict="run_id,config_version",
        )

    def retire_config(self, run_id: str, config_version: int) -> None:
        self.patch("grid_config_versions", {"run_id": run_id, "config_version": config_version}, {"effective_to": utc_now(), "regrid_required": True})
        self.patch("grid_levels", {"run_id": run_id, "config_version": config_version}, {"state": "retired", "retired_at": utc_now()})

    def persist_levels(self, run: dict) -> None:
        config = run.get("config") or {}
        version = int(config.get("config_version") or 1)
        levels = run.get("levels") or []
        if not levels:
            return
        prices = [level.get("price") for level in levels]
        rows = []
        for index, level in enumerate(levels):
            prev_price = prices[index - 1] if index else None
            spacing_abs = None
            spacing_pct = None
            try:
                if prev_price is not None:
                    spacing_abs = str(float(level["price"]) - float(prev_price))
                    spacing_pct = str((float(level["price"]) / float(prev_price)) - 1) if float(prev_price) else None
            except Exception:
                pass
            rows.append(
                {
                    "run_id": run["run_id"],
                    "config_version": version,
                    "level_id": level["level_id"],
                    "level_index": level.get("index"),
                    "side": level.get("side"),
                    "price": level.get("price"),
                    "spacing_absolute": spacing_abs,
                    "spacing_percentage": spacing_pct,
                    "state": level.get("state") or "active",
                    "quantity": level.get("quantity"),
                }
            )
        self.upsert("grid_levels", rows, on_conflict="run_id,config_version,level_id")

    def persist_order_proposal(self, run: dict, proposal: Any, order_kind: str, source_fill_id: str | None = None) -> None:
        self.upsert_with_optional_source_fill_id(
            "grid_order_proposals",
            {
                "proposal_id": proposal.client_order_id,
                "run_id": run["run_id"],
                "bot_id": run["bot_id"],
                "config_version": int((run.get("config") or {}).get("config_version") or 1),
                "level_id": proposal.level_id,
                "client_order_id": proposal.client_order_id,
                "side": proposal.side.value,
                "price": str(proposal.price),
                "quantity": str(proposal.quantity),
                "order_kind": order_kind,
                "status": "PROPOSED",
                "source_fill_id": source_fill_id,
                "created_at": utc_now(),
            },
            on_conflict="proposal_id",
        )

    def persist_order(self, run: dict, order: dict) -> None:
        submitted_at = order.get("submitted_at") or order.get("created_at") or utc_now()
        raw = order.get("raw") or {}
        source_fill_id = order.get("source_fill_id") or (raw.get("gridbot") or {}).get("source_fill_id")
        reduce_only = order.get("reduce_only")
        if reduce_only is None:
            reduce_only = raw.get("reduce_only")
        post_only = order.get("post_only")
        if post_only is None:
            post_only = raw.get("post_only")
        if post_only is None and isinstance(raw.get("meta_data"), dict):
            post_only = raw["meta_data"].get("post_only")
        if post_only is None:
            post_only = order.get("order_kind") != "safety_flatten"
        time_in_force = order.get("time_in_force") or raw.get("time_in_force") or ("ioc" if order.get("order_kind") == "safety_flatten" else "gtc")
        if not source_fill_id and order.get("client_order_id"):
            try:
                existing = self.select(
                    "grid_orders",
                    {
                        "select": "raw",
                        "client_order_id": f"eq.{order.get('client_order_id')}",
                        "limit": 1,
                    },
                )
                source_fill_id = (((existing[0] if existing else {}).get("raw") or {}).get("gridbot") or {}).get("source_fill_id")
            except SupabasePersistenceError:
                source_fill_id = None
        if source_fill_id:
            raw = {**raw, "gridbot": {**(raw.get("gridbot") or {}), "source_fill_id": source_fill_id}}
        gridbot_raw = {
            **(raw.get("gridbot") or {}),
            "replacement_group_key": order.get("replacement_group_key"),
            "source_order_key": order.get("source_order_key"),
            "source_fill_ids": order.get("source_fill_ids"),
            "opens_inventory": order.get("opens_inventory"),
            "projected_inventory_if_filled": order.get("projected_inventory_if_filled"),
            "reserved_long_after": order.get("reserved_long_after"),
            "reserved_short_after": order.get("reserved_short_after"),
        }
        if source_fill_id:
            gridbot_raw["source_fill_id"] = source_fill_id
        raw = {**raw, "gridbot": {key: value for key, value in gridbot_raw.items() if value is not None}}
        self.upsert_with_optional_source_fill_id(
            "grid_orders",
            {
                "order_id": order.get("order_key") or order.get("client_order_id"),
                "run_id": run["run_id"],
                "bot_id": run["bot_id"],
                "config_version": int(order.get("config_version") or (run.get("config") or {}).get("config_version") or 1),
                "level_id": order.get("level_id"),
                "client_order_id": order.get("client_order_id"),
                "exchange_order_id": order.get("exchange_order_id"),
                "side": order.get("side"),
                "price": order.get("price"),
                "requested_quantity": order.get("requested_quantity"),
                "filled_quantity": order.get("filled_quantity") or "0",
                "remaining_quantity": order.get("remaining_quantity"),
                "order_type": "limit_order",
                "time_in_force": time_in_force,
                "post_only": bool(post_only),
                "reduce_only": bool(reduce_only),
                "status": order.get("status"),
                "source_fill_id": source_fill_id,
                "submitted_at": submitted_at,
                "updated_at": utc_now(),
                "cancelled_at": order.get("cancelled_at"),
                "rejection_reason": order.get("rejection_reason") or order.get("cancel_error"),
                "order_kind": order.get("order_kind"),
                "raw": raw,
            },
            on_conflict="client_order_id",
        )

    def persist_fill(self, run: dict, fill_id: str, fill: dict, persist_cycles: bool = True) -> bool:
        client_order_id = str(fill.get("client_order_id") or "")
        orders = run.get("orders") or {}
        exchange_order_id = str(fill.get("order_id") or fill.get("exchange_order_id") or "")
        order = orders.get(client_order_id) or next(
            (row for row in orders.values() if str(row.get("exchange_order_id") or "") == exchange_order_id),
            {},
        )
        price = fill.get("price") or fill.get("fill_price") or order.get("price")
        quantity = fill.get("size") or fill.get("quantity")
        exchange_fill_id = str(fill.get("id") or fill_id)
        exchange_timestamp = fill.get("created_at") if isinstance(fill.get("created_at"), str) else None
        fee = extract_fee(fill)
        multiplier = decimal_value((run.get("product") or {}).get("contract_multiplier"), "1")
        notional = str(fill_notional(decimal_value(price), decimal_value(quantity), multiplier))
        role = normalize_maker_taker_role(fill)
        inserted = self.insert_once_with_optional_config_version(
            "grid_fills",
            {
                "fill_id": fill_id,
                "run_id": run["run_id"],
                "bot_id": run["bot_id"],
                "order_id": order.get("order_key") or client_order_id,
                "level_id": order.get("level_id"),
                "config_version": int(order.get("config_version") or (run.get("config") or {}).get("config_version") or 1),
                "exchange_fill_id": exchange_fill_id,
                "side": str(fill.get("side") or "").lower(),
                "price": price,
                "quantity": quantity,
                "quantity_lots": quantity,
                "base_quantity": str(decimal_value(quantity) * multiplier),
                "notional": notional,
                "notional_value": notional,
                "liquidity_role": role,
                "maker_taker_role": role,
                "fee": str(fee.amount) if fee.amount is not None else None,
                "exchange_fee": str(fee.amount) if fee.amount is not None else None,
                "trading_fee": str(fee.amount) if fee.amount is not None else None,
                "fee_currency": fee.currency,
                "fee_status": fee.status,
                "fee_source": fee.source,
                "exchange_order_id": exchange_order_id,
                "exchange_timestamp": exchange_timestamp,
                "detected_at": utc_now(),
                "rest_detection_latency": None,
                "raw": fill,
            },
            on_conflict="exchange_fill_id",
        )
        if inserted:
            if fee.status == FEE_CONFIRMED:
                    self.insert_once_with_optional_config_version(
                        "grid_exchange_costs",
                    {
                        "cost_id": f"fee_{exchange_fill_id}",
                        "run_id": run["run_id"],
                        "order_id": order.get("order_key") or client_order_id,
                        "fill_id": fill_id,
                        "config_version": int(order.get("config_version") or (run.get("config") or {}).get("config_version") or 1),
                        "cost_type": "trading_fee",
                        "amount": str(fee.amount or 0),
                        "currency": fee.currency or "USD",
                        "direction": "debit",
                        "exchange_transaction_id": exchange_fill_id,
                        "exchange_timestamp": exchange_timestamp,
                        "raw": fill,
                    },
                    on_conflict="cost_id",
                )
            if persist_cycles:
                self.persist_cycles(run)
        return inserted

    def persist_cycles(self, run: dict) -> None:
        accounting = build_run_accounting(run)
        rows = [cycle_to_row(cycle, run.get("bot_id")) for cycle in accounting.cycles]
        if not rows:
            return
        try:
            self.upsert("grid_cycles", rows, on_conflict="run_id,entry_fill_id,exit_fill_id")
        except SupabasePersistenceError as exc:
            if "config_version" not in str(exc) or "42703" not in str(exc):
                raise
            self.upsert(
                "grid_cycles",
                [{key: value for key, value in row.items() if key != "config_version"} for row in rows],
                on_conflict="run_id,entry_fill_id,exit_fill_id",
            )

    def persist_snapshot(self, run: dict, risk: dict, summary: dict | None = None) -> None:
        config_version = int((run.get("config") or {}).get("config_version") or risk.get("config_version") or 1)
        telemetry = _risk_telemetry(risk)
        account_equity = _risk_value(risk, telemetry, "account_equity")
        available_margin = _risk_value(risk, telemetry, "available_margin")
        used_margin = _risk_value(risk, telemetry, "used_margin", "margin_used")
        margin_utilisation_pct = _risk_value(risk, telemetry, "margin_utilisation_pct")
        mark_price = _risk_value(risk, telemetry, "mark_price")
        gridbot_inventory = _risk_value(risk, telemetry, "gridbot_inventory", "position")
        delta_position = _risk_value(risk, telemetry, "delta_position", "position_lots")
        position_notional = _risk_value(risk, telemetry, "position_notional")
        open_buy_quantity = _risk_value(risk, telemetry, "open_buy_quantity_lots")
        open_buy_notional = _risk_value(risk, telemetry, "open_buy_notional")
        open_sell_quantity = _risk_value(risk, telemetry, "open_sell_quantity_lots")
        open_sell_notional = _risk_value(risk, telemetry, "open_sell_notional")
        liquidation_price = _risk_value(risk, telemetry, "liquidation_price")
        grr = None
        try:
            equity = decimal_value(account_equity)
            if equity > 0 and mark_price and gridbot_inventory:
                grr = str((abs(decimal_value(gridbot_inventory)) * decimal_value(mark_price)) / equity)
        except Exception:
            grr = None
        telemetry_freshness = {
            "telemetry_status": telemetry.get("telemetry_status"),
            "account_age_seconds": telemetry.get("account_age_seconds"),
            "position_age_seconds": telemetry.get("position_age_seconds"),
            "order_age_seconds": telemetry.get("order_age_seconds"),
            "market_age_seconds": telemetry.get("market_age_seconds"),
            "unavailable_fields": telemetry.get("unavailable_fields") or [],
            "last_account_sync": telemetry.get("last_account_sync"),
            "last_position_sync": telemetry.get("last_position_sync"),
            "last_order_sync": telemetry.get("last_order_sync"),
            "last_market_sync": telemetry.get("last_market_sync"),
        }
        reconciliation = risk.get("reconciliation") if isinstance(risk.get("reconciliation"), dict) else {}
        replacements = risk.get("replacements") if isinstance(risk.get("replacements"), dict) else {}
        compact_margin_state = {
            "created_at": risk.get("created_at"),
            "snapshot_reason": risk.get("snapshot_reason"),
            "config_version": config_version,
            "account_equity": account_equity,
            "available_margin": available_margin,
            "used_margin": used_margin,
            "margin_utilisation_pct": margin_utilisation_pct,
            "gridbot_inventory": gridbot_inventory,
            "delta_position": delta_position,
            "mark_price": mark_price,
            "position_notional": position_notional,
            "open_buy_quantity_lots": open_buy_quantity,
            "open_buy_notional": open_buy_notional,
            "open_sell_quantity_lots": open_sell_quantity,
            "open_sell_notional": open_sell_notional,
            "liquidation_price": liquidation_price,
            "telemetry": telemetry_freshness,
            "reconciliation": {
                "errors": reconciliation.get("errors") or [],
                "new_fills": reconciliation.get("new_fills"),
                "filled_orders": reconciliation.get("filled_orders"),
                "partial_fills": reconciliation.get("partial_fills"),
                "request_count": reconciliation.get("request_count"),
                "orders_checked": reconciliation.get("orders_checked"),
                "orders_resolved": reconciliation.get("orders_resolved"),
                "cancelled_orders": reconciliation.get("cancelled_orders"),
                "manual_cancelled_orders": reconciliation.get("manual_cancelled_orders"),
                "unresolved_orders": reconciliation.get("unresolved_orders"),
                "position_mismatches": reconciliation.get("position_mismatches"),
                "fill_ledger_mismatches": reconciliation.get("fill_ledger_mismatches"),
                "duplicate_fills_ignored": reconciliation.get("duplicate_fills_ignored"),
                "last_successful_reconcile": reconciliation.get("last_successful_reconcile"),
            },
            "replacements": {
                "created": replacements.get("created"),
                "deferred": replacements.get("deferred"),
                "skipped": replacements.get("skipped"),
                "existing": replacements.get("existing"),
                "cancel_replaced": replacements.get("cancel_replaced"),
                "metrics": replacements.get("metrics") or {},
            },
        }
        payload = {
            "snapshot_id": new_id("snap"),
            "run_id": run["run_id"],
            "timestamp": risk.get("created_at") or utc_now(),
            "eth_price": run.get("reference_price"),
            "active_config_version": config_version,
            "inventory": gridbot_inventory or risk.get("position"),
            "pending_exposure": None,
            "open_orders": risk.get("open_gridbot_orders"),
            "gross_grid_pnl": (summary or {}).get("gross_pnl"),
            "net_grid_pnl": (summary or {}).get("NET_TRADING_PNL_BEFORE_INCOME_TAX"),
            "inventory_pnl": None,
            "exchange_fees": (summary or {}).get("delta_fees"),
            "funding": (summary or {}).get("funding"),
            "account_equity": account_equity,
            "margin_metrics": compact_margin_state,
            "grr": grr,
            "drawdown": None,
            "risk_state": risk.get("risk_state") or "GREEN",
            "execution_mode": run.get("execution_event_mode"),
            "payload": {"run": {"status": run.get("status")}, "risk": compact_margin_state},
        }
        latest_rows = self.select(
            "grid_bot_snapshots",
            {"select": "*", "run_id": f"eq.{run['run_id']}", "order": "timestamp.desc", "limit": 1},
        )
        if latest_rows:
            latest = latest_rows[0]
            latest_ts = _parse_ts(latest.get("timestamp"))
            current_ts = _parse_ts(payload["timestamp"])
            if (
                latest_ts
                and current_ts
                and 0 <= (current_ts - latest_ts).total_seconds() <= SNAPSHOT_DEDUPE_SECONDS
                and _snapshot_dedupe_signature(latest) == _snapshot_dedupe_signature(payload)
            ):
                return False
        self.insert_once("grid_bot_snapshots", payload, on_conflict="snapshot_id")
        self.insert_once(
            "grid_risk_snapshots",
            {
                "snapshot_id": payload["snapshot_id"],
                "run_id": run["run_id"],
                "timestamp": payload["timestamp"],
                "risk_state": payload["risk_state"],
                "inventory_utilisation": None,
                "grr": grr,
                "margin_state": compact_margin_state,
                "current_exposure": {
                    "gridbot_inventory": gridbot_inventory,
                    "delta_position": delta_position,
                    "position_notional": position_notional,
                },
                "projected_exposure": {
                    "open_gridbot_orders": risk.get("open_gridbot_orders"),
                    "open_buy_quantity_lots": open_buy_quantity,
                    "open_buy_notional": open_buy_notional,
                    "open_sell_quantity_lots": open_sell_quantity,
                    "open_sell_notional": open_sell_notional,
                },
                "risk_thresholds": (run.get("config") or {}).get("risk_thresholds") or {},
            },
            on_conflict="snapshot_id",
        )
        return True

    def log_event(self, run: dict | None, event_type: str, payload: dict | None = None) -> None:
        self.insert_once(
            "grid_events",
            {
                "event_id": new_id("evt"),
                "bot_id": (run or {}).get("bot_id"),
                "run_id": (run or {}).get("run_id"),
                "event_type": event_type,
                "payload": payload or {},
                "created_at": utc_now(),
            },
            on_conflict="event_id",
        )

    def sync_health_issues(self, active_issues: list[HealthIssue], resolved_issues: list[HealthIssue] | None = None) -> None:
        now = utc_now()
        try:
            for issue in active_issues:
                payload = {
                    "issue_key": issue.key,
                    "run_id": issue.run_id,
                    "bot_id": issue.context.get("bot_id"),
                    "code": issue.code,
                    "severity": issue.severity,
                    "message": issue.message,
                    "first_seen": issue.first_seen,
                    "last_seen": issue.last_seen or now,
                    "occurrence_count": issue.occurrence_count,
                    "active": True,
                    "resolved_at": None,
                    "operator_attention_required": issue.code in {
                        "ACTIVE_LOCK_CONTRADICTION",
                        "ACCOUNTING_MISMATCH",
                        "FILL_LEDGER_MISMATCH",
                        "GRID_NATURE_INVENTORY_VIOLATION",
                        "GRID_ORDER_ORPHAN",
                        "GRID_ORDER_UNRESOLVED",
                        "LIFECYCLE_STUCK",
                        "MISSING_REPLACEMENT",
                        "POSITION_MISMATCH",
                        "RESERVATION_MISMATCH",
                        "STOP_REQUIRES_ATTENTION",
                        "SUBMITTED_ORDER_UNRESOLVED",
                        "SUPABASE_FAILURE",
                    },
                    "context": issue.context,
                    "updated_at": now,
                }
                self.upsert("grid_health_events", payload, on_conflict="issue_key")
            for issue in resolved_issues or []:
                self.patch(
                    "grid_health_events",
                    {"issue_key": issue.key},
                    {
                        "last_seen": issue.last_seen or now,
                        "occurrence_count": issue.occurrence_count,
                        "active": False,
                        "resolved_at": issue.resolved_at or now,
                        "updated_at": now,
                    },
                )
        except SupabasePersistenceError as exc:
            missing_table = "grid_health_events" in str(exc) and any(token in str(exc) for token in ["42P01", "PGRST205", "PGRST204"])
            if not missing_table:
                raise
            for issue in active_issues:
                self.log_event(
                    {"run_id": issue.run_id, "bot_id": issue.context.get("bot_id")},
                    "GRID_HEALTH_ISSUE_ACTIVE",
                    issue.as_dict(),
                )
            for issue in resolved_issues or []:
                self.log_event(
                    {"run_id": issue.run_id, "bot_id": issue.context.get("bot_id")},
                    "GRID_HEALTH_ISSUE_RESOLVED",
                    issue.as_dict(),
                )

    def recent_health_issues(self, run_id: str | None = None, limit: int = 50) -> dict:
        params = {"select": "*", "order": "last_seen.desc", "limit": max(1, min(int(limit), 200))}
        if run_id:
            params["run_id"] = f"eq.{run_id}"
        rows = self.select("grid_health_events", params)
        return {
            "active": [row for row in rows if row.get("active") is True],
            "resolved": [row for row in rows if row.get("active") is False],
        }

    def resolve_health_issue_codes(self, run_id: str, codes: set[str]) -> int:
        now = utc_now()
        rows = self.select(
            "grid_health_events",
            {
                "select": "issue_key,code,active",
                "run_id": f"eq.{run_id}",
                "limit": 200,
            },
        )
        resolved = 0
        for row in rows:
            if row.get("active") is not True or str(row.get("code") or "") not in codes:
                continue
            issue_key = row.get("issue_key")
            if not issue_key:
                continue
            self.patch(
                "grid_health_events",
                {"issue_key": issue_key},
                {"active": False, "resolved_at": now, "updated_at": now},
            )
            resolved += 1
        return resolved

    def resolve_terminal_health_issues(self, run_id: str, *, keep_codes: set[str] | None = None) -> int:
        terminal_cleanup_codes = {
            "GRID_ORDER_UNRESOLVED",
            "GRID_ORDER_MISSING_UNEXPECTEDLY",
            "LIFECYCLE_STUCK",
            "POSITION_MISMATCH",
            "POSITION_ATTRIBUTION_UNSAFE",
            "STOPPED_WITH_EXPOSURE",
            "WORKER_DEAD_RUNNING",
            "RECONCILIATION_STALE",
        }
        return self.resolve_health_issue_codes(run_id, terminal_cleanup_codes - (keep_codes or set()))

    def persist_summary(self, run: dict, summary: dict) -> None:
        self.insert_once(
            "grid_run_summaries",
            {
                "summary_id": summary["summary_id"],
                "run_id": run["run_id"],
                "bot_id": run["bot_id"],
                "gridbot_version": summary.get("gridbot_version") or "0.1",
                "summary": summary,
                "immutable": True,
                "created_at": summary.get("created_at") or utc_now(),
                "finalised_at": summary.get("stopped_at") or utc_now(),
            },
            on_conflict="run_id",
        )

    def load_run_state(self, run_id: str) -> dict:
        run_rows = self.select("grid_runs", {"select": "*", "run_id": f"eq.{run_id}", "limit": 1})
        if not run_rows:
            raise SupabasePersistenceError(f"Grid run not found in Supabase: {run_id}")
        run_row = run_rows[0]
        bot = (self.select("grid_bots", {"select": "*", "bot_id": f"eq.{run_row['bot_id']}", "limit": 1}) or [{}])[0]
        config_rows = self.select(
            "grid_config_versions",
            {"select": "*", "run_id": f"eq.{run_id}", "config_version": f"eq.{run_row.get('active_config_version')}", "limit": 1},
        )
        config = (config_rows[0].get("config") if config_rows else None) or {}
        config.setdefault("bot_id", run_row["bot_id"])
        config.setdefault("bot_name", bot.get("bot_name"))
        config.setdefault("product_symbol", bot.get("product_symbol") or "ETHUSD")
        config.setdefault("config_version", run_row.get("active_config_version") or 1)
        levels = self.select(
            "grid_levels",
            {
                "select": "level_id,level_index,side,price,quantity,state",
                "run_id": f"eq.{run_id}",
                "config_version": f"eq.{config.get('config_version')}",
                "order": "level_index.asc",
            },
        )
        orders = self.select("grid_orders", {"select": "*", "run_id": f"eq.{run_id}", "order": "submitted_at.asc"})
        fills = self.select("grid_fills", {"select": "*", "run_id": f"eq.{run_id}", "order": "detected_at.asc"})
        exchange_costs = self.select("grid_exchange_costs", {"select": "*", "run_id": f"eq.{run_id}", "order": "created_at.asc"})
        snapshots = self.select("grid_risk_snapshots", {"select": "*", "run_id": f"eq.{run_id}", "order": "timestamp.asc", "limit": 50})
        summary_rows = self.select("grid_run_summaries", {"select": "summary", "run_id": f"eq.{run_id}", "limit": 1})
        event_rows = self.select(
            "grid_events",
            {"select": "event_type,payload,created_at", "run_id": f"eq.{run_id}", "order": "created_at.desc", "limit": 25},
        )
        latest_stage = next(
            (
                (row.get("payload") or {}).get("start_stage")
                for row in event_rows
                if row.get("event_type") == "GRID_RUN_START_STAGE" and (row.get("payload") or {}).get("start_stage")
            ),
            None,
        )
        latest_failure = next(
            (
                (row.get("payload") or {}).get("error")
                for row in event_rows
                if row.get("event_type") == "GRID_RUN_START_FAILED" and (row.get("payload") or {}).get("error")
            ),
            None,
        )
        latest_stage_at = next(
            (
                row.get("created_at")
                for row in event_rows
                if row.get("event_type") == "GRID_RUN_START_STAGE" and (row.get("payload") or {}).get("start_stage")
            ),
            None,
        )
        latest_truth_at = next(
            (
                (row.get("payload") or {}).get("last_successful_reconcile") or row.get("created_at")
                for row in event_rows
                if row.get("event_type") == "REST_RECONCILED"
            ),
            None,
        )
        latest_external_adjustment = next(
            (
                row.get("payload") or {}
                for row in event_rows
                if row.get("event_type") in {"GRID_RUN_EXTERNAL_POSITION_CHANGE", "GRID_RUN_FORCED_POSITION_REDUCTION"}
            ),
            None,
        )
        latest_editing = next(
            (
                row
                for row in event_rows
                if row.get("event_type") == "GRID_RUN_EDITING"
            ),
            None,
        )
        product_id = bot.get("product_id") or 1699
        startup_stage = latest_stage or ("RUNNING" if run_row.get("status") == "RUNNING" else run_row.get("status"))
        startup = {
            "start_stage": startup_stage,
            "orders_expected": len(levels),
            "orders_submitted": len(orders),
            "orders_verified": len([row for row in orders if row.get("exchange_order_id")]),
            "last_error": latest_failure,
            "last_startup_progress_at": latest_stage_at,
            "last_successful_delta_truth_check": latest_truth_at,
        }
        run_state = {
            "run_id": run_id,
            "bot_id": run_row["bot_id"],
            "status": run_row.get("status"),
            "gridbot_version": "0.1",
            "config": config,
            "levels": [
                {
                    "level_id": row["level_id"],
                    "index": row.get("level_index"),
                    "side": row.get("side"),
                    "price": str(row.get("price")),
                    "quantity": str(row.get("quantity") or config.get("lot_size") or "1"),
                    "state": row.get("state"),
                }
                for row in levels
            ],
            "product": {"product_id": product_id, "symbol": bot.get("product_symbol") or "ETHUSD", "contract_multiplier": "1"},
            "reference_price": str(run_row.get("starting_market_price") or "0"),
            "execution_event_mode": run_row.get("execution_event_mode") or "REST_FALLBACK",
            "private_ws_status": "BLOCKED_403",
            "operational_state": run_row.get("operational_state") or "DEGRADED",
            "sequence": len(orders) + 1,
            "orders": {
                row["client_order_id"]: {
                    "order_key": row.get("order_id") or row["client_order_id"],
                    "run_id": run_id,
                    "level_id": row.get("level_id"),
                    "side": row.get("side"),
                    "price": str(row.get("price")),
                    "requested_quantity": str(row.get("requested_quantity")),
                    "filled_quantity": str(row.get("filled_quantity") or "0"),
                    "remaining_quantity": str(row.get("remaining_quantity") or "0"),
                    "client_order_id": row["client_order_id"],
                    "exchange_order_id": str(row.get("exchange_order_id") or ""),
                    "status": row.get("status"),
                    "order_kind": row.get("order_kind"),
                    "config_version": row.get("config_version"),
                    "source_fill_id": row.get("source_fill_id") or ((row.get("raw") or {}).get("gridbot") or {}).get("source_fill_id"),
                    "replacement_group_key": ((row.get("raw") or {}).get("gridbot") or {}).get("replacement_group_key"),
                    "source_order_key": ((row.get("raw") or {}).get("gridbot") or {}).get("source_order_key"),
                    "source_fill_ids": ((row.get("raw") or {}).get("gridbot") or {}).get("source_fill_ids"),
                    "reduce_only": row.get("reduce_only"),
                    "post_only": row.get("post_only"),
                    "time_in_force": row.get("time_in_force"),
                    "opens_inventory": ((row.get("raw") or {}).get("gridbot") or {}).get("opens_inventory"),
                    "projected_inventory_if_filled": ((row.get("raw") or {}).get("gridbot") or {}).get("projected_inventory_if_filled"),
                    "reserved_long_after": ((row.get("raw") or {}).get("gridbot") or {}).get("reserved_long_after"),
                    "reserved_short_after": ((row.get("raw") or {}).get("gridbot") or {}).get("reserved_short_after"),
                    "raw": row.get("raw") or {},
                    "created_at": row.get("submitted_at"),
                    "cancelled_at": row.get("cancelled_at"),
                }
                for row in orders
            },
            "fills": {str(row.get("exchange_fill_id") or row.get("fill_id")): row.get("raw") or row for row in fills},
            "exchange_costs": exchange_costs,
            "replacement_keys": {},
            "risk_snapshots": [row.get("margin_state") or row for row in snapshots],
            "started_at": run_row.get("started_at"),
            "stopped_at": run_row.get("stopped_at"),
            "updated_at": run_row.get("updated_at"),
            "status_updated_at": run_row.get("updated_at"),
            "stop_reason": run_row.get("stop_reason"),
            "summary": summary_rows[0].get("summary") if summary_rows else None,
            "last_reconciled_at": None,
            "start_stage": startup_stage,
            "startup": startup,
        }
        deployment_completeness = _reconstructed_deployment_completeness(config, run_state["levels"], orders)
        run_state["deployment_completeness"] = deployment_completeness
        status = str(run_row.get("status") or "")
        operation = {
            "STARTING": "START",
            "PAUSING": "PAUSE",
            "PAUSED": "PAUSE",
            "RESUMING": "RESUME",
            "EDITING": "EDIT",
            "STOPPING": "STOP",
            "STOP_REQUIRES_ATTENTION": "STOP",
            "RUNNING": "RUNNING",
        }.get(status, status)
        stage = latest_stage if status == "STARTING" and latest_stage else status
        message = {
            "STARTING": "Starting Grid: waiting for Delta verification" if not deployment_completeness.get("complete") else "Starting Grid: finalizing",
            "PAUSING": "Pausing Grid: cancelling resting orders",
            "PAUSED": "Grid paused",
            "RESUMING": "Resuming Grid: waiting for Delta verification",
            "EDITING": "Editing Grid: applying new grid",
            "STOPPING": "Stopping Grid: cancelling and flattening",
            "STOP_REQUIRES_ATTENTION": "Stopping Grid: operator attention required",
            "RUNNING": "Grid running",
        }.get(status)
        started_at = run_row.get("started_at")
        updated_at = latest_stage_at or run_row.get("updated_at")
        elapsed = None
        try:
            if started_at and updated_at:
                elapsed = (_parse_ts(updated_at) - _parse_ts(started_at)).total_seconds()
        except Exception:
            elapsed = None
        run_state["lifecycle_progress"] = {
            "operation": operation,
            "stage": stage,
            "started_at": started_at,
            "last_progress_at": updated_at,
            "elapsed_seconds": elapsed,
            "message": message,
            "expected_orders": deployment_completeness.get("expected", 0),
            "confirmed_orders": deployment_completeness.get("confirmed_open", 0),
            "filled_orders": deployment_completeness.get("filled", 0),
            "deferred_orders": deployment_completeness.get("deferred", 0),
            "ambiguous_orders": deployment_completeness.get("ambiguous", 0),
            "missing_orders": deployment_completeness.get("missing", 0),
            "waiting_orders": deployment_completeness.get("missing", 0) + deployment_completeness.get("ambiguous", 0),
        }
        if latest_external_adjustment:
            run_state["external_position_adjustment"] = latest_external_adjustment
            run_state["external_position_resolution"] = {
                "status": "EXTERNALLY_CHANGED",
                "reason": latest_external_adjustment.get("reason"),
                "classification": latest_external_adjustment.get("classification"),
                "gridbot_inventory_before_resolution": latest_external_adjustment.get("ledger_inventory"),
                "delta_position": latest_external_adjustment.get("delta_position"),
                "external_adjustment_lots": latest_external_adjustment.get("external_adjustment_lots"),
                "resolved_at": latest_external_adjustment.get("detected_at"),
                "accounting_status": "PARTIAL",
                "accounting_warning": "EXTERNAL_POSITION_CLOSE_UNATTRIBUTED",
            }
        if run_row.get("status") == "EDITING":
            change_rows = self.select(
                "grid_parameter_changes",
                {"select": "*", "run_id": f"eq.{run_id}", "to_config_version": f"eq.{config.get('config_version')}", "order": "created_at.desc", "limit": 1},
            )
            if change_rows:
                change = change_rows[0]
                payload = change.get("payload") or {}
                run_state["edit_state"] = {
                    "operation_id": payload.get("operation_id"),
                    "previous_status": "RUNNING",
                    "from_config_version": change.get("from_config_version"),
                    "to_config_version": change.get("to_config_version"),
                    "source_config": payload.get("old_config") or {},
                    "target_config": payload.get("new_config") or config,
                    "stage": "CONFIG_PERSISTED",
                    "reason": change.get("reason"),
                    "started_at": change.get("created_at"),
                    "config_persisted": True,
                }
            elif latest_editing:
                payload = latest_editing.get("payload") or {}
                run_state["edit_state"] = {
                    "operation_id": payload.get("operation_id"),
                    "previous_status": payload.get("previous_status") or "RUNNING",
                    "from_config_version": payload.get("from_config_version"),
                    "to_config_version": payload.get("to_config_version"),
                    "source_config": payload.get("source_config") or {},
                    "target_config": payload.get("target_config") or config,
                    "stage": "FREEZE_PLACEMENT",
                    "reason": payload.get("reason"),
                    "started_at": latest_editing.get("created_at"),
                    "config_persisted": False,
                }
        return run_state

    def status_payload(self) -> dict:
        active = self.active_run()
        active_state = self.load_run_state(active["run_id"]) if active else None
        runs = self.select("grid_runs", {"select": "*", "order": "created_at.desc", "limit": 25})
        events = self.select("grid_events", {"select": "*", "order": "created_at.desc", "limit": 50})
        return {
            "ok": True,
            "source_of_truth": "supabase",
            "active_run_id": active_state.get("run_id") if active_state else None,
            "active_run": active_state,
            "runs": runs,
            "events": list(reversed(events)),
        }
