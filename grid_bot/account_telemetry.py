from __future__ import annotations

import os
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from .delta_testnet_client import DeltaTestnetClient
from .models import ProductSpec, utc_now


class TelemetryStatus(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    UNAVAILABLE = "UNAVAILABLE"


class PositionSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


@dataclass(frozen=True)
class TelemetrySync:
    last_sync: str | None = None
    age_seconds: float | None = None
    ok: bool = False
    error: str | None = None


@dataclass(frozen=True)
class AccountRiskState:
    timestamp: str
    source: str
    telemetry_status: str
    wallet_balance: Decimal | None = None
    account_equity: Decimal | None = None
    available_margin: Decimal | None = None
    used_margin: Decimal | None = None
    margin_utilisation_pct: Decimal | None = None
    initial_margin: Decimal | None = None
    maintenance_margin: Decimal | None = None
    position_product: str | None = None
    position_lots: Decimal | None = None
    position_base_quantity: Decimal | None = None
    position_side: str | None = None
    average_entry_price: Decimal | None = None
    mark_price: Decimal | None = None
    position_notional: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    realized_pnl: Decimal | None = None
    liquidation_price: Decimal | None = None
    open_order_count: int | None = None
    open_buy_order_count: int | None = None
    open_sell_order_count: int | None = None
    open_buy_quantity_lots: Decimal | None = None
    open_sell_quantity_lots: Decimal | None = None
    open_buy_notional: Decimal | None = None
    open_sell_notional: Decimal | None = None
    margin_mode: str | None = None
    leverage: Decimal | None = None
    portfolio_delta: Decimal | None = None
    portfolio_gamma: Decimal | None = None
    portfolio_vega: Decimal | None = None
    portfolio_theta: Decimal | None = None
    last_account_sync: str | None = None
    last_position_sync: str | None = None
    last_order_sync: str | None = None
    last_market_sync: str | None = None
    account_age_seconds: float | None = None
    position_age_seconds: float | None = None
    order_age_seconds: float | None = None
    market_age_seconds: float | None = None
    errors: list[str] = field(default_factory=list)
    unavailable_fields: list[str] = field(default_factory=list)
    capability_matrix: dict[str, str] = field(default_factory=dict)
    units: dict[str, str] = field(default_factory=dict)
    request_counts: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict:
        def convert(value: Any) -> Any:
            if isinstance(value, Decimal):
                return str(value)
            if isinstance(value, list):
                return [convert(item) for item in value]
            if isinstance(value, dict):
                return {key: convert(item) for key, item in value.items()}
            return value

        payload = convert(asdict(self))
        payload["sections"] = {
            "account": {
                "wallet_balance": payload.get("wallet_balance"),
                "account_equity": payload.get("account_equity"),
                "source": "wallet/balances",
            },
            "margin": {
                "available_margin": payload.get("available_margin"),
                "used_margin": payload.get("used_margin"),
                "margin_utilisation_pct": payload.get("margin_utilisation_pct"),
                "initial_margin": payload.get("initial_margin"),
                "maintenance_margin": payload.get("maintenance_margin"),
                "margin_mode": payload.get("margin_mode"),
            },
            "position": {
                "product": payload.get("position_product"),
                "lots": payload.get("position_lots"),
                "base_quantity": payload.get("position_base_quantity"),
                "side": payload.get("position_side"),
                "average_entry_price": payload.get("average_entry_price"),
                "mark_price": payload.get("mark_price"),
                "notional": payload.get("position_notional"),
                "unrealized_pnl": payload.get("unrealized_pnl"),
                "realized_pnl": payload.get("realized_pnl"),
                "liquidation_price": payload.get("liquidation_price"),
            },
            "orders": {
                "open_order_count": payload.get("open_order_count"),
                "open_buy_order_count": payload.get("open_buy_order_count"),
                "open_sell_order_count": payload.get("open_sell_order_count"),
                "open_buy_quantity_lots": payload.get("open_buy_quantity_lots"),
                "open_sell_quantity_lots": payload.get("open_sell_quantity_lots"),
                "open_buy_notional": payload.get("open_buy_notional"),
                "open_sell_notional": payload.get("open_sell_notional"),
            },
            "inventory": {
                "position_lots": payload.get("position_lots"),
                "position_base_quantity": payload.get("position_base_quantity"),
                "open_buy_quantity_lots": payload.get("open_buy_quantity_lots"),
                "open_sell_quantity_lots": payload.get("open_sell_quantity_lots"),
            },
            "risk": {
                "account_equity": payload.get("account_equity"),
                "position_notional": payload.get("position_notional"),
                "margin_utilisation_pct": payload.get("margin_utilisation_pct"),
            },
            "telemetry_health": {
                "status": payload.get("telemetry_status"),
                "last_account_sync": payload.get("last_account_sync"),
                "last_position_sync": payload.get("last_position_sync"),
                "last_order_sync": payload.get("last_order_sync"),
                "last_market_sync": payload.get("last_market_sync"),
                "account_age_seconds": payload.get("account_age_seconds"),
                "position_age_seconds": payload.get("position_age_seconds"),
                "order_age_seconds": payload.get("order_age_seconds"),
                "market_age_seconds": payload.get("market_age_seconds"),
                "errors": payload.get("errors"),
                "unavailable_fields": payload.get("unavailable_fields"),
            },
        }
        return payload


CAPABILITY_MATRIX = {
    "Account Equity": "DIRECT:/wallet/balances meta.net_equity",
    "Wallet Balance": "DIRECT:/wallet/balances USD balance",
    "Available Margin": "DIRECT:/wallet/balances USD available_balance",
    "Used/Blocked Margin": "DIRECT:/wallet/balances USD blocked/order/position margin fields",
    "Margin Utilisation %": "DERIVED:used_margin/account_equity*100 when compatible USD values exist",
    "Initial Margin": "AMBIGUOUS:product exposes initial_margin, not current account initial margin",
    "Maintenance Margin": "AMBIGUOUS:product exposes maintenance_margin, not current account maintenance margin",
    "Current ETHUSD Position": "DIRECT:/positions size; empty successful list means flat",
    "Position Side": "DERIVED:sign(position_lots)",
    "Position Quantity in Lots": "DIRECT:/positions size",
    "Average Entry Price": "DIRECT_WHEN_POSITION_FIELD_PRESENT:/positions entry/average price fields",
    "Mark Price": "DIRECT:/tickers/ETHUSD mark_price",
    "Unrealized P&L": "DIRECT_WHEN_POSITION_FIELD_PRESENT:/positions unrealized pnl fields",
    "Realized P&L": "DIRECT_WHEN_POSITION_FIELD_PRESENT:/positions realized pnl fields",
    "Liquidation Price": "DIRECT_WHEN_POSITION_FIELD_PRESENT:/positions liquidation price fields",
    "Open Buy Order Quantity": "DERIVED:/orders unfilled_size by side",
    "Open Sell Order Quantity": "DERIVED:/orders unfilled_size by side",
    "Open Order Exposure": "DERIVED:open lots * contract multiplier * order/mark price",
    "Margin Mode": "UNAVAILABLE:/profile returned 401; wallet portfolio_margin is not account mode",
    "Leverage": "AMBIGUOUS:ticker/product expose leverage/default_leverage, not account setting",
    "Portfolio Delta": "UNAVAILABLE:futures base exposure reported separately; no portfolio greek endpoint",
    "Portfolio Gamma": "UNAVAILABLE:no portfolio greek endpoint for GridBot Testnet credentials",
    "Portfolio Vega": "UNAVAILABLE:no portfolio greek endpoint for GridBot Testnet credentials",
    "Portfolio Theta": "UNAVAILABLE:no portfolio greek endpoint for GridBot Testnet credentials",
}

UNITS = {
    "wallet_balance": "USD",
    "account_equity": "USD",
    "available_margin": "USD",
    "used_margin": "USD",
    "margin_utilisation_pct": "percent",
    "position_lots": "contracts/lots",
    "position_base_quantity": "ETH equivalent = lots * contract_multiplier",
    "mark_price": "USD per ETH",
    "position_notional": "USD",
    "open_buy_quantity_lots": "contracts/lots",
    "open_sell_quantity_lots": "contracts/lots",
    "open_buy_notional": "USD",
    "open_sell_notional": "USD",
}


def decimal_or_none(value: Any) -> Decimal | None:
    if value in [None, ""]:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _first_decimal(row: dict, keys: list[str]) -> Decimal | None:
    for key in keys:
        value = decimal_or_none(row.get(key))
        if value is not None:
            return value
    return None


def _status_from_syncs(syncs: dict[str, TelemetrySync], stale_after_seconds: float, unavailable_fields: list[str]) -> str:
    critical = [syncs.get("position"), syncs.get("orders"), syncs.get("market")]
    if any(not item or not item.ok for item in critical):
        return TelemetryStatus.UNAVAILABLE.value
    if any((item.age_seconds or 0) > stale_after_seconds for item in critical):
        return TelemetryStatus.STALE.value
    account = syncs.get("account")
    if not account or not account.ok or (account.age_seconds or 0) > stale_after_seconds:
        return TelemetryStatus.DEGRADED.value
    critical_unavailable = {"account_equity", "available_margin", "position_lots", "mark_price"}
    if critical_unavailable.intersection(unavailable_fields):
        return TelemetryStatus.DEGRADED.value
    return TelemetryStatus.HEALTHY.value


def _sync_age(sync_at: str | None, now: datetime | None = None) -> float | None:
    if not sync_at:
        return None
    try:
        timestamp = datetime.fromisoformat(str(sync_at).replace("Z", "+00:00"))
        return max(0.0, ((now or datetime.now(timezone.utc)) - timestamp).total_seconds())
    except Exception:
        return None


def _side_from_lots(lots: Decimal | None) -> str | None:
    if lots is None:
        return None
    if lots > 0:
        return PositionSide.LONG.value
    if lots < 0:
        return PositionSide.SHORT.value
    return PositionSide.FLAT.value


def _rows(payload: dict | None) -> list[dict]:
    result = (payload or {}).get("result")
    return result if isinstance(result, list) else []


def _usd_wallet_row(wallet_payload: dict | None) -> dict:
    for row in _rows(wallet_payload):
        if str(row.get("asset_symbol") or "").upper() == "USD":
            return row
    return {}


def _position_row(positions_payload: dict | None, product_id: int, symbol: str) -> dict | None:
    for row in _rows(positions_payload):
        if str(row.get("product_id") or "") == str(product_id) or str(row.get("product_symbol") or row.get("symbol") or "").upper() == symbol.upper():
            return row
    return None


def _order_quantity(row: dict) -> Decimal:
    return decimal_or_none(row.get("unfilled_size")) or decimal_or_none(row.get("remaining_quantity")) or decimal_or_none(row.get("size")) or Decimal("0")


def normalize_account_risk_state(
    *,
    product: ProductSpec,
    wallet_payload: dict | None,
    positions_payload: dict | None,
    orders_payload: dict | None,
    ticker_payload: dict | None,
    syncs: dict[str, TelemetrySync],
    errors: list[str] | None = None,
    stale_after_seconds: float = 60.0,
    request_counts: dict[str, int] | None = None,
) -> AccountRiskState:
    unavailable: list[str] = []
    error_list = list(errors or [])
    wallet = _usd_wallet_row(wallet_payload)
    wallet_balance = decimal_or_none(wallet.get("balance"))
    account_equity = decimal_or_none(((wallet_payload or {}).get("meta") or {}).get("net_equity"))
    available_margin = decimal_or_none(wallet.get("available_balance"))
    margin_parts = [
        decimal_or_none(wallet.get("blocked_margin")),
        decimal_or_none(wallet.get("order_margin")),
        decimal_or_none(wallet.get("position_margin")),
    ]
    used_margin = sum((part for part in margin_parts if part is not None), Decimal("0")) if any(part is not None for part in margin_parts) else None
    if account_equity is not None and available_margin is not None:
        derived_used = account_equity - available_margin
        if used_margin in [None, Decimal("0")] and derived_used >= 0:
            used_margin = derived_used
    margin_utilisation_pct = None
    if account_equity is not None and used_margin is not None and account_equity > 0:
        margin_utilisation_pct = (used_margin / account_equity) * Decimal("100")

    ticker = (ticker_payload or {}).get("result") if isinstance((ticker_payload or {}).get("result"), dict) else ticker_payload or {}
    mark_price = decimal_or_none(ticker.get("mark_price")) or product.mark_price
    position = _position_row(positions_payload, product.product_id, product.symbol)
    position_lots = decimal_or_none(position.get("size")) if position else Decimal("0") if positions_payload and positions_payload.get("success", True) else None
    position_base_quantity = position_lots * product.contract_multiplier if position_lots is not None else None
    average_entry_price = _first_decimal(position or {}, ["entry_price", "average_entry_price", "avg_entry_price", "average_fill_price"])
    unrealized_pnl = _first_decimal(position or {}, ["unrealized_pnl", "unrealised_pnl", "unrealized_profit", "unrealised_profit"])
    realized_pnl = _first_decimal(position or {}, ["realized_pnl", "realised_pnl"])
    liquidation_price = _first_decimal(position or {}, ["liquidation_price", "liquidation_mark_price"])
    position_notional = abs(position_base_quantity) * mark_price if position_base_quantity is not None and mark_price is not None else None

    buy_count = sell_count = 0
    buy_qty = Decimal("0")
    sell_qty = Decimal("0")
    buy_notional = Decimal("0")
    sell_notional = Decimal("0")
    orders = _rows(orders_payload)
    for order in orders:
        qty = _order_quantity(order)
        price = decimal_or_none(order.get("limit_price")) or decimal_or_none(order.get("price")) or mark_price
        notional = qty * product.contract_multiplier * price if price is not None else Decimal("0")
        if str(order.get("side") or "").lower() == "buy":
            buy_count += 1
            buy_qty += qty
            buy_notional += notional
        elif str(order.get("side") or "").lower() == "sell":
            sell_count += 1
            sell_qty += qty
            sell_notional += notional

    values = {
        "wallet_balance": wallet_balance,
        "account_equity": account_equity,
        "available_margin": available_margin,
        "used_margin": used_margin,
        "position_lots": position_lots,
        "mark_price": mark_price,
    }
    unavailable.extend([key for key, value in values.items() if value is None])
    for key, value in {
        "average_entry_price": average_entry_price,
        "unrealized_pnl": unrealized_pnl,
        "realized_pnl": realized_pnl,
        "liquidation_price": liquidation_price,
        "initial_margin": None,
        "maintenance_margin": None,
        "margin_mode": None,
        "leverage": None,
        "portfolio_delta": None,
        "portfolio_gamma": None,
        "portfolio_vega": None,
        "portfolio_theta": None,
    }.items():
        if value is None:
            unavailable.append(key)

    status = _status_from_syncs(syncs, stale_after_seconds, unavailable)
    now = utc_now()
    return AccountRiskState(
        timestamp=now,
        source="delta_testnet_rest",
        telemetry_status=status,
        wallet_balance=wallet_balance,
        account_equity=account_equity,
        available_margin=available_margin,
        used_margin=used_margin,
        margin_utilisation_pct=margin_utilisation_pct,
        position_product=product.symbol,
        position_lots=position_lots,
        position_base_quantity=position_base_quantity,
        position_side=_side_from_lots(position_lots),
        average_entry_price=average_entry_price,
        mark_price=mark_price,
        position_notional=position_notional,
        unrealized_pnl=unrealized_pnl,
        realized_pnl=realized_pnl,
        liquidation_price=liquidation_price,
        open_order_count=len(orders),
        open_buy_order_count=buy_count,
        open_sell_order_count=sell_count,
        open_buy_quantity_lots=buy_qty,
        open_sell_quantity_lots=sell_qty,
        open_buy_notional=buy_notional,
        open_sell_notional=sell_notional,
        last_account_sync=syncs.get("account").last_sync if syncs.get("account") else None,
        last_position_sync=syncs.get("position").last_sync if syncs.get("position") else None,
        last_order_sync=syncs.get("orders").last_sync if syncs.get("orders") else None,
        last_market_sync=syncs.get("market").last_sync if syncs.get("market") else None,
        account_age_seconds=syncs.get("account").age_seconds if syncs.get("account") else None,
        position_age_seconds=syncs.get("position").age_seconds if syncs.get("position") else None,
        order_age_seconds=syncs.get("orders").age_seconds if syncs.get("orders") else None,
        market_age_seconds=syncs.get("market").age_seconds if syncs.get("market") else None,
        errors=error_list,
        unavailable_fields=sorted(set(unavailable)),
        capability_matrix=CAPABILITY_MATRIX,
        units=UNITS,
        request_counts=request_counts or {},
    )


def risk_increasing_action_allowed(state: AccountRiskState) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if state.telemetry_status in {TelemetryStatus.STALE.value, TelemetryStatus.UNAVAILABLE.value}:
        reasons.append(f"TELEMETRY_{state.telemetry_status}")
    if state.position_lots is None:
        reasons.append("POSITION_UNKNOWN")
    if state.account_equity is None:
        reasons.append("ACCOUNT_EQUITY_UNKNOWN")
    return not reasons, reasons


def risk_reducing_action_allowed(state: AccountRiskState) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if state.position_lots is None:
        reasons.append("POSITION_UNKNOWN")
    if state.open_order_count is None:
        reasons.append("OPEN_ORDERS_UNKNOWN")
    return not reasons, reasons


class AccountTelemetryCache:
    def __init__(
        self,
        client: DeltaTestnetClient | None = None,
        refresh_interval_seconds: float | None = None,
        stale_after_seconds: float | None = None,
    ):
        self.client = client or DeltaTestnetClient()
        self.refresh_interval_seconds = refresh_interval_seconds if refresh_interval_seconds is not None else float(os.getenv("GRIDBOT_V01_ACCOUNT_TELEMETRY_SECONDS", "30"))
        self.stale_after_seconds = stale_after_seconds if stale_after_seconds is not None else float(os.getenv("GRIDBOT_V01_TELEMETRY_STALE_SECONDS", "90"))
        self._last_refresh_monotonic = 0.0
        self._state: AccountRiskState | None = None
        self._last_payloads: dict[str, dict | None] = {"wallet": None, "positions": None, "orders": None, "ticker": None}
        self._last_syncs: dict[str, TelemetrySync] = {}
        self._last_product: ProductSpec | None = None
        self.request_counts = {"wallet": 0, "positions": 0, "orders": 0, "ticker": 0, "product_spec": 0}

    def snapshot(self) -> dict | None:
        return self._with_current_ages(self._state).as_dict() if self._state else None

    def get(self, product_symbol: str = "ETHUSD", force: bool = False) -> AccountRiskState:
        now = time.monotonic()
        if self._state and not force and now - self._last_refresh_monotonic < self.refresh_interval_seconds:
            return self._with_current_ages(self._state)
        self._state = self.refresh(product_symbol)
        self._last_refresh_monotonic = time.monotonic()
        return self._state

    def _with_current_ages(self, state: AccountRiskState) -> AccountRiskState:
        now = datetime.now(timezone.utc)
        syncs = {
            "account": TelemetrySync(state.last_account_sync, _sync_age(state.last_account_sync, now), bool(state.last_account_sync)),
            "position": TelemetrySync(state.last_position_sync, _sync_age(state.last_position_sync, now), bool(state.last_position_sync)),
            "orders": TelemetrySync(state.last_order_sync, _sync_age(state.last_order_sync, now), bool(state.last_order_sync)),
            "market": TelemetrySync(state.last_market_sync, _sync_age(state.last_market_sync, now), bool(state.last_market_sync)),
        }
        return replace(
            state,
            timestamp=utc_now(),
            telemetry_status=_status_from_syncs(syncs, self.stale_after_seconds, state.unavailable_fields),
            account_age_seconds=syncs["account"].age_seconds,
            position_age_seconds=syncs["position"].age_seconds,
            order_age_seconds=syncs["orders"].age_seconds,
            market_age_seconds=syncs["market"].age_seconds,
        )

    def refresh(self, product_symbol: str = "ETHUSD") -> AccountRiskState:
        errors: list[str] = []
        syncs: dict[str, TelemetrySync] = {}
        payloads: dict[str, dict | None] = {"wallet": None, "positions": None, "orders": None, "ticker": None}
        product = None
        now = datetime.now(timezone.utc)

        def capture(name: str, callback):
            sync_name = {"wallet": "account", "positions": "position", "ticker": "market"}.get(name, name)
            try:
                payload = callback()
                self.request_counts[name] = self.request_counts.get(name, 0) + 1
                sync = TelemetrySync(utc_now(), 0.0, True)
                syncs[sync_name] = sync
                self._last_payloads[name] = payload
                self._last_syncs[sync_name] = sync
                return payload
            except Exception as exc:
                message = f"{name}: {type(exc).__name__}: {str(exc)[:200]}"
                errors.append(message)
                previous_sync = self._last_syncs.get(sync_name)
                previous_payload = self._last_payloads.get(name)
                if previous_sync and previous_payload is not None:
                    syncs[sync_name] = TelemetrySync(previous_sync.last_sync, _sync_age(previous_sync.last_sync, now), True, message)
                    return previous_payload
                syncs[sync_name] = TelemetrySync(None, None, False, message)
                return None

        try:
            product = self.client.product_spec(product_symbol)
            self.request_counts["product_spec"] += 1
            self._last_product = product
        except Exception as exc:
            errors.append(f"product_spec: {type(exc).__name__}: {str(exc)[:200]}")
            product = self._last_product or ProductSpec(0, product_symbol, "", Decimal("1"), Decimal("1"), Decimal("1"), Decimal("0.1"), 1, 0, Decimal("0"))

        payloads["wallet"] = capture("wallet", lambda: self.client.wallet())
        payloads["positions"] = capture("positions", lambda: self.client.positions("ETH"))
        payloads["orders"] = capture("orders", lambda: self.client.open_orders(product.product_id if product.product_id else None))
        payloads["ticker"] = capture("ticker", lambda: self.client.ticker(product_symbol))

        return normalize_account_risk_state(
            product=product,
            wallet_payload=payloads["wallet"],
            positions_payload=payloads["positions"],
            orders_payload=payloads["orders"],
            ticker_payload=payloads["ticker"],
            syncs=syncs,
            errors=errors,
            stale_after_seconds=self.stale_after_seconds,
            request_counts=dict(self.request_counts),
        )


account_telemetry_cache = AccountTelemetryCache()
