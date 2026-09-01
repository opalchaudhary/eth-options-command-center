from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

from .config import ACCOUNTING_VERSION
from .models import FillRecord, Side, utc_now


FEE_CONFIRMED = "CONFIRMED"
FEE_PENDING = "PENDING"
FEE_UNAVAILABLE = "UNAVAILABLE"
ACCOUNTING_COMPLETE = "COMPLETE"
ACCOUNTING_PARTIAL = "PARTIAL"
ACCOUNTING_UNAVAILABLE = "UNAVAILABLE"
FUNDING_ATTRIBUTED = "ATTRIBUTED"
FUNDING_PARTIALLY_ATTRIBUTED = "PARTIALLY_ATTRIBUTED"
FUNDING_UNATTRIBUTED = "UNATTRIBUTED"
FUNDING_UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class ExchangeCost:
    run_id: str
    order_id: Optional[str]
    fill_id: Optional[str]
    exchange_cost_type: str
    amount: Decimal
    currency: str
    direction: str
    exchange_transaction_id: Optional[str] = None
    timestamp: Optional[str] = None
    raw_reference: dict | None = None
    config_version: Optional[int] = None


@dataclass(frozen=True)
class FeeExtraction:
    amount: Decimal | None
    currency: str | None
    status: str
    source: str


@dataclass(frozen=True)
class AccountingFill:
    fill_id: str
    run_id: str
    order_id: str | None
    exchange_order_id: str | None
    exchange_fill_id: str | None
    level_id: str | None
    config_version: int | None
    side: Side
    quantity_lots: Decimal
    base_quantity: Decimal
    fill_price: Decimal
    notional_value: Decimal
    maker_taker_role: str
    trading_fee: Decimal | None
    fee_currency: str | None
    fee_status: str
    timestamp: str | None
    source_fill_id: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CycleRecord:
    cycle_id: str
    run_id: str
    config_version: int | None
    entry_config_version: int | None
    exit_config_version: int | None
    entry_fill_id: str
    exit_fill_id: str
    direction: str
    entry_level: str | None
    exit_level: str | None
    quantity_lots: Decimal
    base_quantity: Decimal
    entry_price: Decimal
    exit_price: Decimal
    gross_pnl: Decimal
    entry_fee: Decimal
    exit_fee: Decimal
    total_trading_fees: Decimal
    funding: Decimal
    other_costs: Decimal
    other_credits: Decimal
    net_pnl: Decimal
    opened_at: str | None
    closed_at: str | None
    duration_seconds: int | None
    status: str
    fee_to_gross_profit_ratio: Decimal | None
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PnlSummary:
    gross_realised_pnl: Decimal
    maker_fees: Decimal
    taker_fees: Decimal
    total_exchange_fees: Decimal
    funding_received: Decimal
    funding_paid: Decimal
    net_funding: Decimal
    other_exchange_costs: Decimal
    other_exchange_credits: Decimal
    net_trading_pnl_before_income_tax: Decimal
    effective_cost_bps: Decimal
    accounting_version: str = ACCOUNTING_VERSION


@dataclass(frozen=True)
class RunAccounting:
    fills_total: int
    cycles_completed: int
    gross_realized_pnl: Decimal
    trading_fees: Decimal
    realized_trading_fees: Decimal
    open_inventory_trading_fees: Decimal
    funding_paid: Decimal
    funding_received: Decimal
    funding_net: Decimal
    other_costs: Decimal
    other_credits: Decimal
    net_realized_pnl: Decimal
    unrealized_pnl: Decimal | None
    live_net_pnl: Decimal | None
    fee_to_gross_ratio: Decimal | None
    accounting_status: str
    warnings: list[str]
    cycles: list[CycleRecord]
    remaining_inventory_lots: Decimal
    remaining_inventory_basis: Decimal
    maker_fees: Decimal
    taker_fees: Decimal
    unknown_role_fees: Decimal
    funding_attribution_status: str = FUNDING_UNAVAILABLE
    accounting_version: str = ACCOUNTING_VERSION

    def as_dict(self) -> dict:
        return {
            "fills_total": self.fills_total,
            "cycles_completed": self.cycles_completed,
            "gross_realized_pnl": str(self.gross_realized_pnl),
            "trading_fees": str(self.trading_fees),
            "realized_trading_fees": str(self.realized_trading_fees),
            "open_inventory_trading_fees": str(self.open_inventory_trading_fees),
            "funding_paid": str(self.funding_paid),
            "funding_received": str(self.funding_received),
            "funding_net": str(self.funding_net),
            "other_costs": str(self.other_costs),
            "other_credits": str(self.other_credits),
            "net_realized_pnl": str(self.net_realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl) if self.unrealized_pnl is not None else None,
            "live_net_pnl": str(self.live_net_pnl) if self.live_net_pnl is not None else None,
            "fee_to_gross_ratio": str(self.fee_to_gross_ratio) if self.fee_to_gross_ratio is not None else None,
            "accounting_status": self.accounting_status,
            "warnings": list(self.warnings),
            "remaining_inventory_lots": str(self.remaining_inventory_lots),
            "remaining_inventory_basis": str(self.remaining_inventory_basis),
            "maker_fees": str(self.maker_fees),
            "taker_fees": str(self.taker_fees),
            "unknown_role_fees": str(self.unknown_role_fees),
            "funding_attribution_status": self.funding_attribution_status,
            "accounting_version": self.accounting_version,
        }


def decimal_value(value: Any, default: str = "0") -> Decimal:
    try:
        if value in [None, ""]:
            return Decimal(default)
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def explicit_decimal(value: Any) -> Decimal | None:
    if value in [None, ""]:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def normalize_maker_taker_role(raw: dict[str, Any]) -> str:
    value = str(
        raw.get("maker_taker_role")
        or raw.get("liquidity_role")
        or raw.get("liquidity")
        or raw.get("role")
        or raw.get("execution_role")
        or ""
    ).lower()
    if value in {"m", "maker", "maker_order"}:
        return "maker"
    if value in {"t", "taker", "taker_order"}:
        return "taker"
    return "unknown"


def extract_fee(raw: dict[str, Any]) -> FeeExtraction:
    for key in ["fee", "commission", "trading_fee", "paid_commission", "execution_fee"]:
        if key in raw:
            value = explicit_decimal(raw.get(key))
            if value is not None:
                return FeeExtraction(
                    amount=abs(value),
                    currency=raw.get("fee_currency") or raw.get("commission_asset") or raw.get("commission_currency") or "USD",
                    status=FEE_CONFIRMED,
                    source=f"fill.{key}",
                )
    status = str(raw.get("fee_status") or "").upper()
    if status in {FEE_CONFIRMED, FEE_PENDING, FEE_UNAVAILABLE}:
        amount = explicit_decimal(raw.get("trading_fee"))
        return FeeExtraction(
            amount=amount if status == FEE_CONFIRMED else None,
            currency=raw.get("fee_currency"),
            status=status,
            source=str(raw.get("fee_source") or "persisted"),
        )
    if raw:
        return FeeExtraction(amount=None, currency=raw.get("fee_currency"), status=FEE_PENDING, source="fill_missing_fee")
    return FeeExtraction(amount=None, currency=None, status=FEE_UNAVAILABLE, source="no_fill_payload")


def fill_identity(fill: dict[str, Any], fallback: str = "") -> str:
    return str(fill.get("id") or fill.get("fill_id") or fill.get("trade_id") or fill.get("execution_id") or fallback)


def fill_notional(price: Decimal, quantity_lots: Decimal, contract_multiplier: Decimal) -> Decimal:
    return price * quantity_lots * contract_multiplier


def futures_pnl(entry_side: Side, entry_price: Decimal, exit_price: Decimal, base_quantity: Decimal) -> Decimal:
    if entry_side == Side.BUY:
        return (exit_price - entry_price) * base_quantity
    if entry_side == Side.SELL:
        return (entry_price - exit_price) * base_quantity
    return Decimal("0")


def normalize_fill(run: dict[str, Any], fill_id: str, fill: dict[str, Any]) -> AccountingFill:
    raw = fill.get("raw") if isinstance(fill, dict) and isinstance(fill.get("raw"), dict) else fill
    raw = raw or {}
    orders = run.get("orders") or {}
    client_order_id = str(raw.get("client_order_id") or fill.get("client_order_id") or "")
    exchange_order_id = str(raw.get("order_id") or raw.get("exchange_order_id") or fill.get("exchange_order_id") or "")
    order = orders.get(client_order_id) or next(
        (row for row in orders.values() if str(row.get("exchange_order_id") or "") == exchange_order_id),
        {},
    )
    side = Side(str(raw.get("side") or fill.get("side") or order.get("side") or "").lower())
    price = decimal_value(raw.get("price") or raw.get("fill_price") or fill.get("price") or order.get("price"))
    quantity_lots = decimal_value(raw.get("size") or raw.get("quantity") or raw.get("fill_size") or fill.get("quantity"))
    multiplier = decimal_value((run.get("product") or {}).get("contract_multiplier"), "1")
    fee = extract_fee({**raw, **({k: v for k, v in fill.items() if k not in raw} if isinstance(fill, dict) else {})})
    return AccountingFill(
        fill_id=str(fill.get("fill_id") or fill_id),
        run_id=run.get("run_id"),
        order_id=order.get("order_key") or order.get("order_id") or client_order_id or None,
        exchange_order_id=exchange_order_id or None,
        exchange_fill_id=fill_identity(raw, fill_id),
        level_id=order.get("level_id") or fill.get("level_id"),
        config_version=int(order.get("config_version") or fill.get("config_version")) if (order.get("config_version") or fill.get("config_version")) else None,
        side=side,
        quantity_lots=quantity_lots,
        base_quantity=quantity_lots * multiplier,
        fill_price=price,
        notional_value=fill_notional(price, quantity_lots, multiplier),
        maker_taker_role=normalize_maker_taker_role(raw),
        trading_fee=fee.amount,
        fee_currency=fee.currency,
        fee_status=fee.status,
        timestamp=raw.get("created_at") if isinstance(raw.get("created_at"), str) else fill.get("exchange_timestamp"),
        source_fill_id=order.get("source_fill_id") or ((order.get("raw") or {}).get("gridbot") or {}).get("source_fill_id"),
        raw=raw,
    )


def gross_cycle_pnl(entry: FillRecord, exit: FillRecord, contract_multiplier: Decimal) -> Decimal:
    qty = min(entry.quantity, exit.quantity)
    if entry.side == Side.BUY and exit.side == Side.SELL:
        return (exit.price - entry.price) * qty * contract_multiplier
    if entry.side == Side.SELL and exit.side == Side.BUY:
        return (entry.price - exit.price) * qty * contract_multiplier
    return Decimal("0")


def summarize_pnl(
    gross_realised_pnl: Decimal,
    fills: list[FillRecord],
    costs: list[ExchangeCost],
    notional: Decimal,
) -> PnlSummary:
    maker_fees = sum((fill.fee for fill in fills if fill.liquidity_role == "maker"), Decimal("0"))
    taker_fees = sum((fill.fee for fill in fills if fill.liquidity_role == "taker"), Decimal("0"))
    unknown_fees = sum((fill.fee for fill in fills if fill.liquidity_role not in {"maker", "taker"}), Decimal("0"))
    total_fees = maker_fees + taker_fees + unknown_fees
    funding_received = sum((c.amount for c in costs if c.exchange_cost_type == "funding" and c.direction == "credit"), Decimal("0"))
    funding_paid = sum((c.amount for c in costs if c.exchange_cost_type == "funding" and c.direction == "debit"), Decimal("0"))
    other_costs = sum((c.amount for c in costs if c.exchange_cost_type != "funding" and c.direction == "debit"), Decimal("0"))
    other_credits = sum((c.amount for c in costs if c.exchange_cost_type != "funding" and c.direction == "credit"), Decimal("0"))
    net_funding = funding_received - funding_paid
    net = gross_realised_pnl - total_fees + net_funding - other_costs + other_credits
    effective_cost_bps = Decimal("0") if notional == 0 else ((total_fees + other_costs - other_credits) / notional) * Decimal("10000")
    return PnlSummary(
        gross_realised_pnl=gross_realised_pnl,
        maker_fees=maker_fees,
        taker_fees=taker_fees,
        total_exchange_fees=total_fees,
        funding_received=funding_received,
        funding_paid=funding_paid,
        net_funding=net_funding,
        other_exchange_costs=other_costs,
        other_exchange_credits=other_credits,
        net_trading_pnl_before_income_tax=net,
        effective_cost_bps=effective_cost_bps,
    )


def _allocated_fee(fill: AccountingFill, closed_lots: Decimal) -> Decimal:
    if fill.trading_fee is None or fill.quantity_lots == 0:
        return Decimal("0")
    return fill.trading_fee * (closed_lots / fill.quantity_lots)


def _cycle_status(warnings: list[str]) -> str:
    return ACCOUNTING_COMPLETE if not warnings else ACCOUNTING_PARTIAL


def _interval_seconds(opened_at: str | None, closed_at: str | None) -> int | None:
    if not opened_at or not closed_at:
        return None
    try:
        from datetime import datetime

        return int((datetime.fromisoformat(closed_at.replace("Z", "+00:00")) - datetime.fromisoformat(opened_at.replace("Z", "+00:00"))).total_seconds())
    except Exception:
        return None


def _cost_amounts(costs: list[ExchangeCost]) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    funding_received = sum((c.amount for c in costs if c.exchange_cost_type == "funding" and c.direction == "credit"), Decimal("0"))
    funding_paid = sum((c.amount for c in costs if c.exchange_cost_type == "funding" and c.direction == "debit"), Decimal("0"))
    other_costs = sum((c.amount for c in costs if c.exchange_cost_type not in {"funding", "trading_fee"} and c.direction == "debit"), Decimal("0"))
    other_credits = sum((c.amount for c in costs if c.exchange_cost_type not in {"funding", "trading_fee"} and c.direction == "credit"), Decimal("0"))
    return funding_received, funding_paid, other_costs, other_credits


def normalize_cost(row: ExchangeCost | dict[str, Any]) -> ExchangeCost:
    if isinstance(row, ExchangeCost):
        return row
    return ExchangeCost(
        run_id=str(row.get("run_id") or ""),
        order_id=row.get("order_id"),
        fill_id=row.get("fill_id"),
        exchange_cost_type=str(row.get("exchange_cost_type") or row.get("cost_type") or ""),
        amount=decimal_value(row.get("amount")),
        currency=str(row.get("currency") or "USD"),
        direction=str(row.get("direction") or "debit"),
        exchange_transaction_id=row.get("exchange_transaction_id"),
        timestamp=row.get("exchange_timestamp") or row.get("timestamp"),
        raw_reference=row.get("raw") or row.get("raw_reference"),
        config_version=int(row.get("config_version")) if row.get("config_version") not in [None, ""] else None,
    )


def _fill_sort_key(fill: AccountingFill) -> tuple[str, str]:
    return (fill.timestamp or "", fill.exchange_fill_id or fill.fill_id)


def build_cycle_ledger(run: dict[str, Any]) -> tuple[list[CycleRecord], list[AccountingFill], Decimal, Decimal]:
    fills = sorted(
        [normalize_fill(run, fill_id, fill) for fill_id, fill in (run.get("fills") or {}).items()],
        key=_fill_sort_key,
    )
    open_inventory: list[dict[str, Any]] = []
    cycles: list[CycleRecord] = []
    remaining_basis = Decimal("0")
    remaining_lots = Decimal("0")

    for fill in fills:
        remaining = fill.quantity_lots
        opposite = Side.SELL if fill.side == Side.BUY else Side.BUY
        while remaining > 0 and open_inventory and open_inventory[0]["side"] == opposite:
            entry = open_inventory[0]
            close_lots = min(remaining, entry["remaining_lots"])
            base_quantity = close_lots * decimal_value((run.get("product") or {}).get("contract_multiplier"), "1")
            entry_fee = _allocated_fee(entry["fill"], close_lots)
            exit_fee = _allocated_fee(fill, close_lots)
            warnings = []
            if entry["fill"].fee_status == FEE_PENDING or fill.fee_status == FEE_PENDING:
                warnings.append("FILL_FEE_PENDING")
            if entry["fill"].fee_status == FEE_UNAVAILABLE or fill.fee_status == FEE_UNAVAILABLE:
                warnings.append("FILL_FEE_UNAVAILABLE")
            gross = futures_pnl(entry["side"], entry["price"], fill.fill_price, base_quantity)
            total_fees = entry_fee + exit_fee
            net = gross - total_fees
            direction = "LONG_CYCLE" if entry["side"] == Side.BUY else "SHORT_CYCLE"
            fee_ratio = None if gross == 0 else total_fees / gross if gross > 0 else None
            cycles.append(
                CycleRecord(
                    cycle_id=f"cycle_{entry['fill'].fill_id}_{fill.fill_id}_{len(cycles) + 1}",
                    run_id=fill.run_id,
                    config_version=fill.config_version,
                    entry_config_version=entry["fill"].config_version,
                    exit_config_version=fill.config_version,
                    entry_fill_id=entry["fill"].fill_id,
                    exit_fill_id=fill.fill_id,
                    direction=direction,
                    entry_level=entry["fill"].level_id,
                    exit_level=fill.level_id,
                    quantity_lots=close_lots,
                    base_quantity=base_quantity,
                    entry_price=entry["price"],
                    exit_price=fill.fill_price,
                    gross_pnl=gross,
                    entry_fee=entry_fee,
                    exit_fee=exit_fee,
                    total_trading_fees=total_fees,
                    funding=Decimal("0"),
                    other_costs=Decimal("0"),
                    other_credits=Decimal("0"),
                    net_pnl=net,
                    opened_at=entry["fill"].timestamp,
                    closed_at=fill.timestamp,
                    duration_seconds=_interval_seconds(entry["fill"].timestamp, fill.timestamp),
                    status=_cycle_status(warnings),
                    fee_to_gross_profit_ratio=fee_ratio,
                    warnings=warnings,
                )
            )
            entry["remaining_lots"] -= close_lots
            remaining -= close_lots
            if entry["remaining_lots"] == 0:
                open_inventory.pop(0)
            else:
                break
        if remaining > 0:
            open_inventory.append({"fill": fill, "side": fill.side, "price": fill.fill_price, "remaining_lots": remaining})

    for entry in open_inventory:
        sign = Decimal("1") if entry["side"] == Side.BUY else Decimal("-1")
        remaining_lots += sign * entry["remaining_lots"]
        remaining_basis += sign * entry["price"] * entry["remaining_lots"] * decimal_value((run.get("product") or {}).get("contract_multiplier"), "1")
    return cycles, fills, remaining_lots, remaining_basis


def calculate_unrealized_pnl(
    remaining_lots: Decimal,
    remaining_basis: Decimal,
    mark_price: Decimal | None,
    contract_multiplier: Decimal,
    attribution_clean: bool,
) -> Decimal | None:
    if remaining_lots == 0:
        return Decimal("0")
    if not attribution_clean or mark_price is None:
        return None
    mark_value = mark_price * abs(remaining_lots) * contract_multiplier
    if remaining_lots > 0:
        return mark_value - abs(remaining_basis)
    return abs(remaining_basis) - mark_value


def build_run_accounting(
    run: dict[str, Any],
    *,
    costs: list[ExchangeCost] | None = None,
    mark_price: Decimal | None = None,
    account_position_lots: Decimal | None = None,
) -> RunAccounting:
    costs = [normalize_cost(cost) for cost in (costs if costs is not None else run.get("exchange_costs") or [])]
    cycles, fills, remaining_lots, remaining_basis = build_cycle_ledger(run)
    gross = sum((cycle.gross_pnl for cycle in cycles), Decimal("0"))
    maker_fees = sum((fill.trading_fee or Decimal("0") for fill in fills if fill.maker_taker_role == "maker"), Decimal("0"))
    taker_fees = sum((fill.trading_fee or Decimal("0") for fill in fills if fill.maker_taker_role == "taker"), Decimal("0"))
    unknown_role_fees = sum((fill.trading_fee or Decimal("0") for fill in fills if fill.maker_taker_role == "unknown"), Decimal("0"))
    trading_fees = maker_fees + taker_fees + unknown_role_fees
    realized_trading_fees = sum((cycle.total_trading_fees for cycle in cycles), Decimal("0"))
    open_inventory_trading_fees = trading_fees - realized_trading_fees
    funding_received, funding_paid, other_costs, other_credits = _cost_amounts(costs)
    funding_net = funding_received - funding_paid
    warnings = sorted({warning for cycle in cycles for warning in cycle.warnings})
    if any(fill.fee_status == FEE_PENDING for fill in fills):
        warnings.append("FILL_FEE_PENDING")
    if any(fill.fee_status == FEE_UNAVAILABLE for fill in fills):
        warnings.append("FILL_FEE_UNAVAILABLE")
    trading_fee_cost_keys = [
        str(cost.exchange_transaction_id or cost.fill_id or cost.order_id or "")
        for cost in costs
        if cost.exchange_cost_type == "trading_fee"
    ]
    if len([key for key in trading_fee_cost_keys if key]) != len(set(key for key in trading_fee_cost_keys if key)):
        warnings.append("DUPLICATE_EXCHANGE_COST")
    summary = run.get("summary") if isinstance(run.get("summary"), dict) else {}
    external_resolution = run.get("external_position_adjustment") or run.get("external_position_resolution") or summary.get("external_position_resolution") or {}
    stop_diagnostics = run.get("stop_diagnostics") or {}
    if external_resolution or stop_diagnostics.get("reason") in {"attribution_mismatch", "unresolved_gridbot_order", "flatten_state_ambiguous"}:
        warnings.append("EXTERNAL_POSITION_CLOSE_UNATTRIBUTED")
    attribution_clean = account_position_lots is None or account_position_lots == remaining_lots
    if not attribution_clean:
        warnings.append("FUNDING_ATTRIBUTION_AMBIGUOUS")
    contract_multiplier = decimal_value((run.get("product") or {}).get("contract_multiplier"), "1")
    unrealized = calculate_unrealized_pnl(remaining_lots, remaining_basis, mark_price, contract_multiplier, attribution_clean)
    if unrealized is None and remaining_lots != 0:
        warnings.append("RUN_ACCOUNTING_INCOMPLETE")
    net_realized = gross - realized_trading_fees + funding_net - other_costs + other_credits
    live_net = None if unrealized is None else net_realized + unrealized - open_inventory_trading_fees
    fee_ratio = None if gross <= 0 else trading_fees / gross
    status = ACCOUNTING_COMPLETE
    if warnings:
        status = ACCOUNTING_PARTIAL
    if not fills and run.get("status") not in {"RUNNING", "STOPPING", "STOPPED"}:
        status = ACCOUNTING_UNAVAILABLE
    return RunAccounting(
        fills_total=len(fills),
        cycles_completed=len(cycles),
        gross_realized_pnl=gross,
        trading_fees=trading_fees,
        realized_trading_fees=realized_trading_fees,
        open_inventory_trading_fees=open_inventory_trading_fees,
        funding_paid=funding_paid,
        funding_received=funding_received,
        funding_net=funding_net,
        other_costs=other_costs,
        other_credits=other_credits,
        net_realized_pnl=net_realized,
        unrealized_pnl=unrealized,
        live_net_pnl=live_net,
        fee_to_gross_ratio=fee_ratio,
        accounting_status=status,
        warnings=sorted(set(warnings)),
        cycles=cycles,
        remaining_inventory_lots=remaining_lots,
        remaining_inventory_basis=remaining_basis,
        maker_fees=maker_fees,
        taker_fees=taker_fees,
        unknown_role_fees=unknown_role_fees,
        funding_attribution_status=FUNDING_ATTRIBUTED if attribution_clean and not external_resolution else FUNDING_PARTIALLY_ATTRIBUTED,
    )


def cycle_to_row(cycle: CycleRecord, bot_id: str | None = None) -> dict[str, Any]:
    return {
        "cycle_id": cycle.cycle_id,
        "run_id": cycle.run_id,
        "bot_id": bot_id,
        "config_version": cycle.config_version,
        "entry_config_version": cycle.entry_config_version,
        "exit_config_version": cycle.exit_config_version,
        "entry_fill_id": cycle.entry_fill_id,
        "exit_fill_id": cycle.exit_fill_id,
        "direction": cycle.direction,
        "entry_level": cycle.entry_level,
        "exit_level": cycle.exit_level,
        "level_id": cycle.exit_level,
        "quantity_lots": str(cycle.quantity_lots),
        "base_quantity": str(cycle.base_quantity),
        "entry_price": str(cycle.entry_price),
        "exit_price": str(cycle.exit_price),
        "gross_pnl": str(cycle.gross_pnl),
        "gross_grid_pnl": str(cycle.gross_pnl),
        "entry_fee": str(cycle.entry_fee),
        "exit_fee": str(cycle.exit_fee),
        "total_trading_fees": str(cycle.total_trading_fees),
        "exchange_fees": str(cycle.total_trading_fees),
        "funding": str(cycle.funding),
        "other_costs": str(cycle.other_costs),
        "other_credits": str(cycle.other_credits),
        "other_costs_credits": str(cycle.other_credits - cycle.other_costs),
        "net_pnl": str(cycle.net_pnl),
        "net_grid_pnl": str(cycle.net_pnl),
        "opened_at": cycle.opened_at,
        "closed_at": cycle.closed_at,
        "duration_seconds": cycle.duration_seconds,
        "status": cycle.status,
        "fee_to_gross_profit_ratio": str(cycle.fee_to_gross_profit_ratio) if cycle.fee_to_gross_profit_ratio is not None else None,
        "accounting_warnings": cycle.warnings,
        "created_at": utc_now(),
    }
