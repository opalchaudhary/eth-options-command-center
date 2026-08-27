from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from .config import ACCOUNTING_VERSION
from .models import FillRecord, Side


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
