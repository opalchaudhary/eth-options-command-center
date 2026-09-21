from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any

from .models import GridType, Side


TERMINAL_ORDER_STATUSES = {"cancelled", "closed", "filled", "not_open", "manual_cancelled", "deferred", "blocked"}


@dataclass(frozen=True)
class InventoryReservation:
    long_reserved: Decimal = Decimal("0")
    short_reserved: Decimal = Decimal("0")


@dataclass(frozen=True)
class OrderRiskBreakdown:
    side: Side
    quantity: Decimal
    reducing_quantity: Decimal
    increasing_quantity: Decimal
    long_opening_quantity: Decimal
    short_opening_quantity: Decimal
    projected_inventory: Decimal
    risk_increasing: bool
    risk_reducing: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "side": self.side.value,
            "quantity": str(self.quantity),
            "reducing_quantity": str(self.reducing_quantity),
            "increasing_quantity": str(self.increasing_quantity),
            "long_opening_quantity": str(self.long_opening_quantity),
            "short_opening_quantity": str(self.short_opening_quantity),
            "projected_inventory": str(self.projected_inventory),
            "risk_increasing": self.risk_increasing,
            "risk_reducing": self.risk_reducing,
        }


@dataclass(frozen=True)
class DynamicInventoryReservation:
    current_inventory: Decimal = Decimal("0")
    total_buy_quantity: Decimal = Decimal("0")
    total_sell_quantity: Decimal = Decimal("0")
    long_opening_resting: Decimal = Decimal("0")
    short_opening_resting: Decimal = Decimal("0")
    risk_reducing_resting_quantity: Decimal = Decimal("0")
    risk_increasing_resting_quantity: Decimal = Decimal("0")
    risk_increasing_resting_orders: list[dict[str, Any]] = field(default_factory=list)
    worst_case_long: Decimal = Decimal("0")
    worst_case_short: Decimal = Decimal("0")
    long_reserved: Decimal = Decimal("0")
    short_reserved: Decimal = Decimal("0")

    def as_dict(self) -> dict[str, Any]:
        return {
            "current_inventory": str(self.current_inventory),
            "total_buy_quantity": str(self.total_buy_quantity),
            "total_sell_quantity": str(self.total_sell_quantity),
            "long_opening_resting": str(self.long_opening_resting),
            "short_opening_resting": str(self.short_opening_resting),
            "risk_reducing_resting_quantity": str(self.risk_reducing_resting_quantity),
            "risk_increasing_resting_quantity": str(self.risk_increasing_resting_quantity),
            "risk_increasing_resting_orders": self.risk_increasing_resting_orders,
            "worst_case_long": str(self.worst_case_long),
            "worst_case_short": str(self.worst_case_short),
            "long_reserved": str(self.long_reserved),
            "short_reserved": str(self.short_reserved),
        }


@dataclass(frozen=True)
class OrderSemanticDecision:
    allowed: bool
    projected_inventory: Decimal
    opens_inventory: bool
    reduces_inventory: bool
    opening_quantity: Decimal
    reserved_long_after: Decimal
    reserved_short_after: Decimal
    reason_codes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PostOnlyDecision:
    allowed: bool
    normalized_price: Decimal
    reason_codes: list[str] = field(default_factory=list)


def round_price_for_side(price: Decimal, tick_size: Decimal, side: Side) -> Decimal:
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    rounding = ROUND_FLOOR if side == Side.BUY else ROUND_CEILING
    return (price / tick_size).to_integral_value(rounding=rounding) * tick_size


def validate_post_only_price(side: Side, price: Decimal, best_bid: Decimal | None, best_ask: Decimal | None) -> PostOnlyDecision:
    reasons: list[str] = []
    if side == Side.BUY and best_ask is not None and price >= best_ask:
        reasons.append("POST_ONLY_BUY_WOULD_CROSS_ASK")
    if side == Side.SELL and best_bid is not None and price <= best_bid:
        reasons.append("POST_ONLY_SELL_WOULD_CROSS_BID")
    return PostOnlyDecision(not reasons, price, reasons)


def projected_inventory(current_inventory: Decimal, side: Side, quantity: Decimal) -> Decimal:
    return current_inventory + quantity if side == Side.BUY else current_inventory - quantity


def opening_quantity(current_inventory: Decimal, projected: Decimal, side: Side) -> Decimal:
    if side == Side.BUY:
        return max(Decimal("0"), projected) - max(Decimal("0"), current_inventory)
    return max(Decimal("0"), -projected) - max(Decimal("0"), -current_inventory)


def order_risk_breakdown(current_inventory: Decimal, side: Side, quantity: Decimal) -> OrderRiskBreakdown:
    quantity = max(Decimal("0"), quantity)
    projected = projected_inventory(current_inventory, side, quantity)
    long_opening = max(Decimal("0"), projected) - max(Decimal("0"), current_inventory) if side == Side.BUY else Decimal("0")
    short_opening = max(Decimal("0"), -projected) - max(Decimal("0"), -current_inventory) if side == Side.SELL else Decimal("0")
    increasing = max(Decimal("0"), long_opening + short_opening)
    reducing = max(Decimal("0"), quantity - increasing)
    return OrderRiskBreakdown(
        side=side,
        quantity=quantity,
        reducing_quantity=reducing,
        increasing_quantity=increasing,
        long_opening_quantity=long_opening,
        short_opening_quantity=short_opening,
        projected_inventory=projected,
        risk_increasing=increasing > 0,
        risk_reducing=reducing > 0,
    )


def order_reduces_inventory(current_inventory: Decimal, projected: Decimal) -> bool:
    return abs(projected) < abs(current_inventory)


def _open_order_remaining(order: dict[str, Any]) -> Decimal:
    return max(Decimal("0"), Decimal(str(order.get("remaining_quantity") or order.get("requested_quantity") or "0")))


def _iter_active_open_orders(open_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [order for order in open_orders if str(order.get("status") or "").lower() not in TERMINAL_ORDER_STATUSES]


def dynamic_inventory_reservation(grid_type: GridType, current_inventory: Decimal, open_orders: list[dict[str, Any]]) -> DynamicInventoryReservation:
    total_buy = Decimal("0")
    total_sell = Decimal("0")
    risk_increasing_orders: list[dict[str, Any]] = []

    for order in open_orders:
        if str(order.get("status") or "").lower() in TERMINAL_ORDER_STATUSES:
            continue
        raw_side = str(order.get("side") or "").lower()
        if raw_side not in {Side.BUY.value, Side.SELL.value}:
            continue
        side = Side(raw_side)
        remaining = _open_order_remaining(order)
        if remaining <= 0:
            continue
        if side == Side.BUY:
            total_buy += remaining
        else:
            total_sell += remaining
        breakdown = order_risk_breakdown(current_inventory, side, remaining)
        if breakdown.risk_increasing:
            risk_increasing_orders.append(
                {
                    "client_order_id": order.get("client_order_id"),
                    "exchange_order_id": order.get("exchange_order_id"),
                    "side": side.value,
                    "remaining_quantity": str(remaining),
                    "projected_inventory": str(breakdown.projected_inventory),
                    "increasing_quantity": str(breakdown.increasing_quantity),
                    "stored_opens_inventory": order.get("opens_inventory"),
                }
            )

    worst_case_long = current_inventory + total_buy
    worst_case_short = current_inventory - total_sell
    long_opening = max(Decimal("0"), worst_case_long) - max(Decimal("0"), current_inventory)
    short_opening = max(Decimal("0"), -worst_case_short) - max(Decimal("0"), -current_inventory)
    buy_reducing = min(total_buy, max(Decimal("0"), -current_inventory))
    sell_reducing = min(total_sell, max(Decimal("0"), current_inventory))
    risk_reducing = buy_reducing + sell_reducing
    risk_increasing = long_opening + short_opening
    long_reserved = max(Decimal("0"), current_inventory) + (long_opening if grid_type in {GridType.NEUTRAL, GridType.LONG_BIAS} else Decimal("0"))
    short_reserved = max(Decimal("0"), -current_inventory) + (short_opening if grid_type in {GridType.NEUTRAL, GridType.SHORT_BIAS} else Decimal("0"))
    return DynamicInventoryReservation(
        current_inventory=current_inventory,
        total_buy_quantity=total_buy,
        total_sell_quantity=total_sell,
        long_opening_resting=long_opening,
        short_opening_resting=short_opening,
        risk_reducing_resting_quantity=risk_reducing,
        risk_increasing_resting_quantity=risk_increasing,
        risk_increasing_resting_orders=risk_increasing_orders,
        worst_case_long=worst_case_long,
        worst_case_short=worst_case_short,
        long_reserved=long_reserved,
        short_reserved=short_reserved,
    )


def inventory_reservation(grid_type: GridType, current_inventory: Decimal, open_orders: list[dict[str, Any]]) -> InventoryReservation:
    reservation = dynamic_inventory_reservation(grid_type, current_inventory, open_orders)
    return InventoryReservation(reservation.long_reserved, reservation.short_reserved)


def resting_order_risk_summary(
    grid_type: GridType,
    current_inventory: Decimal,
    max_inventory: Decimal,
    open_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    reservation = dynamic_inventory_reservation(grid_type, current_inventory, open_orders)
    return {
        **reservation.as_dict(),
        "max_inventory": str(max_inventory),
        "position_over_max": abs(current_inventory) > max_inventory if max_inventory > 0 else False,
        "long_reserved_over_max": reservation.long_reserved > max_inventory if max_inventory > 0 else False,
        "short_reserved_over_max": reservation.short_reserved > max_inventory if max_inventory > 0 else False,
        "risk_increasing_resting_exposure": reservation.risk_increasing_resting_quantity > 0,
    }


def evaluate_order_semantics(
    grid_type: GridType,
    current_inventory: Decimal,
    max_inventory: Decimal,
    side: Side,
    quantity: Decimal,
    open_orders: list[dict[str, Any]] | None = None,
) -> OrderSemanticDecision:
    projected = projected_inventory(current_inventory, side, quantity)
    reasons: list[str] = []
    proposed_breakdown = order_risk_breakdown(current_inventory, side, quantity)
    reduces = order_reduces_inventory(current_inventory, projected)
    opening = proposed_breakdown.long_opening_quantity if side == Side.BUY else proposed_breakdown.short_opening_quantity
    opens = opening > 0
    existing_reservation = dynamic_inventory_reservation(grid_type, current_inventory, _iter_active_open_orders(open_orders or []))
    proposed_order = {"side": side.value, "remaining_quantity": str(quantity), "status": "open"}
    reservation = dynamic_inventory_reservation(grid_type, current_inventory, [*(_iter_active_open_orders(open_orders or [])), proposed_order])
    long_after = reservation.long_reserved
    short_after = reservation.short_reserved

    if grid_type == GridType.NEUTRAL:
        if not reduces and (projected > max_inventory or projected < -max_inventory):
            reasons.append("MAX_INVENTORY_EXCEEDED")
        if side == Side.BUY and long_after > max_inventory and long_after > existing_reservation.long_reserved:
            reasons.append("LONG_OPENING_RESERVATION_EXCEEDED")
        if side == Side.SELL and short_after > max_inventory and short_after > existing_reservation.short_reserved:
            reasons.append("SHORT_OPENING_RESERVATION_EXCEEDED")
    elif grid_type == GridType.LONG_BIAS:
        if projected < min(current_inventory, Decimal("0")):
            reasons.append("LONG_BIAS_CANNOT_OPEN_NET_SHORT")
        if not reduces and projected > max_inventory:
            reasons.append("MAX_LONG_INVENTORY_EXCEEDED")
        if side == Side.BUY and long_after > max_inventory and long_after > existing_reservation.long_reserved:
            reasons.append("LONG_OPENING_RESERVATION_EXCEEDED")
    elif grid_type == GridType.SHORT_BIAS:
        if projected > max(current_inventory, Decimal("0")):
            reasons.append("SHORT_BIAS_CANNOT_OPEN_NET_LONG")
        if not reduces and projected < -max_inventory:
            reasons.append("MAX_SHORT_INVENTORY_EXCEEDED")
        if side == Side.SELL and short_after > max_inventory and short_after > existing_reservation.short_reserved:
            reasons.append("SHORT_OPENING_RESERVATION_EXCEEDED")

    return OrderSemanticDecision(
        allowed=not reasons,
        projected_inventory=projected,
        opens_inventory=opens,
        reduces_inventory=reduces,
        opening_quantity=opening,
        reserved_long_after=long_after,
        reserved_short_after=short_after,
        reason_codes=reasons,
    )
