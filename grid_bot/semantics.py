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


def order_reduces_inventory(current_inventory: Decimal, projected: Decimal) -> bool:
    return abs(projected) < abs(current_inventory)


def inventory_reservation(grid_type: GridType, current_inventory: Decimal, open_orders: list[dict[str, Any]]) -> InventoryReservation:
    long_reserved = max(Decimal("0"), current_inventory)
    short_reserved = max(Decimal("0"), -current_inventory)
    for order in open_orders:
        if str(order.get("status") or "").lower() in TERMINAL_ORDER_STATUSES:
            continue
        if not order.get("opens_inventory", True):
            continue
        side = Side(str(order.get("side")).lower())
        remaining = Decimal(str(order.get("remaining_quantity") or order.get("requested_quantity") or "0"))
        if side == Side.BUY and grid_type in {GridType.NEUTRAL, GridType.LONG_BIAS}:
            long_reserved += remaining
        if side == Side.SELL and grid_type in {GridType.NEUTRAL, GridType.SHORT_BIAS}:
            short_reserved += remaining
    return InventoryReservation(long_reserved, short_reserved)


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
    reduces = order_reduces_inventory(current_inventory, projected)
    opening = opening_quantity(current_inventory, projected, side)
    opens = opening > 0
    reservation = inventory_reservation(grid_type, current_inventory, open_orders or [])
    long_after = reservation.long_reserved + (opening if side == Side.BUY else Decimal("0"))
    short_after = reservation.short_reserved + (opening if side == Side.SELL else Decimal("0"))

    if grid_type == GridType.NEUTRAL:
        if not reduces and (projected > max_inventory or projected < -max_inventory):
            reasons.append("MAX_INVENTORY_EXCEEDED")
        if side == Side.BUY and opening > 0 and long_after > max_inventory:
            reasons.append("LONG_OPENING_RESERVATION_EXCEEDED")
        if side == Side.SELL and opening > 0 and short_after > max_inventory:
            reasons.append("SHORT_OPENING_RESERVATION_EXCEEDED")
    elif grid_type == GridType.LONG_BIAS:
        if projected < min(current_inventory, Decimal("0")):
            reasons.append("LONG_BIAS_CANNOT_OPEN_NET_SHORT")
        if not reduces and projected > max_inventory:
            reasons.append("MAX_LONG_INVENTORY_EXCEEDED")
        if side == Side.BUY and opening > 0 and long_after > max_inventory:
            reasons.append("LONG_OPENING_RESERVATION_EXCEEDED")
    elif grid_type == GridType.SHORT_BIAS:
        if projected > max(current_inventory, Decimal("0")):
            reasons.append("SHORT_BIAS_CANNOT_OPEN_NET_LONG")
        if not reduces and projected < -max_inventory:
            reasons.append("MAX_SHORT_INVENTORY_EXCEEDED")
        if side == Side.SELL and opening > 0 and short_after > max_inventory:
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
