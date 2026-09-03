from dataclasses import replace
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP, getcontext

from .models import GridConfig, GridLevel, GridType, Side, SpacingType
from .semantics import evaluate_order_semantics, round_price_for_side, validate_post_only_price

getcontext().prec = 28


NEUTRAL_RANGE_ERROR_MESSAGE = (
    "Selected range is not suitable for a Neutral grid at the current ETH price. "
    "Adjust the range or choose Long/Short."
)


class NeutralGridRangeValidationError(ValueError):
    def __init__(self, details: dict):
        super().__init__(NEUTRAL_RANGE_ERROR_MESSAGE)
        self.details = details


def quantize_price(price: Decimal, tick_size: Decimal) -> Decimal:
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    ticks = (price / tick_size).to_integral_value(rounding=ROUND_HALF_UP)
    return ticks * tick_size


def quantize_quantity(quantity: Decimal, lot_size: Decimal) -> Decimal:
    if lot_size <= 0:
        raise ValueError("lot_size must be positive")
    lots = (quantity / lot_size).to_integral_value(rounding=ROUND_FLOOR)
    return lots * lot_size


def validate_grid_config(config: GridConfig, exchange_lot_size: Decimal | None = None) -> None:
    if config.lower_price <= 0 or config.upper_price <= 0:
        raise ValueError("Grid boundaries must be positive.")
    if config.lower_price >= config.upper_price:
        raise ValueError("lower_price must be below upper_price.")
    if config.grid_count < 2:
        raise ValueError("grid_count must be at least 2.")
    if config.lot_size <= 0:
        raise ValueError("lot_size must be positive.")
    if exchange_lot_size and config.lot_size % exchange_lot_size != 0:
        raise ValueError("lot_size is incompatible with exchange lot size.")
    if config.max_inventory_lots <= 0:
        raise ValueError("max_inventory_lots must be positive.")
    if config.allocated_capital <= 0 or config.risk_capital <= 0:
        raise ValueError("allocated_capital and risk_capital must be positive.")
    if config.margin_mode != "portfolio":
        raise ValueError("DeltaGridBot V0.1 requires portfolio margin mode.")


def validate_neutral_grid_suitability(config: GridConfig, reference_price: Decimal, tick_size: Decimal) -> None:
    if config.grid_type != GridType.NEUTRAL:
        return
    prices = generate_prices(config, tick_size)
    buy_count = len([price for price in prices if price < reference_price])
    sell_count = len(prices) - buy_count
    if buy_count == 0 or sell_count == 0 or abs(buy_count - sell_count) > 1:
        raise ValueError(NEUTRAL_RANGE_ERROR_MESSAGE)


def neutral_grid_balance(config: GridConfig, reference_price: Decimal, tick_size: Decimal) -> dict:
    prices = generate_prices(config, tick_size)
    buy_count = len([price for price in prices if price < reference_price])
    sell_count = len(prices) - buy_count
    return {
        "prices": prices,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "valid": buy_count > 0 and sell_count > 0 and abs(buy_count - sell_count) <= 1,
    }


def _candidate_neutral_range(config: GridConfig, lower_price: Decimal, upper_price: Decimal, reference_price: Decimal, tick_size: Decimal) -> dict | None:
    if lower_price <= 0 or lower_price >= upper_price:
        return None
    try:
        candidate = replace(config, lower_price=lower_price, upper_price=upper_price)
        balance = neutral_grid_balance(candidate, reference_price, tick_size)
    except ValueError:
        return None
    if not balance["valid"]:
        return None
    return {
        "suggested_lower": lower_price,
        "suggested_upper": upper_price,
        "suggested_buy_count": balance["buy_count"],
        "suggested_sell_count": balance["sell_count"],
        "suggested_levels": balance["prices"],
    }


def nearest_valid_neutral_range(config: GridConfig, reference_price: Decimal, tick_size: Decimal) -> dict | None:
    if config.grid_type != GridType.NEUTRAL:
        return None
    entered_lower = config.lower_price
    entered_upper = config.upper_price
    entered_width = entered_upper - entered_lower
    if entered_width <= 0:
        return None

    width_ticks = max(Decimal("1"), (entered_width / tick_size).to_integral_value(rounding=ROUND_HALF_UP))
    entered_width = width_ticks * tick_size
    midpoint_distance_ticks = abs(((entered_lower + entered_upper) / Decimal("2")) - reference_price) / tick_size
    search_ticks = int(max(Decimal("1000"), width_ticks * Decimal("4"), midpoint_distance_ticks * Decimal("4"), Decimal(config.grid_count * 20)))

    def offset_order(limit: int):
        yield 0
        for step in range(1, limit + 1):
            yield -step
            yield step

    for offset in offset_order(search_ticks):
        lower = quantize_price(entered_lower + Decimal(offset) * tick_size, tick_size)
        upper = lower + entered_width
        candidate = _candidate_neutral_range(config, lower, upper, reference_price, tick_size)
        if candidate:
            candidate["width_preserved"] = True
            return candidate

    max_width_delta = int(max(Decimal("1000"), width_ticks * Decimal("2"), Decimal(config.grid_count * 20)))
    candidates: list[tuple[Decimal, Decimal, Decimal, Decimal, dict]] = []
    for width_delta in range(1, max_width_delta + 1):
        for sign in (-1, 1):
            candidate_width_ticks = width_ticks + Decimal(sign * width_delta)
            if candidate_width_ticks <= 0:
                continue
            candidate_width = candidate_width_ticks * tick_size
            for offset in offset_order(search_ticks):
                lower = quantize_price(entered_lower + Decimal(offset) * tick_size, tick_size)
                upper = lower + candidate_width
                candidate = _candidate_neutral_range(config, lower, upper, reference_price, tick_size)
                if candidate:
                    cost = abs(lower - entered_lower) + abs(upper - entered_upper)
                    width_cost = abs(candidate_width - entered_width)
                    candidates.append((width_cost, cost, abs(lower - entered_lower), lower, candidate))
                    break
        if candidates:
            candidates.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
            result = candidates[0][4]
            result["width_preserved"] = False
            return result
    return None


def neutral_range_invalid_details(config: GridConfig, reference_price: Decimal, tick_size: Decimal, reason: str = "NEUTRAL_BUY_SELL_IMBALANCE") -> dict:
    try:
        entered = neutral_grid_balance(config, reference_price, tick_size)
    except ValueError as exc:
        entered = {"prices": [], "buy_count": 0, "sell_count": 0, "valid": False}
        reason = str(exc)
    suggestion = nearest_valid_neutral_range(config, reference_price, tick_size)
    return {
        "code": "NEUTRAL_RANGE_INVALID",
        "message": NEUTRAL_RANGE_ERROR_MESSAGE,
        "reason": reason,
        "current_reference_price": reference_price,
        "entered_lower": config.lower_price,
        "entered_upper": config.upper_price,
        "entered_grid_count": config.grid_count,
        "entered_spacing_type": config.spacing_type.value,
        "entered_buy_count": entered["buy_count"],
        "entered_sell_count": entered["sell_count"],
        "entered_levels": entered["prices"],
        **(suggestion or {}),
    }


def generate_prices(config: GridConfig, tick_size: Decimal) -> list[Decimal]:
    validate_grid_config(config)
    if config.spacing_type == SpacingType.ARITHMETIC:
        step = (config.upper_price - config.lower_price) / Decimal(config.grid_count - 1)
        raw_prices = [config.lower_price + step * Decimal(index) for index in range(config.grid_count)]
    else:
        ratio = (config.upper_price / config.lower_price) ** (Decimal(1) / Decimal(config.grid_count - 1))
        raw_prices = [config.lower_price * (ratio ** Decimal(index)) for index in range(config.grid_count)]

    rounded = [quantize_price(price, tick_size) for price in raw_prices]
    if len(set(rounded)) != len(rounded):
        raise ValueError("Rounded grid levels are not unique; increase range or reduce grid_count.")
    return rounded


def build_grid_levels(config: GridConfig, reference_price: Decimal, tick_size: Decimal) -> list[GridLevel]:
    validate_neutral_grid_suitability(config, reference_price, tick_size)
    prices = generate_prices(config, tick_size)
    levels: list[GridLevel] = []

    for index, price in enumerate(prices):
        side = Side.BUY if price < reference_price else Side.SELL

        levels.append(
            GridLevel(
                level_id=f"L{index + 1:03d}",
                index=index + 1,
                price=price,
                side=side,
                quantity=config.lot_size,
            )
        )

    return levels


def preview_grid(
    config: GridConfig,
    reference_price: Decimal,
    tick_size: Decimal,
    best_bid: Decimal | None = None,
    best_ask: Decimal | None = None,
    current_inventory: Decimal = Decimal("0"),
    open_orders: list[dict] | None = None,
) -> dict:
    levels = build_grid_levels(config, reference_price, tick_size)
    buy_levels = [level for level in levels if level.side == Side.BUY]
    sell_levels = [level for level in levels if level.side == Side.SELL]
    opening_buy_levels = []
    opening_sell_levels = []
    deferred_levels = []
    simulated_open_orders = list(open_orders or [])
    reserved_long = max(Decimal("0"), current_inventory)
    reserved_short = max(Decimal("0"), -current_inventory)
    for level in levels:
        normalized_price = round_price_for_side(level.price, tick_size, level.side)
        post_only = validate_post_only_price(level.side, normalized_price, best_bid, best_ask)
        semantic = evaluate_order_semantics(
            config.grid_type,
            current_inventory,
            config.max_inventory_lots,
            level.side,
            level.quantity,
            simulated_open_orders,
        )
        preview_level = {
            "level_id": level.level_id,
            "index": level.index,
            "side": level.side.value,
            "price": level.price,
            "execution_price": normalized_price,
            "quantity": level.quantity,
            "post_only_safe": post_only.allowed,
            "semantic_allowed": semantic.allowed,
            "opens_inventory": semantic.opens_inventory,
            "reason_codes": [*semantic.reason_codes, *post_only.reason_codes],
        }
        if semantic.allowed and post_only.allowed:
            if level.side == Side.BUY:
                opening_buy_levels.append(preview_level)
            else:
                opening_sell_levels.append(preview_level)
            simulated_open_orders.append(
                {
                    "side": level.side.value,
                    "remaining_quantity": str(level.quantity),
                    "status": "open",
                    "opens_inventory": semantic.opens_inventory,
                }
            )
            reserved_long = semantic.reserved_long_after
            reserved_short = semantic.reserved_short_after
        else:
            deferred_levels.append(preview_level)
    prices = [level.price for level in levels]
    absolute_spacing = None
    pct_spacing = None
    if len(prices) > 1:
        absolute_spacing = prices[1] - prices[0]
        pct_spacing = (prices[1] / prices[0]) - Decimal("1") if prices[0] else None

    return {
        "reference_price": reference_price,
        "grid_type": config.grid_type.value,
        "spacing_type": config.spacing_type.value,
        "lower_price": config.lower_price,
        "upper_price": config.upper_price,
        "levels": levels,
        "buy_levels": buy_levels,
        "sell_levels": sell_levels,
        "total_grid_levels": len(levels),
        "opening_buy_orders_eligible": len(opening_buy_levels),
        "opening_sell_orders_eligible": len(opening_sell_levels),
        "opening_buy_levels": opening_buy_levels,
        "opening_sell_levels": opening_sell_levels,
        "deferred_levels": deferred_levels,
        "reserved_long_exposure": reserved_long,
        "reserved_short_exposure": reserved_short,
        "nature_semantics": {
            GridType.NEUTRAL.value: "inventory may range from -MaxInventory to +MaxInventory",
            GridType.LONG_BIAS.value: "inventory may range from 0 to +MaxInventory; BUY opens, SELL closes",
            GridType.SHORT_BIAS.value: "inventory may range from -MaxInventory to 0; SELL opens, BUY closes",
        }[config.grid_type.value],
        "absolute_spacing": absolute_spacing,
        "percentage_spacing": pct_spacing,
        "lot_size": config.lot_size,
        "max_inventory": config.max_inventory_lots,
        "max_potential_long_inventory": sum((level.quantity for level in buy_levels), Decimal("0")),
        "max_potential_short_inventory": sum((level.quantity for level in sell_levels), Decimal("0")),
        "allocated_capital": config.allocated_capital,
        "risk_capital": config.risk_capital,
        "risk_thresholds": config.risk_thresholds,
    }
