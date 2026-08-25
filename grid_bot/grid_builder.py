from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP, getcontext

from .models import GridConfig, GridLevel, GridType, Side, SpacingType

getcontext().prec = 28


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
    prices = generate_prices(config, tick_size)
    levels: list[GridLevel] = []

    for index, price in enumerate(prices):
        if config.grid_type == GridType.LONG_BIAS:
            side = Side.BUY if price <= reference_price else Side.SELL
        elif config.grid_type == GridType.SHORT_BIAS:
            side = Side.SELL if price >= reference_price else Side.BUY
        else:
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


def preview_grid(config: GridConfig, reference_price: Decimal, tick_size: Decimal) -> dict:
    levels = build_grid_levels(config, reference_price, tick_size)
    buy_levels = [level for level in levels if level.side == Side.BUY]
    sell_levels = [level for level in levels if level.side == Side.SELL]
    prices = [level.price for level in levels]
    absolute_spacing = None
    pct_spacing = None
    if len(prices) > 1:
        absolute_spacing = prices[1] - prices[0]
        pct_spacing = (prices[1] / prices[0]) - Decimal("1") if prices[0] else None

    return {
        "reference_price": reference_price,
        "grid_type": config.grid_type.value,
        "lower_price": config.lower_price,
        "upper_price": config.upper_price,
        "levels": levels,
        "buy_levels": buy_levels,
        "sell_levels": sell_levels,
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

