from decimal import Decimal


def safe_ratio(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator == 0:
        return None
    return numerator / denominator


def grid_efficiency(net_grid_harvest_pnl: Decimal, gross_grid_harvest_pnl: Decimal) -> Decimal | None:
    return safe_ratio(net_grid_harvest_pnl, gross_grid_harvest_pnl)

