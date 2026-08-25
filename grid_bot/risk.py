from dataclasses import dataclass, field
from decimal import Decimal

from .config import DEFAULT_RISK_THRESHOLDS, RISK_MODULE_VERSION
from .models import OrderProposal, RiskState, Side


@dataclass(frozen=True)
class RiskInputs:
    net_inventory: Decimal
    max_inventory: Decimal
    pending_buy_quantity: Decimal = Decimal("0")
    pending_sell_quantity: Decimal = Decimal("0")
    open_order_count: int = 0
    account_equity: Decimal | None = None
    margin_used: Decimal | None = None
    available_margin: Decimal | None = None
    allocated_capital: Decimal = Decimal("0")
    risk_capital: Decimal = Decimal("1")
    projected_adverse_grid_exposure: Decimal = Decimal("0")
    current_drawdown_pct: Decimal = Decimal("0")


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    risk_state: RiskState
    reason_codes: list[str] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)
    formula_version: str = RISK_MODULE_VERSION


def inventory_utilisation(net_inventory: Decimal, max_inventory: Decimal) -> Decimal:
    if max_inventory <= 0:
        return Decimal("1")
    return abs(net_inventory) / max_inventory


def grid_risk_ratio(projected_adverse_grid_exposure: Decimal, risk_capital: Decimal) -> Decimal:
    """GRR v0.1 = projected adverse grid exposure / configured risk capital."""
    if risk_capital <= 0:
        return Decimal("999999")
    return projected_adverse_grid_exposure / risk_capital


def margin_utilisation(margin_used: Decimal | None, account_equity: Decimal | None) -> Decimal | None:
    if margin_used is None or account_equity in [None, Decimal("0")]:
        return None
    return margin_used / account_equity


class GridRiskController:
    def __init__(self, thresholds: dict | None = None):
        self.thresholds = {**DEFAULT_RISK_THRESHOLDS, **(thresholds or {})}

    def evaluate(self, inputs: RiskInputs) -> RiskDecision:
        inv_util = inventory_utilisation(inputs.net_inventory, inputs.max_inventory)
        grr = grid_risk_ratio(inputs.projected_adverse_grid_exposure, inputs.risk_capital)
        margin_util = margin_utilisation(inputs.margin_used, inputs.account_equity)
        reasons: list[str] = []
        state = RiskState.GREEN

        def bump(candidate: RiskState) -> None:
            nonlocal state
            order = [RiskState.GREEN, RiskState.YELLOW, RiskState.ORANGE, RiskState.RED, RiskState.CRITICAL]
            if order.index(candidate) > order.index(state):
                state = candidate

        if inv_util >= Decimal(str(self.thresholds["inventory_warning_utilisation"])):
            bump(RiskState.YELLOW)
        if inv_util >= Decimal(str(self.thresholds["inventory_orange_utilisation"])):
            bump(RiskState.ORANGE)
            reasons.append("INVENTORY_ORANGE")
        if inv_util >= Decimal(str(self.thresholds["inventory_red_utilisation"])):
            bump(RiskState.RED)
            reasons.append("INVENTORY_RED")
        if margin_util is not None:
            if margin_util >= Decimal(str(self.thresholds["margin_yellow_utilisation"])):
                bump(RiskState.YELLOW)
            if margin_util >= Decimal(str(self.thresholds["margin_orange_utilisation"])):
                bump(RiskState.ORANGE)
                reasons.append("MARGIN_ORANGE")
            if margin_util >= Decimal(str(self.thresholds["margin_red_utilisation"])):
                bump(RiskState.RED)
                reasons.append("MARGIN_RED")
            if margin_util >= Decimal(str(self.thresholds["margin_critical_utilisation"])):
                bump(RiskState.CRITICAL)
                reasons.append("MARGIN_CRITICAL")
        if grr >= Decimal(str(self.thresholds["grr_yellow"])):
            bump(RiskState.YELLOW)
        if grr >= Decimal(str(self.thresholds["grr_orange"])):
            bump(RiskState.ORANGE)
            reasons.append("GRR_ORANGE")
        if grr >= Decimal(str(self.thresholds["grr_red"])):
            bump(RiskState.RED)
            reasons.append("GRR_RED")
        if grr >= Decimal(str(self.thresholds["grr_critical"])):
            bump(RiskState.CRITICAL)
            reasons.append("GRR_CRITICAL")
        if inputs.current_drawdown_pct >= Decimal(str(self.thresholds["drawdown_critical_pct"])):
            bump(RiskState.CRITICAL)
            reasons.append("DRAWDOWN_CRITICAL")
        elif inputs.current_drawdown_pct >= Decimal(str(self.thresholds["drawdown_red_pct"])):
            bump(RiskState.RED)
            reasons.append("DRAWDOWN_RED")
        elif inputs.current_drawdown_pct >= Decimal(str(self.thresholds["drawdown_orange_pct"])):
            bump(RiskState.ORANGE)
            reasons.append("DRAWDOWN_ORANGE")
        elif inputs.current_drawdown_pct >= Decimal(str(self.thresholds["drawdown_yellow_pct"])):
            bump(RiskState.YELLOW)

        if inputs.open_order_count >= int(self.thresholds["max_open_orders"]):
            bump(RiskState.RED)
            reasons.append("MAX_OPEN_ORDERS")

        return RiskDecision(
            allowed=state not in {RiskState.RED, RiskState.CRITICAL},
            risk_state=state,
            reason_codes=reasons,
            metrics={
                "inventory_utilisation": str(inv_util),
                "GRR": str(grr),
                "margin_utilisation": str(margin_util) if margin_util is not None else None,
                "formula": "GRR v0.1 = projected_adverse_grid_exposure / risk_capital",
            },
        )

    def check_order(self, proposal: OrderProposal, inputs: RiskInputs) -> RiskDecision:
        projected_net = inputs.net_inventory + proposal.quantity if proposal.side == Side.BUY else inputs.net_inventory - proposal.quantity
        projected = RiskInputs(
            net_inventory=projected_net,
            max_inventory=inputs.max_inventory,
            pending_buy_quantity=inputs.pending_buy_quantity + (proposal.quantity if proposal.side == Side.BUY else Decimal("0")),
            pending_sell_quantity=inputs.pending_sell_quantity + (proposal.quantity if proposal.side == Side.SELL else Decimal("0")),
            open_order_count=inputs.open_order_count + 1,
            account_equity=inputs.account_equity,
            margin_used=inputs.margin_used,
            available_margin=inputs.available_margin,
            allocated_capital=inputs.allocated_capital,
            risk_capital=inputs.risk_capital,
            projected_adverse_grid_exposure=max(abs(projected_net), inputs.projected_adverse_grid_exposure),
            current_drawdown_pct=inputs.current_drawdown_pct,
        )
        decision = self.evaluate(projected)
        if abs(projected_net) > inputs.max_inventory:
            return RiskDecision(False, RiskState.RED, [*decision.reason_codes, "MAX_INVENTORY_EXCEEDED"], decision.metrics)
        return decision

