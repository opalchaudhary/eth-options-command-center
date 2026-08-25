from dataclasses import dataclass, field


@dataclass(frozen=True)
class ReconciliationResult:
    ok: bool
    mismatches: list[dict] = field(default_factory=list)
    source: str = "exchange_authoritative"


def reconcile_orders(local_orders: list[dict], exchange_orders: list[dict]) -> ReconciliationResult:
    local_ids = {order.get("exchange_order_id") for order in local_orders if order.get("exchange_order_id")}
    exchange_ids = {str(order.get("id")) for order in exchange_orders if order.get("id") is not None}
    mismatches = []
    for missing in sorted(exchange_ids - local_ids):
        mismatches.append({"type": "LOCAL_ORDER_MISSING", "exchange_order_id": missing})
    for orphan in sorted(local_ids - exchange_ids):
        mismatches.append({"type": "EXCHANGE_ORDER_MISSING", "exchange_order_id": orphan})
    return ReconciliationResult(ok=not mismatches, mismatches=mismatches)

