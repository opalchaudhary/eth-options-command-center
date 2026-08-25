from decimal import Decimal

from .models import OrderProposal, Side


def make_client_order_id(run_id: str, level_id: str, side: Side, sequence: int) -> str:
    raw = f"DGB01-{run_id[-8:]}-{level_id}-{side.value[0].upper()}-{sequence}"
    return raw[:32]


def order_payload(product_id: int, proposal: OrderProposal) -> dict:
    return {
        "product_id": product_id,
        "side": proposal.side.value,
        "size": str(proposal.quantity),
        "limit_price": str(proposal.price),
        "order_type": "limit_order",
        "time_in_force": proposal.time_in_force,
        "post_only": proposal.post_only,
        "reduce_only": proposal.reduce_only,
        "client_order_id": proposal.client_order_id,
    }

