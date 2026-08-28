from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Optional
from uuid import uuid4


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:16]}"


class GridType(str, Enum):
    NEUTRAL = "neutral"
    LONG_BIAS = "long_bias"
    SHORT_BIAS = "short_bias"


class SpacingType(str, Enum):
    ARITHMETIC = "arithmetic"
    GEOMETRIC = "geometric"


class GridStatus(str, Enum):
    CREATED = "CREATED"
    STARTING = "STARTING"
    START_FAILED = "START_FAILED"
    RUNNING = "RUNNING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    RESUMING = "RESUMING"
    REGRID_PENDING = "REGRID_PENDING"
    STOPPING = "STOPPING"
    STOP_REQUIRES_ATTENTION = "STOP_REQUIRES_ATTENTION"
    STOPPED = "STOPPED"
    SUMMARY_PENDING_RECONCILIATION = "SUMMARY_PENDING_RECONCILIATION"


class ExecutionEventMode(str, Enum):
    PRIVATE_WS = "PRIVATE_WS"
    REST_FALLBACK = "REST_FALLBACK"


class OperationalState(str, Enum):
    NORMAL = "NORMAL"
    DEGRADED = "DEGRADED"
    DEGRADED_RECONCILIATION = "DEGRADED_RECONCILIATION"


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class RiskState(str, Enum):
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    ORANGE = "ORANGE"
    RED = "RED"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True)
class ProductSpec:
    product_id: int
    symbol: str
    contract_type: str
    contract_multiplier: Decimal
    lot_size: Decimal
    min_quantity: Decimal
    tick_size: Decimal
    price_precision: int
    quantity_precision: int
    mark_price: Decimal
    last_price: Optional[Decimal] = None
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None


@dataclass(frozen=True)
class GridConfig:
    bot_id: str
    config_version: int
    bot_name: str
    product_symbol: str
    grid_type: GridType
    lower_price: Decimal
    upper_price: Decimal
    grid_count: int
    spacing_type: SpacingType
    lot_size: Decimal
    max_inventory_lots: Decimal
    allocated_capital: Decimal
    risk_capital: Decimal
    margin_mode: str = "portfolio"
    initial_inventory: Decimal = Decimal("0")
    notes: Optional[str] = None
    risk_thresholds: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=utc_now)
    immutable: bool = True


@dataclass(frozen=True)
class GridLevel:
    level_id: str
    index: int
    price: Decimal
    side: Side
    quantity: Decimal


@dataclass(frozen=True)
class OrderProposal:
    run_id: str
    level_id: str
    side: Side
    price: Decimal
    quantity: Decimal
    client_order_id: str
    post_only: bool = True
    time_in_force: str = "gtc"
    reduce_only: bool = False


@dataclass
class GridRun:
    run_id: str
    bot_id: str
    status: GridStatus
    config_version: int
    started_at: Optional[str] = None
    stopped_at: Optional[str] = None
    reference_price: Optional[Decimal] = None
    stop_reason: Optional[str] = None


@dataclass
class OrderRecord:
    order_id: str
    run_id: str
    level_id: str
    side: Side
    price: Decimal
    requested_quantity: Decimal
    filled_quantity: Decimal = Decimal("0")
    remaining_quantity: Decimal = Decimal("0")
    average_fill_price: Optional[Decimal] = None
    client_order_id: str = ""
    exchange_order_id: Optional[str] = None
    status: str = "PROPOSED"
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FillRecord:
    fill_id: str
    order_id: str
    run_id: str
    side: Side
    price: Decimal
    quantity: Decimal
    liquidity_role: str = "unknown"
    fee: Decimal = Decimal("0")
    fee_currency: str = "USD"
    exchange_fill_id: Optional[str] = None
    raw: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=utc_now)
    config_version: Optional[int] = None


def to_record_dict(value: Any) -> Any:
    if hasattr(value, "__dataclass_fields__"):
        return {key: to_record_dict(item) for key, item in asdict(value).items()}
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, list):
        return [to_record_dict(item) for item in value]
    if isinstance(value, dict):
        return {key: to_record_dict(item) for key, item in value.items()}
    return value
