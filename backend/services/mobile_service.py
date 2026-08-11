from datetime import datetime, timezone

from backend.services import account_snapshot_service, market_data_service
from backend.services.cache import ttl_cache
from backend.services.json_utils import to_jsonable
from iron_fly_engine import build_iron_fly_recommendation


GREEK_KEYS = ("delta", "gamma", "theta", "vega")
BALANCE_KEYS = ("balance", "available_balance", "blocked_margin", "order_margin", "position_margin")
POSITION_KEYS = (
    "symbol",
    "contract_type",
    "size",
    "entry_price",
    "mark_price",
    "liquidation_price",
    "margin",
    "realized_pnl",
    "unrealized_pnl",
    "computed_delta",
    "computed_gamma",
    "computed_theta",
    "computed_vega",
)


def utc_timestamp():
    return datetime.now(timezone.utc).isoformat()


def error_payload(code, message):
    return {
        "ok": False,
        "error": {
            "code": code,
            "message": message,
        },
        "timestamp": utc_timestamp(),
    }


def _number(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _greeks(values):
    values = values or {}
    return {key: _number(values.get(key)) for key in GREEK_KEYS}


def _balances(values):
    values = values or {}
    return {key: _number(values.get(key)) for key in BALANCE_KEYS}


def _asset_balances(balance_summary):
    rows = []
    for asset, values in sorted((balance_summary.get("by_asset") or {}).items()):
        rows.append({
            "asset_symbol": asset,
            **_balances(values),
        })
    return rows


def _sum_asset_balance(balance_summary, key):
    total = 0.0
    seen = False
    for values in (balance_summary.get("by_asset") or {}).values():
        number = _number(values.get(key))
        if number is not None:
            total += number
            seen = True
    return total if seen else None


def _margin_utilization(available_balance, blocked_margin):
    available = _number(available_balance)
    blocked = _number(blocked_margin)
    if available is None or blocked is None:
        return None
    denominator = available + blocked
    if denominator <= 0:
        return None
    return round(blocked / denominator * 100, 2)


def _position(row):
    return {
        key: _number(row.get(key)) if key not in {"symbol", "contract_type"} else row.get(key)
        for key in POSITION_KEYS
    }


def serialize_subwallets(snapshot):
    snapshot = snapshot or {}
    aggregate = snapshot.get("aggregate") or {}
    aggregate_available = _number(aggregate.get("available_balance"))
    aggregate_blocked = _number(aggregate.get("blocked_margin"))

    accounts = []
    for account in snapshot.get("accounts") or []:
        balance_summary = account.get("balance_summary") or {}
        available = _sum_asset_balance(balance_summary, "available_balance")
        blocked = _sum_asset_balance(balance_summary, "blocked_margin")
        balances = _asset_balances(balance_summary)
        accounts.append({
            "id": account.get("id"),
            "label": account.get("label"),
            "kind": account.get("kind"),
            "ok": bool(account.get("ok")),
            "error": account.get("error"),
            "net_equity": _number(balance_summary.get("net_equity")),
            "balance": _sum_asset_balance(balance_summary, "balance"),
            "available_balance": available,
            "blocked_margin": blocked,
            "order_margin": _sum_asset_balance(balance_summary, "order_margin"),
            "position_margin": _sum_asset_balance(balance_summary, "position_margin"),
            "margin_utilization_pct": _margin_utilization(available, blocked),
            "position_count": int(account.get("position_count") or 0),
            "greeks": _greeks(account.get("greeks")),
            "balances": balances,
            "positions": [_position(position) for position in (account.get("positions") or [])],
        })

    return {
        "ok": bool(snapshot.get("ok")),
        "last_updated": utc_timestamp(),
        "aggregate": {
            "net_equity": _number(aggregate.get("net_equity")),
            "balance": _number(aggregate.get("balance")),
            "available_balance": aggregate_available,
            "blocked_margin": aggregate_blocked,
            "order_margin": _number(aggregate.get("order_margin")),
            "position_margin": _number(aggregate.get("position_margin")),
            "margin_utilization_pct": _margin_utilization(aggregate_available, aggregate_blocked),
            "greeks": _greeks(aggregate.get("greeks")),
        },
        "accounts": accounts,
        "source": {
            "account_count": len(accounts),
            "healthy_account_count": sum(1 for account in accounts if account.get("ok")),
            "cache": snapshot.get("cache"),
        },
    }


def _leg(row):
    row = row or {}
    return {
        "action": row.get("action"),
        "option_type": row.get("option_type"),
        "strike": _number(row.get("strike")),
        "quantity": _number(row.get("quantity")),
        "mark_price": _number(row.get("mark_price")),
        "open_interest": _number(row.get("oi") if row.get("oi") is not None else row.get("open_interest")),
        "volume": _number(row.get("volume")),
        "iv": _number(row.get("iv")),
        "delta": _number(row.get("delta")),
        "gamma": _number(row.get("gamma")),
        "theta": _number(row.get("theta")),
        "vega": _number(row.get("vega")),
    }


def _payoff(row):
    row = row or {}
    return {
        "net_credit": _number(row.get("net_credit")),
        "max_profit": _number(row.get("max_profit")),
        "max_loss": _number(row.get("max_loss")),
        "lower_breakeven": _number(row.get("lower_breakeven")),
        "upper_breakeven": _number(row.get("upper_breakeven")),
        "return_on_risk": _number(row.get("return_on_risk_pct")),
        "wing_width": _number(row.get("wing_width")),
    }


def _iron_fly_candidate(row, include_details=True):
    if not row:
        return None
    payload = {
        "expiry": row.get("expiry"),
        "dte": _number(row.get("dte")),
        "center_strike": _number(row.get("center_strike")),
        "wing_width": _number(row.get("wing_width")),
        "score": _number(row.get("score")),
        "status": row.get("status"),
        "ranking_reason": row.get("ranking_reason"),
        "liquidity_score": _number(row.get("liquidity_score")),
        "expected_move": _number(row.get("expected_move")),
        "median_iv": _number(row.get("median_iv")),
        "realized_vol_pct": _number(row.get("realized_vol_pct")),
        "iv_rv_spread": _number(row.get("iv_rv_spread")),
        "component_scores": to_jsonable(row.get("component_scores") or {}),
        "net_greeks": _greeks(row.get("net_greeks")),
        "payoff": _payoff(row.get("payoff")),
    }
    if include_details:
        payload["legs"] = [_leg(leg) for leg in (row.get("legs") or [])]
    return payload


def serialize_iron_fly(result):
    result = result or {}
    return {
        "ok": bool(result.get("ok")),
        "last_updated": utc_timestamp(),
        "generated_at": result.get("generated_at"),
        "symbol": result.get("symbol"),
        "recommendation": result.get("recommendation"),
        "iron_fly_score": _number(result.get("iron_fly_score")),
        "confidence": result.get("confidence"),
        "selected": _iron_fly_candidate(result.get("selected")),
        "top_alternatives": [
            _iron_fly_candidate(item)
            for item in (result.get("top_alternatives") or [])
        ],
        "expiry_comparison": to_jsonable(result.get("expiry_comparison") or []),
        "risk_factors": to_jsonable(result.get("risk_factors") or []),
        "entry_conditions": to_jsonable(result.get("entry_conditions") or []),
        "adjustment_triggers": to_jsonable(result.get("adjustment_triggers") or []),
        "stop_loss_logic": result.get("stop_loss_logic"),
        "profit_booking_logic": result.get("profit_booking_logic"),
        "time_based_exit": result.get("time_based_exit"),
        "research_only": bool(result.get("research_only")),
        "cache": result.get("cache"),
    }


@ttl_cache(15)
def get_mobile_subwallets():
    return serialize_subwallets(account_snapshot_service.get_accounts_snapshot())


@ttl_cache(45)
def get_mobile_iron_fly():
    return serialize_iron_fly(build_iron_fly_recommendation())


@ttl_cache(30)
def get_mobile_market_summary():
    market = market_data_service.get_eth_market()
    return {
        "ok": bool(market.get("ok")),
        "symbol": market.get("symbol"),
        "spot_price": _number(market.get("spot_price")),
        "mark_price": _number(market.get("mark_price")),
        "last_updated": utc_timestamp(),
    }


def get_mobile_home():
    subwallets = get_mobile_subwallets()
    iron_fly = get_mobile_iron_fly()
    market = get_mobile_market_summary()
    accounts = subwallets.get("accounts") or []
    selected = iron_fly.get("selected") or {}

    return {
        "ok": True,
        "last_updated": utc_timestamp(),
        "backend": {
            "ok": True,
            "service": "deltaforge-mobile-api",
            "version": "1",
        },
        "market": market,
        "subwallets": {
            "ok": subwallets.get("ok"),
            "account_count": len(accounts),
            "healthy_account_count": sum(1 for account in accounts if account.get("ok")),
            "total_positions": sum(int(account.get("position_count") or 0) for account in accounts),
            "aggregate": subwallets.get("aggregate"),
            "last_updated": subwallets.get("last_updated"),
        },
        "iron_fly": {
            "ok": iron_fly.get("ok"),
            "generated_at": iron_fly.get("generated_at"),
            "recommendation": iron_fly.get("recommendation"),
            "iron_fly_score": iron_fly.get("iron_fly_score"),
            "confidence": iron_fly.get("confidence"),
            "selected": {
                "expiry": selected.get("expiry"),
                "dte": selected.get("dte"),
                "center_strike": selected.get("center_strike"),
                "wing_width": selected.get("wing_width"),
            } if selected else None,
            "last_updated": iron_fly.get("last_updated"),
        },
    }
