from datetime import datetime, timezone

import pandas as pd

from recommendation_journal import _request, read_table
from validation_config import (
    ETH_LOT_SIZE,
    INR_PER_USDT,
    MAX_MARGIN_USAGE_PCT,
    MAX_RISK_PER_TRADE_PCT,
    PAPER_WALLET_CAPITAL_INR,
    PAPER_WALLET_CAPITAL_USDT,
    usdt_to_inr,
)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value, default=0):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _strategy_side(strategy, directional_bias):
    if strategy in ["Bull Put Credit Spread"]:
        return "BULLISH"
    if strategy in ["Bear Call Credit Spread", "Put Broken Wing Butterfly"]:
        return "BEARISH"
    if strategy in ["Debit Spread"]:
        if directional_bias in ["Bullish", "Mild Bullish"]:
            return "BULLISH"
        if directional_bias in ["Bearish", "Mild Bearish"]:
            return "BEARISH"
    if strategy in ["Iron Fly", "Iron Condor", "Short Strangle", "Short Straddle with Hedge"]:
        return "RANGE"
    return "NEUTRAL"


def _leg_widths(legs):
    calls = sorted(
        [_safe_float(leg.get("strike")) for leg in legs if leg.get("option") == "C" and leg.get("strike")],
    )
    puts = sorted(
        [_safe_float(leg.get("strike")) for leg in legs if leg.get("option") == "P" and leg.get("strike")],
    )
    widths = []

    if len(calls) >= 2:
        widths.append(abs(calls[-1] - calls[0]))

    if len(puts) >= 2:
        widths.append(abs(puts[-1] - puts[0]))

    return widths


def estimate_trade_risk(recommendation):
    rec_json = recommendation.get("recommendation_json") or {}
    strategy = recommendation.get("suggested_strategy") or rec_json.get("strategy")
    pricing = rec_json.get("pricing") or {}
    legs = rec_json.get("legs") or []
    net_credit = _safe_float(pricing.get("net_credit_usdt"))
    net_debit = _safe_float(pricing.get("net_debit_usdt"))
    widths = _leg_widths(legs)
    max_width = max(widths) if widths else 0

    if strategy in ["No Trade", "Wait / Defined-Risk Spread Only"] or not legs:
        return None

    if strategy in ["Bull Put Credit Spread", "Bear Call Credit Spread", "Iron Condor", "Iron Fly"]:
        risk_per_eth = max(max_width - net_credit, 0)
        margin_per_lot = max_width * ETH_LOT_SIZE
        premium_per_lot = net_credit * ETH_LOT_SIZE
    elif strategy in ["Debit Spread", "Put Broken Wing Butterfly"]:
        risk_per_eth = net_debit if net_debit else abs(_safe_float(pricing.get("net_premium_usdt")))
        margin_per_lot = risk_per_eth * ETH_LOT_SIZE
        premium_per_lot = -risk_per_eth * ETH_LOT_SIZE
    else:
        spot = _safe_float(recommendation.get("spot_price"))
        risk_per_eth = max(spot * 0.04, abs(_safe_float(pricing.get("net_premium_usdt"))))
        margin_per_lot = risk_per_eth * ETH_LOT_SIZE
        premium_per_lot = net_credit * ETH_LOT_SIZE

    risk_per_lot = max(risk_per_eth * ETH_LOT_SIZE, 0.01)
    max_risk_usdt = PAPER_WALLET_CAPITAL_USDT * MAX_RISK_PER_TRADE_PCT
    max_margin_usdt = PAPER_WALLET_CAPITAL_USDT * MAX_MARGIN_USAGE_PCT
    lots_by_risk = int(max_risk_usdt // risk_per_lot)
    lots_by_margin = int(max_margin_usdt // max(margin_per_lot, 0.01))
    lots = max(0, min(lots_by_risk, lots_by_margin))

    if lots <= 0:
        return None

    return {
        "lots": lots,
        "eth_quantity": round(lots * ETH_LOT_SIZE, 4),
        "risk_per_lot_usdt": round(risk_per_lot, 4),
        "max_risk_usdt": round(risk_per_lot * lots, 4),
        "max_risk_inr": usdt_to_inr(risk_per_lot * lots),
        "margin_used_usdt": round(margin_per_lot * lots, 4),
        "margin_used_inr": usdt_to_inr(margin_per_lot * lots),
        "entry_premium_usdt": round(premium_per_lot * lots, 4),
        "risk_per_eth_usdt": round(risk_per_eth, 4),
    }


def create_paper_trade(recommendation):
    recommendation_id = recommendation.get("id")

    if not recommendation_id:
        return None

    existing = read_table(
        "paper_trades",
        {
            "select": "*",
            "recommendation_id": f"eq.{recommendation_id}",
            "limit": 1,
        },
    )

    if not existing.empty:
        return existing.iloc[0].to_dict()

    risk = estimate_trade_risk(recommendation)

    if not risk:
        return None

    strategy = recommendation.get("suggested_strategy")
    side = _strategy_side(strategy, recommendation.get("directional_bias"))
    spot = _safe_float(recommendation.get("spot_price"))

    payload = {
        "recommendation_id": recommendation_id,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "status": "OPEN",
        "strategy": strategy,
        "side": side,
        "expiry_label": recommendation.get("expiry_label"),
        "entry_spot": spot,
        "current_spot": spot,
        "lots": risk["lots"],
        "eth_quantity": risk["eth_quantity"],
        "entry_premium_usdt": risk["entry_premium_usdt"],
        "margin_used_usdt": risk["margin_used_usdt"],
        "margin_used_inr": risk["margin_used_inr"],
        "max_risk_usdt": risk["max_risk_usdt"],
        "max_risk_inr": risk["max_risk_inr"],
        "wallet_capital_inr": PAPER_WALLET_CAPITAL_INR,
        "wallet_capital_usdt": round(PAPER_WALLET_CAPITAL_USDT, 4),
        "inr_per_usdt": INR_PER_USDT,
        "eth_lot_size": ETH_LOT_SIZE,
        "unrealized_pnl_usdt": 0,
        "unrealized_pnl_inr": 0,
        "realized_pnl_usdt": 0,
        "realized_pnl_inr": 0,
        "exit_reason": None,
        "trade_json": {
            "recommendation": recommendation,
            "risk": risk,
        },
    }

    result = _request("POST", "paper_trades", payload=payload, prefer="return=representation")

    if isinstance(result, list) and result:
        return result[0]

    return payload


def estimate_spot_proxy_pnl(trade, current_spot):
    entry_spot = _safe_float(trade.get("entry_spot"))
    current_spot = _safe_float(current_spot)
    eth_quantity = _safe_float(trade.get("eth_quantity"))
    max_risk = _safe_float(trade.get("max_risk_usdt"))
    entry_premium = _safe_float(trade.get("entry_premium_usdt"))
    side = trade.get("side")

    if not entry_spot or not current_spot or not eth_quantity:
        return 0

    move_usdt = (current_spot - entry_spot) * eth_quantity

    if side == "BEARISH":
        pnl = -move_usdt
    elif side == "BULLISH":
        pnl = move_usdt
    elif side == "RANGE":
        move_pct = abs(current_spot - entry_spot) / entry_spot
        pnl = entry_premium - (move_pct * max_risk * 1.8)
    else:
        pnl = 0

    return round(max(-max_risk, min(max_risk * 1.5, pnl)), 4)


def update_open_paper_trades(current_spot):
    open_trades = get_open_paper_trades(limit=200)
    updated = []

    for _, trade in open_trades.iterrows():
        pnl = estimate_spot_proxy_pnl(trade, current_spot)
        payload = {
            "updated_at": _now_iso(),
            "current_spot": _safe_float(current_spot),
            "unrealized_pnl_usdt": pnl,
            "unrealized_pnl_inr": usdt_to_inr(pnl),
        }

        max_risk = _safe_float(trade.get("max_risk_usdt"))
        entry_premium = abs(_safe_float(trade.get("entry_premium_usdt")))
        expiry_time = pd.to_datetime(trade.get("expiry_label"), utc=True, errors="coerce")

        if not pd.isna(expiry_time) and pd.Timestamp.utcnow() >= expiry_time:
            payload.update(
                {
                    "status": "EXPIRED",
                    "closed_at": _now_iso(),
                    "exit_reason": "EXPIRY",
                    "exit_spot": _safe_float(current_spot),
                    "realized_pnl_usdt": pnl,
                    "realized_pnl_inr": usdt_to_inr(pnl),
                }
            )
        elif max_risk and pnl <= -max_risk * 0.5:
            payload.update(
                {
                    "status": "CLOSED",
                    "closed_at": _now_iso(),
                    "exit_reason": "SL",
                    "exit_spot": _safe_float(current_spot),
                    "realized_pnl_usdt": pnl,
                    "realized_pnl_inr": usdt_to_inr(pnl),
                }
            )
        elif entry_premium and pnl >= entry_premium * 0.6:
            payload.update(
                {
                    "status": "CLOSED",
                    "closed_at": _now_iso(),
                    "exit_reason": "TP",
                    "exit_spot": _safe_float(current_spot),
                    "realized_pnl_usdt": pnl,
                    "realized_pnl_inr": usdt_to_inr(pnl),
                }
            )

        result = _request(
            "PATCH",
            "paper_trades",
            payload=payload,
            params={"id": f"eq.{trade.get('id')}"},
            prefer="return=representation",
        )

        if isinstance(result, list) and result:
            updated.append(result[0])

    return updated


def get_open_paper_trades(limit=50):
    return read_table(
        "paper_trades",
        {
            "select": "*",
            "status": "eq.OPEN",
            "order": "created_at.desc",
            "limit": limit,
        },
    )


def get_closed_paper_trades(limit=100):
    return read_table(
        "paper_trades",
        {
            "select": "*",
            "status": "neq.OPEN",
            "order": "updated_at.desc",
            "limit": limit,
        },
    )


def get_all_paper_trades(limit=500):
    return read_table(
        "paper_trades",
        {
            "select": "*",
            "order": "created_at.desc",
            "limit": limit,
        },
    )
