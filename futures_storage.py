from datetime import datetime, timezone

import pandas as pd

from engine_persistence import (
    build_decision_hash,
    compact_decision_reason,
    safe_engine_payload,
    should_persist_engine_snapshot,
)
from recommendation_journal import _request, read_table
from validation_config import INR_PER_USDT, usdt_to_inr
from futures_risk import FUTURES_STARTING_BALANCE_INR, FUTURES_STARTING_BALANCE_USDT


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def safe_float(value, default=0.0):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def json_safe(value):
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    return value


def get_open_futures_positions(limit=20):
    return read_table(
        "futures_paper_trades",
        {
            "select": "id,trade_id,created_at,updated_at,status,direction,entry_price,current_price,stop_loss,take_profit,liquidation_price_estimate,leverage,position_size,margin_used_usdt,risk_usdt,expected_reward_usdt,rr_ratio,unrealized_pnl_usdt,realized_pnl_usdt,reason_for_entry,exit_reason,confidence_score,market_regime_at_entry",
            "status": "eq.OPEN",
            "order": "created_at.desc",
            "limit": limit,
        },
    )


def get_closed_futures_trades(limit=200):
    return read_table(
        "futures_paper_trades",
        {
            "select": "id,trade_id,created_at,updated_at,closed_at,status,direction,entry_price,exit_price,stop_loss,take_profit,leverage,position_size,margin_used_usdt,realized_pnl_usdt,reason_for_entry,exit_reason,confidence_score",
            "status": "neq.OPEN",
            "order": "updated_at.desc",
            "limit": limit,
        },
    )


def get_futures_journal(limit=200):
    return read_table(
        "futures_trade_journal",
        {
            "select": "id,created_at,trade_id,event_type,price,pnl_usdt,wallet_equity_usdt,reason",
            "order": "created_at.desc",
            "limit": limit,
        },
    )


def get_futures_training_dataset(limit=200):
    return read_table(
        "futures_model_training_dataset",
        {
            "select": "id,created_at,updated_at,trade_id,label,final_outcome,max_favorable_excursion,max_adverse_excursion,model_ready",
            "order": "created_at.desc",
            "limit": limit,
        },
    )


def get_latest_futures_engine_run():
    runs = read_table(
        "futures_trading_engine_runs",
        {
            "select": "id,created_at,cycle_started_at,cycle_finished_at,status,run_status,action,error,interval_seconds,opened_trade_id,selected_direction,selected_score,open_position_count,closed_trade_count,symbol,engine_name,market_regime,selected_strategy,selected_expiry,selected_strikes,confidence_score,risk_score,margin_used,pnl,unrealized_pnl,realized_pnl,decision_reason,decision_hash,payload_truncated",
            "order": "created_at.desc",
            "limit": 1,
        },
    )

    if runs.empty:
        return {}

    return runs.iloc[0].to_dict()


def get_latest_wallet_row():
    wallet = read_table(
        "futures_paper_wallet",
        {
            "select": "id,created_at,starting_capital_inr,starting_capital_usdt,available_balance_usdt,used_margin_usdt,realized_pnl_usdt,unrealized_pnl_usdt,current_equity_usdt,margin_health_pct,book_greeks",
            "order": "created_at.desc",
            "limit": 1,
        },
    )

    if wallet.empty:
        return {}

    return wallet.iloc[0].to_dict()


def realized_pnl_from_closed(closed_trades):
    if closed_trades is None or closed_trades.empty or "realized_pnl_usdt" not in closed_trades:
        return 0.0

    return float(pd.to_numeric(closed_trades["realized_pnl_usdt"], errors="coerce").fillna(0).sum())


def unrealized_pnl_from_open(open_positions):
    if open_positions is None or open_positions.empty or "unrealized_pnl_usdt" not in open_positions:
        return 0.0

    return float(pd.to_numeric(open_positions["unrealized_pnl_usdt"], errors="coerce").fillna(0).sum())


def margin_from_open(open_positions):
    if open_positions is None or open_positions.empty or "margin_used_usdt" not in open_positions:
        return 0.0

    return float(pd.to_numeric(open_positions["margin_used_usdt"], errors="coerce").fillna(0).sum())


def wallet_state(open_positions=None, closed_trades=None):
    open_positions = open_positions if open_positions is not None else get_open_futures_positions()
    closed_trades = closed_trades if closed_trades is not None else get_closed_futures_trades(limit=1000)
    wallet_row = get_latest_wallet_row()

    realized = realized_pnl_from_closed(closed_trades)
    unrealized = unrealized_pnl_from_open(open_positions)
    used_margin = margin_from_open(open_positions)
    equity = FUTURES_STARTING_BALANCE_USDT + realized + unrealized
    available = max(equity - used_margin, 0)
    current_balance = FUTURES_STARTING_BALANCE_USDT + realized

    total_trades = 0 if closed_trades.empty else len(closed_trades)
    winning_trades = 0
    losing_trades = 0

    if not closed_trades.empty and "realized_pnl_usdt" in closed_trades:
        pnl_series = pd.to_numeric(closed_trades["realized_pnl_usdt"], errors="coerce").fillna(0)
        winning_trades = int((pnl_series > 0).sum())
        losing_trades = int((pnl_series < 0).sum())

    win_rate = round((winning_trades / total_trades) * 100, 2) if total_trades else 0
    peak_equity = max(safe_float(wallet_row.get("equity_usdt")), FUTURES_STARTING_BALANCE_USDT, equity)
    drawdown = max(0, (peak_equity - equity) / peak_equity * 100) if peak_equity else 0
    max_drawdown = max(drawdown, safe_float(wallet_row.get("max_drawdown_pct")))

    return {
        "starting_balance_inr": FUTURES_STARTING_BALANCE_INR,
        "starting_balance_usdt": round(FUTURES_STARTING_BALANCE_USDT, 4),
        "current_balance_usdt": round(current_balance, 4),
        "current_balance_inr": usdt_to_inr(current_balance),
        "available_balance_usdt": round(available, 4),
        "available_balance_inr": usdt_to_inr(available),
        "used_margin_usdt": round(used_margin, 4),
        "used_margin_inr": usdt_to_inr(used_margin),
        "unrealized_pnl_usdt": round(unrealized, 4),
        "unrealized_pnl_inr": usdt_to_inr(unrealized),
        "realized_pnl_usdt": round(realized, 4),
        "realized_pnl_inr": usdt_to_inr(realized),
        "equity_usdt": round(equity, 4),
        "equity_inr": usdt_to_inr(equity),
        "max_drawdown_pct": round(max_drawdown, 2),
        "total_trades": total_trades,
        "winning_trades": winning_trades,
        "losing_trades": losing_trades,
        "win_rate": win_rate,
        "open_positions": 0 if open_positions.empty else len(open_positions),
        "inr_per_usdt": INR_PER_USDT,
    }


def persist_wallet_snapshot(wallet):
    payload = {
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "starting_balance_inr": wallet.get("starting_balance_inr"),
        "starting_balance_usdt": wallet.get("starting_balance_usdt"),
        "current_balance_usdt": wallet.get("current_balance_usdt"),
        "current_balance_inr": wallet.get("current_balance_inr"),
        "available_balance_usdt": wallet.get("available_balance_usdt"),
        "used_margin_usdt": wallet.get("used_margin_usdt"),
        "unrealized_pnl_usdt": wallet.get("unrealized_pnl_usdt"),
        "realized_pnl_usdt": wallet.get("realized_pnl_usdt"),
        "equity_usdt": wallet.get("equity_usdt"),
        "max_drawdown_pct": wallet.get("max_drawdown_pct"),
        "total_trades": wallet.get("total_trades"),
        "winning_trades": wallet.get("winning_trades"),
        "losing_trades": wallet.get("losing_trades"),
        "win_rate": wallet.get("win_rate"),
        "status": "ACTIVE",
        "snapshot_json": json_safe(wallet),
    }
    return _request("POST", "futures_paper_wallet", payload=payload, prefer="return=minimal")


def create_futures_trade(decision, wallet):
    risk = decision.get("risk") or {}
    context = decision.get("context") or {}
    trade_id = f"FUT-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    payload = {
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "trade_id": trade_id,
        "symbol": decision.get("symbol", "ETHUSDT.PERP"),
        "direction": decision.get("direction"),
        "status": "OPEN",
        "entry_price": decision.get("entry_price"),
        "mark_price": decision.get("entry_price"),
        "stop_loss": decision.get("stop_loss"),
        "take_profit": decision.get("take_profit"),
        "trailing_stop": decision.get("trailing_stop"),
        "liquidation_price_estimate": risk.get("liquidation_price_estimate"),
        "leverage": risk.get("leverage"),
        "lots": risk.get("lots"),
        "position_size_eth": risk.get("position_size_eth"),
        "margin_used_usdt": risk.get("margin_required_usdt"),
        "risk_amount_usdt": risk.get("risk_amount_usdt"),
        "risk_pct": risk.get("risk_pct"),
        "expected_reward_usdt": risk.get("expected_reward_usdt"),
        "reward_pct": risk.get("reward_pct"),
        "rr_ratio": risk.get("rr_ratio"),
        "realized_pnl_usdt": 0,
        "realized_pnl_inr": 0,
        "unrealized_pnl_usdt": 0,
        "entry_confidence_score": decision.get("confidence_score"),
        "entry_reason": decision.get("reason"),
        "market_regime": context.get("market_regime"),
        "trend_context": context.get("trend_context"),
        "smc_context": json_safe(context.get("smc_context")),
        "volume_context": json_safe(context.get("volume_context")),
        "options_context": json_safe(context.get("options_context")),
        "liquidation_context": json_safe(
            {
                "liquidation_distance_pct": risk.get("liquidation_distance_pct"),
                "minimum_required_pct": 8,
            }
        ),
        "raw_snapshot_json": json_safe(context),
    }
    result = _request("POST", "futures_paper_trades", payload=payload, prefer="return=representation")
    trade = result[0] if isinstance(result, list) and result else payload
    record_journal_event(
        trade.get("trade_id") or trade_id,
        "OPEN",
        "Opened autonomous futures paper trade.",
        decision.get("entry_price"),
        wallet.get("equity_usdt"),
        0,
        "RISK_ACCEPTED",
        "OPEN",
        decision.get("reason"),
        {"decision": decision, "wallet": wallet},
    )
    create_training_seed(trade, decision)
    return trade


def patch_futures_trade(row_id, payload):
    payload = {**payload, "updated_at": now_iso()}
    result = _request(
        "PATCH",
        "futures_paper_trades",
        payload=json_safe(payload),
        params={"id": f"eq.{row_id}"},
        prefer="return=representation",
    )
    return result[0] if isinstance(result, list) and result else payload


def record_journal_event(
    trade_id,
    event_type,
    event_description,
    price,
    wallet_equity_usdt,
    pnl_usdt,
    risk_state,
    action_taken,
    reason,
    raw_data=None,
):
    payload = {
        "created_at": now_iso(),
        "trade_id": trade_id,
        "event_type": event_type,
        "event_description": event_description,
        "price": price,
        "wallet_equity_usdt": wallet_equity_usdt,
        "pnl_usdt": pnl_usdt,
        "risk_state": risk_state,
        "action_taken": action_taken,
        "reason": reason,
        "raw_data_json": json_safe(raw_data or {}),
    }
    return _request("POST", "futures_trade_journal", payload=payload, prefer="return=minimal")


def create_training_seed(trade, decision):
    payload = {
        "created_at": now_iso(),
        "trade_id": trade.get("trade_id"),
        "features_json": json_safe(decision.get("context") or {}),
        "label": "OPEN_UNLABELED",
        "pnl_after_15m": None,
        "pnl_after_30m": None,
        "pnl_after_1h": None,
        "pnl_after_3h": None,
        "max_favorable_excursion": 0,
        "max_adverse_excursion": 0,
        "final_outcome": None,
        "model_ready": False,
    }
    return _request("POST", "futures_model_training_dataset", payload=payload, prefer="return=minimal")


def update_training_outcome(trade, final_outcome, mfe, mae):
    return _request(
        "PATCH",
        "futures_model_training_dataset",
        payload=json_safe(
            {
                "label": "GOOD_TRADE" if safe_float(trade.get("realized_pnl_usdt")) > 0 else "BAD_TRADE",
                "max_favorable_excursion": mfe,
                "max_adverse_excursion": mae,
                "final_outcome": final_outcome,
                "model_ready": True,
            }
        ),
        params={"trade_id": f"eq.{trade.get('trade_id')}"},
        prefer="return=minimal",
    )


def record_futures_engine_run(
    status,
    cycle_started_at=None,
    action=None,
    error=None,
    interval_seconds=None,
    evaluation=None,
):
    evaluation = evaluation or {}
    decision = evaluation.get("decision") or {}
    opened_trade = evaluation.get("opened_trade") or {}
    open_positions = evaluation.get("open_positions")
    closed_trades = evaluation.get("closed_trades")
    normalized_action = action or evaluation.get("action") or decision.get("direction") or "no_trade"
    decision_hash = build_decision_hash(
        "futures_directional",
        "ETHUSD",
        normalized_action,
        decision.get("direction"),
        None,
        [decision.get("entry_price"), decision.get("stop_loss"), decision.get("take_profit")],
        decision.get("market_regime"),
        decision.get("confidence_score"),
        (decision.get("risk") or {}).get("risk_amount_usdt") if isinstance(decision.get("risk"), dict) else None,
    )

    if not should_persist_engine_snapshot(
        "futures_trading_engine_runs",
        "ETHUSD",
        decision_hash,
        normalized_action,
        decision.get("direction"),
        None,
        [decision.get("entry_price"), decision.get("stop_loss"), decision.get("take_profit")],
    ):
        return {
            "created_at": now_iso(),
            "status": status,
            "action": normalized_action,
            "decision_hash": decision_hash,
            "skipped_persistence": True,
        }

    cycle_json, truncated = safe_engine_payload(
        {
            "action": normalized_action,
            "error": error,
            "decision": {
                "direction": decision.get("direction"),
                "reason": decision.get("reason"),
                "confidence_score": decision.get("confidence_score"),
                "entry_price": decision.get("entry_price"),
                "stop_loss": decision.get("stop_loss"),
                "take_profit": decision.get("take_profit"),
            },
            "opened_trade_id": opened_trade.get("id") if isinstance(opened_trade, dict) else None,
            "position_updates": evaluation.get("position_updates") or [],
        },
        table_name="futures_trading_engine_runs",
    )

    payload = {
        "created_at": now_iso(),
        "cycle_started_at": cycle_started_at,
        "cycle_finished_at": now_iso(),
        "status": status,
        "run_status": status,
        "action": normalized_action,
        "error": error,
        "interval_seconds": interval_seconds,
        "opened_trade_id": opened_trade.get("id") if isinstance(opened_trade, dict) else None,
        "symbol": "ETHUSD",
        "engine_name": "futures_directional",
        "market_regime": decision.get("market_regime"),
        "selected_strategy": decision.get("direction"),
        "selected_expiry": None,
        "selected_strikes": json_safe(
            {
                "entry_price": decision.get("entry_price"),
                "stop_loss": decision.get("stop_loss"),
                "take_profit": decision.get("take_profit"),
            }
        ),
        "selected_direction": decision.get("direction"),
        "selected_score": decision.get("confidence_score"),
        "confidence_score": decision.get("confidence_score"),
        "risk_score": (decision.get("risk") or {}).get("risk_amount_usdt") if isinstance(decision.get("risk"), dict) else None,
        "margin_used": (decision.get("risk") or {}).get("margin_required_usdt") if isinstance(decision.get("risk"), dict) else None,
        "pnl": (opened_trade or {}).get("realized_pnl_usdt") if isinstance(opened_trade, dict) else None,
        "decision_reason": compact_decision_reason(decision.get("reason") or normalized_action),
        "decision_hash": decision_hash,
        "snapshot_refs": json_safe({"opened_trade_id": opened_trade.get("id") if isinstance(opened_trade, dict) else None}),
        "payload_truncated": truncated,
        "open_position_count": 0 if open_positions is None or open_positions.empty else len(open_positions),
        "closed_trade_count": 0 if closed_trades is None or closed_trades.empty else len(closed_trades),
        "cycle_json": json_safe(cycle_json),
    }
    result = _request("POST", "futures_trading_engine_runs", payload=payload, prefer="return=representation")

    if isinstance(result, list) and result:
        return result[0]

    return payload
