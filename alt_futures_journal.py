from datetime import datetime, timezone

import pandas as pd

from alt_futures_risk import ALT_FUTURES_STARTING_BALANCE_INR, ALT_FUTURES_STARTING_BALANCE_USDT, safe_float
from engine_persistence import (
    build_decision_hash,
    compact_decision_reason,
    safe_engine_payload,
    should_persist_engine_snapshot,
    top_candidates_summary,
)
from recommendation_journal import _request, read_table
from validation_config import INR_PER_USDT, usdt_to_inr


def now_iso():
    return datetime.now(timezone.utc).isoformat()


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


def get_open_alt_trades(limit=10):
    return read_table(
        "alt_futures_trade_journal",
        {
            "select": "id,trade_id,created_at,updated_at,status,symbol,direction,entry_price,current_price,stop_loss,take_profit_1,take_profit_2,trailing_stop,liquidation_price_estimate,leverage,position_size,margin_used_usdt,risk_usdt,expected_reward_usdt,rr_ratio,unrealized_pnl_usdt,pnl_usdt,reason_for_entry,market_regime_at_entry,scanner_score_at_entry,trade_confidence",
            "status": "eq.OPEN",
            "order": "created_at.desc",
            "limit": limit,
        },
    )


def get_closed_alt_trades(limit=200):
    return read_table(
        "alt_futures_trade_journal",
        {
            "select": "id,trade_id,created_at,updated_at,closed_at,status,symbol,direction,entry_price,exit_price,stop_loss,take_profit_1,take_profit_2,leverage,position_size,margin_used_usdt,pnl_usdt,reason_for_entry,reason_for_exit,scanner_score_at_entry,trade_confidence",
            "status": "neq.OPEN",
            "order": "created_at.desc",
            "limit": limit,
        },
    )


def get_alt_trade_events(limit=300):
    return read_table(
        "alt_futures_trade_events",
        {
            "select": "id,created_at,trade_id,event_type,price,pnl_usdt,wallet_equity_usdt,reason",
            "order": "created_at.desc",
            "limit": limit,
        },
    )


def get_latest_scanner_snapshots(limit=250):
    return read_table(
        "alt_futures_scanner_snapshots",
        {
            "select": "id,created_at,symbol,price,score,classification,direction,funding_rate,open_interest,oi_change_pct,volume,volume_change_pct,spread,spread_pct,liquidity_score,trend_score,smc_score,final_reason,selected,decision_hash,payload_truncated",
            "order": "created_at.desc",
            "limit": limit,
        },
    )


def get_latest_engine_run():
    runs = read_table(
        "alt_futures_engine_runs",
        {
            "select": "id,created_at,cycle_started_at,cycle_finished_at,status,run_status,action,error,interval_seconds,opened_trade_id,selected_symbol,selected_direction,selected_score,open_position_count,scanned_symbol_count,decision_hash,payload_truncated,decision_reason",
            "order": "created_at.desc",
            "limit": 1,
        },
    )

    if runs.empty:
        return {}

    return runs.iloc[0].to_dict()


def get_latest_wallet_row():
    wallet = read_table(
        "alt_futures_wallet_ledger",
        {
            "select": "id,created_at,wallet_balance_inr,wallet_balance_usdt,equity_inr,equity_usdt,available_balance_usdt,used_margin_usdt,realized_pnl_usdt,unrealized_pnl_usdt,max_drawdown_pct,event_type,trade_id,notes",
            "order": "created_at.desc",
            "limit": 1,
        },
    )

    if wallet.empty:
        return {}

    return wallet.iloc[0].to_dict()


def realized_pnl_from_closed(closed_trades):
    if closed_trades is None or closed_trades.empty or "pnl_usdt" not in closed_trades:
        return 0.0

    return float(pd.to_numeric(closed_trades["pnl_usdt"], errors="coerce").fillna(0).sum())


def unrealized_pnl_from_open(open_trades):
    if open_trades is None or open_trades.empty or "unrealized_pnl_usdt" not in open_trades:
        return 0.0

    return float(pd.to_numeric(open_trades["unrealized_pnl_usdt"], errors="coerce").fillna(0).sum())


def margin_from_open(open_trades):
    if open_trades is None or open_trades.empty or "margin_used_usdt" not in open_trades:
        return 0.0

    return float(pd.to_numeric(open_trades["margin_used_usdt"], errors="coerce").fillna(0).sum())


def wallet_state(open_trades=None, closed_trades=None):
    open_trades = open_trades if open_trades is not None else get_open_alt_trades()
    closed_trades = closed_trades if closed_trades is not None else get_closed_alt_trades(limit=1000)
    wallet_row = get_latest_wallet_row()

    realized = realized_pnl_from_closed(closed_trades)
    unrealized = unrealized_pnl_from_open(open_trades)
    used_margin = margin_from_open(open_trades)
    equity = ALT_FUTURES_STARTING_BALANCE_USDT + realized + unrealized
    current_balance = ALT_FUTURES_STARTING_BALANCE_USDT + realized
    available = max(equity - used_margin, 0)
    total_trades = 0 if closed_trades.empty else int((closed_trades["status"] == "CLOSED").sum()) if "status" in closed_trades else len(closed_trades)
    winning_trades = 0
    losing_trades = 0

    if not closed_trades.empty and "pnl_usdt" in closed_trades:
        pnl_series = pd.to_numeric(closed_trades["pnl_usdt"], errors="coerce").fillna(0)
        winning_trades = int((pnl_series > 0).sum())
        losing_trades = int((pnl_series < 0).sum())

    win_rate = round((winning_trades / total_trades) * 100, 2) if total_trades else 0
    peak_equity = max(safe_float(wallet_row.get("equity_usdt")), ALT_FUTURES_STARTING_BALANCE_USDT, equity)
    drawdown = max(0, (peak_equity - equity) / peak_equity * 100) if peak_equity else 0
    max_drawdown = max(drawdown, safe_float(wallet_row.get("max_drawdown_pct")))

    return {
        "starting_balance_inr": ALT_FUTURES_STARTING_BALANCE_INR,
        "starting_balance_usdt": round(ALT_FUTURES_STARTING_BALANCE_USDT, 4),
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
        "open_positions": 0 if open_trades.empty else len(open_trades),
        "inr_per_usdt": INR_PER_USDT,
    }


def persist_scanner_snapshots(candidates):
    rows = []
    top_symbols = {item.get("symbol") for item in top_candidates_summary(candidates, limit=5)}

    for candidate in candidates:
        selected = bool(candidate.get("selected"))
        if not selected and candidate.get("symbol") not in top_symbols:
            continue

        decision_hash = build_decision_hash(
            "alt_futures_scanner",
            candidate.get("symbol"),
            "scan_selected" if selected else "scan_watch",
            candidate.get("direction") or candidate.get("classification"),
            None,
            [candidate.get("price")],
            candidate.get("market_regime"),
            candidate.get("score"),
            candidate.get("spread_pct"),
        )
        compact_candidate, truncated = safe_engine_payload(candidate, "alt_futures_scanner_snapshots")
        rows.append(
            {
                "created_at": now_iso(),
                "engine_name": "alt_futures_scanner",
                "action": "scan_selected" if selected else "scan_watch",
                "symbol": candidate.get("symbol"),
                "price": candidate.get("price"),
                "score": candidate.get("score"),
                "classification": candidate.get("classification"),
                "direction": candidate.get("direction"),
                "indicators_json": json_safe(candidate.get("indicators") or {}),
                "funding_rate": candidate.get("funding_rate"),
                "open_interest": candidate.get("open_interest"),
                "oi_change_pct": candidate.get("oi_change_pct"),
                "volume": candidate.get("volume"),
                "volume_change_pct": candidate.get("volume_change_pct"),
                "spread": candidate.get("spread"),
                "spread_pct": candidate.get("spread_pct"),
                "liquidity_score": candidate.get("scores", {}).get("liquidity"),
                "trend_score": candidate.get("scores", {}).get("trend"),
                "smc_score": candidate.get("scores", {}).get("smc"),
                "final_reason": candidate.get("reason"),
                "selected": selected,
                "decision_hash": decision_hash,
                "payload_truncated": truncated,
                "raw_snapshot_json": json_safe(compact_candidate),
            }
        )

    if not rows:
        return None

    return _request("POST", "alt_futures_scanner_snapshots", payload=rows, prefer="return=minimal")


def persist_wallet_snapshot(wallet, event_type="SNAPSHOT", trade_id=None, notes=None):
    payload = {
        "created_at": now_iso(),
        "wallet_balance_inr": wallet.get("current_balance_inr"),
        "wallet_balance_usdt": wallet.get("current_balance_usdt"),
        "equity_inr": wallet.get("equity_inr"),
        "equity_usdt": wallet.get("equity_usdt"),
        "available_balance_usdt": wallet.get("available_balance_usdt"),
        "used_margin_usdt": wallet.get("used_margin_usdt"),
        "realized_pnl_usdt": wallet.get("realized_pnl_usdt"),
        "unrealized_pnl_usdt": wallet.get("unrealized_pnl_usdt"),
        "max_drawdown_pct": wallet.get("max_drawdown_pct"),
        "event_type": event_type,
        "trade_id": trade_id,
        "notes": notes,
        "ledger_json": json_safe(wallet),
    }
    return _request("POST", "alt_futures_wallet_ledger", payload=payload, prefer="return=minimal")


def create_alt_trade(decision, wallet, status="OPEN"):
    risk = decision.get("risk") or {}
    candidate = decision.get("candidate") or {}
    trade_id = f"ALT-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    payload = {
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "trade_id": trade_id,
        "symbol": decision.get("symbol"),
        "direction": decision.get("direction"),
        "entry_price": decision.get("entry_price"),
        "stop_loss": decision.get("stop_loss"),
        "take_profit_1": decision.get("take_profit_1"),
        "take_profit_2": decision.get("take_profit_2"),
        "trailing_stop": decision.get("trailing_stop"),
        "liquidation_price_estimate": risk.get("liquidation_price_estimate"),
        "leverage": risk.get("leverage"),
        "position_size": risk.get("position_size"),
        "margin_used_usdt": risk.get("margin_required_usdt"),
        "margin_used_inr": risk.get("margin_required_inr"),
        "risk_usdt": risk.get("risk_amount_usdt"),
        "risk_inr": risk.get("risk_amount_inr"),
        "expected_reward_usdt": risk.get("expected_reward_usdt"),
        "expected_reward_inr": risk.get("expected_reward_inr"),
        "rr_ratio": risk.get("rr_ratio"),
        "wallet_before_usdt": wallet.get("equity_usdt"),
        "wallet_after_usdt": wallet.get("equity_usdt"),
        "status": status,
        "pnl_inr": 0,
        "pnl_usdt": 0,
        "unrealized_pnl_usdt": 0,
        "reason_for_entry": decision.get("reason"),
        "market_regime_at_entry": candidate.get("market_regime"),
        "scanner_score_at_entry": decision.get("candidate_score"),
        "trade_confidence": decision.get("trade_confidence"),
        "raw_trade_json": json_safe({"decision": decision, "wallet": wallet}),
    }
    result = _request("POST", "alt_futures_trade_journal", payload=payload, prefer="return=representation")
    trade = result[0] if isinstance(result, list) and result else payload

    record_trade_event(
        trade.get("trade_id") or trade_id,
        "OPEN" if status == "OPEN" else status,
        decision.get("entry_price"),
        0,
        wallet.get("equity_usdt"),
        decision.get("reason"),
        {"decision": decision, "wallet": wallet},
    )
    persist_wallet_snapshot(wallet, event_type="TRADE_OPEN" if status == "OPEN" else status, trade_id=trade.get("trade_id") or trade_id)
    return trade


def patch_alt_trade(row_id, payload):
    payload = {**payload, "updated_at": now_iso()}
    result = _request(
        "PATCH",
        "alt_futures_trade_journal",
        payload=json_safe(payload),
        params={"id": f"eq.{row_id}"},
        prefer="return=representation",
    )
    return result[0] if isinstance(result, list) and result else payload


def record_trade_event(trade_id, event_type, price, pnl_usdt, wallet_equity_usdt, reason, raw_data=None):
    payload = {
        "created_at": now_iso(),
        "trade_id": trade_id,
        "event_type": event_type,
        "price": price,
        "pnl_usdt": pnl_usdt,
        "pnl_inr": usdt_to_inr(pnl_usdt),
        "wallet_equity_usdt": wallet_equity_usdt,
        "reason": reason,
        "raw_event_json": json_safe(raw_data or {}),
    }
    return _request("POST", "alt_futures_trade_events", payload=payload, prefer="return=minimal")


def record_engine_run(status, cycle_started_at=None, action=None, error=None, interval_seconds=None, evaluation=None):
    evaluation = evaluation or {}
    decision = evaluation.get("decision") or {}
    opened_trade = evaluation.get("opened_trade") or {}
    open_trades = evaluation.get("open_trades")
    candidates = evaluation.get("candidates") or []
    normalized_action = action or evaluation.get("action") or decision.get("direction") or "no_trade"
    decision_hash = build_decision_hash(
        "alt_futures",
        decision.get("symbol") or "MULTI",
        normalized_action,
        decision.get("direction"),
        None,
        [decision.get("entry_price"), decision.get("stop_loss"), decision.get("take_profit_1"), decision.get("take_profit_2")],
        (decision.get("candidate") or {}).get("market_regime") if isinstance(decision.get("candidate"), dict) else None,
        decision.get("trade_confidence") or decision.get("candidate_score"),
        (decision.get("risk") or {}).get("risk_amount_usdt") if isinstance(decision.get("risk"), dict) else None,
    )

    if not should_persist_engine_snapshot(
        "alt_futures_engine_runs",
        decision.get("symbol") or "MULTI",
        decision_hash,
        normalized_action,
        decision.get("direction"),
        None,
        [decision.get("entry_price"), decision.get("stop_loss"), decision.get("take_profit_1"), decision.get("take_profit_2")],
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
                "symbol": decision.get("symbol"),
                "direction": decision.get("direction"),
                "reason": decision.get("reason"),
                "candidate_score": decision.get("candidate_score"),
                "entry_price": decision.get("entry_price"),
                "stop_loss": decision.get("stop_loss"),
                "take_profit_1": decision.get("take_profit_1"),
                "take_profit_2": decision.get("take_profit_2"),
            },
            "top_candidates": top_candidates_summary(candidates, limit=3),
            "opened_trade_id": opened_trade.get("id") if isinstance(opened_trade, dict) else None,
            "position_updates": evaluation.get("position_updates") or [],
        },
        table_name="alt_futures_engine_runs",
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
        "symbol": decision.get("symbol") or "MULTI",
        "engine_name": "alt_futures",
        "market_regime": (decision.get("candidate") or {}).get("market_regime") if isinstance(decision.get("candidate"), dict) else None,
        "selected_strategy": decision.get("direction"),
        "selected_strikes": json_safe(
            {
                "entry_price": decision.get("entry_price"),
                "stop_loss": decision.get("stop_loss"),
                "take_profit_1": decision.get("take_profit_1"),
                "take_profit_2": decision.get("take_profit_2"),
            }
        ),
        "top_candidates_summary": json_safe(top_candidates_summary(candidates, limit=3)),
        "confidence_score": decision.get("trade_confidence") or decision.get("candidate_score"),
        "risk_score": (decision.get("risk") or {}).get("risk_amount_usdt") if isinstance(decision.get("risk"), dict) else None,
        "margin_used": (decision.get("risk") or {}).get("margin_required_usdt") if isinstance(decision.get("risk"), dict) else None,
        "decision_reason": compact_decision_reason(decision.get("reason") or normalized_action),
        "decision_hash": decision_hash,
        "snapshot_refs": json_safe({"opened_trade_id": opened_trade.get("id") if isinstance(opened_trade, dict) else None}),
        "payload_truncated": truncated,
        "selected_symbol": decision.get("symbol"),
        "selected_direction": decision.get("direction"),
        "selected_score": decision.get("candidate_score"),
        "open_position_count": 0 if open_trades is None or open_trades.empty else len(open_trades),
        "scanned_symbol_count": len(candidates),
        "cycle_json": json_safe(cycle_json),
    }
    result = _request("POST", "alt_futures_engine_runs", payload=payload, prefer="return=representation")
    return result[0] if isinstance(result, list) and result else payload
