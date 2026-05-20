from validation_config import usdt_to_inr

from alt_futures_journal import (
    create_alt_trade,
    get_alt_trade_events,
    get_closed_alt_trades,
    get_latest_engine_run,
    get_latest_scanner_snapshots,
    get_open_alt_trades,
    json_safe,
    now_iso,
    patch_alt_trade,
    persist_scanner_snapshots,
    persist_wallet_snapshot,
    record_trade_event,
    wallet_state,
)
from alt_futures_risk import (
    calculate_alt_position,
    choose_leverage,
    margin_cap_pct,
    risk_pct_for_score,
    safe_float,
    validate_position_risk,
)
from alt_futures_scanner import scan_alt_futures


MIN_SCORE_TO_TRADE = 70
MAX_SPREAD_PCT = 0.18
MIN_LIQUIDITY_SCORE = 6
BREAKEVEN_TRIGGER_R = 0.8
TRAILING_TRIGGER_R = 1.25
TRAILING_LOCK_R = 0.65


def _round(value, digits=6):
    try:
        return round(float(value), digits)
    except Exception:
        return None


def _stop_pct(candidate):
    indicators = candidate.get("indicators") or {}
    atr_pct = safe_float(indicators.get("atr_pct"), 1.2) / 100
    volatility_pct = safe_float(indicators.get("volatility_pct"), 2.0) / 100
    base = max(atr_pct * 1.6, volatility_pct * 0.55, 0.009)
    return min(max(base, 0.008), 0.045)


def _build_trade_prices(candidate):
    direction = candidate.get("direction")
    entry = safe_float(candidate.get("price"))
    stop_distance = entry * _stop_pct(candidate)
    rr1 = 1.65 if safe_float(candidate.get("score")) < 82 else 1.9
    rr2 = 2.35 if safe_float(candidate.get("score")) < 82 else 2.8

    if direction == "LONG":
        stop_loss = entry - stop_distance
        take_profit_1 = entry + stop_distance * rr1
        take_profit_2 = entry + stop_distance * rr2
        trailing_stop = entry - stop_distance * 0.75
    elif direction == "SHORT":
        stop_loss = entry + stop_distance
        take_profit_1 = entry - stop_distance * rr1
        take_profit_2 = entry - stop_distance * rr2
        trailing_stop = entry + stop_distance * 0.75
    else:
        return None, None, None, None, None

    return _round(entry), _round(stop_loss), _round(take_profit_1), _round(take_profit_2), _round(trailing_stop)


def _candidate_is_tradeable(candidate):
    if candidate.get("direction") not in ["LONG", "SHORT"]:
        return False
    if safe_float(candidate.get("score")) < MIN_SCORE_TO_TRADE:
        return False
    spread_pct = safe_float(candidate.get("spread_pct"), 999)
    if spread_pct > MAX_SPREAD_PCT:
        return False
    if safe_float(candidate.get("scores", {}).get("liquidity")) < MIN_LIQUIDITY_SCORE:
        return False
    if candidate.get("classification") in ["AVOID", "NO_TRADE", "WATCHLIST"]:
        return False
    return True


def select_best_candidate(candidates):
    for candidate in candidates:
        if _candidate_is_tradeable(candidate):
            return candidate
    return candidates[0] if candidates else None


def decide_trade(candidates, wallet):
    if not candidates:
        return {"direction": "NO_TRADE", "reason": "No alt futures candidates were scanned.", "candidate_score": 0}

    candidate = select_best_candidate(candidates)
    rejection_reasons = []

    if not candidate:
        return {"direction": "NO_TRADE", "reason": "No alt futures candidate is available.", "candidate_score": 0}

    if wallet.get("open_positions"):
        rejection_reasons.append("One active alt futures trade already exists.")

    if not _candidate_is_tradeable(candidate):
        rejection_reasons.append(candidate.get("reason") or "Best candidate did not pass scanner filters.")

    entry, stop_loss, tp1, tp2, trailing_stop = _build_trade_prices(candidate)
    indicators = candidate.get("indicators") or {}
    stop_pct = abs(entry - stop_loss) / entry * 100 if entry and stop_loss else 0
    leverage = choose_leverage(candidate.get("score"), indicators.get("volatility_pct"), stop_pct, candidate.get("spread_pct"))
    risk_pct = risk_pct_for_score(candidate.get("score"), indicators.get("volatility_pct"), candidate.get("spread_pct"))
    max_margin_pct = margin_cap_pct(candidate.get("score"), indicators.get("volatility_pct"))
    risk = calculate_alt_position(
        wallet.get("equity_usdt"),
        wallet.get("available_balance_usdt"),
        entry,
        stop_loss,
        tp1,
        candidate.get("direction"),
        leverage,
        risk_pct,
        max_margin_pct,
    )
    rejection_reasons.extend(validate_position_risk(risk, entry, stop_loss))

    margin_after = safe_float(wallet.get("used_margin_usdt")) + safe_float((risk or {}).get("margin_required_usdt"))
    equity = max(safe_float(wallet.get("equity_usdt")), 1)
    if margin_after / equity > 0.35:
        rejection_reasons.append("Post-trade margin usage would exceed 35% of the alt futures wallet.")

    if rejection_reasons:
        return {
            "symbol": candidate.get("symbol"),
            "direction": "NO_TRADE",
            "reason": " ".join(rejection_reasons),
            "candidate_score": candidate.get("score"),
            "trade_confidence": candidate.get("classification"),
            "entry_price": entry,
            "stop_loss": stop_loss,
            "take_profit_1": tp1,
            "take_profit_2": tp2,
            "trailing_stop": trailing_stop,
            "risk": risk or {},
            "candidate": candidate,
        }

    reason = (
        f"{candidate.get('classification')} {candidate.get('symbol')} selected with score "
        f"{candidate.get('score'):.0f}; RR {risk.get('rr_ratio')}, leverage {leverage}x. "
        f"{candidate.get('reason')}"
    )
    return {
        "symbol": candidate.get("symbol"),
        "direction": candidate.get("direction"),
        "reason": reason,
        "candidate_score": candidate.get("score"),
        "trade_confidence": candidate.get("classification"),
        "entry_price": entry,
        "stop_loss": stop_loss,
        "take_profit_1": tp1,
        "take_profit_2": tp2,
        "trailing_stop": trailing_stop,
        "risk": risk,
        "candidate": candidate,
    }


def _position_pnl(trade, mark_price):
    direction = trade.get("direction")
    entry = safe_float(trade.get("entry_price"))
    size = safe_float(trade.get("position_size"))

    if direction == "LONG":
        return (mark_price - entry) * size

    if direction == "SHORT":
        return (entry - mark_price) * size

    return 0.0


def _r_multiple(trade, pnl):
    return pnl / max(safe_float(trade.get("risk_usdt")), 0.01)


def _stop_update(trade, mark_price, pnl):
    direction = trade.get("direction")
    entry = safe_float(trade.get("entry_price"))
    current_stop = safe_float(trade.get("trailing_stop") or trade.get("stop_loss"))
    initial_stop = safe_float(trade.get("stop_loss"))
    risk_per_unit = abs(entry - initial_stop)
    r_multiple = _r_multiple(trade, pnl)
    new_stop = current_stop
    reason = None

    if r_multiple >= BREAKEVEN_TRIGGER_R:
        if direction == "LONG" and current_stop < entry:
            new_stop = entry
            reason = "Moved alt futures stop to breakeven after favorable move."
        elif direction == "SHORT" and current_stop > entry:
            new_stop = entry
            reason = "Moved alt futures stop to breakeven after favorable move."

    if r_multiple >= TRAILING_TRIGGER_R:
        if direction == "LONG":
            trail = mark_price - risk_per_unit * TRAILING_LOCK_R
            if trail > new_stop:
                new_stop = trail
                reason = "Raised alt futures trailing stop to lock profit."
        elif direction == "SHORT":
            trail = mark_price + risk_per_unit * TRAILING_LOCK_R
            if trail < new_stop:
                new_stop = trail
                reason = "Lowered alt futures trailing stop to lock profit."

    return _round(new_stop), reason


def _exit_signal(trade, latest_candidate, mark_price, pnl):
    direction = trade.get("direction")
    stop = safe_float(trade.get("trailing_stop") or trade.get("stop_loss"))
    tp1 = safe_float(trade.get("take_profit_1"))
    candidate_direction = latest_candidate.get("direction") if latest_candidate else None
    candidate_score = safe_float(latest_candidate.get("score")) if latest_candidate else 0

    if direction == "LONG":
        if mark_price <= stop:
            return "SL", f"Mark price {mark_price} hit stop/trailing stop {stop}."
        if mark_price >= tp1:
            return "TP", f"Mark price {mark_price} hit take profit 1 {tp1}."
        if candidate_direction == "SHORT" and candidate_score >= 70:
            return "ENGINE_EXIT", "Latest scanner flipped against the open long."

    if direction == "SHORT":
        if mark_price >= stop:
            return "SL", f"Mark price {mark_price} hit stop/trailing stop {stop}."
        if mark_price <= tp1:
            return "TP", f"Mark price {mark_price} hit take profit 1 {tp1}."
        if candidate_direction == "LONG" and candidate_score >= 70:
            return "ENGINE_EXIT", "Latest scanner flipped against the open short."

    if pnl <= -safe_float(trade.get("risk_usdt")) * 1.05:
        return "RISK_EXIT", "Unrealized loss exceeded planned risk buffer."

    return None, None


def update_open_trades(candidates, wallet):
    open_trades = get_open_alt_trades()
    updates = []

    if open_trades.empty:
        return updates

    candidate_by_symbol = {candidate.get("symbol"): candidate for candidate in candidates}

    for _, row in open_trades.iterrows():
        trade = row.to_dict()
        latest = candidate_by_symbol.get(trade.get("symbol"), {})
        mark_price = safe_float(latest.get("price") or trade.get("entry_price"))
        pnl = _position_pnl(trade, mark_price)
        raw = trade.get("raw_trade_json") if isinstance(trade.get("raw_trade_json"), dict) else {}
        mfe = max(safe_float(raw.get("max_favorable_excursion")), pnl)
        mae = min(safe_float(raw.get("max_adverse_excursion")), pnl)
        new_stop, stop_reason = _stop_update(trade, mark_price, pnl)
        exit_code, exit_reason = _exit_signal(trade, latest, mark_price, pnl)
        raw_update = {
            **raw,
            "latest_candidate": latest,
            "max_favorable_excursion": mfe,
            "max_adverse_excursion": mae,
        }
        payload = {
            "unrealized_pnl_usdt": round(pnl, 4),
            "raw_trade_json": json_safe(raw_update),
        }

        if stop_reason:
            payload["trailing_stop"] = new_stop

        if exit_code:
            payload.update(
                {
                    "status": "CLOSED",
                    "exit_price": mark_price,
                    "pnl_usdt": round(pnl, 4),
                    "pnl_inr": usdt_to_inr(pnl),
                    "unrealized_pnl_usdt": 0,
                    "reason_for_exit": exit_reason,
                    "closed_at": now_iso(),
                }
            )

        updated = patch_alt_trade(trade.get("id"), payload)
        updates.append(updated)

        if stop_reason:
            record_trade_event(
                trade.get("trade_id"),
                "MODIFY",
                mark_price,
                pnl,
                wallet.get("equity_usdt"),
                stop_reason,
                {"trade": trade, "latest_candidate": latest},
            )

        if exit_code:
            record_trade_event(
                trade.get("trade_id"),
                "CLOSE",
                mark_price,
                pnl,
                wallet.get("equity_usdt"),
                exit_reason,
                {"trade": trade, "latest_candidate": latest, "exit_code": exit_code},
            )

    return updates


def auto_trade_cycle(enabled=True, persist=True):
    candidates = scan_alt_futures()
    open_before = get_open_alt_trades()
    closed_before = get_closed_alt_trades(limit=1000)
    wallet_before = wallet_state(open_before, closed_before)
    position_updates = update_open_trades(candidates, wallet_before)
    open_after = get_open_alt_trades()
    closed_after = get_closed_alt_trades(limit=1000)
    wallet = wallet_state(open_after, closed_after)
    decision = decide_trade(candidates, wallet)
    opened_trade = None
    skipped_trade = None

    for candidate in candidates:
        candidate["selected"] = candidate.get("symbol") == decision.get("symbol") and decision.get("direction") != "NO_TRADE"

    if enabled and decision.get("direction") in ["LONG", "SHORT"]:
        opened_trade = create_alt_trade(decision, wallet)
        action = f"Opened alt futures paper trade: {decision.get('symbol')} {decision.get('direction')}"
    else:
        action = "No trade: " + decision.get("reason", "no alt candidate passed safety rules")
        if not wallet.get("open_positions") and decision.get("symbol") and safe_float(decision.get("candidate_score")) > 0:
            skipped_trade = create_alt_trade(decision, wallet, status="SKIPPED")

    final_wallet = wallet_state(get_open_alt_trades(), get_closed_alt_trades(limit=1000))

    if persist:
        persist_scanner_snapshots(candidates)
        persist_wallet_snapshot(final_wallet, notes=action)

    return {
        "wallet": final_wallet,
        "candidates": candidates,
        "decision": decision,
        "opened_trade": opened_trade,
        "skipped_trade": skipped_trade,
        "position_updates": position_updates,
        "open_trades": get_open_alt_trades(),
        "closed_trades": get_closed_alt_trades(),
        "events": get_alt_trade_events(),
        "scanner_history": get_latest_scanner_snapshots(),
        "engine_status": get_latest_engine_run(),
        "action": action,
    }


def alt_futures_dashboard_data(run_cycle=False):
    if run_cycle:
        return auto_trade_cycle(enabled=True, persist=True)

    open_trades = get_open_alt_trades()
    closed_trades = get_closed_alt_trades()
    wallet = wallet_state(open_trades, closed_trades)
    history = get_latest_scanner_snapshots()
    candidates = []

    if not history.empty:
        latest_time = history.iloc[0].get("created_at")
        latest = history[history["created_at"] == latest_time].copy()
        for _, row in latest.iterrows():
            raw = row.get("raw_snapshot_json")
            candidates.append(raw if isinstance(raw, dict) else row.to_dict())

    decision = decide_trade(candidates, wallet) if candidates else {"direction": "NO_TRADE", "reason": "Waiting for the first scanner cycle.", "candidate_score": 0}
    return {
        "wallet": wallet,
        "candidates": candidates,
        "decision": decision,
        "opened_trade": None,
        "position_updates": [],
        "open_trades": open_trades,
        "closed_trades": closed_trades,
        "events": get_alt_trade_events(),
        "scanner_history": history,
        "engine_status": get_latest_engine_run(),
        "action": "Loaded latest alt futures scanner state.",
    }
