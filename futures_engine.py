import pandas as pd

from database_reader import get_latest_ohlcv_data
from rule_insights import build_rule_based_insights, get_available_expiries
from validation_config import usdt_to_inr
from futures_risk import (
    calculate_futures_position,
    choose_leverage,
    margin_cap_pct,
    risk_pct_for_confidence,
    safe_float,
    validate_position_risk,
)
from futures_storage import (
    create_futures_trade,
    get_closed_futures_trades,
    get_futures_journal,
    get_futures_training_dataset,
    get_latest_futures_engine_run,
    get_open_futures_positions,
    json_safe,
    patch_futures_trade,
    persist_wallet_snapshot,
    record_journal_event,
    update_training_outcome,
    wallet_state,
)


SYMBOL = "ETHUSDT.PERP"
MIN_CONFIDENCE_TO_TRADE = 66
MAX_CONFLICT_TO_TRADE = 62
BREAKEVEN_TRIGGER_R = 0.8
TRAILING_TRIGGER_R = 1.2
TRAILING_LOCK_R = 0.6


def _latest_expiry():
    expiries = get_available_expiries(limit=20)
    return expiries[0] if expiries else None


def _safe_round(value, digits=2):
    try:
        return round(float(value), digits)
    except Exception:
        return None


def _atr_percent(symbol="ETHUSD", resolution="5m", limit=80):
    candles = get_latest_ohlcv_data(symbol=symbol, resolution=resolution, limit=limit)

    if candles.empty or len(candles) < 15:
        return None

    high = pd.to_numeric(candles["high"], errors="coerce")
    low = pd.to_numeric(candles["low"], errors="coerce")
    close = pd.to_numeric(candles["close"], errors="coerce")
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = true_range.rolling(14).mean().iloc[-1]
    last_close = close.iloc[-1]

    if pd.isna(atr) or not last_close:
        return None

    return float(atr / last_close)


def collect_market_context():
    expiry = _latest_expiry()

    if not expiry:
        return {
            "status": "NO_DATA",
            "reason": "No option expiry is available, so the futures engine cannot build a cross-market context.",
        }

    insights = build_rule_based_insights(expiry)
    raw = insights.get("raw_input_snapshot") or {}
    price_action = raw.get("price_action") or {}
    smc_context = raw.get("smc_context") or {}
    volume_context = raw.get("profile_context") or {}
    volatility_context = raw.get("volatility_context") or {}
    orderbook = raw.get("orderbook") or {}
    mark_price = insights.get("spot_price") or orderbook.get("eth_price")
    atr_pct = _atr_percent()

    return {
        "status": "OK" if mark_price else "NO_PRICE",
        "symbol": SYMBOL,
        "expiry_label": expiry,
        "mark_price": mark_price,
        "spot_price": insights.get("spot_price"),
        "market_regime": insights.get("market_regime"),
        "volatility_regime": insights.get("volatility_regime"),
        "directional_bias": insights.get("directional_bias"),
        "confidence_score": insights.get("confidence_score"),
        "signal_conflict_score": insights.get("signal_conflict_score"),
        "trap_risk": insights.get("trap_risk"),
        "pinning_score": insights.get("pinning_score"),
        "expected_move": insights.get("expected_move"),
        "max_pain": insights.get("max_pain"),
        "pcr": insights.get("pcr"),
        "median_iv": insights.get("median_iv"),
        "realized_vol_pct": insights.get("realized_vol_pct"),
        "iv_rv_spread": insights.get("iv_rv_spread"),
        "momentum": insights.get("momentum") or price_action.get("momentum"),
        "trend_context": price_action.get("regime") or insights.get("directional_bias"),
        "support": insights.get("downside_support") or insights.get("put_wall"),
        "resistance": insights.get("upside_resistance") or insights.get("call_wall"),
        "key_insights": insights.get("key_insights") or [],
        "risk_warnings": insights.get("risk_warnings") or [],
        "smc_context": smc_context,
        "volume_context": volume_context,
        "options_context": {
            "pcr": insights.get("pcr"),
            "max_pain": insights.get("max_pain"),
            "median_iv": insights.get("median_iv"),
            "realized_vol_pct": insights.get("realized_vol_pct"),
            "iv_rv_spread": insights.get("iv_rv_spread"),
            "net_delta": insights.get("net_delta"),
            "net_gamma": insights.get("net_gamma"),
            "call_wall": insights.get("call_wall"),
            "put_wall": insights.get("put_wall"),
            "option_selling_environment": insights.get("option_selling_environment"),
        },
        "volatility_context": volatility_context,
        "orderbook": orderbook,
        "atr_pct": atr_pct,
        "raw_insights": insights,
    }


def _direction_score(context):
    long_score = 0
    short_score = 0
    notes = []
    bias = context.get("directional_bias")
    momentum = context.get("momentum")
    regime = context.get("market_regime")
    pcr = safe_float(context.get("pcr"), None)
    iv_rv_spread = safe_float(context.get("iv_rv_spread"), None)
    mark = safe_float(context.get("mark_price"))
    max_pain = safe_float(context.get("max_pain"), None)

    if bias == "Bullish":
        long_score += 32
        notes.append("directional bias is bullish")
    elif bias == "Mild Bullish":
        long_score += 18
        notes.append("directional bias is mildly bullish")
    elif bias == "Bearish":
        short_score += 32
        notes.append("directional bias is bearish")
    elif bias == "Mild Bearish":
        short_score += 18
        notes.append("directional bias is mildly bearish")

    if momentum == "Bullish":
        long_score += 16
        notes.append("momentum confirms upside")
    elif momentum == "Bearish":
        short_score += 16
        notes.append("momentum confirms downside")

    if regime in ["Directional Expansion", "Breakout Risk"]:
        long_score += 8 if long_score >= short_score else 0
        short_score += 8 if short_score > long_score else 0
        notes.append(f"market regime is {regime}")
    elif regime in ["Pinning / Range", "Balanced / Two-Sided"]:
        long_score -= 10
        short_score -= 10
        notes.append(f"market regime is {regime}, reducing trend conviction")

    if pcr is not None:
        if pcr < 0.75:
            long_score += 8
            notes.append("PCR is call-skewed")
        elif pcr > 1.25:
            short_score += 8
            notes.append("PCR is put-skewed")

    if iv_rv_spread is not None:
        if iv_rv_spread <= -5 and momentum in ["Bullish", "Bearish"]:
            if momentum == "Bullish":
                long_score += 6
            else:
                short_score += 6
            notes.append("realized volatility is running above IV, supporting directional follow-through")
        elif iv_rv_spread >= 15:
            long_score -= 6
            short_score -= 6
            notes.append("IV is rich versus RV, reducing futures trend conviction")

    if mark and max_pain:
        if mark > max_pain * 1.006:
            long_score += 4
        elif mark < max_pain * 0.994:
            short_score += 4

    return long_score, short_score, notes


def _stop_pct(context):
    atr_pct = safe_float(context.get("atr_pct"), None)
    volatility_label = str(context.get("volatility_regime") or "").lower()

    base = atr_pct * 1.8 if atr_pct else 0.01

    if "elevated" in volatility_label:
        base = max(base, 0.018)
    elif "expansion" in volatility_label:
        base = max(base, 0.014)
    elif "compression" in volatility_label:
        base = max(base, 0.007)

    return min(max(base, 0.006), 0.028)


def _build_prices(direction, context):
    entry = safe_float(context.get("mark_price"))
    stop_distance = entry * _stop_pct(context)
    support = safe_float(context.get("support"), None)
    resistance = safe_float(context.get("resistance"), None)
    confidence = safe_float(context.get("confidence_score"))
    rr = 1.8 if confidence < 78 else 2.1

    if direction == "LONG":
        stop_loss = entry - stop_distance
        if support and support < entry:
            stop_loss = min(stop_loss, support * 0.998)
        take_profit = entry + abs(entry - stop_loss) * rr
        if resistance and resistance > entry:
            take_profit = min(take_profit, max(entry + abs(entry - stop_loss) * 1.55, resistance * 0.998))
    else:
        stop_loss = entry + stop_distance
        if resistance and resistance > entry:
            stop_loss = max(stop_loss, resistance * 1.002)
        take_profit = entry - abs(stop_loss - entry) * rr
        if support and support < entry:
            take_profit = max(take_profit, min(entry - abs(stop_loss - entry) * 1.55, support * 1.002))

    return _safe_round(entry), _safe_round(stop_loss), _safe_round(take_profit)


def decide_trade(context, wallet):
    if context.get("status") != "OK":
        return {
            "direction": "NO_TRADE",
            "reason": context.get("reason") or "No valid mark price is available.",
            "confidence_score": 0,
            "context": context,
        }

    open_positions = wallet.get("open_positions", 0)
    confidence = safe_float(context.get("confidence_score"))
    conflict = safe_float(context.get("signal_conflict_score"))
    trap_risk = context.get("trap_risk")
    volatility = context.get("volatility_regime")
    long_score, short_score, notes = _direction_score(context)
    rejection_reasons = []

    if open_positions:
        rejection_reasons.append("One active ETH futures position already exists.")
    if confidence < MIN_CONFIDENCE_TO_TRADE:
        rejection_reasons.append(f"Confidence {confidence:.0f} is below {MIN_CONFIDENCE_TO_TRADE}.")
    if conflict > MAX_CONFLICT_TO_TRADE:
        rejection_reasons.append(f"Signal conflict {conflict:.0f} is above {MAX_CONFLICT_TO_TRADE}.")
    if trap_risk == "High":
        rejection_reasons.append("Trap risk is high.")
    if "Extreme" in str(volatility or ""):
        rejection_reasons.append("Volatility is too extreme for autonomous futures risk.")

    if abs(long_score - short_score) < 12:
        rejection_reasons.append("Long and short scores are too close.")

    direction = "LONG" if long_score > short_score else "SHORT"
    entry, stop_loss, take_profit = _build_prices(direction, context)
    leverage = choose_leverage(confidence, volatility, conflict)
    risk_pct = risk_pct_for_confidence(confidence, volatility, conflict)
    max_margin_pct = margin_cap_pct(confidence, volatility, conflict)
    position = calculate_futures_position(
        wallet.get("equity_usdt"),
        wallet.get("available_balance_usdt"),
        entry,
        stop_loss,
        take_profit,
        direction,
        leverage,
        risk_pct,
        max_margin_pct,
    )
    rejection_reasons.extend(validate_position_risk(position, entry, stop_loss))

    if rejection_reasons:
        return {
            "symbol": SYMBOL,
            "direction": "NO_TRADE",
            "reason": " ".join(rejection_reasons),
            "confidence_score": confidence,
            "long_score": long_score,
            "short_score": short_score,
            "entry_price": entry,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "risk": position or {},
            "context": context,
            "notes": notes,
        }

    reason = (
        f"{direction} selected: confidence {confidence:.0f}, conflict {conflict:.0f}, "
        f"RR {position.get('rr_ratio')}, leverage {leverage}x; " + ", ".join(notes[:4])
    )
    return {
        "symbol": SYMBOL,
        "direction": direction,
        "reason": reason,
        "confidence_score": confidence,
        "long_score": long_score,
        "short_score": short_score,
        "entry_price": entry,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "trailing_stop": None,
        "risk": position,
        "context": context,
        "notes": notes,
    }


def _position_pnl(trade, mark_price):
    direction = trade.get("direction")
    entry = safe_float(trade.get("entry_price"))
    size = safe_float(trade.get("position_size_eth"))

    if direction == "LONG":
        return (mark_price - entry) * size

    if direction == "SHORT":
        return (entry - mark_price) * size

    return 0.0


def _r_multiple(trade, pnl):
    risk = max(safe_float(trade.get("risk_amount_usdt")), 0.01)
    return pnl / risk


def _exit_signal(trade, context, mark_price, pnl):
    direction = trade.get("direction")
    stop = safe_float(trade.get("trailing_stop") or trade.get("stop_loss"))
    take_profit = safe_float(trade.get("take_profit"))
    conflict = safe_float(context.get("signal_conflict_score"))
    latest_bias = context.get("directional_bias")

    if direction == "LONG":
        if mark_price <= stop:
            return "SL", f"Mark price {mark_price} hit stop/trailing stop {stop}."
        if mark_price >= take_profit:
            return "TP", f"Mark price {mark_price} hit take profit {take_profit}."
        if latest_bias in ["Bearish", "Mild Bearish"] and conflict < 55:
            return "ENGINE_EXIT", "Directional bias flipped against the long position."

    if direction == "SHORT":
        if mark_price >= stop:
            return "SL", f"Mark price {mark_price} hit stop/trailing stop {stop}."
        if mark_price <= take_profit:
            return "TP", f"Mark price {mark_price} hit take profit {take_profit}."
        if latest_bias in ["Bullish", "Mild Bullish"] and conflict < 55:
            return "ENGINE_EXIT", "Directional bias flipped against the short position."

    if context.get("trap_risk") == "High" and conflict >= 55:
        return "ENGINE_EXIT", "Trap risk rose to High while conflict stayed elevated."

    if "Elevated" in str(context.get("volatility_regime") or "") and conflict >= 70:
        return "ENGINE_EXIT", "Volatility and signal conflict deteriorated together."

    return None, None


def _stop_update(trade, mark_price, pnl):
    direction = trade.get("direction")
    entry = safe_float(trade.get("entry_price"))
    current_stop = safe_float(trade.get("trailing_stop") or trade.get("stop_loss"))
    initial_stop = safe_float(trade.get("stop_loss"))
    risk_per_eth = abs(entry - initial_stop)
    r_multiple = _r_multiple(trade, pnl)
    new_stop = current_stop
    reason = None

    if r_multiple >= BREAKEVEN_TRIGGER_R:
        if direction == "LONG":
            breakeven = entry
            if current_stop < breakeven:
                new_stop = breakeven
                reason = "Moved stop to breakeven after favorable move."
        elif direction == "SHORT":
            breakeven = entry
            if current_stop > breakeven:
                new_stop = breakeven
                reason = "Moved stop to breakeven after favorable move."

    if r_multiple >= TRAILING_TRIGGER_R:
        if direction == "LONG":
            trail = mark_price - risk_per_eth * TRAILING_LOCK_R
            if trail > new_stop:
                new_stop = trail
                reason = "Raised trailing stop to lock profit."
        elif direction == "SHORT":
            trail = mark_price + risk_per_eth * TRAILING_LOCK_R
            if trail < new_stop:
                new_stop = trail
                reason = "Lowered trailing stop to lock profit."

    return _safe_round(new_stop), reason


def update_open_positions(context, wallet):
    open_positions = get_open_futures_positions()
    updates = []

    if open_positions.empty or context.get("status") != "OK":
        return updates

    mark_price = safe_float(context.get("mark_price"))

    for _, row in open_positions.iterrows():
        trade = row.to_dict()
        pnl = _position_pnl(trade, mark_price)
        pnl_inr = usdt_to_inr(pnl)
        trade_json = trade.get("raw_snapshot_json") if isinstance(trade.get("raw_snapshot_json"), dict) else {}
        mfe = max(safe_float(trade_json.get("max_favorable_excursion")), pnl)
        mae = min(safe_float(trade_json.get("max_adverse_excursion")), pnl)
        new_stop, stop_reason = _stop_update(trade, mark_price, pnl)
        exit_code, exit_reason = _exit_signal(trade, context, mark_price, pnl)
        raw_snapshot = {
            **trade_json,
            "latest_context": context,
            "max_favorable_excursion": mfe,
            "max_adverse_excursion": mae,
        }
        payload = {
            "mark_price": mark_price,
            "unrealized_pnl_usdt": round(pnl, 4),
            "raw_snapshot_json": json_safe(raw_snapshot),
        }

        if stop_reason:
            payload["trailing_stop"] = new_stop

        if exit_code:
            payload.update(
                {
                    "status": "CLOSED",
                    "exit_price": mark_price,
                    "realized_pnl_usdt": round(pnl, 4),
                    "realized_pnl_inr": pnl_inr,
                    "unrealized_pnl_usdt": 0,
                    "exit_confidence_score": context.get("confidence_score"),
                    "exit_reason": exit_reason,
                }
            )

        updated = patch_futures_trade(trade.get("id"), payload)
        updates.append(updated)

        if stop_reason:
            record_journal_event(
                trade.get("trade_id"),
                "MODIFY",
                stop_reason,
                mark_price,
                wallet.get("equity_usdt"),
                pnl,
                "PROFIT_PROTECTION",
                "UPDATE_TRAILING_STOP",
                stop_reason,
                {"trade": trade, "context": context},
            )

        if exit_code:
            record_journal_event(
                trade.get("trade_id"),
                "CLOSE",
                exit_reason,
                mark_price,
                wallet.get("equity_usdt"),
                pnl,
                exit_code,
                "CLOSE",
                exit_reason,
                {"trade": trade, "context": context},
            )
            closed_trade = {**trade, **payload}
            update_training_outcome(closed_trade, exit_code, mfe, mae)

    return updates


def auto_trade_cycle(enabled=True, persist=True):
    context = collect_market_context()
    open_before = get_open_futures_positions()
    closed_before = get_closed_futures_trades(limit=1000)
    wallet_before = wallet_state(open_before, closed_before)
    position_updates = update_open_positions(context, wallet_before)
    open_after = get_open_futures_positions()
    closed_after = get_closed_futures_trades(limit=1000)
    wallet = wallet_state(open_after, closed_after)
    decision = decide_trade(context, wallet)
    opened_trade = None

    if enabled and decision.get("direction") != "NO_TRADE":
        opened_trade = create_futures_trade(decision, wallet)
        action = "Opened futures paper trade"
    else:
        action = "No trade: " + decision.get("reason", "setup did not pass filters")

    final_wallet = wallet_state(get_open_futures_positions(), get_closed_futures_trades(limit=1000))

    if persist:
        persist_wallet_snapshot(final_wallet)

    return {
        "wallet": final_wallet,
        "context": context,
        "decision": decision,
        "opened_trade": opened_trade,
        "position_updates": position_updates,
        "open_positions": get_open_futures_positions(),
        "closed_trades": get_closed_futures_trades(),
        "journal": get_futures_journal(),
        "training_dataset": get_futures_training_dataset(),
        "engine_status": get_latest_futures_engine_run(),
        "action": action,
    }


def futures_dashboard_data(run_cycle=True):
    if run_cycle:
        return auto_trade_cycle(enabled=True, persist=True)

    open_positions = get_open_futures_positions()
    closed_trades = get_closed_futures_trades()
    wallet = wallet_state(open_positions, closed_trades)
    context = collect_market_context()
    decision = decide_trade(context, wallet)
    return {
        "wallet": wallet,
        "context": context,
        "decision": decision,
        "opened_trade": None,
        "position_updates": [],
        "open_positions": open_positions,
        "closed_trades": closed_trades,
        "journal": get_futures_journal(),
        "training_dataset": get_futures_training_dataset(),
        "engine_status": get_latest_futures_engine_run(),
        "action": "Previewed futures engine decision.",
    }
