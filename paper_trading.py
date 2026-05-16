from datetime import datetime, timezone

import pandas as pd

from recommendation_journal import _request, read_table, save_recommendation_snapshot
from rule_insights import build_rule_based_insights, get_available_expiries, price_strategy_legs
from validation_config import (
    ETH_LOT_SIZE,
    INR_PER_USDT,
    MAX_MARGIN_USAGE_PCT,
    MAX_RISK_PER_TRADE_PCT,
    PAPER_WALLET_CAPITAL_INR,
    PAPER_WALLET_CAPITAL_USDT,
    usdt_to_inr,
)


MIN_FREE_MARGIN_PCT = 0.55
MAX_SINGLE_TRADE_MARGIN_PCT = 0.30
MIN_SELECTION_SCORE = 65
TARGET_PROFIT_PCT = 0.55
STOP_LOSS_PCT = 0.60


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value, default=0):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _json_safe(value):
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _strategy_side(strategy, directional_bias):
    if strategy in ["Bull Put Credit Spread", "Bull Call Debit Spread"]:
        return "BULLISH"
    if strategy in ["Bear Call Credit Spread", "Bear Put Debit Spread", "Put Broken Wing Butterfly"]:
        return "BEARISH"
    if strategy in ["Debit Spread", "Directional Debit Spread"]:
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


def _pricing_from_recommendation(recommendation):
    rec_json = recommendation.get("recommendation_json") or {}
    return rec_json.get("pricing") or {}, rec_json.get("legs") or []


def _greeks_from_legs(legs, lots=1):
    totals = {"delta": 0, "gamma": 0, "theta": 0, "vega": 0}
    signed_legs = []

    for leg in legs or []:
        action = str(leg.get("action", "")).lower()
        sign = -1 if action.startswith("sell") else 1
        multiplier = sign * lots * ETH_LOT_SIZE
        leg_greeks = {}

        for greek in totals:
            value = _safe_float(leg.get(greek), 0) * multiplier
            totals[greek] += value
            leg_greeks[greek] = round(value, 6)

        signed = dict(leg)
        signed["signed_greeks"] = leg_greeks
        signed_legs.append(signed)

    return {key: round(value, 6) for key, value in totals.items()}, signed_legs


def classify_greek_health(greeks, equity_usdt=None):
    equity_usdt = equity_usdt or PAPER_WALLET_CAPITAL_USDT
    delta_limit = max(0.12, equity_usdt / 3500)
    gamma_limit = 0.006
    vega_limit = max(0.20, equity_usdt / 2500)

    delta = abs(_safe_float(greeks.get("delta")))
    gamma = abs(_safe_float(greeks.get("gamma")))
    vega = abs(_safe_float(greeks.get("vega")))

    if delta > delta_limit * 1.5 or gamma > gamma_limit * 1.5 or vega > vega_limit * 1.5:
        return "Dangerous"

    if delta > delta_limit or gamma > gamma_limit or vega > vega_limit:
        return "Caution"

    return "Healthy"


def estimate_trade_risk(recommendation):
    rec_json = recommendation.get("recommendation_json") or {}
    strategy = recommendation.get("suggested_strategy") or rec_json.get("strategy")
    pricing, legs = _pricing_from_recommendation(recommendation)
    risk_reward = rec_json.get("risk_reward") or {}
    net_credit = _safe_float(pricing.get("net_credit_usdt"))
    net_debit = _safe_float(pricing.get("net_debit_usdt"))
    widths = _leg_widths(legs)
    max_width = max(widths) if widths else 0

    if strategy in ["No Trade", "Wait / Defined-Risk Spread Only"] or not legs:
        return None

    max_loss_from_engine = _safe_float(risk_reward.get("max_loss_usdt"))

    if strategy in ["Bull Put Credit Spread", "Bear Call Credit Spread", "Iron Condor", "Iron Fly"]:
        risk_per_eth = max_loss_from_engine or max(max_width - net_credit, 0)
        margin_per_lot = max(max_width * ETH_LOT_SIZE, risk_per_eth * ETH_LOT_SIZE)
        premium_per_lot = net_credit * ETH_LOT_SIZE
    elif strategy in [
        "Debit Spread",
        "Directional Debit Spread",
        "Bull Call Debit Spread",
        "Bear Put Debit Spread",
        "Put Broken Wing Butterfly",
    ]:
        risk_per_eth = max_loss_from_engine or net_debit or abs(_safe_float(pricing.get("net_premium_usdt")))
        margin_per_lot = risk_per_eth * ETH_LOT_SIZE
        premium_per_lot = -risk_per_eth * ETH_LOT_SIZE
    else:
        spot = _safe_float(recommendation.get("spot_price"))
        risk_per_eth = max_loss_from_engine or max(spot * 0.04, abs(_safe_float(pricing.get("net_premium_usdt"))))
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
        "lots_by_risk": lots_by_risk,
        "lots_by_margin": lots_by_margin,
        "risk_per_lot_usdt": round(risk_per_lot, 4),
        "max_risk_usdt": round(risk_per_lot * lots, 4),
        "max_risk_inr": usdt_to_inr(risk_per_lot * lots),
        "margin_used_usdt": round(margin_per_lot * lots, 4),
        "margin_used_inr": usdt_to_inr(margin_per_lot * lots),
        "entry_premium_usdt": round(premium_per_lot * lots, 4),
        "risk_per_eth_usdt": round(risk_per_eth, 4),
    }


def _trade_json(trade):
    value = trade.get("trade_json")
    return value if isinstance(value, dict) else {}


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


def running_book_greeks(open_trades=None):
    open_trades = open_trades if open_trades is not None else get_open_paper_trades(limit=200)
    totals = {"delta": 0, "gamma": 0, "theta": 0, "vega": 0}

    if open_trades.empty:
        return totals

    for _, trade in open_trades.iterrows():
        trade_json = _trade_json(trade.to_dict())
        greeks = trade_json.get("current_greeks") or trade_json.get("entry_greeks") or {}

        for greek in totals:
            totals[greek] += _safe_float(greeks.get(greek))

    return {key: round(value, 6) for key, value in totals.items()}


def wallet_state(open_trades=None, closed_trades=None):
    open_trades = open_trades if open_trades is not None else get_open_paper_trades(limit=200)
    closed_trades = closed_trades if closed_trades is not None else get_closed_paper_trades(limit=500)

    used_margin = 0 if open_trades.empty else float(pd.to_numeric(open_trades["margin_used_usdt"], errors="coerce").fillna(0).sum())
    unrealized = 0 if open_trades.empty else float(pd.to_numeric(open_trades["unrealized_pnl_usdt"], errors="coerce").fillna(0).sum())
    realized = 0 if closed_trades.empty else float(pd.to_numeric(closed_trades["realized_pnl_usdt"], errors="coerce").fillna(0).sum())
    equity = PAPER_WALLET_CAPITAL_USDT + realized + unrealized
    available_margin = max(equity - used_margin, 0)
    margin_health = (available_margin / equity * 100) if equity else 0
    greeks = running_book_greeks(open_trades)

    return {
        "starting_capital_inr": PAPER_WALLET_CAPITAL_INR,
        "starting_capital_usdt": round(PAPER_WALLET_CAPITAL_USDT, 4),
        "current_equity_usdt": round(equity, 4),
        "current_equity_inr": usdt_to_inr(equity),
        "available_margin_usdt": round(available_margin, 4),
        "available_margin_inr": usdt_to_inr(available_margin),
        "used_margin_usdt": round(used_margin, 4),
        "used_margin_inr": usdt_to_inr(used_margin),
        "realized_pnl_usdt": round(realized, 4),
        "realized_pnl_inr": usdt_to_inr(realized),
        "unrealized_pnl_usdt": round(unrealized, 4),
        "unrealized_pnl_inr": usdt_to_inr(unrealized),
        "margin_health_pct": round(margin_health, 2),
        "book_greeks": greeks,
        "greek_health": classify_greek_health(greeks, equity),
    }


def _strategy_current_value(legs, lots):
    value = 0

    for leg in legs or []:
        price = _safe_float(leg.get("mark_price"))
        action = str(leg.get("action", "")).lower()
        signed = price if action.startswith("buy") else -price
        value += signed * lots * ETH_LOT_SIZE

    return round(value, 4)


def estimate_trade_mtm(trade, insights):
    trade_json = _trade_json(trade)
    entry_legs = trade_json.get("recommendation", {}).get("recommendation_json", {}).get("legs") or []
    current_pricing = price_strategy_legs(trade.get("expiry_label"), entry_legs) if entry_legs else {}
    current_legs = current_pricing.get("legs") or entry_legs
    lots = int(trade.get("lots") or 0)
    entry_value = _safe_float(trade.get("entry_premium_usdt"))
    current_value = _strategy_current_value(current_legs, lots)
    strategy = trade.get("strategy")

    if strategy in ["Bull Put Credit Spread", "Bear Call Credit Spread", "Iron Condor", "Iron Fly"]:
        pnl = entry_value + current_value
    else:
        pnl = current_value - abs(entry_value)

    max_risk = _safe_float(trade.get("max_risk_usdt"))
    pnl = max(-max_risk, min(max_risk * 2, pnl))
    greeks, signed_legs = _greeks_from_legs(current_legs, lots)

    return {
        "current_value_usdt": round(current_value, 4),
        "unrealized_pnl_usdt": round(pnl, 4),
        "unrealized_pnl_inr": usdt_to_inr(pnl),
        "current_greeks": greeks,
        "current_legs": signed_legs,
    }


def _exit_signal(trade, insights, mtm, wallet):
    max_risk = _safe_float(trade.get("max_risk_usdt"))
    entry_credit_or_debit = abs(_safe_float(trade.get("entry_premium_usdt")))
    pnl = _safe_float(mtm.get("unrealized_pnl_usdt"))
    expiry_time = pd.to_datetime(trade.get("expiry_label"), utc=True, errors="coerce")
    recommendation_side = _strategy_side(insights.get("best_strategy"), insights.get("directional_bias"))
    greek_health = classify_greek_health(
        wallet.get("book_greeks", {}),
        wallet.get("current_equity_usdt"),
    )

    if not pd.isna(expiry_time) and pd.Timestamp.utcnow() >= expiry_time:
        return {
            "code": "EXPIRY",
            "label": "Expiry reached",
            "detail": f"Trade expired at {trade.get('expiry_label')}.",
        }

    if entry_credit_or_debit and pnl >= entry_credit_or_debit * TARGET_PROFIT_PCT:
        target = entry_credit_or_debit * TARGET_PROFIT_PCT
        return {
            "code": "TP",
            "label": "Target profit hit",
            "detail": f"Unrealized P&L {round(pnl, 4)} USDT reached target {round(target, 4)} USDT.",
        }

    if max_risk and pnl <= -max_risk * STOP_LOSS_PCT:
        stop = -max_risk * STOP_LOSS_PCT
        return {
            "code": "SL",
            "label": "Max loss hit",
            "detail": f"Unrealized P&L {round(pnl, 4)} USDT breached stop {round(stop, 4)} USDT.",
        }

    if wallet.get("margin_health_pct", 100) < 45:
        return {
            "code": "ENGINE_EXIT",
            "label": "Margin health deteriorated",
            "detail": f"Margin health fell to {wallet.get('margin_health_pct')}%, below the 45% safety floor.",
        }

    if greek_health == "Dangerous":
        return {
            "code": "ENGINE_EXIT",
            "label": "Greeks became dangerous",
            "detail": f"Book Greek health became Dangerous with Greeks {wallet.get('book_greeks', {})}.",
        }

    if recommendation_side not in ["NEUTRAL", trade.get("side")] and trade.get("side") != "RANGE":
        return {
            "code": "ENGINE_EXIT",
            "label": "Recommendation flipped",
            "detail": f"Latest recommendation side is {recommendation_side}, against open trade side {trade.get('side')}.",
        }

    if not pd.isna(expiry_time):
        hours_to_expiry = max(0, (expiry_time - pd.Timestamp.utcnow()).total_seconds() / 3600)
        if hours_to_expiry <= 2:
            return {
                "code": "ENGINE_EXIT",
                "label": "Near-expiry time exit",
                "detail": f"Only {round(hours_to_expiry, 2)} hours remain to expiry.",
            }

    return None


def update_open_paper_trades(current_spot=None, auto_exit=False):
    open_trades = get_open_paper_trades(limit=200)
    updated = []

    if open_trades.empty:
        return updated

    for _, trade_row in open_trades.iterrows():
        trade = trade_row.to_dict()

        try:
            insights = build_rule_based_insights(trade.get("expiry_label"))
            spot = current_spot or insights.get("spot_price")
            mtm = estimate_trade_mtm(trade, insights)
            wallet = wallet_state(open_trades=open_trades)
            exit_signal = _exit_signal(trade, insights, mtm, wallet)
            trade_json = _trade_json(trade)
            trade_json.update(
                {
                    "current_greeks": mtm.get("current_greeks"),
                    "current_legs": mtm.get("current_legs"),
                    "latest_insights": insights,
                }
            )
            payload = {
                "updated_at": _now_iso(),
                "current_spot": _safe_float(spot),
                "unrealized_pnl_usdt": mtm["unrealized_pnl_usdt"],
                "unrealized_pnl_inr": mtm["unrealized_pnl_inr"],
                "trade_json": _json_safe(trade_json),
            }

            if exit_signal and (auto_exit or exit_signal["code"] == "EXPIRY"):
                exit_wallet = wallet_state(open_trades=open_trades)
                trade_json["exit_signal"] = exit_signal
                trade_json["exit_reason_detail"] = exit_signal.get("detail")
                payload.update(
                    {
                        "status": "EXPIRED" if exit_signal["code"] == "EXPIRY" else "CLOSED",
                        "closed_at": _now_iso(),
                        "exit_reason": exit_signal["code"],
                        "exit_reason_label": exit_signal.get("label"),
                        "exit_reason_detail": exit_signal.get("detail"),
                        "exit_signal": _json_safe(exit_signal),
                        "exit_spot": _safe_float(spot),
                        "exit_premium_usdt": mtm.get("current_value_usdt"),
                        "realized_pnl_usdt": mtm["unrealized_pnl_usdt"],
                        "realized_pnl_inr": mtm["unrealized_pnl_inr"],
                        "wallet_after": _json_safe(exit_wallet),
                        "exit_greeks": _json_safe(mtm.get("current_greeks") or {}),
                        "current_greeks": _json_safe(mtm.get("current_greeks") or {}),
                        "trade_json": _json_safe(trade_json),
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
        except Exception as e:
            print("Paper trade update failed:", e)

    return updated


def _liquidity_score(legs):
    if not legs:
        return 0

    scores = []

    for leg in legs:
        oi = _safe_float(leg.get("oi"))
        mark = _safe_float(leg.get("mark_price"))
        score = min(20, oi / 10) + min(10, mark * 2)
        scores.append(score)

    return round(sum(scores) / max(len(scores), 1), 2)


def _candidate_score(insights, risk, wallet, open_trades):
    strategy = insights.get("best_strategy")
    pricing = insights.get("strategy_pricing") or {}
    risk_reward = insights.get("strategy_risk_reward") or {}
    legs = pricing.get("legs") or insights.get("strategy_legs") or []
    confidence = _safe_float(insights.get("confidence_score"))
    conflict = _safe_float(insights.get("signal_conflict_score"))
    rr = _safe_float(risk_reward.get("reward_risk"))
    effective_return = _safe_float(risk_reward.get("effective_return_pct"))
    margin_pct = risk["margin_used_usdt"] / max(wallet["current_equity_usdt"], 1)
    free_after = (wallet["available_margin_usdt"] - risk["margin_used_usdt"]) / max(wallet["current_equity_usdt"], 1)
    liquidity = _liquidity_score(legs)
    expiry_bucket = (insights.get("expiry_profile") or {}).get("bucket")
    side = _strategy_side(strategy, insights.get("directional_bias"))
    existing_same_side = 0

    if open_trades is not None and not open_trades.empty and "side" in open_trades:
        existing_same_side = int((open_trades["side"] == side).sum())

    score = 0
    score += min(30, confidence * 0.30)
    score += min(25, rr * 12)
    score += min(10, effective_return / 15)
    score += min(10, liquidity / 3)
    score -= min(25, conflict * 0.25)
    score -= max(0, margin_pct - 0.15) * 100
    score -= existing_same_side * 8

    if expiry_bucket in ["D1", "D3", "WEEKLY"]:
        score += 6
    elif expiry_bucket in ["0DTE", "MONTHLY"]:
        score -= 4

    if risk_reward.get("quality") == "Good":
        score += 10
    elif risk_reward.get("quality") != "Acceptable":
        score -= 25

    reasons = []

    if confidence < 60:
        reasons.append("Low confidence")
    if conflict >= 65:
        reasons.append("High signal conflict")
    if rr < 0.25 and strategy in ["Bull Put Credit Spread", "Bear Call Credit Spread", "Iron Condor", "Iron Fly"]:
        reasons.append("Poor RR")
    if rr < 1 and strategy in ["Bull Call Debit Spread", "Bear Put Debit Spread"]:
        reasons.append("Poor RR")
    if margin_pct > MAX_SINGLE_TRADE_MARGIN_PCT:
        reasons.append("Margin too high")
    if free_after < MIN_FREE_MARGIN_PCT:
        reasons.append("Free margin would be too low")
    if liquidity < 5:
        reasons.append("Liquidity issue")
    if existing_same_side and confidence < 75:
        reasons.append("Overlapping same-direction risk")

    return round(score, 2), reasons


def evaluate_paper_trade_candidates(limit_expiries=6, persist=True, update_positions=True, auto_exit=False):
    position_updates = []

    if update_positions:
        position_updates = update_open_paper_trades(auto_exit=auto_exit)

    open_trades = get_open_paper_trades(limit=200)
    closed_trades = get_closed_paper_trades(limit=500)
    wallet = wallet_state(open_trades, closed_trades)
    expiries = get_available_expiries(limit=500)[:limit_expiries]
    candidates = []

    for expiry in expiries:
        try:
            insights = build_rule_based_insights(expiry)
            recommendation = save_recommendation_snapshot(insights)
            risk = estimate_trade_risk(recommendation)
            rejected = []

            if not risk:
                rejected.append("No executable risk model")
                score = 0
            else:
                score, rejected = _candidate_score(insights, risk, wallet, open_trades)

            candidates.append(
                {
                    "expiry_label": expiry,
                    "strategy": insights.get("best_strategy"),
                    "confidence_score": insights.get("confidence_score"),
                    "selection_score": score,
                    "reward_risk": (insights.get("strategy_risk_reward") or {}).get("reward_risk"),
                    "effective_return_pct": (insights.get("strategy_risk_reward") or {}).get("effective_return_pct"),
                    "margin_used_usdt": risk.get("margin_used_usdt") if risk else None,
                    "max_risk_usdt": risk.get("max_risk_usdt") if risk else None,
                    "status": "Rejected" if rejected or score < MIN_SELECTION_SCORE else "Candidate",
                    "rejection_reasons": rejected or ([] if score >= MIN_SELECTION_SCORE else ["Low selection score"]),
                    "entry_reason": _entry_reason(insights, score),
                    "insights": insights,
                    "recommendation": recommendation,
                    "risk": risk,
                }
            )
        except Exception as e:
            candidates.append(
                {
                    "expiry_label": expiry,
                    "strategy": None,
                    "selection_score": 0,
                    "status": "Rejected",
                    "rejection_reasons": [f"Evaluation error: {e}"],
                    "insights": {},
                    "recommendation": None,
                    "risk": None,
                }
            )

    candidates = sorted(candidates, key=lambda item: item.get("selection_score", 0), reverse=True)
    selected = next(
        (item for item in candidates if item.get("status") == "Candidate" and item.get("risk")),
        None,
    )
    if persist:
        _record_evaluation_cycle(candidates, selected, wallet)
        _record_wallet_snapshot(wallet)

    return {
        "wallet": wallet,
        "open_trades": open_trades,
        "closed_trades": closed_trades,
        "candidates": candidates,
        "selected": selected,
        "position_updates": position_updates,
        "last_evaluation_time": _now_iso(),
    }


def _record_evaluation_cycle(candidates, selected, wallet):
    selected_key = (
        selected.get("expiry_label"),
        selected.get("strategy"),
    ) if selected else None

    for candidate in candidates:
        recommendation = candidate.get("recommendation") or {}
        candidate_key = (candidate.get("expiry_label"), candidate.get("strategy"))
        payload = {
            "created_at": _now_iso(),
            "expiry_label": candidate.get("expiry_label"),
            "strategy": candidate.get("strategy"),
            "recommendation_id": recommendation.get("id"),
            "selected": bool(selected_key and candidate_key == selected_key),
            "selection_score": candidate.get("selection_score"),
            "rejection_reasons": _json_safe(candidate.get("rejection_reasons") or []),
            "wallet_state": _json_safe(wallet),
            "risk_json": _json_safe(candidate.get("risk") or {}),
            "insight_json": _json_safe(candidate.get("insights") or {}),
            "candidate_json": _json_safe(
                {key: value for key, value in candidate.items() if key not in ["insights", "recommendation", "risk"]}
            ),
        }
        _request("POST", "paper_recommendation_evaluations", payload=payload, prefer="return=minimal")


def _record_wallet_snapshot(wallet):
    payload = {
        "created_at": _now_iso(),
        "starting_capital_inr": wallet.get("starting_capital_inr"),
        "starting_capital_usdt": wallet.get("starting_capital_usdt"),
        "available_margin_usdt": wallet.get("available_margin_usdt"),
        "used_margin_usdt": wallet.get("used_margin_usdt"),
        "realized_pnl_usdt": wallet.get("realized_pnl_usdt"),
        "unrealized_pnl_usdt": wallet.get("unrealized_pnl_usdt"),
        "current_equity_usdt": wallet.get("current_equity_usdt"),
        "margin_health_pct": wallet.get("margin_health_pct"),
        "book_greeks": _json_safe(wallet.get("book_greeks") or {}),
        "snapshot_json": _json_safe(wallet),
    }
    _request("POST", "paper_wallet_snapshots", payload=payload, prefer="return=minimal")


def _entry_reason(insights, score):
    risk_reward = insights.get("strategy_risk_reward") or {}
    return (
        f"Score {score}; confidence {insights.get('confidence_score')}; "
        f"RR {risk_reward.get('reward_risk')}; regime {insights.get('market_regime')}."
    )


def create_paper_trade(recommendation, risk=None, selection=None, wallet_before=None):
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

    risk = risk or estimate_trade_risk(recommendation)

    if not risk:
        return None

    rec_json = recommendation.get("recommendation_json") or {}
    legs = rec_json.get("legs") or []
    strategy = recommendation.get("suggested_strategy")
    side = _strategy_side(strategy, recommendation.get("directional_bias"))
    spot = _safe_float(recommendation.get("spot_price"))
    wallet_before = wallet_before or wallet_state()
    greeks, signed_legs = _greeks_from_legs(legs, risk["lots"])
    entry_reason = (selection or {}).get("entry_reason") or _entry_reason(
        {
            "confidence_score": recommendation.get("confidence_score"),
            "market_regime": recommendation.get("market_regime"),
            "strategy_risk_reward": rec_json.get("risk_reward") or {},
        },
        (selection or {}).get("selection_score") or 0,
    )
    selection_score = (selection or {}).get("selection_score")

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
        "selection_score": selection_score,
        "entry_reason": entry_reason,
        "wallet_before": _json_safe(wallet_before),
        "entry_greeks": _json_safe(greeks),
        "current_greeks": _json_safe(greeks),
        "trade_json": _json_safe(
            {
                "recommendation": recommendation,
                "risk": risk,
                "selection": selection or {},
                "entry_reason": entry_reason,
                "selection_score": selection_score,
                "wallet_before": wallet_before,
                "entry_greeks": greeks,
                "entry_legs": signed_legs,
                "market_snapshot": recommendation.get("raw_input_snapshot"),
            }
        ),
    }

    result = _request("POST", "paper_trades", payload=payload, prefer="return=representation")

    if isinstance(result, list) and result:
        return result[0]

    return payload


def auto_trade_cycle(enabled=True, limit_expiries=6, persist=True):
    open_before = get_open_paper_trades(limit=200)
    had_open_before = not open_before.empty
    evaluation = evaluate_paper_trade_candidates(
        limit_expiries=limit_expiries,
        persist=persist,
        update_positions=True,
        auto_exit=enabled,
    )

    if not enabled:
        evaluation["action"] = (
            "Manual refresh: positions marked to market and candidates evaluated. "
            "Auto trading is disabled, so no new trades were opened."
        )
        return evaluation

    if evaluation["wallet"].get("margin_health_pct", 100) < MIN_FREE_MARGIN_PCT * 100:
        evaluation["action"] = "No trade: margin health below required buffer"
        return evaluation

    open_trades = evaluation.get("open_trades")

    if had_open_before:
        if open_trades is None or open_trades.empty:
            evaluation["action"] = "Position updated/closed; no replacement trade opened in the same refresh cycle"
        else:
            evaluation["action"] = "Monitoring existing open position(s); no new trade opened while the book has active risk"
        evaluation["selected"] = None
        return evaluation

    selected = evaluation.get("selected")

    if not selected:
        evaluation["action"] = "No trade: no candidate passed safety rules"
        return evaluation

    if open_trades is not None and not open_trades.empty:
        same_expiry = open_trades[open_trades["expiry_label"] == selected["expiry_label"]]
        if not same_expiry.empty:
            evaluation["action"] = "No trade: selected expiry already has an open paper position"
            return evaluation

    trade = create_paper_trade(
        selected["recommendation"],
        risk=selected["risk"],
        selection=selected,
        wallet_before=evaluation["wallet"],
    )
    evaluation["opened_trade"] = trade
    evaluation["action"] = "Opened paper trade" if trade else "No trade: open failed"
    return evaluation


def manual_close_trade(trade_id, reason="MANUAL"):
    trades = read_table("paper_trades", {"select": "*", "id": f"eq.{trade_id}", "limit": 1})

    if trades.empty:
        return None

    trade = trades.iloc[0].to_dict()
    pnl = _safe_float(trade.get("unrealized_pnl_usdt"))
    trade_json = _trade_json(trade)
    exit_greeks = trade_json.get("current_greeks") or trade.get("current_greeks") or {}
    wallet_after = wallet_state()
    trade_json["exit_signal"] = {
        "code": reason,
        "label": "Manual close" if reason == "MANUAL" else reason,
        "detail": "Position was closed manually from the Paper Trading page.",
    }
    trade_json["exit_reason_detail"] = trade_json["exit_signal"]["detail"]
    payload = {
        "status": "CLOSED",
        "closed_at": _now_iso(),
        "updated_at": _now_iso(),
        "exit_reason": reason,
        "exit_reason_label": "Manual close" if reason == "MANUAL" else reason,
        "exit_reason_detail": trade_json["exit_signal"]["detail"],
        "exit_signal": _json_safe(trade_json["exit_signal"]),
        "realized_pnl_usdt": pnl,
        "realized_pnl_inr": usdt_to_inr(pnl),
        "exit_spot": _safe_float(trade.get("current_spot")),
        "wallet_after": _json_safe(wallet_after),
        "exit_greeks": _json_safe(exit_greeks),
        "trade_json": _json_safe(trade_json),
    }
    result = _request(
        "PATCH",
        "paper_trades",
        payload=payload,
        params={"id": f"eq.{trade_id}"},
        prefer="return=representation",
    )

    if isinstance(result, list) and result:
        return result[0]

    return payload


def paper_trading_dashboard_data(auto_enabled=False, run_evaluation=False, limit_expiries=6):
    if auto_enabled or run_evaluation:
        evaluation = auto_trade_cycle(
            enabled=auto_enabled,
            limit_expiries=limit_expiries,
            persist=True,
        )
    else:
        open_trades = get_open_paper_trades(limit=100)
        closed_trades = get_closed_paper_trades(limit=200)
        evaluation = {
            "wallet": wallet_state(open_trades, closed_trades),
            "open_trades": open_trades,
            "closed_trades": closed_trades,
            "candidates": [],
            "selected": None,
            "last_evaluation_time": None,
            "action": "Idle: click Refresh Paper Trading to evaluate candidates.",
        }

    open_trades = get_open_paper_trades(limit=100)
    closed_trades = get_closed_paper_trades(limit=200)
    wallet = wallet_state(open_trades, closed_trades)

    return {
        **evaluation,
        "wallet": wallet,
        "open_trades": open_trades,
        "closed_trades": closed_trades,
    }
