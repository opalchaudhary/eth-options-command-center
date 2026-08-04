import hashlib
import json
from datetime import datetime, timezone

import pandas as pd

from engine_persistence import compact_json_payload, should_persist_engine_snapshot
from recommendation_journal import _request, read_table
from strategy_decision_config import DEFAULT_STRATEGY_CONFIG


def _decision_hash(result):
    payload = {
        "engine_name": result.get("engine_name"),
        "symbol": result.get("symbol"),
        "futures": (result.get("futures") or {}).get("recommendation"),
        "covered_call": (result.get("covered_call") or {}).get("status"),
        "covered_put": (result.get("covered_put") or {}).get("status"),
        "iron_fly": result.get("recommendation"),
        "selected_expiry": ((result.get("selected") or {}).get("expiry") if isinstance(result.get("selected"), dict) else None),
        "selected_strategy": _selected_strategy(result),
    }
    raw = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _selected_strategy(result):
    if result.get("engine_name") == "futures_covered_research":
        return (result.get("futures") or {}).get("recommendation")
    return result.get("recommendation")


def _selected_expiry(result):
    selected = result.get("selected") or {}
    if isinstance(selected, dict):
        return selected.get("expiry")
    return None


def _selected_strikes(result):
    selected = result.get("selected") or {}
    if not isinstance(selected, dict):
        return []
    return [
        leg.get("strike")
        for leg in selected.get("legs") or []
        if isinstance(leg, dict) and leg.get("strike") is not None
    ]


def _payload(result):
    selected_strategy = _selected_strategy(result)
    selected_expiry = _selected_expiry(result)
    selected_strikes = _selected_strikes(result)
    decision_hash = _decision_hash(result)
    return {
        "recommendation_key": decision_hash,
        "created_at": result.get("generated_at") or datetime.now(timezone.utc).isoformat(),
        "symbol": result.get("symbol", "ETHUSD"),
        "engine_name": result.get("engine_name"),
        "run_status": "ok" if result.get("ok") else "error",
        "action": "research_recommendation",
        "spot_price": ((result.get("futures") or {}).get("suggested_entry_zone") if result.get("engine_name") == "futures_covered_research" else None),
        "expiry_label": selected_expiry,
        "market_regime": (result.get("futures") or {}).get("market_regime"),
        "directional_bias": selected_strategy,
        "suggested_strategy": selected_strategy,
        "confidence_score": (result.get("futures") or {}).get("overall_score") or result.get("iron_fly_score") or 0,
        "signal_conflict_score": len(result.get("risk_factors") or []),
        "warnings": result.get("risk_factors") or result.get("unavailable_inputs") or [],
        "reasoning_text": json.dumps(compact_json_payload(result, max_depth=2, max_list_items=8), default=str),
        "raw_input_snapshot": {},
        "recommendation_json": compact_json_payload(result, max_depth=4, max_list_items=30),
        "selected_strategy": selected_strategy,
        "selected_expiry": selected_expiry,
        "selected_strikes": selected_strikes,
        "risk_score": len(result.get("risk_factors") or []),
        "decision_reason": selected_strategy,
        "decision_hash": decision_hash,
        "payload_truncated": False,
    }


def maybe_save_recommendation(result, persist=True):
    if not persist:
        return {"persisted": False, "reason": "Persistence disabled."}

    payload = _payload(result)
    should_save = should_persist_engine_snapshot(
        payload["engine_name"],
        payload["symbol"],
        payload["decision_hash"],
        payload["action"],
        payload["selected_strategy"],
        payload["selected_expiry"],
        payload["selected_strikes"],
        min_interval_seconds=DEFAULT_STRATEGY_CONFIG.persistence.min_interval_seconds,
    )
    if not should_save:
        return {"persisted": False, "reason": "Decision unchanged and heartbeat interval not due."}

    saved = _request(
        "POST",
        "recommendation_journal",
        payload=payload,
        params={"on_conflict": "recommendation_key"},
        prefer="resolution=merge-duplicates,return=representation",
    )
    return {"persisted": bool(saved is not None), "record": saved[0] if isinstance(saved, list) and saved else None}


def latest_strategy_history(engine_name=None, limit=None):
    limit = limit or DEFAULT_STRATEGY_CONFIG.persistence.history_limit
    params = {
        "select": "id,created_at,symbol,engine_name,selected_strategy,selected_expiry,selected_strikes,confidence_score,risk_score,decision_hash,recommendation_json",
        "order": "created_at.desc",
        "limit": limit,
    }
    if engine_name:
        params["engine_name"] = f"eq.{engine_name}"
    df = read_table("recommendation_journal", params)
    if df.empty:
        return []
    return df.where(pd.notna(df), None).to_dict(orient="records")
