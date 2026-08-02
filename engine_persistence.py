import hashlib
import json
import logging
import time
from datetime import datetime, timezone


logger = logging.getLogger(__name__)

MAX_ENGINE_PAYLOAD_BYTES = 250 * 1024
_LAST_ENGINE_SNAPSHOTS = {}


def estimate_payload_size(payload):
    try:
        return len(json.dumps(payload or {}, default=str, separators=(",", ":")).encode("utf-8"))
    except Exception:
        return 0


def compact_json_payload(payload, max_depth=2, max_list_items=20, max_string_length=1000):
    def compact(value, depth):
        if depth > max_depth:
            if isinstance(value, dict):
                return {"_truncated": True, "_type": "dict", "_keys": list(value.keys())[:max_list_items]}
            if isinstance(value, list):
                return {"_truncated": True, "_type": "list", "_count": len(value)}
            return str(value)[:max_string_length]

        if isinstance(value, dict):
            items = list(value.items())[:max_list_items]
            result = {str(key): compact(item, depth + 1) for key, item in items}
            if len(value) > max_list_items:
                result["_truncated_keys"] = len(value) - max_list_items
            return result

        if isinstance(value, list):
            result = [compact(item, depth + 1) for item in value[:max_list_items]]
            if len(value) > max_list_items:
                result.append({"_truncated_items": len(value) - max_list_items})
            return result

        if isinstance(value, tuple):
            return compact(list(value), depth)

        if hasattr(value, "isoformat"):
            return value.isoformat()

        if isinstance(value, str) and len(value) > max_string_length:
            return value[:max_string_length] + "...[truncated]"

        return value

    return compact(payload, 0)


def summarize_engine_payload(payload):
    payload = payload or {}
    selected = payload.get("selected") or payload.get("decision") or {}
    wallet = payload.get("wallet") or {}
    candidates = payload.get("candidates") or []
    return {
        "action": payload.get("action"),
        "selected": compact_json_payload(selected, max_depth=2, max_list_items=12),
        "top_candidates": top_candidates_summary(candidates),
        "position_update_count": len(payload.get("position_updates") or []),
        "wallet": {
            "available_margin_usdt": wallet.get("available_margin_usdt") or wallet.get("available_balance_usdt"),
            "used_margin_usdt": wallet.get("used_margin_usdt"),
            "realized_pnl_usdt": wallet.get("realized_pnl_usdt"),
            "unrealized_pnl_usdt": wallet.get("unrealized_pnl_usdt"),
            "equity_usdt": wallet.get("current_equity_usdt") or wallet.get("equity_usdt"),
        },
    }


def safe_engine_payload(payload, table_name="engine_log"):
    payload = payload or {}
    compacted = compact_json_payload(payload)
    truncated = estimate_payload_size(payload) > MAX_ENGINE_PAYLOAD_BYTES
    if truncated:
        compacted = summarize_engine_payload(payload)
        logger.warning(
            "Skipped raw engine payload for %s because it exceeded %s bytes",
            table_name,
            MAX_ENGINE_PAYLOAD_BYTES,
        )
    return compacted, truncated


def _bucket(value, step=10):
    try:
        return int(round(float(value) / step) * step)
    except Exception:
        return None


def normalize_strikes(selected_strikes):
    if selected_strikes is None:
        return []
    if isinstance(selected_strikes, dict):
        values = selected_strikes.values()
    elif isinstance(selected_strikes, (list, tuple, set)):
        values = selected_strikes
    else:
        values = [selected_strikes]
    normalized = []
    for value in values:
        try:
            normalized.append(round(float(value), 2))
        except Exception:
            if value not in [None, ""]:
                normalized.append(str(value))
    return sorted(normalized, key=str)


def build_decision_hash(
    engine_name,
    symbol,
    action,
    selected_strategy=None,
    selected_expiry=None,
    selected_strikes=None,
    market_regime=None,
    confidence_score=None,
    risk_score=None,
):
    parts = {
        "symbol": symbol,
        "engine_name": engine_name,
        "action": action,
        "selected_strategy": selected_strategy,
        "selected_expiry": selected_expiry,
        "selected_strikes": normalize_strikes(selected_strikes),
        "market_regime": market_regime,
        "confidence_bucket": _bucket(confidence_score, 10),
        "risk_bucket": _bucket(risk_score, 10),
    }
    raw = json.dumps(parts, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _snapshot_key(engine_name, symbol):
    return f"{engine_name}:{symbol or 'UNKNOWN'}"


def should_persist_engine_snapshot(
    engine_name,
    symbol,
    decision_hash,
    action,
    selected_strategy,
    selected_expiry,
    selected_strikes,
    min_interval_seconds=300,
):
    key = _snapshot_key(engine_name, symbol)
    now = time.monotonic()
    normalized_strikes = normalize_strikes(selected_strikes)
    latest = _LAST_ENGINE_SNAPSHOTS.get(key)

    if not latest:
        _LAST_ENGINE_SNAPSHOTS[key] = {
            "persisted_at": now,
            "decision_hash": decision_hash,
            "action": action,
            "selected_strategy": selected_strategy,
            "selected_expiry": selected_expiry,
            "selected_strikes": normalized_strikes,
        }
        return True

    changed = (
        latest.get("action") != action
        or latest.get("selected_strategy") != selected_strategy
        or latest.get("selected_expiry") != selected_expiry
        or latest.get("selected_strikes") != normalized_strikes
        or latest.get("decision_hash") != decision_hash
    )
    heartbeat_due = now - latest.get("persisted_at", 0) >= min_interval_seconds

    if changed or heartbeat_due:
        latest.update(
            {
                "persisted_at": now,
                "decision_hash": decision_hash,
                "action": action,
                "selected_strategy": selected_strategy,
                "selected_expiry": selected_expiry,
                "selected_strikes": normalized_strikes,
            }
        )
        return True

    return False


def extract_strategy_strikes(recommendation):
    recommendation = recommendation or {}
    rec_json = recommendation.get("recommendation_json") or recommendation
    legs = rec_json.get("legs") or recommendation.get("legs") or []
    strikes = []
    for leg in legs:
        if isinstance(leg, dict) and leg.get("strike") is not None:
            strikes.append(leg.get("strike"))
    return normalize_strikes(strikes)


def top_candidates_summary(candidates, limit=3):
    rows = []
    for item in candidates or []:
        recommendation = item.get("recommendation") if isinstance(item, dict) else {}
        risk = item.get("risk") if isinstance(item, dict) else {}
        rows.append(
            {
                "symbol": item.get("symbol"),
                "expiry_label": item.get("expiry_label"),
                "strategy": item.get("strategy") or item.get("classification") or item.get("direction"),
                "score": item.get("selection_score") or item.get("score") or item.get("candidate_score"),
                "status": item.get("status") or item.get("classification"),
                "selected": bool(item.get("selected")),
                "rejection_reasons": item.get("rejection_reasons") or [],
                "margin_used_usdt": risk.get("margin_used_usdt") if isinstance(risk, dict) else None,
                "recommendation_id": recommendation.get("id") if isinstance(recommendation, dict) else None,
            }
        )
    return sorted(rows, key=lambda row: row.get("score") or 0, reverse=True)[:limit]


def compact_decision_reason(text, max_length=1200):
    if text is None:
        return None
    text = str(text)
    return text if len(text) <= max_length else text[:max_length] + "...[truncated]"


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()
