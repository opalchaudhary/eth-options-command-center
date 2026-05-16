import hashlib
import json
from datetime import datetime, timezone

import pandas as pd
import requests

from database_reader import HEADERS, SUPABASE_KEY, SUPABASE_URL


JSON_HEADERS = {
    **HEADERS,
    "Content-Type": "application/json",
}


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value):
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _json_safe(value):
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}

    if isinstance(value, list):
        return [_json_safe(v) for v in value]

    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]

    if hasattr(value, "isoformat"):
        return value.isoformat()

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    return value


def _request(method, table_name, payload=None, params=None, prefer="return=representation"):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None

    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    headers = {**JSON_HEADERS, "Prefer": prefer}

    try:
        response = requests.request(
            method,
            url,
            headers=headers,
            params=params,
            json=payload,
            timeout=15,
        )

        if response.status_code not in [200, 201, 204]:
            print(f"Supabase {method} failed for {table_name}:", response.status_code, response.text)
            return None

        if not response.text:
            return []

        return response.json()

    except Exception as e:
        print(f"Supabase request error for {table_name}:", e)
        return None


def read_table(table_name, params=None):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return pd.DataFrame()

    url = f"{SUPABASE_URL}/rest/v1/{table_name}"

    try:
        response = requests.get(url, headers=HEADERS, params=params or {}, timeout=15)

        if response.status_code != 200:
            print(f"Supabase read failed for {table_name}:", response.status_code, response.text)
            return pd.DataFrame()

        data = response.json()
        return pd.DataFrame(data) if data else pd.DataFrame()

    except Exception as e:
        print(f"Supabase read error for {table_name}:", e)
        return pd.DataFrame()


def _first_leg_by_action(strategy_legs, action_prefix):
    for leg in strategy_legs or []:
        action = str(leg.get("action", "")).lower()
        if action.startswith(action_prefix):
            return leg

    return {}


def build_recommendation_key(insights):
    timestamp = pd.Timestamp.utcnow().floor("5min").isoformat()
    sell_leg = _first_leg_by_action(insights.get("strategy_legs"), "sell")
    buy_leg = _first_leg_by_action(insights.get("strategy_legs"), "buy")
    parts = [
        timestamp,
        insights.get("expiry_label"),
        round(_safe_float(insights.get("spot_price")) or 0, 1),
        insights.get("market_regime"),
        insights.get("directional_bias"),
        insights.get("best_strategy"),
        sell_leg.get("strike"),
        buy_leg.get("strike"),
        insights.get("confidence_score"),
        insights.get("signal_conflict_score"),
    ]
    raw_key = "|".join(str(part) for part in parts)
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def build_recommendation_payload(insights):
    strategy_legs = insights.get("strategy_legs") or []
    sell_leg = _first_leg_by_action(strategy_legs, "sell")
    buy_leg = _first_leg_by_action(strategy_legs, "buy")
    reasoning = insights.get("key_insights") or []

    recommendation = {
        "strategy": insights.get("best_strategy"),
        "legs": strategy_legs,
        "pricing": insights.get("strategy_pricing"),
        "expiry_profile": insights.get("expiry_profile"),
    }

    return {
        "recommendation_key": build_recommendation_key(insights),
        "created_at": insights.get("generated_at") or _now_iso(),
        "spot_price": _safe_float(insights.get("spot_price")),
        "expiry_label": insights.get("expiry_label"),
        "market_regime": insights.get("market_regime"),
        "directional_bias": insights.get("directional_bias"),
        "suggested_strategy": insights.get("best_strategy"),
        "suggested_sell_strike": _safe_float(sell_leg.get("strike")),
        "suggested_hedge_strike": _safe_float(buy_leg.get("strike")),
        "confidence_score": int(insights.get("confidence_score") or 0),
        "signal_conflict_score": int(insights.get("signal_conflict_score") or 0),
        "warnings": _json_safe(insights.get("risk_warnings") or []),
        "reasoning_text": "\n".join(reasoning),
        "raw_input_snapshot": _json_safe(insights.get("raw_input_snapshot") or {}),
        "recommendation_json": _json_safe(recommendation),
    }


def save_recommendation_snapshot(insights):
    payload = build_recommendation_payload(insights)
    result = _request(
        "POST",
        "recommendation_journal",
        payload=payload,
        params={"on_conflict": "recommendation_key"},
        prefer="resolution=merge-duplicates,return=representation",
    )

    if isinstance(result, list) and result:
        return result[0]

    return payload


def get_latest_recommendations(limit=25):
    return read_table(
        "recommendation_journal",
        {
            "select": "*",
            "order": "created_at.desc",
            "limit": limit,
        },
    )
