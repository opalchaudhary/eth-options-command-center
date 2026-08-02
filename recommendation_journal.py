import hashlib
import json
from datetime import datetime, timezone

import pandas as pd
import requests

from database_reader import HEADERS, SUPABASE_KEY, SUPABASE_URL
from engine_persistence import build_decision_hash, compact_json_payload, safe_engine_payload


JSON_HEADERS = {
    **HEADERS,
    "Content-Type": "application/json",
}


LEGACY_INSERT_COLUMNS = {
    "paper_recommendation_evaluations": {
        "created_at",
        "expiry_label",
        "strategy",
        "recommendation_id",
        "selected",
        "selection_score",
        "rejection_reasons",
        "wallet_state",
        "risk_json",
        "insight_json",
        "candidate_json",
    },
    "paper_trading_engine_runs": {
        "created_at",
        "cycle_started_at",
        "cycle_finished_at",
        "status",
        "action",
        "error",
        "interval_seconds",
        "limit_expiries",
        "opened_trade_id",
        "selected_strategy",
        "selected_expiry_label",
        "selected_score",
        "open_trade_count",
        "closed_trade_count",
        "cycle_json",
    },
    "futures_trading_engine_runs": {
        "created_at",
        "cycle_started_at",
        "cycle_finished_at",
        "status",
        "action",
        "error",
        "interval_seconds",
        "opened_trade_id",
        "selected_direction",
        "selected_score",
        "open_position_count",
        "closed_trade_count",
        "cycle_json",
    },
    "alt_futures_scanner_snapshots": {
        "created_at",
        "symbol",
        "price",
        "score",
        "classification",
        "direction",
        "indicators_json",
        "funding_rate",
        "open_interest",
        "oi_change_pct",
        "volume",
        "volume_change_pct",
        "spread",
        "spread_pct",
        "liquidity_score",
        "trend_score",
        "smc_score",
        "final_reason",
        "selected",
        "raw_snapshot_json",
    },
    "alt_futures_engine_runs": {
        "created_at",
        "cycle_started_at",
        "cycle_finished_at",
        "status",
        "action",
        "error",
        "interval_seconds",
        "opened_trade_id",
        "selected_symbol",
        "selected_direction",
        "selected_score",
        "open_position_count",
        "scanned_symbol_count",
        "cycle_json",
    },
    "recommendation_journal": {
        "recommendation_key",
        "created_at",
        "spot_price",
        "expiry_label",
        "market_regime",
        "directional_bias",
        "suggested_strategy",
        "suggested_sell_strike",
        "suggested_hedge_strike",
        "confidence_score",
        "signal_conflict_score",
        "warnings",
        "reasoning_text",
        "raw_input_snapshot",
        "recommendation_json",
    },
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
            fallback_columns = LEGACY_INSERT_COLUMNS.get(table_name)
            if method.upper() == "POST" and fallback_columns and payload:
                if isinstance(payload, list):
                    fallback_payload = [
                        {key: value for key, value in row.items() if key in fallback_columns}
                        for row in payload
                    ]
                else:
                    fallback_payload = {key: value for key, value in payload.items() if key in fallback_columns}
                fallback_response = requests.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=fallback_payload,
                    timeout=15,
                )
                if fallback_response.status_code in [200, 201, 204]:
                    print(f"Supabase {table_name} write used legacy-column fallback. Run production optimization migration.")
                    if not fallback_response.text:
                        return []
                    return fallback_response.json()
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
        "risk_reward": insights.get("strategy_risk_reward"),
        "expiry_profile": insights.get("expiry_profile"),
    }
    selected_strikes = [
        leg.get("strike")
        for leg in strategy_legs
        if isinstance(leg, dict) and leg.get("strike") is not None
    ]
    decision_hash = build_decision_hash(
        "recommendation_journal",
        "ETHUSD",
        "recommend",
        insights.get("best_strategy"),
        insights.get("expiry_label"),
        selected_strikes,
        insights.get("market_regime"),
        insights.get("confidence_score"),
        insights.get("signal_conflict_score"),
    )
    raw_snapshot, payload_truncated = safe_engine_payload(
        insights.get("raw_input_snapshot") or {},
        table_name="recommendation_journal.raw_input_snapshot",
    )

    return {
        "recommendation_key": build_recommendation_key(insights),
        "created_at": insights.get("generated_at") or _now_iso(),
        "symbol": "ETHUSD",
        "engine_name": "recommendation_journal",
        "run_status": "ok",
        "action": "recommend",
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
        "raw_input_snapshot": _json_safe(raw_snapshot),
        "recommendation_json": _json_safe(compact_json_payload(recommendation, max_depth=3, max_list_items=20)),
        "selected_strategy": insights.get("best_strategy"),
        "selected_expiry": insights.get("expiry_label"),
        "selected_strikes": _json_safe(selected_strikes),
        "risk_score": int(insights.get("signal_conflict_score") or 0),
        "decision_reason": "\n".join(reasoning)[:1200],
        "decision_hash": decision_hash,
        "payload_truncated": payload_truncated,
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
            "select": "id,recommendation_key,created_at,symbol,spot_price,expiry_label,market_regime,directional_bias,suggested_strategy,suggested_sell_strike,suggested_hedge_strike,confidence_score,signal_conflict_score,warnings,reasoning_text,recommendation_json,decision_hash,payload_truncated",
            "order": "created_at.desc",
            "limit": limit,
        },
    )
