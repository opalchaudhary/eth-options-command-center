from datetime import datetime, timezone

import pandas as pd

from delta_api import get_eth_options, get_eth_spot_price
from recommendation_journal import _request, read_table


ETH_LOT_SIZE = 0.01
REGIME_RANGE = "Range Bound / Theta Friendly"
REGIME_MILD_BULL = "Mild Bullish"
REGIME_MILD_BEAR = "Mild Bearish"
REGIME_STRONG_BULL = "Strong Bullish Trend"
REGIME_STRONG_BEAR = "Strong Bearish Trend"
REGIME_BREAKOUT = "Breakout Risk"
REGIME_HIGH_VOL = "High Volatility / Gamma Risk"
REGIME_PINNING = "Expiry Pinning"
REGIME_TRAP = "Liquidity Trap / Manipulation Zone"


def safe_float(value, default=0.0):
    try:
        if value is None or pd.isna(value):
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


def _clamp(value, low=0, high=100):
    return max(low, min(high, safe_float(value)))


def _range_mid(low, high):
    return (safe_float(low) + safe_float(high)) / 2


def _outside_range_score(value, low, high, scale):
    value = safe_float(value)
    if low <= value <= high:
        return 0
    distance = low - value if value < low else value - high
    return _clamp((distance / max(abs(scale), 0.000001)) * 100)


def derive_margin_metrics(book_greeks):
    wallet_size = max(safe_float(book_greeks.get("wallet_size")), 0)
    unrealized_pnl = safe_float(book_greeks.get("unrealized_pnl"))
    equity = max(wallet_size + unrealized_pnl, 0)
    available_to_withdraw = safe_float(
        book_greeks.get("margin_available_to_withdraw"),
        book_greeks.get("available_to_withdraw"),
    )

    if book_greeks.get("margin_used") is not None:
        margin_used = max(safe_float(book_greeks.get("margin_used")), 0)
    elif equity > 0:
        available_to_withdraw = min(max(available_to_withdraw, 0), equity)
        margin_used = max(equity - available_to_withdraw, 0)
    else:
        margin_used = 0

    margin_usage = margin_used / equity if equity > 0 else 0

    return {
        "equity": round(equity, 6),
        "margin_available_to_withdraw": round(max(available_to_withdraw, 0), 6),
        "margin_used": round(margin_used, 6),
        "margin_usage": round(margin_usage, 6),
        "margin_usage_pct": round(margin_usage * 100, 4),
    }


def normalize_option_chain(option_chain_df):
    if option_chain_df is None or option_chain_df.empty:
        return pd.DataFrame()

    df = option_chain_df.copy()
    if "option_type" not in df.columns and "type" in df.columns:
        df["option_type"] = df["type"]

    if "expiry_label" not in df.columns:
        if "expiry" in df.columns:
            df["expiry_label"] = df["expiry"]
        elif "expiry_date" in df.columns:
            df["expiry_label"] = df["expiry_date"]

    for col in ["strike", "mark_price", "oi", "volume", "iv", "delta", "gamma", "theta", "vega"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.dropna(subset=["strike"]).reset_index(drop=True)


def get_available_option_chain(expiry_labels=None):
    expiries = expiry_labels or []
    frames = []

    for expiry in expiries:
        df = read_table(
            "option_chain_snapshots",
            {
                "select": "snapshot_time,expiry_label,expiry_date,strike,option_type,mark_price,oi,volume,iv,delta,gamma,theta,vega",
                "expiry_label": f"eq.{expiry}",
                "order": "snapshot_time.desc",
                "limit": 1600,
            },
        )
        if df.empty or "snapshot_time" not in df.columns:
            continue
        df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], utc=True, errors="coerce")
        latest = df["snapshot_time"].max()
        frames.append(df[df["snapshot_time"] == latest].copy())

    if frames:
        return normalize_option_chain(pd.concat(frames, ignore_index=True))

    try:
        live = get_eth_options()
        return normalize_option_chain(live)
    except Exception:
        return pd.DataFrame()


def _map_existing_regime(insights):
    existing = insights.get("market_regime")
    vol = insights.get("volatility_regime")
    bias = insights.get("directional_bias")
    pinning_score = safe_float(insights.get("pinning_score"))
    trap_risk = insights.get("trap_risk")
    iv_rv = safe_float(insights.get("iv_rv_spread"), None)
    momentum = insights.get("momentum")

    if trap_risk == "High":
        return REGIME_TRAP
    if existing in ["Breakout Risk", "Breakout / Invalidation Risk"]:
        return REGIME_BREAKOUT
    if vol in ["Expansion / Long Vol Favored", "Elevated"] and pinning_score < 65:
        return REGIME_HIGH_VOL
    if pinning_score >= 70 or existing == "Pinning / Range":
        return REGIME_PINNING
    if bias == "Bullish" and momentum == "Bullish":
        return REGIME_STRONG_BULL
    if bias == "Bearish" and momentum == "Bearish":
        return REGIME_STRONG_BEAR
    if bias in ["Bullish", "Mild Bullish"]:
        return REGIME_MILD_BULL
    if bias in ["Bearish", "Mild Bearish"]:
        return REGIME_MILD_BEAR
    if iv_rv is not None and iv_rv <= -5:
        return REGIME_BREAKOUT
    return REGIME_RANGE


def get_current_market_regime(selected_expiry=None, insights=None):
    warnings = []
    context = {}

    try:
        if insights is None:
            from rule_insights import build_rule_based_insights, get_available_expiries

            expiry = selected_expiry or (get_available_expiries(limit=20) or [None])[0]
            if expiry:
                insights = build_rule_based_insights(expiry)
            else:
                insights = {}
        context = dict(insights or {})
        regime = _map_existing_regime(context) if context else REGIME_RANGE
    except Exception as exc:
        regime = REGIME_RANGE
        warnings.append(f"Market regime fallback used because Insights context failed: {exc}")

    context["book_optimizer_regime"] = regime
    context["warnings"] = warnings + list(context.get("risk_warnings") or [])[:4]
    return context


def get_ideal_greeks_for_regime(market_regime, wallet_size, margin_used):
    wallet_size = max(safe_float(wallet_size), 1)
    theta_unit = max(wallet_size * 0.0005, 0.25)
    theta_ceiling = max(wallet_size * 0.014, 4.0)

    templates = {
        REGIME_RANGE: {
            "delta": (-0.10, 0.10),
            "gamma": (-0.004, 0.002),
            "theta": (theta_unit, theta_ceiling),
            "vega": (-2.0, 0.50),
            "max_margin_usage": 0.42,
            "explanation": "Range-bound conditions can support positive theta, but only while delta stays near neutral and short gamma remains controlled.",
        },
        REGIME_MILD_BULL: {
            "delta": (0.05, 0.25),
            "gamma": (-0.002, 0.006),
            "theta": (-theta_unit, theta_ceiling * 0.8),
            "vega": (-0.75, 1.75),
            "max_margin_usage": 0.40,
            "explanation": "Mild bullish conditions allow modest positive delta without carrying heavy negative gamma.",
        },
        REGIME_MILD_BEAR: {
            "delta": (-0.25, -0.05),
            "gamma": (-0.002, 0.006),
            "theta": (-theta_unit, theta_ceiling * 0.8),
            "vega": (-0.75, 1.75),
            "max_margin_usage": 0.40,
            "explanation": "Mild bearish conditions allow modest negative delta while avoiding fragile short-gamma exposure.",
        },
        REGIME_STRONG_BULL: {
            "delta": (0.20, 0.45),
            "gamma": (-0.001, 0.010),
            "theta": (-theta_unit * 1.5, theta_ceiling * 0.7),
            "vega": (-0.25, 2.50),
            "max_margin_usage": 0.38,
            "explanation": "A strong bullish trend rewards positive delta alignment, but heavy negative gamma can turn a winning view into liquidation pressure.",
        },
        REGIME_STRONG_BEAR: {
            "delta": (-0.45, -0.20),
            "gamma": (-0.001, 0.010),
            "theta": (-theta_unit * 1.5, theta_ceiling * 0.7),
            "vega": (-0.25, 2.50),
            "max_margin_usage": 0.38,
            "explanation": "A strong bearish trend rewards negative delta alignment, while short gamma and short vega need to stay light.",
        },
        REGIME_BREAKOUT: {
            "delta": (-0.10, 0.10),
            "gamma": (-0.0005, 0.012),
            "theta": (-theta_unit * 2.0, theta_ceiling * 0.45),
            "vega": (0.0, 3.00),
            "max_margin_usage": 0.35,
            "explanation": "Breakout risk favors neutral delta, near-flat to positive gamma, and no extra short-premium chasing.",
        },
        REGIME_HIGH_VOL: {
            "delta": (-0.12, 0.12),
            "gamma": (-0.001, 0.010),
            "theta": (-theta_unit, theta_ceiling * 0.65),
            "vega": (-0.25, 1.75),
            "max_margin_usage": 0.35,
            "explanation": "High volatility requires smaller size and better convexity because gamma shocks can dominate theta income.",
        },
        REGIME_PINNING: {
            "delta": (-0.08, 0.08),
            "gamma": (-0.0035, 0.0025),
            "theta": (theta_unit, theta_ceiling),
            "vega": (-1.75, 0.25),
            "max_margin_usage": 0.35,
            "explanation": "Pinning can support positive theta and mild short vega, but size should come down near expiry.",
        },
        REGIME_TRAP: {
            "delta": (-0.05, 0.05),
            "gamma": (0.0, 0.012),
            "theta": (-theta_unit * 2.0, theta_ceiling * 0.35),
            "vega": (0.0, 2.50),
            "max_margin_usage": 0.25,
            "explanation": "Trap conditions call for smaller, defined-risk exposure with flat delta and no naked short premium.",
        },
    }
    selected = templates.get(market_regime, templates[REGIME_RANGE])
    margin_usage = safe_float(margin_used) / wallet_size

    return {
        "ideal_delta_range": selected["delta"],
        "ideal_gamma_range": selected["gamma"],
        "ideal_theta_range": selected["theta"],
        "ideal_vega_range": selected["vega"],
        "max_margin_usage": selected["max_margin_usage"],
        "current_margin_usage": margin_usage,
        "explanation": selected["explanation"],
    }


def classify_book_greek_health(book_greeks, market_context):
    market_context = market_context or {}
    regime = market_context.get("book_optimizer_regime") or market_context.get("market_regime") or REGIME_RANGE
    margin_metrics = derive_margin_metrics(book_greeks)
    ideal = get_ideal_greeks_for_regime(
        regime,
        margin_metrics.get("equity") or book_greeks.get("wallet_size"),
        margin_metrics.get("margin_used"),
    )
    delta = safe_float(book_greeks.get("delta"))
    gamma = safe_float(book_greeks.get("gamma"))
    theta = safe_float(book_greeks.get("theta"))
    vega = safe_float(book_greeks.get("vega"))
    margin_usage = margin_metrics.get("margin_usage")
    iv_rv = safe_float((market_context or {}).get("iv_rv_spread"), None)

    d_low, d_high = ideal["ideal_delta_range"]
    g_low, g_high = ideal["ideal_gamma_range"]
    t_low, t_high = ideal["ideal_theta_range"]
    v_low, v_high = ideal["ideal_vega_range"]

    delta_score = _outside_range_score(delta, d_low, d_high, 0.30)
    gamma_score = _outside_range_score(gamma, g_low, g_high, 0.008)
    theta_score = _outside_range_score(theta, t_low, t_high, max(abs(t_high), 1))
    vega_score = _outside_range_score(vega, v_low, v_high, 2.5)
    margin_score = _clamp((margin_usage / max(ideal["max_margin_usage"], 0.01)) * 70)
    regime_mismatch = 0
    reasoning = []

    if gamma < -0.003 and regime in [REGIME_BREAKOUT, REGIME_HIGH_VOL, REGIME_STRONG_BULL, REGIME_STRONG_BEAR, REGIME_TRAP]:
        regime_mismatch += 30
        reasoning.append("Negative gamma is too heavy for a trend, breakout, or trap-risk regime.")
    if theta > t_high and gamma < 0:
        theta_score = max(theta_score, 45)
        reasoning.append("Theta is high, but it appears to be funded by short gamma risk.")
    if vega < -1.0 and (regime in [REGIME_BREAKOUT, REGIME_HIGH_VOL, REGIME_TRAP] or (iv_rv is not None and iv_rv <= -5)):
        regime_mismatch += 25
        reasoning.append("Negative vega is fragile while volatility expansion or breakout risk is present.")
    if abs(delta) > 0.25 and regime in [REGIME_RANGE, REGIME_PINNING, REGIME_BREAKOUT, REGIME_TRAP]:
        regime_mismatch += 20
        reasoning.append("Delta is too directional for a neutral, pinning, or breakout-risk tape.")
    if margin_usage > ideal["max_margin_usage"]:
        reasoning.append("Margin usage is above the regime-safe cap, so adjustment capacity is reduced.")

    if not reasoning:
        reasoning.append("Greek exposure is broadly aligned with the current regime and margin usage is manageable.")

    component_scores = {
        "delta_risk": round(delta_score, 1),
        "gamma_risk": round(gamma_score, 1),
        "theta_quality_risk": round(theta_score, 1),
        "vega_risk": round(vega_score, 1),
        "margin_risk": round(margin_score, 1),
        "regime_mismatch_risk": round(_clamp(regime_mismatch), 1),
    }
    risk_score = round(
        _clamp(
            delta_score * 0.18
            + gamma_score * 0.22
            + theta_score * 0.13
            + vega_score * 0.14
            + margin_score * 0.18
            + regime_mismatch * 0.15
        ),
        1,
    )

    if risk_score < 25:
        health_status = "Healthy"
    elif risk_score < 45:
        health_status = "Caution"
    elif risk_score < 70:
        health_status = "Risky"
    else:
        health_status = "Dangerous"

    return {
        "health_status": health_status,
        "risk_score": risk_score,
        "component_scores": component_scores,
        "reasoning": reasoning,
    }


def compare_book_with_ideal(book_greeks, ideal_greeks):
    labels = [
        ("Delta", "delta", "ideal_delta_range"),
        ("Gamma", "gamma", "ideal_gamma_range"),
        ("Theta", "theta", "ideal_theta_range"),
        ("Vega", "vega", "ideal_vega_range"),
    ]
    rows = []

    for label, key, range_key in labels:
        current = safe_float(book_greeks.get(key))
        low, high = ideal_greeks[range_key]
        if low <= current <= high:
            status = "OK"
            adjustment = "No immediate adjustment required."
        elif current > high:
            status = "Too High" if label not in ["Delta"] else "Too bullish"
            adjustment = {
                "Delta": "Reduce positive delta with a put, call spread sale, or small short futures hedge.",
                "Gamma": "Reduce convexity only if theta/risk budget requires it; avoid selling naked gamma in volatile regimes.",
                "Theta": "Do not add more short premium; reduce crowded short-gamma theta.",
                "Vega": "Reduce positive vega with spreads or controlled premium sale only if the regime allows it.",
            }[label]
        else:
            status = "Too Low" if label not in ["Delta"] else "Too bearish"
            adjustment = {
                "Delta": "Reduce negative delta with a call, put spread sale, or small long futures hedge.",
                "Gamma": "Add long ATM/near-ATM options or reduce short ATM exposure.",
                "Theta": "Add controlled defined-risk premium only if gamma and margin are safe.",
                "Vega": "Add long options or avoid further short premium.",
            }[label]

        danger = "Dangerous" if label == "Gamma" and current < low * 1.8 and current < 0 else status
        rows.append(
            {
                "Greek": label,
                "Current": round(current, 6),
                "Ideal Min": round(low, 6),
                "Ideal Max": round(high, 6),
                "Status": danger,
                "Required Adjustment": adjustment,
            }
        )

    margin_metrics = derive_margin_metrics(book_greeks)
    margin_usage = margin_metrics.get("margin_usage")
    max_margin = ideal_greeks["max_margin_usage"]
    rows.append(
        {
            "Greek": "Margin Usage",
            "Current": round(margin_usage * 100, 2),
            "Ideal Min": 0,
            "Ideal Max": round(max_margin * 100, 2),
            "Status": "OK" if margin_usage <= max_margin else "Dangerous",
            "Required Adjustment": "Calculated from equity minus withdrawable balance. Keep new trades within remaining margin capacity; reduce exposure first if above cap.",
        }
    )
    return pd.DataFrame(rows)


def _expiry_bucket(expiry):
    timestamp = pd.to_datetime(expiry, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return "Unknown"
    days = max((timestamp - pd.Timestamp.utcnow()).total_seconds() / 86400, 0)
    if days <= 1:
        return "D0/D1 Tactical"
    if days <= 3:
        return "D2/D3 Balanced"
    if days <= 10:
        return "Weekly"
    return "Monthly"


def _choose_option(df, option_type, spot_price, target="atm", expiry_bucket=None):
    if df.empty:
        return {}
    candidates = df[df["option_type"].eq(option_type)].copy()
    if expiry_bucket:
        candidates = candidates[candidates["expiry_bucket"].eq(expiry_bucket)].copy()
    if candidates.empty:
        candidates = df[df["option_type"].eq(option_type)].copy()
    if candidates.empty:
        return {}

    if target == "otm_call":
        candidates["distance"] = candidates["strike"].apply(lambda strike: strike - spot_price if strike >= spot_price else 10**9)
    elif target == "otm_put":
        candidates["distance"] = candidates["strike"].apply(lambda strike: spot_price - strike if strike <= spot_price else 10**9)
    else:
        candidates["distance"] = (candidates["strike"] - spot_price).abs()

    candidates = candidates.sort_values(["distance", "mark_price"], ascending=[True, True])
    row = candidates.iloc[0].to_dict()
    return row


def _option_lot_capacity(book_greeks, ideal_greeks, option_row, action):
    margin_metrics = derive_margin_metrics(book_greeks)
    equity = max(safe_float(margin_metrics.get("equity")), 1)
    margin_used = safe_float(margin_metrics.get("margin_used"))
    remaining_margin = max(equity * ideal_greeks["max_margin_usage"] - margin_used, 0)
    mark = max(safe_float(option_row.get("mark_price")), 0.01)
    spot = max(safe_float(book_greeks.get("spot_price")), mark * 10, 1)

    if action == "BUY":
        per_lot = mark * ETH_LOT_SIZE
    else:
        per_lot = max(mark * ETH_LOT_SIZE * 2, spot * ETH_LOT_SIZE * 0.08)

    affordable = int(max(remaining_margin // max(per_lot, 0.01), 0))
    return {
        "conservative": max(1, min(affordable, 2)) if affordable >= 1 else 0,
        "balanced": max(1, min(affordable, 4)) if affordable >= 1 else 0,
        "aggressive": max(1, min(affordable, 8)) if affordable >= 3 else 0,
        "remaining_margin": remaining_margin,
        "estimated_margin_or_debit_per_lot": round(per_lot, 4),
    }


def _impact(option_row, action, lots):
    sign = 1 if action == "BUY" else -1
    return {
        "delta": round(sign * safe_float(option_row.get("delta")) * ETH_LOT_SIZE * lots, 6),
        "gamma": round(sign * safe_float(option_row.get("gamma")) * ETH_LOT_SIZE * lots, 6),
        "theta": round(sign * safe_float(option_row.get("theta")) * ETH_LOT_SIZE * lots, 6),
        "vega": round(sign * safe_float(option_row.get("vega")) * ETH_LOT_SIZE * lots, 6),
    }


def _candidate(label, row, action, lots, reason, warning):
    if not row or lots <= 0:
        return None
    return {
        "profile": label,
        "expiry": row.get("expiry_label"),
        "expiry_bucket": row.get("expiry_bucket"),
        "instrument_type": "CE" if row.get("option_type") == "call_options" else "PE",
        "action": action,
        "strike": row.get("strike"),
        "lots": int(lots),
        "aggressive_lots_if_safe": None,
        "mark_price": row.get("mark_price"),
        "expected_greek_impact": _impact(row, action, lots),
        "reason": reason,
        "risk_warning": warning,
    }


def suggest_book_adjustments(book_greeks, ideal_greeks, option_chain_df, expiries=None):
    df = normalize_option_chain(option_chain_df)
    if df.empty:
        return {
            "candidates": [],
            "warnings": ["Option chain data is missing. Greek health analysis is available, but strike-level suggestions need live option chain data."],
        }

    spot = safe_float(book_greeks.get("spot_price"))
    if not spot:
        spot_data = get_eth_spot_price()
        spot = safe_float(spot_data.get("spot_price") or spot_data.get("mark_price"))
    if not spot:
        spot = safe_float(df["strike"].median())

    df["expiry_bucket"] = df["expiry_label"].apply(_expiry_bucket)
    if expiries:
        df = df[df["expiry_label"].isin(expiries)].copy()

    delta = safe_float(book_greeks.get("delta"))
    gamma = safe_float(book_greeks.get("gamma"))
    theta = safe_float(book_greeks.get("theta"))
    vega = safe_float(book_greeks.get("vega"))
    d_low, d_high = ideal_greeks["ideal_delta_range"]
    g_low, _ = ideal_greeks["ideal_gamma_range"]
    t_low, _ = ideal_greeks["ideal_theta_range"]
    v_low, _ = ideal_greeks["ideal_vega_range"]
    regime = book_greeks.get("market_regime") or REGIME_RANGE
    candidates = []

    if delta > d_high:
        put = _choose_option(df, "put_options", spot, "atm", "D2/D3 Balanced")
        cap = _option_lot_capacity(book_greeks, ideal_greeks, put, "BUY")
        candidates.append(
            _candidate(
                "Conservative",
                put,
                "BUY",
                cap["conservative"],
                "Book delta is too positive. A near-ATM put reduces bullish exposure while adding gamma and vega protection.",
                "Debit is limited, but decay will hurt if ETH pins and volatility falls.",
            )
        )
        call = _choose_option(df, "call_options", spot, "otm_call", "Weekly")
        cap = _option_lot_capacity(book_greeks, ideal_greeks, call, "SELL")
        if regime in [REGIME_RANGE, REGIME_PINNING] and gamma >= g_low:
            candidates.append(
                _candidate(
                    "Balanced",
                    call,
                    "SELL",
                    cap["balanced"],
                    "Controlled OTM call sale trims positive delta and can add theta in a range or pinning regime.",
                    "Do not use this if breakout risk rises; short calls add negative gamma.",
                )
            )
    elif delta < d_low:
        call = _choose_option(df, "call_options", spot, "atm", "D2/D3 Balanced")
        cap = _option_lot_capacity(book_greeks, ideal_greeks, call, "BUY")
        candidates.append(
            _candidate(
                "Conservative",
                call,
                "BUY",
                cap["conservative"],
                "Book delta is too negative. A near-ATM call reduces bearish exposure while adding gamma and vega protection.",
                "Debit is limited, but decay will hurt if ETH pins and volatility falls.",
            )
        )
        put = _choose_option(df, "put_options", spot, "otm_put", "Weekly")
        cap = _option_lot_capacity(book_greeks, ideal_greeks, put, "SELL")
        if regime in [REGIME_RANGE, REGIME_PINNING] and gamma >= g_low:
            candidates.append(
                _candidate(
                    "Balanced",
                    put,
                    "SELL",
                    cap["balanced"],
                    "Controlled OTM put sale trims negative delta and can add theta in a range or pinning regime.",
                    "Short puts add downside gap risk; avoid if bearish trend pressure returns.",
                )
            )
    elif gamma < g_low or vega < v_low:
        call = _choose_option(df, "call_options", spot, "atm", "D2/D3 Balanced")
        put = _choose_option(df, "put_options", spot, "atm", "D2/D3 Balanced")
        call_cap = _option_lot_capacity(book_greeks, ideal_greeks, call, "BUY")
        put_cap = _option_lot_capacity(book_greeks, ideal_greeks, put, "BUY")
        lots = min(call_cap["conservative"], put_cap["conservative"])
        if call and put and lots > 0:
            combined = dict(call)
            combined["option_type"] = "call_options + put_options"
            candidates.append(
                {
                    "profile": "Conservative",
                    "expiry": call.get("expiry_label"),
                    "expiry_bucket": call.get("expiry_bucket"),
                    "instrument_type": "ATM Straddle",
                    "action": "BUY",
                    "strike": call.get("strike"),
                    "lots": int(lots),
                    "aggressive_lots_if_safe": min(call_cap["aggressive"], put_cap["aggressive"]) or None,
                    "mark_price": safe_float(call.get("mark_price")) + safe_float(put.get("mark_price")),
                    "expected_greek_impact": {
                        key: round(_impact(call, "BUY", lots)[key] + _impact(put, "BUY", lots)[key], 6)
                        for key in ["delta", "gamma", "theta", "vega"]
                    },
                    "reason": "Gamma or vega is too low for the current regime. A small ATM straddle adds convexity instead of adding more short premium.",
                    "risk_warning": "Long straddles bleed theta; use this as protection, not a passive income trade.",
                }
            )
    elif theta < t_low and regime in [REGIME_RANGE, REGIME_PINNING]:
        call = _choose_option(df, "call_options", spot, "otm_call", "D0/D1 Tactical")
        put = _choose_option(df, "put_options", spot, "otm_put", "D0/D1 Tactical")
        call_cap = _option_lot_capacity(book_greeks, ideal_greeks, call, "SELL")
        put_cap = _option_lot_capacity(book_greeks, ideal_greeks, put, "SELL")
        lots = min(call_cap["conservative"], put_cap["conservative"])
        if call and put and lots > 0:
            candidates.append(
                {
                    "profile": "Conservative",
                    "expiry": call.get("expiry_label"),
                    "expiry_bucket": call.get("expiry_bucket"),
                    "instrument_type": "Defined Iron Condor Starter",
                    "action": "SELL",
                    "strike": f"{put.get('strike')} PE / {call.get('strike')} CE",
                    "lots": int(lots),
                    "aggressive_lots_if_safe": min(call_cap["aggressive"], put_cap["aggressive"]) or None,
                    "mark_price": safe_float(call.get("mark_price")) + safe_float(put.get("mark_price")),
                    "expected_greek_impact": {
                        key: round(_impact(call, "SELL", lots)[key] + _impact(put, "SELL", lots)[key], 6)
                        for key in ["delta", "gamma", "theta", "vega"]
                    },
                    "reason": "Theta is below target while the regime is range/pinning friendly. Selling small OTM wings can add controlled theta.",
                    "risk_warning": "Keep this defined-risk in real execution; naked short wings can become dangerous during a breakout.",
                }
            )

    def _future_candidate(profile, lots):
        future_direction = "SHORT" if delta > _range_mid(d_low, d_high) else "LONG"
        return {
            "profile": profile,
            "expiry": "Perpetual",
            "expiry_bucket": "Immediate Hedge",
            "instrument_type": "Future",
            "action": future_direction,
            "strike": "ETHUSDT.PERP",
            "lots": lots,
            "aggressive_lots_if_safe": 2 if profile != "Aggressive" else None,
            "mark_price": spot,
            "expected_greek_impact": {
                "delta": round((-ETH_LOT_SIZE if future_direction == "SHORT" else ETH_LOT_SIZE) * lots, 6),
                "gamma": 0,
                "theta": 0,
                "vega": 0,
            },
            "reason": "A futures hedge adjusts delta without adding option decay or vega exposure.",
            "risk_warning": "Futures hedges need active stop management because they do not provide convexity.",
        }

    clean_preview = [candidate for candidate in candidates if candidate]
    present_profiles = {candidate["profile"] for candidate in clean_preview}
    for profile, lots in [("Conservative", 1), ("Balanced", 1), ("Aggressive", 2)]:
        if len([c for c in candidates if c]) >= 3:
            break
        if profile not in present_profiles:
            candidates.append(_future_candidate(profile, lots))
            present_profiles.add(profile)

    clean = [candidate for candidate in candidates if candidate]
    ordered = []
    for label in ["Conservative", "Balanced", "Aggressive"]:
        for candidate in clean:
            if candidate["profile"] == label and candidate not in ordered:
                ordered.append(candidate)
                break
    for candidate in clean:
        if len(ordered) >= 3:
            break
        if candidate not in ordered:
            ordered.append(candidate)

    return {"candidates": ordered[:3], "warnings": []}


def build_final_verdict(health, book_greeks, ideal_greeks, market_context, adjustments):
    status = health.get("health_status")
    regime = market_context.get("book_optimizer_regime") or market_context.get("market_regime")
    reason = " ".join(health.get("reasoning") or [])
    best = (adjustments.get("candidates") or [{}])[0]
    best_text = "Wait for option-chain data before choosing a strike."
    if best:
        best_text = (
            f"Best adjustment is {best.get('action')} {best.get('lots')} lot(s) of "
            f"{best.get('instrument_type')} {best.get('strike')} for {best.get('expiry')}."
        )
    d_low, d_high = ideal_greeks["ideal_delta_range"]
    g_low, g_high = ideal_greeks["ideal_gamma_range"]
    t_low, t_high = ideal_greeks["ideal_theta_range"]

    return (
        f"Your book is currently {status} in a {regime} regime. {reason} "
        f"{best_text} Ideal target is delta between {d_low:.2f} and {d_high:.2f}, "
        f"gamma between {g_low:.4f} and {g_high:.4f}, theta between {t_low:.2f} and {t_high:.2f}, "
        f"and margin usage below {ideal_greeks['max_margin_usage'] * 100:.0f}%."
    )


def save_book_optimization_snapshot(
    book_greeks,
    market_context,
    health,
    ideal_greeks,
    suggested_actions,
    comparison=None,
):
    d_low, d_high = ideal_greeks["ideal_delta_range"]
    g_low, g_high = ideal_greeks["ideal_gamma_range"]
    t_low, t_high = ideal_greeks["ideal_theta_range"]
    v_low, v_high = ideal_greeks["ideal_vega_range"]
    margin_metrics = derive_margin_metrics(book_greeks)
    payload = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "spot_price": safe_float(book_greeks.get("spot_price"), None),
        "expiry_context": _json_safe(book_greeks.get("expiry_context")),
        "market_regime": market_context.get("book_optimizer_regime") or market_context.get("market_regime"),
        "input_delta": safe_float(book_greeks.get("delta"), None),
        "input_gamma": safe_float(book_greeks.get("gamma"), None),
        "input_theta": safe_float(book_greeks.get("theta"), None),
        "input_vega": safe_float(book_greeks.get("vega"), None),
        "margin_used": margin_metrics.get("margin_used"),
        "margin_available_to_withdraw": margin_metrics.get("margin_available_to_withdraw"),
        "portfolio_equity": margin_metrics.get("equity"),
        "margin_usage_pct": margin_metrics.get("margin_usage_pct"),
        "wallet_size": safe_float(book_greeks.get("wallet_size"), None),
        "health_status": health.get("health_status"),
        "risk_score": health.get("risk_score"),
        "ideal_delta_min": d_low,
        "ideal_delta_max": d_high,
        "ideal_gamma_min": g_low,
        "ideal_gamma_max": g_high,
        "ideal_theta_min": t_low,
        "ideal_theta_max": t_high,
        "ideal_vega_min": v_low,
        "ideal_vega_max": v_high,
        "suggested_action_json": _json_safe(suggested_actions),
        "reasoning_json": _json_safe(
            {
                "health": health,
                "market_context": market_context,
                "comparison": comparison.to_dict("records") if isinstance(comparison, pd.DataFrame) else comparison,
                "strategy_type": book_greeks.get("strategy_type"),
                "unrealized_pnl": book_greeks.get("unrealized_pnl"),
                "margin_metrics": margin_metrics,
            }
        ),
    }
    result = _request("POST", "book_optimization_journal", payload=payload, prefer="return=representation")
    if isinstance(result, list) and result:
        return result[0]
    return result


def get_recent_book_optimization_snapshots(limit=25):
    return read_table(
        "book_optimization_journal",
        {
            "select": "id,created_at,spot_price,market_regime,health_status,risk_score,book_greeks,ideal_greeks,comparison_json,final_verdict",
            "order": "created_at.desc",
            "limit": limit,
        },
    )
