import requests
import pandas as pd
from datetime import datetime, timezone

from database_reader import (
    HEADERS,
    SUPABASE_URL,
    get_latest_ohlcv_data,
    get_market_events,
    get_smc_zones,
    get_volume_profile,
)


DEFAULT_SYMBOL = "ETHUSD"
DEFAULT_RESOLUTION = "5m"
DATA_SOURCE_LABELS = {
    "analytics": "analytics_snapshots",
    "option_chain": "option_chain_snapshots",
    "orderbook": "orderbook_insights",
    "premium_decay": "premium_decay_snapshots",
    "ohlcv": "eth_ohlcv",
    "market_events": "eth_market_events",
    "smc_zones": "eth_smc_zones",
    "volume_profile": "eth_volume_profile",
}


def _safe_float(value, default=None):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _bounded_score(value, low=0, high=100):
    value = _safe_float(value, low)
    return max(low, min(high, value))


def _unique_items(items):
    unique = []
    seen = set()

    for item in items:
        if not item or item in seen:
            continue

        unique.append(item)
        seen.add(item)

    return unique


def _utc_now():
    return datetime.now(timezone.utc)


def _read_table(table_name, params):
    if not SUPABASE_URL or not HEADERS.get("apikey"):
        return pd.DataFrame()

    url = f"{SUPABASE_URL}/rest/v1/{table_name}"

    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=15)

        if response.status_code != 200:
            print(f"Failed to read {table_name}:", response.status_code, response.text)
            return pd.DataFrame()

        data = response.json()

        if not data:
            return pd.DataFrame()

        return pd.DataFrame(data)

    except Exception as e:
        print(f"Error reading {table_name}:", e)
        return pd.DataFrame()


def get_available_expiries(limit=500):
    df = _read_table(
        "analytics_snapshots",
        {
            "select": "expiry_label,snapshot_time",
            "order": "snapshot_time.desc",
            "limit": limit,
        },
    )

    if df.empty or "expiry_label" not in df.columns:
        return []

    expiries = []

    for expiry in df["expiry_label"].dropna():
        if expiry not in expiries:
            expiries.append(expiry)

    return expiries


def _latest_rows_for_expiry(table_name, expiry_label, order_col="snapshot_time", limit=1000):
    params = {
        "select": "*",
        "expiry_label": f"eq.{expiry_label}",
        "order": f"{order_col}.desc",
        "limit": limit,
    }

    df = _read_table(table_name, params)

    if df.empty or order_col not in df.columns:
        return df

    df[order_col] = pd.to_datetime(df[order_col], utc=True, errors="coerce")
    latest_time = df[order_col].max()

    return df[df[order_col] == latest_time].copy().reset_index(drop=True)


def _latest_snapshot_pair_for_expiry(
    table_name,
    expiry_label,
    order_col="snapshot_time",
    limit=2500,
):
    params = {
        "select": "*",
        "expiry_label": f"eq.{expiry_label}",
        "order": f"{order_col}.desc",
        "limit": limit,
    }

    df = _read_table(table_name, params)

    if df.empty or order_col not in df.columns:
        return df, pd.DataFrame()

    df[order_col] = pd.to_datetime(df[order_col], utc=True, errors="coerce")
    snapshot_times = sorted(df[order_col].dropna().unique(), reverse=True)

    if not snapshot_times:
        return df, pd.DataFrame()

    latest = df[df[order_col] == snapshot_times[0]].copy().reset_index(drop=True)
    previous = pd.DataFrame()

    if len(snapshot_times) > 1:
        previous = df[df[order_col] == snapshot_times[1]].copy().reset_index(drop=True)

    return latest, previous


def _latest_orderbook(symbol=DEFAULT_SYMBOL):
    df = _read_table(
        "orderbook_insights",
        {
            "select": "*",
            "symbol": f"eq.{symbol}",
            "order": "timestamp.desc",
            "limit": 1,
        },
    )

    return df.iloc[0].to_dict() if not df.empty else {}


def _prepare_option_chain(df):
    if df.empty:
        return df

    df = df.copy()

    numeric_cols = [
        "strike",
        "mark_price",
        "oi",
        "volume",
        "iv",
        "delta",
        "gamma",
        "theta",
        "vega",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _chain_metrics(option_df):
    if option_df.empty or "option_type" not in option_df.columns:
        return {}

    calls = option_df[option_df["option_type"] == "call_options"].copy()
    puts = option_df[option_df["option_type"] == "put_options"].copy()

    total_call_oi = calls["oi"].sum() if "oi" in calls.columns else 0
    total_put_oi = puts["oi"].sum() if "oi" in puts.columns else 0
    pcr = total_put_oi / total_call_oi if total_call_oi else None

    highest_call_oi = (
        calls.sort_values("oi", ascending=False).head(1)
        if "oi" in calls.columns
        else pd.DataFrame()
    )
    highest_put_oi = (
        puts.sort_values("oi", ascending=False).head(1)
        if "oi" in puts.columns
        else pd.DataFrame()
    )

    return {
        "pcr": pcr,
        "net_delta": option_df["delta"].sum() if "delta" in option_df else None,
        "net_gamma": option_df["gamma"].sum() if "gamma" in option_df else None,
        "net_theta": option_df["theta"].sum() if "theta" in option_df else None,
        "median_iv": option_df["iv"].median() if "iv" in option_df else None,
        "highest_call_oi_strike": (
            highest_call_oi["strike"].iloc[0] if not highest_call_oi.empty else None
        ),
        "highest_put_oi_strike": (
            highest_put_oi["strike"].iloc[0] if not highest_put_oi.empty else None
        ),
    }


def _chain_positioning(option_df, spot_price, analytics):
    context = {
        "call_wall": None,
        "put_wall": None,
        "upside_resistance": "Unknown",
        "downside_support": "Unknown",
        "range_regime": False,
        "notes": [],
    }

    required_cols = {"option_type", "strike", "oi"}

    if option_df.empty or not spot_price or not required_cols.issubset(option_df.columns):
        return context

    calls = option_df[
        (option_df["option_type"] == "call_options") & (option_df["strike"] > spot_price)
    ].copy()
    puts = option_df[
        (option_df["option_type"] == "put_options") & (option_df["strike"] < spot_price)
    ].copy()

    if not calls.empty:
        call_wall = calls.sort_values("oi", ascending=False).iloc[0]
        context["call_wall"] = _safe_float(call_wall.get("strike"))
        context["notes"].append(f"call wall near {_format_strike(context['call_wall'])}")

    if not puts.empty:
        put_wall = puts.sort_values("oi", ascending=False).iloc[0]
        context["put_wall"] = _safe_float(put_wall.get("strike"))
        context["notes"].append(f"put wall near {_format_strike(context['put_wall'])}")

    if context["put_wall"] and context["call_wall"]:
        if context["put_wall"] <= spot_price <= context["call_wall"]:
            context["range_regime"] = True
            context["notes"].append("spot is trapped between put and call walls")

    call_oi_above = calls["oi"].sum() if "oi" in calls else 0
    put_oi_below = puts["oi"].sum() if "oi" in puts else 0

    if put_oi_below and call_oi_above / put_oi_below >= 1.35:
        context["upside_resistance"] = "Strong"
        context["notes"].append("call OI above spot is much heavier than put OI below spot")
    elif call_oi_above:
        context["upside_resistance"] = "Present"

    if call_oi_above and put_oi_below / call_oi_above >= 1.35:
        context["downside_support"] = "Strong"
        context["notes"].append("put OI below spot is much heavier than call OI above spot")
    elif put_oi_below:
        context["downside_support"] = "Present"

    expected_upper = analytics.get("expected_move_upper")
    expected_lower = analytics.get("expected_move_lower")

    if expected_upper and context["call_wall"] and context["call_wall"] >= expected_upper:
        context["notes"].append("call wall sits outside the expected move upper boundary")

    if expected_lower and context["put_wall"] and context["put_wall"] <= expected_lower:
        context["notes"].append("put wall sits outside the expected move lower boundary")

    return context


def _latest_analytics(expiry_label):
    df = _latest_rows_for_expiry("analytics_snapshots", expiry_label, limit=50)

    if df.empty:
        return {}

    row = df.iloc[0].to_dict()

    for col in [
        "spot_price",
        "max_pain",
        "atm_strike",
        "pcr",
        "atm_straddle_price",
        "expected_move_pct",
        "expected_move_upper",
        "expected_move_lower",
    ]:
        if col in row:
            row[col] = _safe_float(row.get(col))

    return row


def _latest_premium_decay(expiry_label):
    df = _latest_rows_for_expiry("premium_decay_snapshots", expiry_label, limit=50)

    if df.empty:
        return {}

    row = df.iloc[0].to_dict()

    for col in ["atm_strike", "atm_ce_price", "atm_pe_price", "atm_straddle_price"]:
        if col in row:
            row[col] = _safe_float(row.get(col))

    return row


def _latest_premium_pair(expiry_label):
    latest_df, previous_df = _latest_snapshot_pair_for_expiry(
        "premium_decay_snapshots",
        expiry_label,
        limit=100,
    )

    def clean_row(df):
        if df.empty:
            return {}

        row = df.iloc[0].to_dict()

        for col in ["atm_strike", "atm_ce_price", "atm_pe_price", "atm_straddle_price"]:
            if col in row:
                row[col] = _safe_float(row.get(col))

        return row

    return clean_row(latest_df), clean_row(previous_df)


def _expiry_profile(expiry_label):
    profile = {
        "expiry_timestamp": None,
        "days_to_expiry": None,
        "bucket": "UNKNOWN",
        "notes": [],
    }

    expiry_time = pd.to_datetime(expiry_label, utc=True, errors="coerce")

    if pd.isna(expiry_time):
        profile["notes"].append("expiry could not be parsed")
        return profile

    now = pd.Timestamp(_utc_now())
    hours_to_expiry = max(0, (expiry_time - now).total_seconds() / 3600)
    days_to_expiry = hours_to_expiry / 24

    profile["expiry_timestamp"] = expiry_time.isoformat()
    profile["days_to_expiry"] = days_to_expiry

    if hours_to_expiry <= 24:
        profile["bucket"] = "0DTE"
    elif days_to_expiry <= 1.75:
        profile["bucket"] = "D1"
    elif days_to_expiry <= 3.75:
        profile["bucket"] = "D3"
    elif days_to_expiry <= 10:
        profile["bucket"] = "WEEKLY"
    else:
        profile["bucket"] = "MONTHLY"

    profile["notes"].append(f"expiry bucket is {profile['bucket']}")
    return profile


def _realized_volatility_pct(ohlcv_df):
    if ohlcv_df.empty or len(ohlcv_df) < 20:
        return None

    data = ohlcv_df.tail(60).copy()
    data["range_pct"] = ((data["high"] - data["low"]) / data["close"]) * 100

    return data["range_pct"].mean()


def _recent_structure(events_df):
    if events_df.empty:
        return "neutral", None

    structure = events_df[events_df["event_type"].isin(["bos", "choch"])].copy()

    if structure.empty:
        return "neutral", None

    last_event = structure.sort_values("event_time").iloc[-1]
    direction = last_event.get("direction", "neutral")
    event_type = last_event.get("event_type")

    return direction, event_type


def _pct_distance(price, reference):
    price = _safe_float(price)
    reference = _safe_float(reference)

    if price is None or reference in [None, 0]:
        return None

    return abs(price - reference) / reference


def _near_active_zone(zones_df, spot_price, zone_type=None, pct_window=0.006):
    if zones_df.empty or not spot_price:
        return pd.DataFrame()

    data = zones_df.copy()

    if zone_type:
        data = data[data["zone_type"] == zone_type]

    if data.empty:
        return data

    lower = spot_price * (1 - pct_window)
    upper = spot_price * (1 + pct_window)

    return data[(data["price_low"] <= upper) & (data["price_high"] >= lower)].copy()


def _profile_context(profile_df, spot_price, analytics):
    context = {
        "poc_price": None,
        "near_hvn": False,
        "near_lvn": False,
        "expiry_magnet": False,
        "breakout_risk": 0,
        "notes": [],
        "warnings": [],
    }

    if (
        profile_df.empty
        or not spot_price
        or "volume" not in profile_df.columns
        or "price_level" not in profile_df.columns
    ):
        return context

    profile = profile_df.copy()
    profile["price_level"] = pd.to_numeric(profile["price_level"], errors="coerce")
    profile["volume"] = pd.to_numeric(profile["volume"], errors="coerce")
    profile = profile.dropna(subset=["price_level", "volume"])

    if profile.empty:
        return context

    poc = profile.loc[profile["volume"].idxmax()]
    context["poc_price"] = poc.get("price_level")

    volume_rank = profile["volume"].rank(pct=True)
    profile["volume_rank"] = volume_rank
    nearby = profile[
        profile["price_level"].between(spot_price * 0.995, spot_price * 1.005)
    ]

    if not nearby.empty:
        if nearby["volume_rank"].max() >= 0.75:
            context["near_hvn"] = True
            context["notes"].append("spot is near a high-volume node")

        if nearby["volume_rank"].min() <= 0.25:
            context["near_lvn"] = True
            context["breakout_risk"] += 15
            context["warnings"].append("Spot is near a low-volume node; breakout risk is higher.")

    max_pain = analytics.get("max_pain")
    expected_move = analytics.get("atm_straddle_price") or spot_price * 0.035

    if max_pain and context["poc_price"]:
        if abs(max_pain - context["poc_price"]) <= expected_move * 0.25:
            context["expiry_magnet"] = True
            context["notes"].append("max pain and volume POC are in the same zone")

    upper = analytics.get("expected_move_upper")
    lower = analytics.get("expected_move_lower")

    for boundary in [upper, lower]:
        if boundary is None:
            continue

        near_boundary = profile[
            profile["price_level"].between(boundary * 0.995, boundary * 1.005)
        ]

        if not near_boundary.empty and near_boundary["volume_rank"].min() <= 0.25:
            context["breakout_risk"] += 20
            context["warnings"].append(
                "Expected-move boundary is near a low-volume area; a break may accelerate."
            )
            break

    return context


def _pinning_score(analytics, chain, spot_price, profile_context=None, smc_context=None):
    max_pain = analytics.get("max_pain")
    atm_strike = analytics.get("atm_strike")
    expected_move = analytics.get("atm_straddle_price")
    expected_upper = analytics.get("expected_move_upper")
    expected_lower = analytics.get("expected_move_lower")
    call_wall = chain.get("highest_call_oi_strike")
    put_wall = chain.get("highest_put_oi_strike")
    net_gamma = chain.get("net_gamma")
    pcr = analytics.get("pcr") or chain.get("pcr")

    score = 20

    if not spot_price or not max_pain:
        return score

    if _pct_distance(spot_price, max_pain) is not None:
        if _pct_distance(spot_price, max_pain) <= 0.005:
            score += 25

    if atm_strike and _pct_distance(spot_price, atm_strike) is not None:
        if _pct_distance(spot_price, atm_strike) <= 0.005:
            score += 20

    if expected_lower and expected_upper and expected_lower <= spot_price <= expected_upper:
        score += 20

    if pcr is not None and 0.8 <= pcr <= 1.2:
        score += 15

    reference_move = expected_move if expected_move else spot_price * 0.035
    distance_to_pain = abs(spot_price - max_pain)

    if distance_to_pain <= reference_move * 0.20:
        score += 35
    elif distance_to_pain <= reference_move * 0.40:
        score += 25
    elif distance_to_pain <= reference_move * 0.70:
        score += 12

    if atm_strike and abs(max_pain - atm_strike) <= reference_move * 0.35:
        score += 15

    if call_wall and put_wall and put_wall <= spot_price <= call_wall:
        score += 15

    if net_gamma is not None and net_gamma > 0:
        score += 10

    if profile_context and profile_context.get("near_hvn"):
        score += 15

    if profile_context and profile_context.get("expiry_magnet"):
        score += 10

    has_profile_breakout_risk = profile_context and profile_context.get("breakout_risk", 0) >= 20
    has_smc_break_risk = smc_context and smc_context.get("zone_break_risk")

    if has_profile_breakout_risk:
        score -= 20

    if has_smc_break_risk:
        score -= 25

    if has_profile_breakout_risk or has_smc_break_risk:
        score = min(score, 65)

    if has_profile_breakout_risk and has_smc_break_risk:
        score = min(score, 55)

    return int(_bounded_score(score))


def _volatility_regime(analytics, chain, realized_vol_pct):
    expected_move_pct = analytics.get("expected_move_pct")
    median_iv = chain.get("median_iv")

    if expected_move_pct is not None:
        if expected_move_pct < 2:
            return "Compressed"
        if expected_move_pct > 5:
            return "Elevated"

    if realized_vol_pct is not None:
        if realized_vol_pct < 0.35:
            return "Compressed"
        if realized_vol_pct > 0.9:
            return "Elevated"

    if median_iv is not None:
        if median_iv < 45:
            return "Compressed"
        if median_iv > 80:
            return "Elevated"

    return "Normal"


def _volatility_context(analytics, premium, previous_premium, option_df, previous_option_df, realized_vol_pct):
    context = {
        "regime": _volatility_regime(analytics, chain={}, realized_vol_pct=realized_vol_pct),
        "option_selling_environment": "Neutral",
        "gamma_risk": False,
        "notes": [],
        "warnings": [],
    }

    expected_move_pct = analytics.get("expected_move_pct")
    current_straddle = premium.get("atm_straddle_price") or analytics.get("atm_straddle_price")
    previous_straddle = previous_premium.get("atm_straddle_price")

    current_iv = option_df["iv"].median() if not option_df.empty and "iv" in option_df else None
    previous_iv = (
        previous_option_df["iv"].median()
        if not previous_option_df.empty and "iv" in previous_option_df
        else None
    )

    current_gamma = option_df["gamma"].abs().median() if not option_df.empty and "gamma" in option_df else None
    previous_gamma = (
        previous_option_df["gamma"].abs().median()
        if not previous_option_df.empty and "gamma" in previous_option_df
        else None
    )
    current_theta = option_df["theta"].abs().median() if not option_df.empty and "theta" in option_df else None

    straddle_falling = (
        current_straddle is not None
        and previous_straddle is not None
        and current_straddle < previous_straddle
    )
    straddle_rising = (
        current_straddle is not None
        and previous_straddle is not None
        and current_straddle > previous_straddle
    )
    iv_rising = current_iv is not None and previous_iv is not None and current_iv > previous_iv
    gamma_rising = (
        current_gamma is not None
        and previous_gamma is not None
        and previous_gamma > 0
        and current_gamma >= previous_gamma * 1.25
    )

    if expected_move_pct is not None and expected_move_pct < 2 and straddle_falling:
        context["regime"] = "Compression / Short Vol Favored"
        context["notes"].append("expected move is low and ATM straddle is falling")
    elif straddle_rising and iv_rising:
        context["regime"] = "Expansion / Long Vol Favored"
        context["notes"].append("ATM straddle and IV are rising")
    else:
        context["regime"] = _volatility_regime(
            {**analytics, "expected_move_pct": expected_move_pct},
            {"median_iv": current_iv},
            realized_vol_pct,
        )

    if current_theta is not None and current_gamma is not None:
        if current_theta >= 1 and current_gamma <= 0.0025:
            context["option_selling_environment"] = "Favorable"
            context["notes"].append("theta is high while gamma is low/moderate")
        elif current_gamma > 0.004:
            context["option_selling_environment"] = "Unfavorable"

    if gamma_rising:
        context["gamma_risk"] = True
        context["warnings"].append("Short option risk is increasing because gamma is rising sharply.")

    return context


def _price_action_context(ohlcv_df):
    context = {
        "momentum": "Neutral",
        "regime": None,
        "notes": [],
    }

    if ohlcv_df.empty or len(ohlcv_df) < 5:
        return context

    data = ohlcv_df.copy()
    for col in ["open", "high", "low", "close", "volume"]:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")

    data = data.dropna(subset=["high", "low", "close", "volume"])

    if len(data) < 5:
        return context

    last = data.iloc[-1]
    prev = data.iloc[-2]
    recent = data.tail(5).copy()
    prior = data.tail(10).head(5).copy() if len(data) >= 10 else data.head(len(data) - 5).copy()

    volume_rising = last["volume"] > prev["volume"]

    if last["close"] > prev["close"] and volume_rising:
        context["momentum"] = "Bullish"
        context["notes"].append("close is higher with rising volume")
    elif last["close"] < prev["close"] and volume_rising:
        context["momentum"] = "Bearish"
        context["notes"].append("close is lower with rising volume")

    recent_range = (recent["high"] - recent["low"]).mean()
    prior_range = (prior["high"] - prior["low"]).mean() if not prior.empty else None
    recent_volume = recent["volume"].mean()
    prior_volume = prior["volume"].mean() if not prior.empty else None

    if prior_range and prior_volume:
        if recent_range < prior_range * 0.75 and recent_volume < prior_volume:
            context["regime"] = "Compression"
            context["notes"].append("candle range is shrinking while volume is falling")
        elif recent_range > prior_range * 1.35 and recent_volume > prior_volume * 1.25:
            context["regime"] = "Expansion"
            context["notes"].append("range expanded with a volume spike")

    return context


def _smc_context(zones_df, spot_price, best_call_sell_strike=None, best_put_sell_strike=None):
    context = {
        "inside_demand": False,
        "inside_supply": False,
        "zone_break_risk": False,
        "call_sell_confidence": "Normal",
        "put_sell_confidence": "Normal",
        "notes": [],
        "warnings": [],
    }

    if zones_df.empty or not spot_price:
        return context

    active = zones_df[zones_df["status"] == "active"].copy() if "status" in zones_df else zones_df.copy()

    if active.empty or "price_low" not in active.columns or "price_high" not in active.columns:
        return context

    active["price_low"] = pd.to_numeric(active["price_low"], errors="coerce")
    active["price_high"] = pd.to_numeric(active["price_high"], errors="coerce")
    active = active.dropna(subset=["price_low", "price_high"])

    if active.empty:
        return context

    inside = active[(active["price_low"] <= spot_price) & (active["price_high"] >= spot_price)]

    for _, zone in inside.iterrows():
        zone_type = str(zone.get("zone_type", "")).lower()
        direction = str(zone.get("direction", "")).lower()

        if "demand" in zone_type or direction == "bullish":
            context["inside_demand"] = True
            context["notes"].append("spot is inside an active demand/support zone")

        if "supply" in zone_type or direction == "bearish":
            context["inside_supply"] = True
            context["notes"].append("spot is inside an active supply/resistance zone")

    def near_strike(strike):
        if strike is None:
            return pd.DataFrame()

        lower = strike * 0.995
        upper = strike * 1.005
        return active[(active["price_low"] <= upper) & (active["price_high"] >= lower)]

    call_zone = near_strike(best_call_sell_strike)
    if not call_zone.empty and "direction" in call_zone:
        bearish_zone = call_zone[
            call_zone["direction"].astype(str).str.lower().eq("bearish")
        ]
        if not bearish_zone.empty:
            context["call_sell_confidence"] = "High"
            context["notes"].append("strong supply zone is near the call sell strike")

    put_zone = near_strike(best_put_sell_strike)
    if not put_zone.empty and "direction" in put_zone:
        bullish_zone = put_zone[
            put_zone["direction"].astype(str).str.lower().eq("bullish")
        ]
        if not bullish_zone.empty:
            context["put_sell_confidence"] = "High"
            context["notes"].append("strong demand zone is near the put sell strike")

    invalidation_risk = pd.DataFrame()
    if "direction" in active:
        window = spot_price * 0.006
        invalidation_risk = active[
            (
                (active["direction"].astype(str).str.lower() == "bullish")
                & (spot_price < active["price_low"])
                & ((active["price_low"] - spot_price) <= window)
            )
            | (
                (active["direction"].astype(str).str.lower() == "bearish")
                & (spot_price > active["price_high"])
                & ((spot_price - active["price_high"]) <= window)
            )
        ]

    if not invalidation_risk.empty:
        context["zone_break_risk"] = True
        context["warnings"].append(
            "Spot is just beyond a nearby active SMC zone; treat the setup as breakout/invalidation risk."
        )

    return context


def _directional_bias(
    chain,
    orderbook,
    structure_direction,
    analytics,
    price_action=None,
    smc_context=None,
    chain_context=None,
):
    score = 0
    notes = []

    net_delta = chain.get("net_delta")
    pcr = chain.get("pcr") or analytics.get("pcr")
    orderbook_bias = orderbook.get("bias")

    if net_delta is not None:
        if net_delta > 1:
            score += 2
            notes.append("positive net delta")
        elif net_delta < -1:
            score -= 2
            notes.append("negative net delta")

    if pcr is not None:
        if pcr > 1.3:
            score += 1
            notes.append("put OI dominance")
        elif pcr < 0.7:
            score -= 1
            notes.append("call OI dominance")

    if structure_direction == "bullish":
        score += 2
        notes.append("bullish SMC structure")
    elif structure_direction == "bearish":
        score -= 2
        notes.append("bearish SMC structure")

    if orderbook_bias == "Mild Bullish":
        score += 1
        notes.append("bid-side orderbook support")
    elif orderbook_bias == "Mild Bearish":
        score -= 1
        notes.append("ask-side orderbook pressure")

    if price_action:
        if price_action.get("momentum") == "Bullish":
            score += 1
            notes.append("bullish price/volume momentum")
        elif price_action.get("momentum") == "Bearish":
            score -= 1
            notes.append("bearish price/volume momentum")

    if smc_context:
        if smc_context.get("inside_demand") and orderbook_bias == "Mild Bullish":
            score += 2
            notes.append("spot near SMC demand with bullish orderbook")
        if smc_context.get("inside_supply") and orderbook_bias == "Mild Bearish":
            score -= 2
            notes.append("spot near SMC supply with bearish orderbook")

    if chain_context:
        if chain_context.get("upside_resistance") == "Strong":
            score -= 1
            notes.append("strong upside OI resistance")
        if chain_context.get("downside_support") == "Strong":
            score += 1
            notes.append("strong downside OI support")

    if score >= 3:
        return "Bullish", notes
    if score <= -3:
        return "Bearish", notes
    if score > 0:
        return "Mild Bullish", notes
    if score < 0:
        return "Mild Bearish", notes

    return "Neutral", notes


def _signal_value(label):
    if label in ["Bullish", "Mild Bullish"]:
        return 1
    if label in ["Bearish", "Mild Bearish"]:
        return -1
    return 0


def _signal_conflict_score(
    directional_bias,
    structure_direction,
    orderbook,
    chain_context,
    volatility_context,
    price_action,
    smc_context,
    profile_context,
    pinning_score,
):
    signals = []

    def add_signal(name, direction, weight, reason):
        if direction == 0:
            return
        signals.append(
            {
                "name": name,
                "direction": direction,
                "weight": weight,
                "reason": reason,
            }
        )

    add_signal("trend", _signal_value(directional_bias), 2.0, directional_bias)

    if structure_direction == "bullish":
        add_signal("smc_structure", 1, 1.5, "bullish SMC structure")
    elif structure_direction == "bearish":
        add_signal("smc_structure", -1, 1.5, "bearish SMC structure")

    orderbook_bias = orderbook.get("bias")
    add_signal("orderbook", _signal_value(orderbook_bias), 1.0, orderbook_bias)

    if chain_context.get("upside_resistance") == "Strong":
        add_signal("option_chain", -1, 1.0, "strong upside OI resistance")
    if chain_context.get("downside_support") == "Strong":
        add_signal("option_chain", 1, 1.0, "strong downside OI support")

    if volatility_context.get("regime") == "Expansion / Long Vol Favored":
        add_signal("volatility", _signal_value(directional_bias), 1.0, "long-vol expansion")
    elif volatility_context.get("regime") == "Compression / Short Vol Favored":
        add_signal("volatility", 0, 1.0, "short-vol compression")

    momentum = price_action.get("momentum")
    if momentum == "Bullish":
        add_signal("momentum", 1, 1.2, "bullish price/volume momentum")
    elif momentum == "Bearish":
        add_signal("momentum", -1, 1.2, "bearish price/volume momentum")

    if smc_context.get("inside_demand"):
        add_signal("smc_zone", 1, 1.2, "inside active demand zone")
    if smc_context.get("inside_supply"):
        add_signal("smc_zone", -1, 1.2, "inside active supply zone")

    if profile_context.get("expiry_magnet") or profile_context.get("near_hvn"):
        add_signal("volume_profile", 0, 1.0, "volume profile supports pinning")

    if pinning_score >= 70:
        add_signal("pinning", 0, 1.4, "high pinning/max-pain score")

    bullish_weight = sum(s["weight"] for s in signals if s["direction"] > 0)
    bearish_weight = sum(s["weight"] for s in signals if s["direction"] < 0)
    directional_total = bullish_weight + bearish_weight

    score = 0
    notes = []

    if directional_total > 0:
        opposing = min(bullish_weight, bearish_weight)
        score += (opposing / directional_total) * 70

        if bullish_weight and bearish_weight:
            notes.append("bullish and bearish signal groups disagree")

    if smc_context.get("zone_break_risk"):
        score += 18
        notes.append("SMC invalidation risk conflicts with clean pinning")

    if profile_context.get("breakout_risk", 0) >= 20:
        score += 15
        notes.append("LVN/boundary breakout risk conflicts with range assumptions")

    if volatility_context.get("gamma_risk") and pinning_score >= 60:
        score += 12
        notes.append("rising gamma conflicts with short-vol/pinning assumptions")

    if pinning_score >= 70 and volatility_context.get("regime") == "Expansion / Long Vol Favored":
        score += 15
        notes.append("pinning and volatility expansion disagree")

    if not notes:
        notes.append("major signal groups are broadly aligned")

    return int(_bounded_score(score)), notes


def _market_regime(
    volatility_regime,
    directional_bias,
    pinning_score,
    recent_structure_type,
    price_action=None,
    chain_context=None,
    profile_context=None,
    smc_context=None,
):
    if smc_context and smc_context.get("zone_break_risk"):
        return "Breakout / Invalidation Risk"

    if profile_context and profile_context.get("breakout_risk", 0) >= 20:
        return "Breakout Risk"

    if volatility_regime == "Expansion / Long Vol Favored":
        return "Directional Expansion"

    if price_action and price_action.get("regime") == "Expansion":
        return "Directional Expansion"

    if pinning_score >= 70 and volatility_regime != "Elevated":
        return "Pinning / Range"

    if volatility_regime == "Elevated" and directional_bias in ["Bullish", "Bearish"]:
        return "Directional Expansion"

    if chain_context and chain_context.get("range_regime") and pinning_score >= 50:
        return "Pinning / Range"

    if recent_structure_type == "choch":
        return "Transition / Reversal Watch"

    if directional_bias in ["Mild Bullish", "Mild Bearish", "Neutral"]:
        return "Balanced / Two-Sided"

    return "Directional"


def _apply_expiry_adjustments(
    strategy,
    confidence_score,
    expiry_profile,
    market_regime,
    volatility_context,
    profile_context,
    smc_context,
    conflict_score,
):
    adjusted_strategy = strategy
    adjusted_confidence = confidence_score
    notes = []
    bucket = expiry_profile.get("bucket")
    gamma_risk = volatility_context.get("gamma_risk")
    breakout_risk = (
        market_regime in ["Breakout Risk", "Breakout / Invalidation Risk"]
        or profile_context.get("breakout_risk", 0) >= 20
        or smc_context.get("zone_break_risk")
    )
    short_vol_strategies = [
        "Iron Fly",
        "Iron Condor",
        "Short Strangle",
        "Short Straddle with Hedge",
    ]

    if bucket == "0DTE":
        if adjusted_strategy in short_vol_strategies and breakout_risk:
            adjusted_strategy = "Wait / Defined-Risk Spread Only"
            adjusted_confidence -= 20
            notes.append("0DTE short-vol setup blocked because breakout risk is active")
        elif gamma_risk:
            adjusted_confidence -= 12
            notes.append("0DTE gamma risk reduces confidence")

    if bucket in ["D1", "D3"] and gamma_risk:
        if adjusted_strategy in short_vol_strategies:
            adjusted_strategy = "Debit Spread"
            notes.append("near-expiry gamma risk favors directional debit spread over short premium")
        adjusted_confidence -= 8

    if bucket in ["WEEKLY", "MONTHLY"] and adjusted_strategy in ["Iron Condor", "Debit Spread"]:
        notes.append(f"{bucket.lower()} expiry allows wider wings and more patient management")

    if conflict_score >= 65:
        adjusted_confidence -= 20
        if adjusted_strategy not in ["No Trade", "Wait / Defined-Risk Spread Only"]:
            adjusted_strategy = "Wait / Defined-Risk Spread Only"
        notes.append("high signal conflict blocks aggressive strategy selection")
    elif conflict_score >= 45:
        adjusted_confidence -= 10
        notes.append("moderate signal conflict reduces confidence")

    return adjusted_strategy, int(_bounded_score(adjusted_confidence)), notes


def _trap_risk(orderbook, zones_df, spot_price):
    risk_score = 20
    warnings = []

    trap_label = str(orderbook.get("trap_risk", ""))
    spread_quality = orderbook.get("spread_quality")
    execution_signal = str(orderbook.get("execution_signal", ""))
    imbalance = _safe_float(orderbook.get("imbalance_ratio"))

    if "High" in trap_label:
        risk_score += 40
        warnings.append("Order book trap risk is high; avoid naked directional trades.")
    elif "Medium" in trap_label:
        risk_score += 25
        warnings.append("Order book has a concentrated visible wall; spoofing risk is elevated.")

    if spread_quality == "Poor":
        risk_score += 20
        warnings.append("Spread quality is poor; avoid market orders.")
    elif spread_quality == "Average":
        risk_score += 8

    if imbalance is not None and (imbalance > 1.8 or imbalance < 0.55):
        risk_score += 15
        warnings.append("Order book imbalance is stretched and may reverse quickly.")

    if "Weak" in execution_signal:
        risk_score += 10
        warnings.append("Execution signal is weak; reduce confidence in entries.")

    nearby_liquidity = _near_active_zone(zones_df, spot_price, pct_window=0.005)
    nearby_liquidity = nearby_liquidity[
        nearby_liquidity["zone_type"].isin(["buy_side_liquidity", "sell_side_liquidity"])
    ] if not nearby_liquidity.empty else nearby_liquidity

    if not nearby_liquidity.empty:
        risk_score += 20
        warnings.append("Price is close to an active liquidity zone; sweep risk is present.")

    if risk_score >= 70:
        return "High", warnings
    if risk_score >= 45:
        return "Medium", warnings

    return "Low", warnings


def _sell_strike_score(
    row,
    spot_price,
    expected_move,
    option_type,
    expected_upper=None,
    expected_lower=None,
):
    strike = _safe_float(row.get("strike"), 0)
    premium = _safe_float(row.get("mark_price"), 0)
    oi = _safe_float(row.get("oi"), 0)
    delta = abs(_safe_float(row.get("delta"), 0))
    gamma = abs(_safe_float(row.get("gamma"), 0))

    if not spot_price or not strike:
        return -1

    distance = abs(strike - spot_price)
    min_distance = expected_move * 0.65 if expected_move else spot_price * 0.025

    if option_type == "call_options" and strike <= spot_price:
        return -1

    if option_type == "put_options" and strike >= spot_price:
        return -1

    distance_score = 35 if distance >= min_distance else 15
    boundary_score = 0

    if option_type == "call_options" and expected_upper:
        boundary_score = 20 if strike >= expected_upper else -10

    if option_type == "put_options" and expected_lower:
        boundary_score = 20 if strike <= expected_lower else -10

    delta_score = 30 if 0.05 <= delta <= 0.22 else 12
    oi_score = min(20, oi / 10)
    premium_score = min(10, premium / 2)
    gamma_score = 5 if gamma <= 0.002 else 1

    return distance_score + boundary_score + delta_score + oi_score + premium_score + gamma_score


def _best_sell_strikes(option_df, spot_price, expected_move, analytics=None):
    if option_df.empty or "option_type" not in option_df.columns:
        return None, None

    analytics = analytics or {}
    expected_upper = analytics.get("expected_move_upper")
    expected_lower = analytics.get("expected_move_lower")
    results = {}

    for option_type, key in [
        ("call_options", "call"),
        ("put_options", "put"),
    ]:
        candidates = option_df[option_df["option_type"] == option_type].copy()

        if candidates.empty:
            results[key] = None
            continue

        candidates["rule_score"] = candidates.apply(
            lambda row: _sell_strike_score(
                row,
                spot_price,
                expected_move,
                option_type,
                expected_upper,
                expected_lower,
            ),
            axis=1,
        )

        candidates = candidates[candidates["rule_score"] >= 0]

        if candidates.empty:
            results[key] = None
            continue

        best = candidates.sort_values(
            ["rule_score", "oi", "mark_price"],
            ascending=[False, False, False],
        ).iloc[0]

        results[key] = _safe_float(best.get("strike"))

    return results.get("call"), results.get("put")


def _format_strike(strike):
    strike = _safe_float(strike)

    if strike is None:
        return "NA"

    if float(strike).is_integer():
        return str(int(strike))

    return str(round(strike, 2))


def _nearest_strike(strikes, target):
    clean_strikes = [_safe_float(strike) for strike in strikes]
    clean_strikes = [strike for strike in clean_strikes if strike is not None]

    if not clean_strikes or target is None:
        return None

    return min(clean_strikes, key=lambda strike: abs(strike - target))


def _hedge_strike(option_df, option_type, sell_strike):
    if option_df.empty or sell_strike is None:
        return None

    strikes = sorted(
        option_df[option_df["option_type"] == option_type]["strike"].dropna().unique()
    )

    if option_type == "call_options":
        hedge_candidates = [strike for strike in strikes if strike > sell_strike]
        return hedge_candidates[0] if hedge_candidates else None

    if option_type == "put_options":
        hedge_candidates = [strike for strike in strikes if strike < sell_strike]
        return hedge_candidates[-1] if hedge_candidates else None

    return None


def _strategy_legs(
    strategy,
    option_df,
    spot_price,
    expected_move,
    atm_strike,
    best_call_sell_strike,
    best_put_sell_strike,
    directional_bias,
):
    if option_df.empty:
        return []

    if strategy == "No Trade":
        return []

    strikes = sorted(option_df["strike"].dropna().unique())
    atm = atm_strike or _nearest_strike(strikes, spot_price)
    move = expected_move or (spot_price * 0.035 if spot_price else None)

    call_buy_strike = _hedge_strike(option_df, "call_options", best_call_sell_strike)
    put_buy_strike = _hedge_strike(option_df, "put_options", best_put_sell_strike)

    if strategy == "Bear Call Credit Spread":
        return [
            {"action": "Sell", "strike": best_call_sell_strike, "option": "C"},
            {"action": "Buy", "strike": call_buy_strike, "option": "C"},
        ]

    if strategy == "Bull Put Credit Spread":
        return [
            {"action": "Sell", "strike": best_put_sell_strike, "option": "P"},
            {"action": "Buy", "strike": put_buy_strike, "option": "P"},
        ]

    if strategy == "Iron Condor":
        return [
            {"action": "Sell", "strike": best_call_sell_strike, "option": "C"},
            {"action": "Buy", "strike": call_buy_strike, "option": "C"},
            {"action": "Sell", "strike": best_put_sell_strike, "option": "P"},
            {"action": "Buy", "strike": put_buy_strike, "option": "P"},
        ]

    if strategy == "Short Strangle":
        return [
            {"action": "Sell", "strike": best_call_sell_strike, "option": "C"},
            {"action": "Sell", "strike": best_put_sell_strike, "option": "P"},
        ]

    if strategy in ["Iron Fly", "Short Straddle with Hedge"]:
        upper_wing = _nearest_strike(strikes, atm + move) if atm and move else call_buy_strike
        lower_wing = _nearest_strike(strikes, atm - move) if atm and move else put_buy_strike

        if strategy == "Iron Fly":
            return [
                {"action": "Sell", "strike": atm, "option": "C"},
                {"action": "Sell", "strike": atm, "option": "P"},
                {"action": "Buy", "strike": upper_wing, "option": "C"},
                {"action": "Buy", "strike": lower_wing, "option": "P"},
            ]

        return [
            {"action": "Sell", "strike": atm, "option": "C"},
            {"action": "Sell", "strike": atm, "option": "P"},
            {"action": "Optional Buy Hedge", "strike": upper_wing, "option": "C"},
            {"action": "Optional Buy Hedge", "strike": lower_wing, "option": "P"},
        ]

    if strategy == "Debit Spread":
        if directional_bias in ["Bullish", "Mild Bullish"]:
            buy_call = atm
            sell_call = best_call_sell_strike or _nearest_strike(strikes, atm + move)
            return [
                {"action": "Buy", "strike": buy_call, "option": "C"},
                {"action": "Sell", "strike": sell_call, "option": "C"},
            ]

        buy_put = atm
        sell_put = best_put_sell_strike or _nearest_strike(strikes, atm - move)
        return [
            {"action": "Buy", "strike": buy_put, "option": "P"},
            {"action": "Sell", "strike": sell_put, "option": "P"},
        ]

    if strategy == "Put Broken Wing Butterfly":
        middle_put = _nearest_strike(strikes, atm - (move * 0.5)) if atm and move else best_put_sell_strike
        lower_put = best_put_sell_strike or _nearest_strike(strikes, atm - move)
        far_lower_put = _nearest_strike(strikes, lower_put - move) if lower_put and move else put_buy_strike

        return [
            {"action": "Buy", "strike": atm, "option": "P"},
            {"action": "Sell", "strike": middle_put, "option": "P"},
            {"action": "Sell", "strike": lower_put, "option": "P"},
            {"action": "Buy", "strike": far_lower_put, "option": "P"},
        ]

    if strategy == "Wait / Defined-Risk Spread Only":
        if directional_bias in ["Mild Bearish", "Bearish"]:
            return [
                {"action": "Sell", "strike": best_call_sell_strike, "option": "C"},
                {"action": "Buy", "strike": call_buy_strike, "option": "C"},
            ]

        if directional_bias in ["Mild Bullish", "Bullish"]:
            return [
                {"action": "Sell", "strike": best_put_sell_strike, "option": "P"},
                {"action": "Buy", "strike": put_buy_strike, "option": "P"},
            ]

        return []

    return []


def _strategy_text(legs):
    lines = []

    for leg in legs:
        strike = _format_strike(leg.get("strike"))
        option = leg.get("option", "")
        lines.append(f"{leg.get('action')} - {strike} {option}".strip())

    return lines


def _has_executable_legs(legs):
    if not legs:
        return False

    return all(_safe_float(leg.get("strike")) is not None for leg in legs)


def _best_strategy(
    market_regime,
    volatility_regime,
    directional_bias,
    pinning_score,
    trap_risk,
    chain,
    price_action=None,
    chain_context=None,
    volatility_context=None,
    profile_context=None,
    smc_context=None,
):
    net_gamma = chain.get("net_gamma")
    momentum = price_action.get("momentum") if price_action else "Neutral"
    vol_label = volatility_context.get("regime") if volatility_context else volatility_regime
    option_selling_environment = (
        volatility_context.get("option_selling_environment")
        if volatility_context
        else "Neutral"
    )
    gamma_risk = volatility_context.get("gamma_risk") if volatility_context else False

    contradictory = (
        (directional_bias in ["Bullish", "Mild Bullish"] and momentum == "Bearish")
        or (directional_bias in ["Bearish", "Mild Bearish"] and momentum == "Bullish")
    )

    if trap_risk == "High" and contradictory:
        return "No Trade"

    if trap_risk == "High":
        return "Wait / Defined-Risk Spread Only"

    breakout_risk = (
        market_regime in ["Breakout Risk", "Breakout / Invalidation Risk"]
        or (profile_context and profile_context.get("breakout_risk", 0) >= 20)
        or (smc_context and smc_context.get("zone_break_risk"))
    )

    if breakout_risk:
        if momentum in ["Bullish", "Bearish"] and directional_bias in [
            "Bullish",
            "Bearish",
            "Mild Bullish",
            "Mild Bearish",
        ]:
            return "Debit Spread"
        return "Wait / Defined-Risk Spread Only"

    long_vol_risk = (
        vol_label == "Expansion / Long Vol Favored"
        or gamma_risk
        or option_selling_environment == "Unfavorable"
    )

    if long_vol_risk:
        if momentum in ["Bullish", "Bearish"] and directional_bias in [
            "Bullish",
            "Bearish",
            "Mild Bullish",
            "Mild Bearish",
        ]:
            return "Debit Spread"
        return "Wait / Defined-Risk Spread Only"

    if (
        pinning_score > 70
        and vol_label in ["Compressed", "Compression / Short Vol Favored", "Normal"]
    ):
        if net_gamma is not None and net_gamma > 0:
            return "Iron Fly"
        return "Short Straddle with Hedge"

    if (
        50 <= pinning_score <= 70
        and chain_context
        and chain_context.get("range_regime")
    ):
        return "Iron Condor"

    if market_regime == "Pinning / Range" and volatility_regime == "Normal":
        return "Iron Condor"

    if vol_label in ["Compressed", "Compression / Short Vol Favored"] and directional_bias == "Neutral":
        return "Short Strangle"

    if (
        vol_label in ["Elevated", "Expansion / Long Vol Favored"]
        and momentum in ["Bullish", "Bearish"]
        and directional_bias in ["Bullish", "Bearish", "Mild Bullish", "Mild Bearish"]
    ):
        return "Debit Spread"

    if vol_label == "Elevated" and directional_bias in ["Mild Bearish", "Bearish"]:
        return "Put Broken Wing Butterfly"

    if directional_bias in ["Mild Bullish", "Bullish"]:
        return "Bull Put Credit Spread"

    if directional_bias in ["Mild Bearish", "Bearish"]:
        return "Bear Call Credit Spread"

    return "Iron Condor"


def _confidence_score(
    data_flags,
    directional_bias,
    market_regime,
    trap_risk,
    pinning_score,
    orderbook=None,
    volatility_context=None,
    price_action=None,
    smc_context=None,
    profile_context=None,
):
    score = 25

    score += sum(8 for available in data_flags.values() if available)

    if directional_bias in ["Bullish", "Bearish", "Neutral"]:
        score += 8

    if market_regime in ["Pinning / Range", "Directional Expansion"]:
        score += 8

    if pinning_score >= 70:
        score += 7

    if trap_risk == "Medium":
        score -= 10
    elif trap_risk == "High":
        score -= 25

    if orderbook and "Weak" in str(orderbook.get("execution_signal", "")):
        score -= 15

    if volatility_context and volatility_context.get("gamma_risk"):
        score -= 8

    if price_action and price_action.get("momentum") in ["Bullish", "Bearish"]:
        score += 4

    if smc_context:
        if smc_context.get("inside_demand") or smc_context.get("inside_supply"):
            score += 4
        if smc_context.get("zone_break_risk"):
            score -= 25

    if profile_context and profile_context.get("breakout_risk", 0) >= 20:
        score -= min(25, profile_context.get("breakout_risk", 0))

    if market_regime in ["Breakout Risk", "Breakout / Invalidation Risk"]:
        score = min(score, 70)

    if (
        smc_context
        and smc_context.get("zone_break_risk")
        and profile_context
        and profile_context.get("breakout_risk", 0) >= 20
    ):
        score = min(score, 60)

    return int(_bounded_score(score))


def _leg_price(option_df, strike, option_code):
    strike = _safe_float(strike)

    if option_df.empty or strike is None or "option_type" not in option_df.columns:
        return None

    option_type = "call_options" if option_code == "C" else "put_options"
    data = option_df[
        (option_df["option_type"] == option_type)
        & (option_df["strike"].astype(float) == float(strike))
    ]

    if data.empty or "mark_price" not in data.columns:
        return None

    return _safe_float(data.iloc[0].get("mark_price"))


def _strategy_pricing(option_df, strategy_legs):
    priced_legs = []
    net_credit = 0
    net_debit = 0

    for leg in strategy_legs:
        price = _leg_price(option_df, leg.get("strike"), leg.get("option"))
        priced_leg = dict(leg)
        priced_leg["mark_price"] = price
        priced_legs.append(priced_leg)

        if price is None:
            continue

        action = str(leg.get("action", "")).lower()
        if action.startswith("sell"):
            net_credit += price
        elif action.startswith("buy") or action.startswith("optional buy"):
            net_debit += price

    net_premium = net_credit - net_debit

    return {
        "legs": priced_legs,
        "net_credit_usdt": round(net_premium, 4) if net_premium > 0 else 0,
        "net_debit_usdt": round(abs(net_premium), 4) if net_premium < 0 else 0,
        "net_premium_usdt": round(net_premium, 4),
    }


def build_rule_based_insights(expiry_label, symbol=DEFAULT_SYMBOL, resolution=DEFAULT_RESOLUTION):
    generated_at = _utc_now().isoformat()
    expiry_profile = _expiry_profile(expiry_label)
    analytics = _latest_analytics(expiry_label)
    premium, previous_premium = _latest_premium_pair(expiry_label)
    option_latest_df, option_previous_df = _latest_snapshot_pair_for_expiry(
        "option_chain_snapshots",
        expiry_label,
        limit=2400,
    )
    option_df = _prepare_option_chain(option_latest_df)
    previous_option_df = _prepare_option_chain(option_previous_df)
    orderbook = _latest_orderbook(symbol=symbol)
    ohlcv_df = get_latest_ohlcv_data(symbol=symbol, resolution=resolution, limit=300)
    events_df = get_market_events(symbol=symbol, resolution=resolution, limit=200)
    zones_df = get_smc_zones(symbol=symbol, resolution=resolution, status="active", limit=200)
    profile_df = get_volume_profile(symbol=symbol, resolution=resolution, limit=100)

    chain = _chain_metrics(option_df)
    analytics["pcr"] = analytics.get("pcr") or chain.get("pcr")

    spot_price = analytics.get("spot_price") or orderbook.get("eth_price")
    expected_move = analytics.get("atm_straddle_price") or premium.get("atm_straddle_price")

    realized_vol_pct = _realized_volatility_pct(ohlcv_df)
    profile_context = _profile_context(profile_df, spot_price, analytics)
    chain_context = _chain_positioning(option_df, spot_price, analytics)

    if chain_context.get("call_wall"):
        chain["highest_call_oi_strike"] = chain_context.get("call_wall")

    if chain_context.get("put_wall"):
        chain["highest_put_oi_strike"] = chain_context.get("put_wall")

    best_call_sell_strike, best_put_sell_strike = _best_sell_strikes(
        option_df,
        spot_price,
        expected_move,
        analytics,
    )
    smc_context = _smc_context(
        zones_df,
        spot_price,
        best_call_sell_strike,
        best_put_sell_strike,
    )
    price_action = _price_action_context(ohlcv_df)
    volatility_context = _volatility_context(
        analytics,
        premium,
        previous_premium,
        option_df,
        previous_option_df,
        realized_vol_pct,
    )
    volatility_regime = volatility_context.get("regime")

    structure_direction, structure_type = _recent_structure(events_df)
    directional_bias, bias_notes = _directional_bias(
        chain,
        orderbook,
        structure_direction,
        analytics,
        price_action,
        smc_context,
        chain_context,
    )

    pinning_score = _pinning_score(
        analytics,
        chain,
        spot_price,
        profile_context,
        smc_context,
    )
    conflict_score, conflict_notes = _signal_conflict_score(
        directional_bias,
        structure_direction,
        orderbook,
        chain_context,
        volatility_context,
        price_action,
        smc_context,
        profile_context,
        pinning_score,
    )
    trap_risk, trap_warnings = _trap_risk(orderbook, zones_df, spot_price)
    market_regime = _market_regime(
        volatility_regime,
        directional_bias,
        pinning_score,
        structure_type,
        price_action,
        chain_context,
        profile_context,
        smc_context,
    )
    best_strategy = _best_strategy(
        market_regime,
        volatility_regime,
        directional_bias,
        pinning_score,
        trap_risk,
        chain,
        price_action,
        chain_context,
        volatility_context,
        profile_context,
        smc_context,
    )
    initial_strategy = best_strategy
    strategy_legs = _strategy_legs(
        best_strategy,
        option_df,
        spot_price,
        expected_move,
        analytics.get("atm_strike") or premium.get("atm_strike"),
        best_call_sell_strike,
        best_put_sell_strike,
        directional_bias,
    )
    strategy_text = _strategy_text(strategy_legs)
    no_executable_strategy = (
        best_strategy not in ["No Trade", "Wait / Defined-Risk Spread Only"]
        and not _has_executable_legs(strategy_legs)
    )

    if best_strategy == "No Trade":
        strategy_text = ["No trade - high trap risk with conflicting directional signals."]
    elif best_strategy == "Wait / Defined-Risk Spread Only" and not strategy_text:
        strategy_text = [
            "Wait - risk is elevated and directional signals are not clean enough for a defined-risk spread."
        ]
    elif no_executable_strategy:
        strategy_text = [
            "No executable strikes found for the recommended structure in the latest option-chain snapshot."
        ]

    data_flags = {
        "analytics": bool(analytics),
        "option_chain": not option_df.empty,
        "orderbook": bool(orderbook),
        "premium_decay": bool(premium),
        "ohlcv": not ohlcv_df.empty,
        "market_events": not events_df.empty,
        "smc_zones": not zones_df.empty,
        "volume_profile": not profile_df.empty,
    }
    missing_sources = [
        DATA_SOURCE_LABELS.get(source, source)
        for source, available in data_flags.items()
        if not available
    ]

    confidence_score = _confidence_score(
        data_flags,
        directional_bias,
        market_regime,
        trap_risk,
        pinning_score,
        orderbook,
        volatility_context,
        price_action,
        smc_context,
        profile_context,
    )

    best_strategy, confidence_score, expiry_notes = _apply_expiry_adjustments(
        best_strategy,
        confidence_score,
        expiry_profile,
        market_regime,
        volatility_context,
        profile_context,
        smc_context,
        conflict_score,
    )

    if best_strategy != initial_strategy:
        strategy_legs = _strategy_legs(
            best_strategy,
            option_df,
            spot_price,
            expected_move,
            analytics.get("atm_strike") or premium.get("atm_strike"),
            best_call_sell_strike,
            best_put_sell_strike,
            directional_bias,
        )
        strategy_text = _strategy_text(strategy_legs)
        no_executable_strategy = (
            best_strategy not in ["No Trade", "Wait / Defined-Risk Spread Only"]
            and not _has_executable_legs(strategy_legs)
        )

        if best_strategy == "Wait / Defined-Risk Spread Only" and not strategy_text:
            strategy_text = [
                "Wait - risk is elevated and directional signals are not clean enough for a defined-risk spread."
            ]

    if no_executable_strategy:
        confidence_score = min(confidence_score, 50)

    strategy_pricing = _strategy_pricing(option_df, strategy_legs)

    poc_price = profile_context.get("poc_price")

    key_insights = [
        f"Market regime is {market_regime}.",
        f"Volatility regime is {volatility_regime}.",
        f"Directional bias is {directional_bias} based on {', '.join(bias_notes) if bias_notes else 'mixed signals'}.",
        f"Signal conflict score is {conflict_score}/100 based on {', '.join(conflict_notes)}.",
        f"Pinning score is {pinning_score}/100 around max pain {analytics.get('max_pain')}.",
        f"Expiry behavior is {expiry_profile.get('bucket')}.",
        f"Option selling environment is {volatility_context.get('option_selling_environment')}.",
        f"Recommended strategy is {best_strategy}.",
    ]

    key_insights.extend(strategy_text)

    for note in chain_context.get("notes", []):
        key_insights.append(f"Option chain: {note}.")

    for note in volatility_context.get("notes", []):
        key_insights.append(f"Volatility: {note}.")

    for note in price_action.get("notes", []):
        key_insights.append(f"Price action: {note}.")

    for note in smc_context.get("notes", []):
        key_insights.append(f"SMC: {note}.")

    for note in profile_context.get("notes", []):
        key_insights.append(f"Volume profile: {note}.")

    for note in expiry_notes:
        key_insights.append(f"Expiry adjustment: {note}.")

    if poc_price is not None:
        key_insights.append(f"Volume profile POC is near {round(float(poc_price), 2)}.")

    if best_call_sell_strike:
        key_insights.append(f"Best call sell strike by rules: {best_call_sell_strike}.")

    if best_put_sell_strike:
        key_insights.append(f"Best put sell strike by rules: {best_put_sell_strike}.")

    key_insights = _unique_items(key_insights)

    risk_warnings = list(trap_warnings)
    risk_warnings.extend(volatility_context.get("warnings", []))
    risk_warnings.extend(smc_context.get("warnings", []))
    risk_warnings.extend(profile_context.get("warnings", []))

    if confidence_score < 55:
        if missing_sources:
            risk_warnings.append(
                "Confidence is low because these source tables are missing or empty: "
                + ", ".join(missing_sources)
                + "."
            )
        else:
            risk_warnings.append("Confidence is low because available signals disagree.")

    if trap_risk == "High":
        risk_warnings.append("Trap risk is high; prefer no trade or strictly defined-risk structures.")

    if best_strategy == "No Trade":
        risk_warnings.append("No Trade selected because trap risk is high and directional signals conflict.")

    if conflict_score >= 65:
        risk_warnings.append("Signal conflict is high; avoid treating this as a clean setup.")

    if no_executable_strategy:
        risk_warnings.append("Recommended structure has incomplete strikes in the latest option-chain snapshot.")

    if volatility_regime == "Elevated" and best_strategy in ["Short Strangle", "Short Straddle with Hedge"]:
        risk_warnings.append("Short premium is risky in elevated volatility unless hedged and sized small.")

    risk_warnings = _unique_items(risk_warnings)

    if not risk_warnings:
        risk_warnings.append("No major rule-based risk warning is active.")

    return {
        "generated_at": generated_at,
        "expiry_label": expiry_label,
        "expiry_profile": expiry_profile,
        "market_regime": market_regime,
        "volatility_regime": volatility_regime,
        "directional_bias": directional_bias,
        "pinning_score": pinning_score,
        "trap_risk": trap_risk,
        "best_strategy": best_strategy,
        "strategy_legs": strategy_legs,
        "strategy_pricing": strategy_pricing,
        "strategy_text": strategy_text,
        "best_call_sell_strike": best_call_sell_strike,
        "best_put_sell_strike": best_put_sell_strike,
        "confidence_score": confidence_score,
        "signal_conflict_score": conflict_score,
        "signal_conflict_notes": conflict_notes,
        "key_insights": key_insights,
        "risk_warnings": risk_warnings,
        "spot_price": spot_price,
        "expected_move": expected_move,
        "max_pain": analytics.get("max_pain"),
        "atm_strike": analytics.get("atm_strike") or premium.get("atm_strike"),
        "pcr": analytics.get("pcr"),
        "net_delta": chain.get("net_delta"),
        "net_gamma": chain.get("net_gamma"),
        "median_iv": chain.get("median_iv"),
        "data_flags": data_flags,
        "missing_sources": missing_sources,
        "option_selling_environment": volatility_context.get("option_selling_environment"),
        "momentum": price_action.get("momentum"),
        "call_wall": chain_context.get("call_wall"),
        "put_wall": chain_context.get("put_wall"),
        "upside_resistance": chain_context.get("upside_resistance"),
        "downside_support": chain_context.get("downside_support"),
        "raw_input_snapshot": {
            "analytics": analytics,
            "premium": premium,
            "previous_premium": previous_premium,
            "orderbook": orderbook,
            "chain": chain,
            "chain_context": chain_context,
            "volatility_context": volatility_context,
            "price_action": price_action,
            "smc_context": smc_context,
            "profile_context": profile_context,
            "expiry_profile": expiry_profile,
            "data_flags": data_flags,
        },
    }
