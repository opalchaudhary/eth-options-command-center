import math

import pandas as pd


COMPONENT_WEIGHTS = {
    "orderbook_score": 0.25,
    "oi_cluster_score": 0.20,
    "volatility_score": 0.15,
    "smc_trap_score": 0.15,
    "volume_imbalance_score": 0.10,
    "gamma_pressure_score": 0.10,
    "funding_bias_score": 0.05,
}


def _empty(df):
    return df is None or not isinstance(df, pd.DataFrame) or df.empty


def _to_numeric(df, cols):
    clean_df = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()

    for col in cols:
        if col in clean_df.columns:
            clean_df[col] = pd.to_numeric(clean_df[col], errors="coerce")

    return clean_df


def _safe_float(value, default=None):
    try:
        if value is None or pd.isna(value):
            return default
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            return default
        return value
    except Exception:
        return default


def _overlap_score(zone_low, zone_high, level, tolerance):
    level = _safe_float(level)
    if level is None:
        return 0

    if zone_low <= level <= zone_high:
        return 100

    distance = min(abs(level - zone_low), abs(level - zone_high))
    if tolerance <= 0 or distance > tolerance:
        return 0

    return normalize_score(tolerance - distance, 0, tolerance)


def normalize_score(value, min_value=None, max_value=None):
    value = _safe_float(value, 0)
    min_value = _safe_float(min_value, 0)
    max_value = _safe_float(max_value, 100)

    if max_value == min_value:
        return 0

    score = ((value - min_value) / (max_value - min_value)) * 100
    return round(max(0, min(100, score)), 2)


def proximity_weight(zone_mid, spot_price, max_distance=None):
    zone_mid = _safe_float(zone_mid)
    spot_price = _safe_float(spot_price)

    if zone_mid is None or spot_price is None or spot_price <= 0:
        return 0.5

    if max_distance is None:
        max_distance = max(spot_price * 0.08, 1)

    distance = abs(zone_mid - spot_price)
    return round(max(0.15, 1 - (distance / max_distance)), 4)


def _atr(ohlcv_df, period=14):
    if _empty(ohlcv_df) or not {"high", "low", "close"}.issubset(ohlcv_df.columns):
        return None

    clean_df = _to_numeric(ohlcv_df, ["high", "low", "close"]).dropna(subset=["high", "low", "close"])
    if len(clean_df) < 3:
        return None

    prev_close = clean_df["close"].shift(1)
    true_range = pd.concat(
        [
            clean_df["high"] - clean_df["low"],
            (clean_df["high"] - prev_close).abs(),
            (clean_df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return _safe_float(true_range.tail(period).mean())


def _realized_volatility(ohlcv_df):
    if _empty(ohlcv_df) or "close" not in ohlcv_df.columns:
        return None

    close = pd.to_numeric(ohlcv_df["close"], errors="coerce").dropna()
    if len(close) < 5:
        return None

    returns = close.pct_change().dropna()
    if returns.empty:
        return None

    return _safe_float(returns.tail(96).std() * math.sqrt(365 * 24 * 12) * 100)


def build_price_zones(spot_price, ohlcv_df=None, zone_count=28):
    spot_price = _safe_float(spot_price)
    if spot_price is None or spot_price <= 0:
        return pd.DataFrame()

    atr = _atr(ohlcv_df)
    zone_width = max((atr or 0) * 0.75, spot_price * 0.0035, 5)
    total_range = max(zone_width * zone_count / 2, spot_price * 0.06)
    lower = spot_price - total_range

    rows = []
    for index in range(zone_count):
        zone_low = lower + (index * zone_width)
        zone_high = zone_low + zone_width
        zone_mid = (zone_low + zone_high) / 2

        if zone_high < spot_price:
            direction = "downside_long_liquidation"
        elif zone_low > spot_price:
            direction = "upside_short_liquidation"
        else:
            direction = "neutral"

        rows.append(
            {
                "zone_low": round(zone_low, 2),
                "zone_high": round(zone_high, 2),
                "zone_mid": round(zone_mid, 2),
                "direction": direction,
            }
        )

    return pd.DataFrame(rows)


def score_orderbook_layer(zones_df, orderbook_df, spot_price):
    scores = pd.Series(0.0, index=zones_df.index)
    if _empty(orderbook_df):
        return scores, False

    required_prices = ["nearest_bid_wall_price", "nearest_ask_wall_price"]
    if not any(col in orderbook_df.columns for col in required_prices):
        return scores, False

    clean_df = _to_numeric(
        orderbook_df,
        [
            "nearest_bid_wall_price",
            "nearest_bid_wall_size",
            "nearest_ask_wall_price",
            "nearest_ask_wall_size",
            "imbalance_ratio",
            "bid_depth",
            "ask_depth",
        ],
    )

    latest = clean_df.tail(1).iloc[0]
    bid_size = _safe_float(latest.get("nearest_bid_wall_size"), 0)
    ask_size = _safe_float(latest.get("nearest_ask_wall_size"), 0)
    size_max = max(bid_size, ask_size, 1)
    tolerance = max((_safe_float(zones_df["zone_high"].iloc[0]) - _safe_float(zones_df["zone_low"].iloc[0])) * 1.75, 1)

    for index, row in zones_df.iterrows():
        direction = row["direction"]
        level_score = 0
        size_score = 0

        if direction == "downside_long_liquidation":
            level_score = _overlap_score(row["zone_low"], row["zone_high"], latest.get("nearest_bid_wall_price"), tolerance)
            size_score = normalize_score(bid_size, 0, size_max)
        elif direction == "upside_short_liquidation":
            level_score = _overlap_score(row["zone_low"], row["zone_high"], latest.get("nearest_ask_wall_price"), tolerance)
            size_score = normalize_score(ask_size, 0, size_max)

        scores.loc[index] = (level_score * 0.65 + size_score * 0.35) * proximity_weight(row["zone_mid"], spot_price)

    return scores.clip(0, 100).round(2), True


def score_ohlcv_layer(zones_df, ohlcv_df, spot_price):
    scores = pd.Series(0.0, index=zones_df.index)
    if _empty(ohlcv_df) or not {"high", "low", "close"}.issubset(ohlcv_df.columns):
        return scores, False

    clean_df = _to_numeric(ohlcv_df, ["open", "high", "low", "close", "volume"]).dropna(subset=["high", "low", "close"])
    if len(clean_df) < 5:
        return scores, False

    atr = _atr(clean_df) or max(spot_price * 0.003, 1)
    recent = clean_df.tail(80)
    rv = _realized_volatility(clean_df) or 0
    candle_range = (recent["high"] - recent["low"]).abs()
    range_score = normalize_score(candle_range.tail(20).mean(), 0, max(atr * 2.2, 1))
    tolerance = max(atr * 0.8, 2)

    if "open" not in recent.columns:
        recent = recent.copy()
        recent["open"] = recent["close"]

    upper_wicks = recent[recent["high"] > recent[["open", "close"]].max(axis=1)]
    lower_wicks = recent[recent["low"] < recent[["open", "close"]].min(axis=1)]

    for index, row in zones_df.iterrows():
        if row["direction"] == "upside_short_liquidation":
            wick_score = max((_overlap_score(row["zone_low"], row["zone_high"], high, tolerance) for high in upper_wicks["high"]), default=0)
            sr_score = max((_overlap_score(row["zone_low"], row["zone_high"], high, tolerance) for high in recent["high"].tail(30)), default=0)
        elif row["direction"] == "downside_long_liquidation":
            wick_score = max((_overlap_score(row["zone_low"], row["zone_high"], low, tolerance) for low in lower_wicks["low"]), default=0)
            sr_score = max((_overlap_score(row["zone_low"], row["zone_high"], low, tolerance) for low in recent["low"].tail(30)), default=0)
        else:
            wick_score = 0
            sr_score = 0

        scores.loc[index] = (
            wick_score * 0.42
            + sr_score * 0.28
            + range_score * 0.18
            + normalize_score(rv, 25, 120) * 0.12
        ) * proximity_weight(row["zone_mid"], spot_price)

    return scores.clip(0, 100).round(2), True


def score_option_layer(zones_df, expiry_df, option_snapshot_df=None, analytics_history_df=None, max_pain=None, ohlcv_df=None, spot_price=None):
    oi_scores = pd.Series(0.0, index=zones_df.index)
    gamma_scores = pd.Series(0.0, index=zones_df.index)

    if _empty(expiry_df) or "strike" not in expiry_df.columns:
        return oi_scores, gamma_scores, False

    clean_df = _to_numeric(expiry_df, ["strike", "oi", "volume", "gamma", "iv"]).dropna(subset=["strike"])
    if clean_df.empty:
        return oi_scores, gamma_scores, False

    type_col = "type" if "type" in clean_df.columns else "option_type" if "option_type" in clean_df.columns else None
    oi_by_strike = clean_df.groupby("strike", as_index=False)["oi"].sum() if "oi" in clean_df.columns else pd.DataFrame()
    gamma_by_strike = clean_df.groupby("strike", as_index=False)["gamma"].sum() if "gamma" in clean_df.columns else pd.DataFrame()
    oi_max = _safe_float(oi_by_strike["oi"].max() if not oi_by_strike.empty else 0, 0) or 1
    gamma_max = _safe_float(gamma_by_strike["gamma"].abs().max() if not gamma_by_strike.empty else 0, 0) or 1
    tolerance = max((_safe_float(zones_df["zone_high"].iloc[0]) - _safe_float(zones_df["zone_low"].iloc[0])) * 1.3, 1)

    change_by_strike = pd.DataFrame()
    if not _empty(option_snapshot_df) and {"snapshot_time", "strike", "oi"}.issubset(option_snapshot_df.columns):
        snap = _to_numeric(option_snapshot_df, ["strike", "oi"]).dropna(subset=["snapshot_time", "strike"])
        if not snap.empty:
            latest_time = snap["snapshot_time"].max()
            earliest_time = snap["snapshot_time"].min()
            latest = snap[snap["snapshot_time"] == latest_time].groupby("strike", as_index=False)["oi"].sum()
            earliest = snap[snap["snapshot_time"] == earliest_time].groupby("strike", as_index=False)["oi"].sum()
            change_by_strike = latest.merge(earliest, on="strike", how="left", suffixes=("_latest", "_earliest"))
            change_by_strike["oi_change"] = change_by_strike["oi_latest"] - change_by_strike["oi_earliest"].fillna(0)

    iv = _safe_float(clean_df["iv"].mean() if "iv" in clean_df.columns else None)
    rv = _realized_volatility(ohlcv_df)
    iv_rv_bonus = 0
    if iv is not None and rv is not None and iv > rv:
        iv_rv_bonus = normalize_score(iv - rv, 0, max(rv, 1))

    pcr_bonus = 0
    if type_col and "oi" in clean_df.columns:
        calls = clean_df[clean_df[type_col].isin(["call_options", "CE", "call"])]["oi"].sum()
        puts = clean_df[clean_df[type_col].isin(["put_options", "PE", "put"])]["oi"].sum()
        if calls:
            pcr = puts / calls
            pcr_bonus = normalize_score(abs(pcr - 1), 0, 1)

    for index, row in zones_df.iterrows():
        matching_oi = [
            normalize_score(strike_row["oi"], 0, oi_max) * _overlap_score(row["zone_low"], row["zone_high"], strike_row["strike"], tolerance) / 100
            for _, strike_row in oi_by_strike.iterrows()
        ]
        matching_gamma = [
            normalize_score(abs(strike_row["gamma"]), 0, gamma_max) * _overlap_score(row["zone_low"], row["zone_high"], strike_row["strike"], tolerance) / 100
            for _, strike_row in gamma_by_strike.iterrows()
        ]
        change_score = 0
        if not change_by_strike.empty and "oi_change" in change_by_strike.columns:
            max_change = max(change_by_strike["oi_change"].abs().max(), 1)
            change_score = max(
                (
                    normalize_score(abs(change_row["oi_change"]), 0, max_change)
                    * _overlap_score(row["zone_low"], row["zone_high"], change_row["strike"], tolerance)
                    / 100
                    for _, change_row in change_by_strike.iterrows()
                ),
                default=0,
            )

        max_pain_score = _overlap_score(row["zone_low"], row["zone_high"], max_pain, tolerance * 1.5)
        oi_scores.loc[index] = (
            max(matching_oi, default=0) * 0.58
            + change_score * 0.18
            + max_pain_score * 0.14
            + pcr_bonus * 0.10
        ) * proximity_weight(row["zone_mid"], spot_price)
        gamma_scores.loc[index] = (
            max(matching_gamma, default=0) * 0.68
            + iv_rv_bonus * 0.32
        ) * proximity_weight(row["zone_mid"], spot_price)

    return oi_scores.clip(0, 100).round(2), gamma_scores.clip(0, 100).round(2), True


def score_volume_layer(zones_df, profile_df=None, ohlcv_df=None, spot_price=None):
    scores = pd.Series(0.0, index=zones_df.index)
    available = False

    profile = pd.DataFrame()
    if not _empty(profile_df) and {"price_level", "volume"}.issubset(profile_df.columns):
        profile = _to_numeric(profile_df, ["price_level", "volume"]).dropna(subset=["price_level", "volume"])
        available = not profile.empty

    ohlcv = pd.DataFrame()
    if not _empty(ohlcv_df) and {"high", "low", "volume"}.issubset(ohlcv_df.columns):
        ohlcv = _to_numeric(ohlcv_df, ["high", "low", "volume"]).dropna(subset=["high", "low", "volume"]).tail(80)
        available = available or not ohlcv.empty

    if not available:
        return scores, False

    volume_max = max(_safe_float(profile["volume"].max() if not profile.empty else 0, 0), 1)
    candle_volume_max = max(_safe_float(ohlcv["volume"].max() if not ohlcv.empty else 0, 0), 1)
    tolerance = max((_safe_float(zones_df["zone_high"].iloc[0]) - _safe_float(zones_df["zone_low"].iloc[0])) * 1.2, 1)

    for index, row in zones_df.iterrows():
        profile_score = 0
        if not profile.empty:
            profile_score = max(
                (
                    normalize_score(level["volume"], 0, volume_max)
                    * _overlap_score(row["zone_low"], row["zone_high"], level["price_level"], tolerance)
                    / 100
                    for _, level in profile.iterrows()
                ),
                default=0,
            )

        candle_score = 0
        if not ohlcv.empty:
            touched = ohlcv[(ohlcv["high"] >= row["zone_low"]) & (ohlcv["low"] <= row["zone_high"])]
            if not touched.empty:
                candle_score = normalize_score(touched["volume"].mean(), 0, candle_volume_max)

        scores.loc[index] = (profile_score * 0.62 + candle_score * 0.38) * proximity_weight(row["zone_mid"], spot_price)

    return scores.clip(0, 100).round(2), True


def score_smc_layer(zones_df, smc_zones_df=None, events_df=None, spot_price=None):
    scores = pd.Series(0.0, index=zones_df.index)
    available = False

    smc = pd.DataFrame()
    if not _empty(smc_zones_df) and {"price_low", "price_high"}.issubset(smc_zones_df.columns):
        smc = _to_numeric(smc_zones_df, ["price_low", "price_high", "strength"]).dropna(subset=["price_low", "price_high"])
        available = not smc.empty

    events = pd.DataFrame()
    if not _empty(events_df) and "price" in events_df.columns:
        events = _to_numeric(events_df, ["price", "reference_price", "strength"]).dropna(subset=["price"])
        available = available or not events.empty

    if not available:
        return scores, False

    tolerance = max((_safe_float(zones_df["zone_high"].iloc[0]) - _safe_float(zones_df["zone_low"].iloc[0])) * 1.5, 1)
    type_weights = {
        "buy_side_liquidity": 100,
        "sell_side_liquidity": 100,
        "liquidity": 85,
        "order_block": 72,
        "fvg": 68,
        "supply": 70,
        "demand": 70,
    }

    for index, row in zones_df.iterrows():
        zone_scores = []
        for _, smc_row in smc.iterrows():
            mid = (smc_row["price_low"] + smc_row["price_high"]) / 2
            overlap = _overlap_score(row["zone_low"], row["zone_high"], mid, tolerance)
            zone_type = str(smc_row.get("zone_type", "")).lower()
            direction = str(smc_row.get("direction", "")).lower()
            type_score = type_weights.get(zone_type, 55)

            directional_fit = 1
            if row["direction"] == "upside_short_liquidation" and ("sell_side" in zone_type or direction == "bullish"):
                directional_fit = 0.65
            if row["direction"] == "downside_long_liquidation" and ("buy_side" in zone_type or direction == "bearish"):
                directional_fit = 0.65

            strength = normalize_score(_safe_float(smc_row.get("strength"), 1), 0, 3)
            zone_scores.append(overlap * (type_score * 0.75 + strength * 0.25) / 100 * directional_fit)

        event_scores = []
        for _, event_row in events.iterrows():
            event_score = _overlap_score(row["zone_low"], row["zone_high"], event_row.get("price"), tolerance)
            event_type = str(event_row.get("event_type", "")).lower()
            if event_type in ["bos", "choch", "swing_high", "swing_low"]:
                event_score *= 0.88
            event_scores.append(event_score)

        scores.loc[index] = (max(zone_scores, default=0) * 0.72 + max(event_scores, default=0) * 0.28) * proximity_weight(row["zone_mid"], spot_price)

    return scores.clip(0, 100).round(2), True


def score_market_context_layer(zones_df, ohlcv_df=None, analytics_history_df=None, orderbook_df=None, spot_price=None):
    scores = pd.Series(0.0, index=zones_df.index)
    available = False
    trend = "neutral"
    compression_score = 0
    bias_score = 0

    if not _empty(ohlcv_df) and "close" in ohlcv_df.columns:
        close = pd.to_numeric(ohlcv_df["close"], errors="coerce").dropna()
        if len(close) >= 30:
            available = True
            fast = close.tail(12).mean()
            slow = close.tail(30).mean()
            trend = "bullish" if fast > slow else "bearish" if fast < slow else "neutral"
            atr = _atr(ohlcv_df) or 0
            recent_range = _safe_float((close.tail(20).max() - close.tail(20).min()), 0)
            compression_score = 100 - normalize_score(recent_range, 0, max(atr * 8, 1))

    if not _empty(analytics_history_df) and "pcr" in analytics_history_df.columns:
        pcr = _safe_float(pd.to_numeric(analytics_history_df["pcr"], errors="coerce").dropna().tail(1).mean())
        if pcr is not None:
            available = True
            bias_score = normalize_score(abs(pcr - 1), 0, 1.2)

    if not _empty(orderbook_df) and "imbalance_ratio" in orderbook_df.columns:
        imbalance = _safe_float(pd.to_numeric(orderbook_df["imbalance_ratio"], errors="coerce").dropna().tail(1).mean())
        if imbalance is not None:
            available = True
            bias_score = max(bias_score, normalize_score(abs(imbalance - 1), 0, 1.5))

    if not available:
        return scores, False

    for index, row in zones_df.iterrows():
        directional_bonus = 0
        if row["direction"] == "upside_short_liquidation" and trend == "bullish":
            directional_bonus = 22
        elif row["direction"] == "downside_long_liquidation" and trend == "bearish":
            directional_bonus = 22

        scores.loc[index] = (
            compression_score * 0.42
            + bias_score * 0.36
            + directional_bonus
        ) * proximity_weight(row["zone_mid"], spot_price)

    return scores.clip(0, 100).round(2), True


def classify_confidence(score, available_layer_count=0):
    score = _safe_float(score, 0)

    if available_layer_count <= 2:
        return "Weak"
    if score >= 80 and available_layer_count >= 5:
        return "Extreme"
    if score >= 65 and available_layer_count >= 4:
        return "High"
    if score >= 45 and available_layer_count >= 3:
        return "Moderate"
    return "Weak"


def generate_primary_reason(row):
    component_labels = {
        "orderbook_score": "order-book wall",
        "oi_cluster_score": "options OI cluster",
        "volatility_score": "wick/volatility rejection",
        "smc_trap_score": "SMC liquidity trap",
        "volume_imbalance_score": "volume concentration",
        "gamma_pressure_score": "gamma/IV pressure",
        "funding_bias_score": "market context bias",
    }
    leaders = sorted(component_labels, key=lambda col: _safe_float(row.get(col), 0), reverse=True)
    strongest = [component_labels[col] for col in leaders if _safe_float(row.get(col), 0) >= 35][:2]

    if not strongest:
        return "No dominant liquidation pressure layer."

    return "Driven by " + " and ".join(strongest) + "."


def generate_trading_warning(row):
    direction = row.get("direction")
    confidence = row.get("confidence_level")

    if direction == "upside_short_liquidation":
        return f"{confidence} upside squeeze risk near {row['zone_low']:,.0f}-{row['zone_high']:,.0f}; avoid fading strength without confirmation."
    if direction == "downside_long_liquidation":
        return f"{confidence} downside cascade risk near {row['zone_low']:,.0f}-{row['zone_high']:,.0f}; protect longs if price accepts below support."
    return "Neutral zone around spot; wait for acceptance outside the range."


def generate_strategy_bias(row):
    direction = row.get("direction")
    score = _safe_float(row.get("liquidation_magnet_score"), 0)

    if direction == "upside_short_liquidation" and score >= 55:
        return "Upside magnet; prefer breakout continuation or wait for failed sweep reversal."
    if direction == "downside_long_liquidation" and score >= 55:
        return "Downside magnet; prefer breakdown continuation or wait for reclaim after sweep."
    if direction == "upside_short_liquidation":
        return "Mild upside liquidity draw; confirmation needed."
    if direction == "downside_long_liquidation":
        return "Mild downside liquidity draw; confirmation needed."
    return "No directional liquidation edge."


def build_composite_liquidation_heatmap(
    spot_price,
    orderbook_df=None,
    ohlcv_df=None,
    expiry_df=None,
    option_snapshot_df=None,
    analytics_history_df=None,
    smc_zones_df=None,
    events_df=None,
    profile_df=None,
    max_pain=None,
):
    zones_df = build_price_zones(spot_price, ohlcv_df=ohlcv_df)
    if zones_df.empty:
        return zones_df

    availability = {}
    zones_df["orderbook_score"], availability["orderbook_score"] = score_orderbook_layer(zones_df, orderbook_df, spot_price)
    zones_df["volatility_score"], availability["volatility_score"] = score_ohlcv_layer(zones_df, ohlcv_df, spot_price)
    (
        zones_df["oi_cluster_score"],
        zones_df["gamma_pressure_score"],
        option_available,
    ) = score_option_layer(
        zones_df,
        expiry_df,
        option_snapshot_df=option_snapshot_df,
        analytics_history_df=analytics_history_df,
        max_pain=max_pain,
        ohlcv_df=ohlcv_df,
        spot_price=spot_price,
    )
    availability["oi_cluster_score"] = option_available
    availability["gamma_pressure_score"] = option_available
    zones_df["volume_imbalance_score"], availability["volume_imbalance_score"] = score_volume_layer(
        zones_df,
        profile_df=profile_df,
        ohlcv_df=ohlcv_df,
        spot_price=spot_price,
    )
    zones_df["smc_trap_score"], availability["smc_trap_score"] = score_smc_layer(
        zones_df,
        smc_zones_df=smc_zones_df,
        events_df=events_df,
        spot_price=spot_price,
    )
    zones_df["funding_bias_score"], availability["funding_bias_score"] = score_market_context_layer(
        zones_df,
        ohlcv_df=ohlcv_df,
        analytics_history_df=analytics_history_df,
        orderbook_df=orderbook_df,
        spot_price=spot_price,
    )

    available_count = sum(1 for value in availability.values() if value)
    total_weight = sum(
        weight
        for component, weight in COMPONENT_WEIGHTS.items()
        if availability.get(component)
    )

    if total_weight <= 0:
        return pd.DataFrame()

    weighted_score = pd.Series(0.0, index=zones_df.index)
    for component, weight in COMPONENT_WEIGHTS.items():
        if availability.get(component):
            weighted_score += zones_df[component].fillna(0) * weight

    zones_df["liquidation_magnet_score"] = (weighted_score / total_weight).clip(0, 100).round(2)
    zones_df["confidence_level"] = zones_df["liquidation_magnet_score"].apply(
        lambda score: classify_confidence(score, available_count)
    )
    zones_df["primary_reason"] = zones_df.apply(generate_primary_reason, axis=1)
    zones_df["trading_warning"] = zones_df.apply(generate_trading_warning, axis=1)
    zones_df["strategy_bias"] = zones_df.apply(generate_strategy_bias, axis=1)
    zones_df["available_layer_count"] = available_count

    return zones_df.sort_values("zone_mid", ascending=False).reset_index(drop=True)
