import pandas as pd


def detect_swing_points(df, left=3, right=3):
    """
    Detect swing highs and swing lows.
    """

    events = []

    if df is None or df.empty:
        return events

    for i in range(left, len(df) - right):
        current_high = df["high"].iloc[i]
        current_low = df["low"].iloc[i]

        left_highs = df["high"].iloc[i - left:i]
        right_highs = df["high"].iloc[i + 1:i + 1 + right]

        left_lows = df["low"].iloc[i - left:i]
        right_lows = df["low"].iloc[i + 1:i + 1 + right]

        candle_time = df["candle_time"].iloc[i]

        if current_high > left_highs.max() and current_high > right_highs.max():
            events.append({
                "event_type": "swing_high",
                "direction": "bearish",
                "event_time": candle_time,
                "price": float(current_high),
                "reference_price": None,
                "strength": 1,
                "metadata": {"index": int(i)}
            })

        if current_low < left_lows.min() and current_low < right_lows.min():
            events.append({
                "event_type": "swing_low",
                "direction": "bullish",
                "event_time": candle_time,
                "price": float(current_low),
                "reference_price": None,
                "strength": 1,
                "metadata": {"index": int(i)}
            })

    return events


def detect_bos_choch(df, swing_events):
    """
    Detect BOS and CHoCH using swing highs/lows.
    Simple first version.
    """

    events = []

    swing_highs = [e for e in swing_events if e["event_type"] == "swing_high"]
    swing_lows = [e for e in swing_events if e["event_type"] == "swing_low"]

    if not swing_highs or not swing_lows:
        return events

    last_structure = None

    for i in range(1, len(df)):
        close = df["close"].iloc[i]
        candle_time = df["candle_time"].iloc[i]

        recent_highs = [
            e for e in swing_highs
            if e["event_time"] < candle_time
        ]

        recent_lows = [
            e for e in swing_lows
            if e["event_time"] < candle_time
        ]

        if not recent_highs or not recent_lows:
            continue

        last_high = recent_highs[-1]["price"]
        last_low = recent_lows[-1]["price"]

        if close > last_high:
            event_type = "bos" if last_structure in [None, "bullish"] else "choch"
            last_structure = "bullish"

            events.append({
                "event_type": event_type,
                "direction": "bullish",
                "event_time": candle_time,
                "price": float(close),
                "reference_price": float(last_high),
                "strength": 1,
                "metadata": {"broken_level": float(last_high)}
            })

        elif close < last_low:
            event_type = "bos" if last_structure in [None, "bearish"] else "choch"
            last_structure = "bearish"

            events.append({
                "event_type": event_type,
                "direction": "bearish",
                "event_time": candle_time,
                "price": float(close),
                "reference_price": float(last_low),
                "strength": 1,
                "metadata": {"broken_level": float(last_low)}
            })

    return events


def detect_fvg_zones(df):
    """
    Detect Fair Value Gaps.
    Bullish FVG: candle 1 high < candle 3 low
    Bearish FVG: candle 1 low > candle 3 high
    """

    zones = []

    if df is None or len(df) < 3:
        return zones

    for i in range(2, len(df)):
        c1 = df.iloc[i - 2]
        c3 = df.iloc[i]

        if c1["high"] < c3["low"]:
            zones.append({
                "zone_type": "fvg",
                "direction": "bullish",
                "start_time": c1["candle_time"],
                "end_time": c3["candle_time"],
                "price_low": float(c1["high"]),
                "price_high": float(c3["low"]),
                "strength": 1,
                "status": "active",
                "metadata": {"index": int(i)}
            })

        if c1["low"] > c3["high"]:
            zones.append({
                "zone_type": "fvg",
                "direction": "bearish",
                "start_time": c1["candle_time"],
                "end_time": c3["candle_time"],
                "price_low": float(c3["high"]),
                "price_high": float(c1["low"]),
                "strength": 1,
                "status": "active",
                "metadata": {"index": int(i)}
            })

    return zones


def detect_liquidity_zones(swing_events, tolerance=3):
    """
    Detect equal highs / equal lows as liquidity zones.
    """

    zones = []

    highs = [e for e in swing_events if e["event_type"] == "swing_high"]
    lows = [e for e in swing_events if e["event_type"] == "swing_low"]

    for group, direction, zone_name in [
        (highs, "bearish", "buy_side_liquidity"),
        (lows, "bullish", "sell_side_liquidity"),
    ]:
        for i in range(1, len(group)):
            prev = group[i - 1]
            curr = group[i]

            if abs(curr["price"] - prev["price"]) <= tolerance:
                price_low = min(prev["price"], curr["price"])
                price_high = max(prev["price"], curr["price"])

                zones.append({
                    "zone_type": zone_name,
                    "direction": direction,
                    "start_time": prev["event_time"],
                    "end_time": curr["event_time"],
                    "price_low": float(price_low),
                    "price_high": float(price_high),
                    "strength": 1,
                    "status": "active",
                    "metadata": {
                        "previous_price": prev["price"],
                        "current_price": curr["price"],
                        "tolerance": tolerance
                    }
                })

    return zones


def detect_order_blocks(df, structure_events):
    """
    Simple order block detection:
    Before bullish BOS/CHoCH, last bearish candle = bullish OB.
    Before bearish BOS/CHoCH, last bullish candle = bearish OB.
    """

    zones = []

    for event in structure_events:
        if event["event_type"] not in ["bos", "choch"]:
            continue

        event_time = event["event_time"]
        direction = event["direction"]

        past_df = df[df["candle_time"] < event_time].tail(10)

        if past_df.empty:
            continue

        if direction == "bullish":
            candidates = past_df[past_df["close"] < past_df["open"]]
            zone_direction = "bullish"
        else:
            candidates = past_df[past_df["close"] > past_df["open"]]
            zone_direction = "bearish"

        if candidates.empty:
            continue

        ob = candidates.iloc[-1]

        zones.append({
            "zone_type": "order_block",
            "direction": zone_direction,
            "start_time": ob["candle_time"],
            "end_time": event_time,
            "price_low": float(ob["low"]),
            "price_high": float(ob["high"]),
            "strength": 1,
            "status": "active",
            "metadata": {
                "source_event": event["event_type"],
                "break_price": event["price"]
            }
        })

    return zones


def calculate_volume_profile(df, bins=40):
    """
    Approximate volume profile using OHLCV.
    Distributes candle volume across price bins touched by candle range.
    """

    if df is None or df.empty:
        return []

    min_price = df["low"].min()
    max_price = df["high"].max()

    if min_price == max_price:
        return []

    price_bins = pd.interval_range(
        start=min_price,
        end=max_price,
        periods=bins
    )

    profile = {interval.mid: 0 for interval in price_bins}

    for _, row in df.iterrows():
        candle_range = row["high"] - row["low"]

        if candle_range <= 0:
            continue

        touched_bins = [
            interval for interval in price_bins
            if interval.left <= row["high"] and interval.right >= row["low"]
        ]

        if not touched_bins:
            continue

        volume_per_bin = row["volume"] / len(touched_bins)

        for interval in touched_bins:
            profile[interval.mid] += volume_per_bin

    result = []

    for price_level, volume in profile.items():
        result.append({
            "price_level": float(price_level),
            "volume": float(volume),
            "profile_type": "ohlcv_approx",
            "metadata": {"bins": bins}
        })

    return result


def run_smc_analysis(df):
    """
    Main function.
    Returns:
    - market events
    - zones
    - volume profile
    """

    swing_events = detect_swing_points(df)
    structure_events = detect_bos_choch(df, swing_events)

    fvg_zones = detect_fvg_zones(df)
    liquidity_zones = detect_liquidity_zones(swing_events)
    order_blocks = detect_order_blocks(df, structure_events)

    volume_profile = calculate_volume_profile(df)

    all_events = swing_events + structure_events
    all_zones = fvg_zones + liquidity_zones + order_blocks

    return all_events, all_zones, volume_profile