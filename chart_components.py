import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from analytics import calculate_max_pain
from chart_engine import create_eth_candlestick_chart, create_volume_profile_chart


CALL_TYPE = "call_options"
PUT_TYPE = "put_options"
CALL_LABEL = "CE"
PUT_LABEL = "PE"


def _empty_guard(df, required_cols):
    if df is None or df.empty:
        return True

    return any(col not in df.columns for col in required_cols)


def _numeric(df, cols):
    clean_df = df.copy()

    for col in cols:
        if col in clean_df.columns:
            clean_df[col] = pd.to_numeric(clean_df[col], errors="coerce")

    return clean_df


def _option_label(option_type):
    if option_type == CALL_TYPE:
        return CALL_LABEL
    if option_type == PUT_TYPE:
        return PUT_LABEL
    return str(option_type)


def _base_layout(fig, title, height=430):
    fig.update_layout(
        title=title,
        height=height,
        template="plotly_white",
        margin=dict(l=20, r=20, t=55, b=35),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
    )
    return fig


def _add_atm_and_max_oi(fig, expiry_df, atm_strike=None):
    if atm_strike is not None:
        fig.add_vline(
            x=atm_strike,
            line_width=2,
            line_dash="dash",
            line_color="#111827",
            annotation_text=f"ATM {atm_strike:g}",
            annotation_position="top",
        )

    if _empty_guard(expiry_df, ["strike", "type", "oi"]):
        return

    oi_df = _numeric(expiry_df, ["strike", "oi"]).dropna(subset=["strike", "oi"])

    for option_type, color, label in [
        (CALL_TYPE, "#d92d20", "Max CE OI"),
        (PUT_TYPE, "#039855", "Max PE OI"),
    ]:
        side_df = oi_df[oi_df["type"] == option_type]

        if side_df.empty:
            continue

        max_strike = side_df.sort_values("oi", ascending=False).iloc[0]["strike"]

        fig.add_vline(
            x=max_strike,
            line_width=1,
            line_dash="dot",
            line_color=color,
            annotation_text=f"{label} {max_strike:g}",
            annotation_position="bottom",
        )


def _group_by_strike_type(df, value_col, agg="sum"):
    if _empty_guard(df, ["strike", "type", value_col]):
        return pd.DataFrame()

    clean_df = _numeric(df, ["strike", value_col]).dropna(subset=["strike"])

    grouped = (
        clean_df.groupby(["strike", "type"], as_index=False)[value_col]
        .agg(agg)
        .sort_values("strike")
    )

    grouped["side"] = grouped["type"].map(_option_label)

    return grouped


def render_open_interest_chart(expiry_df, atm_strike=None):
    grouped = _group_by_strike_type(expiry_df, "oi", "sum")

    if grouped.empty:
        return None

    fig = go.Figure()

    for side, color in [(CALL_LABEL, "#d92d20"), (PUT_LABEL, "#039855")]:
        side_df = grouped[grouped["side"] == side]
        fig.add_trace(
            go.Bar(
                x=side_df["strike"],
                y=side_df["oi"],
                name=f"{side} OI",
                marker_color=color,
                opacity=0.82,
            )
        )

    _add_atm_and_max_oi(fig, expiry_df, atm_strike)
    fig.update_xaxes(title_text="Strike")
    fig.update_yaxes(title_text="Open Interest")
    fig.update_layout(barmode="group")
    return _base_layout(fig, "Open Interest by Strike")


def render_oi_change_chart(expiry_df, snapshot_df=None, atm_strike=None):
    if _empty_guard(expiry_df, ["strike", "type", "oi"]):
        return None

    current = _group_by_strike_type(expiry_df, "oi", "sum")
    value_col = "oi"
    title = "Current OI by Strike"

    if snapshot_df is not None and not snapshot_df.empty:
        required = ["snapshot_time", "strike", "option_type", "oi"]

        if not _empty_guard(snapshot_df, required):
            snap = _numeric(snapshot_df, ["strike", "oi"]).dropna(
                subset=["snapshot_time", "strike", "oi"]
            )
            latest_time = snap["snapshot_time"].max()
            earliest_time = snap["snapshot_time"].min()

            latest = snap[snap["snapshot_time"] == latest_time]
            earliest = snap[snap["snapshot_time"] == earliest_time]

            latest_grouped = latest.groupby(["strike", "option_type"], as_index=False)["oi"].sum()
            earliest_grouped = earliest.groupby(["strike", "option_type"], as_index=False)["oi"].sum()

            change_df = latest_grouped.merge(
                earliest_grouped,
                on=["strike", "option_type"],
                how="left",
                suffixes=("_latest", "_earliest"),
            )
            change_df["oi_change"] = change_df["oi_latest"] - change_df["oi_earliest"].fillna(0)
            change_df["type"] = change_df["option_type"]
            change_df["side"] = change_df["type"].map(_option_label)
            current = change_df.rename(columns={"oi_change": "oi_change"})
            value_col = "oi_change"
            title = "Open Interest Change by Strike"

    fig = go.Figure()

    for side, color in [(CALL_LABEL, "#d92d20"), (PUT_LABEL, "#039855")]:
        side_df = current[current["side"] == side]
        fig.add_trace(
            go.Bar(
                x=side_df["strike"],
                y=side_df[value_col],
                name=f"{side} OI Change" if value_col == "oi_change" else f"{side} OI",
                marker_color=color,
                opacity=0.82,
            )
        )

    if value_col == "oi_change":
        fig.add_hline(y=0, line_width=1, line_color="#98a2b3")

    _add_atm_and_max_oi(fig, expiry_df, atm_strike)
    fig.update_xaxes(title_text="Strike")
    fig.update_yaxes(title_text="OI Change" if value_col == "oi_change" else "Open Interest")
    fig.update_layout(barmode="group")
    return _base_layout(fig, title)


def render_volume_vs_strike_chart(expiry_df, atm_strike=None):
    grouped = _group_by_strike_type(expiry_df, "volume", "sum")

    if grouped.empty:
        return None

    fig = go.Figure()

    for side, color in [(CALL_LABEL, "#f97066"), (PUT_LABEL, "#32d583")]:
        side_df = grouped[grouped["side"] == side]
        fig.add_trace(
            go.Bar(
                x=side_df["strike"],
                y=side_df["volume"],
                name=f"{side} Volume",
                marker_color=color,
                opacity=0.82,
            )
        )

    _add_atm_and_max_oi(fig, expiry_df, atm_strike)
    fig.update_xaxes(title_text="Strike")
    fig.update_yaxes(title_text="Volume")
    fig.update_layout(barmode="group")
    return _base_layout(fig, "Volume vs Strike")


def render_iv_chart(expiry_df, atm_strike=None):
    grouped = _group_by_strike_type(expiry_df, "iv", "mean")

    if grouped.empty:
        return None

    fig = go.Figure()

    for side, color in [(CALL_LABEL, "#d92d20"), (PUT_LABEL, "#039855")]:
        side_df = grouped[grouped["side"] == side]
        fig.add_trace(
            go.Scatter(
                x=side_df["strike"],
                y=side_df["iv"],
                mode="lines+markers",
                name=f"{side} IV",
                line=dict(color=color, width=2),
            )
        )

    _add_atm_and_max_oi(fig, expiry_df, atm_strike)
    fig.update_xaxes(title_text="Strike")
    fig.update_yaxes(title_text="Implied Volatility")
    return _base_layout(fig, "IV Skew by Strike")


def render_iv_vs_rv_chart(expiry_df, ohlcv_df):
    if _empty_guard(expiry_df, ["iv"]) or _empty_guard(ohlcv_df, ["close"]):
        return None

    iv_values = pd.to_numeric(expiry_df["iv"], errors="coerce").dropna()
    close = pd.to_numeric(ohlcv_df["close"], errors="coerce").dropna()

    if iv_values.empty or len(close) < 3:
        return None

    returns = close.pct_change().dropna()

    if returns.empty:
        return None

    iv = iv_values.mean()
    rv = returns.std() * (365 * 24 * 12) ** 0.5
    spread = iv - rv

    fig = make_subplots(
        rows=1,
        cols=2,
        specs=[[{"type": "bar"}, {"type": "indicator"}]],
        column_widths=[0.68, 0.32],
    )

    fig.add_trace(
        go.Bar(
            x=["Average IV", "Realized Volatility", "IV - RV"],
            y=[iv, rv, spread],
            marker_color=["#2e90fa", "#12b76a", "#f79009" if spread >= 0 else "#f04438"],
            name="Volatility",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Indicator(
            mode="number+delta",
            value=spread,
            delta={"reference": 0},
            title={"text": "IV - RV Spread"},
        ),
        row=1,
        col=2,
    )
    fig.update_yaxes(title_text="Volatility", row=1, col=1)
    return _base_layout(fig, "IV vs RV and Spread", height=390)


def render_spot_chart(ohlcv_df):
    return create_eth_candlestick_chart(
        ohlcv_df,
        events_df=None,
        zones_df=None,
        title="ETH Spot Chart",
    )


def render_volume_profile_chart(profile_df):
    fig = create_volume_profile_chart(
        profile_df,
        title="Volume Profile by Price",
    )

    if fig is not None:
        fig.update_layout(template="plotly_white")

    return fig


def render_premium_decay_chart(premium_df):
    if _empty_guard(premium_df, ["snapshot_time"]):
        return None

    value_cols = [
        col
        for col in ["atm_ce_price", "atm_pe_price", "atm_straddle_price"]
        if col in premium_df.columns
    ]

    if not value_cols:
        return None

    clean_df = _numeric(premium_df, value_cols).dropna(subset=["snapshot_time"])

    if clean_df.empty:
        return None

    fig = go.Figure()
    labels = {
        "atm_ce_price": "ATM CE",
        "atm_pe_price": "ATM PE",
        "atm_straddle_price": "ATM Straddle",
    }

    for col in value_cols:
        fig.add_trace(
            go.Scatter(
                x=clean_df["snapshot_time"],
                y=clean_df[col],
                mode="lines+markers",
                name=labels[col],
            )
        )

    fig.update_xaxes(title_text="Time")
    fig.update_yaxes(title_text="Premium")
    return _base_layout(fig, "Premium Decay Over Time")


def render_pcr_trend_chart(analytics_df, option_snapshot_df=None):
    if analytics_df is not None and not analytics_df.empty and "pcr" in analytics_df.columns:
        clean_df = _numeric(analytics_df, ["pcr"]).dropna(subset=["snapshot_time", "pcr"])

        if not clean_df.empty:
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=clean_df["snapshot_time"],
                    y=clean_df["pcr"],
                    mode="lines+markers",
                    name="OI PCR",
                    line=dict(color="#2e90fa", width=2),
                )
            )
            fig.add_hline(y=1, line_dash="dash", line_color="#98a2b3")
            fig.update_xaxes(title_text="Time")
            fig.update_yaxes(title_text="PCR")
            return _base_layout(fig, "PCR Trend")

    if _empty_guard(option_snapshot_df, ["snapshot_time", "option_type", "oi", "volume"]):
        return None

    snap = _numeric(option_snapshot_df, ["oi", "volume"]).dropna(subset=["snapshot_time"])
    grouped = snap.groupby(["snapshot_time", "option_type"], as_index=False)[["oi", "volume"]].sum()

    pivot_oi = grouped.pivot(index="snapshot_time", columns="option_type", values="oi")
    pivot_volume = grouped.pivot(index="snapshot_time", columns="option_type", values="volume")

    fig = go.Figure()

    if CALL_TYPE in pivot_oi.columns and PUT_TYPE in pivot_oi.columns:
        fig.add_trace(
            go.Scatter(
                x=pivot_oi.index,
                y=pivot_oi[PUT_TYPE] / pivot_oi[CALL_TYPE].replace(0, pd.NA),
                mode="lines+markers",
                name="OI PCR",
            )
        )

    if CALL_TYPE in pivot_volume.columns and PUT_TYPE in pivot_volume.columns:
        fig.add_trace(
            go.Scatter(
                x=pivot_volume.index,
                y=pivot_volume[PUT_TYPE] / pivot_volume[CALL_TYPE].replace(0, pd.NA),
                mode="lines+markers",
                name="Volume PCR",
            )
        )

    if not fig.data:
        return None

    fig.add_hline(y=1, line_dash="dash", line_color="#98a2b3")
    fig.update_xaxes(title_text="Time")
    fig.update_yaxes(title_text="PCR")
    return _base_layout(fig, "PCR Trend")


def render_max_pain_shift_chart(analytics_df):
    if _empty_guard(analytics_df, ["snapshot_time", "max_pain"]):
        return None

    clean_df = _numeric(analytics_df, ["max_pain"]).dropna(subset=["snapshot_time", "max_pain"])

    if clean_df.empty:
        return None

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=clean_df["snapshot_time"],
            y=clean_df["max_pain"],
            mode="lines+markers",
            name="Max Pain",
            line=dict(color="#7a5af8", width=2),
        )
    )
    fig.update_xaxes(title_text="Time")
    fig.update_yaxes(title_text="Strike")
    return _base_layout(fig, "Max Pain Shift")


def render_max_pain_curve_chart(expiry_df):
    _, pain_df = calculate_max_pain(expiry_df)

    if pain_df is None or pain_df.empty:
        return None

    max_pain_row = pain_df.sort_values("pain").iloc[0]
    max_pain = max_pain_row["strike"]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=pain_df["strike"],
            y=pain_df["pain"],
            mode="lines+markers",
            name="Pain",
            line=dict(color="#2e90fa", width=2),
        )
    )
    fig.add_vline(
        x=max_pain,
        line_width=2,
        line_dash="dash",
        line_color="#f79009",
        annotation_text=f"Max Pain {max_pain:g}",
        annotation_position="top",
    )
    fig.update_xaxes(title_text="Strike")
    fig.update_yaxes(title_text="Pain")
    return _base_layout(fig, "Max Pain Curve")


def render_premium_by_strike_chart(expiry_df, atm_strike=None):
    grouped = _group_by_strike_type(expiry_df, "mark_price", "mean")

    if grouped.empty:
        return None

    fig = go.Figure()

    for side, color in [(CALL_LABEL, "#d92d20"), (PUT_LABEL, "#039855")]:
        side_df = grouped[grouped["side"] == side]
        fig.add_trace(
            go.Scatter(
                x=side_df["strike"],
                y=side_df["mark_price"],
                mode="lines+markers",
                name=f"{side} Premium",
                line=dict(color=color, width=2),
            )
        )

    _add_atm_and_max_oi(fig, expiry_df, atm_strike)
    fig.update_xaxes(title_text="Strike")
    fig.update_yaxes(title_text="Premium")
    return _base_layout(fig, "Premium by Strike")


def render_smc_liquidity_zone_charts(zones_df):
    if _empty_guard(zones_df, ["zone_type", "price_low", "price_high"]):
        return None

    clean_df = _numeric(zones_df, ["price_low", "price_high", "strength"]).dropna(
        subset=["price_low", "price_high"]
    )

    if clean_df.empty:
        return None

    clean_df = clean_df.copy()
    clean_df["mid_price"] = (clean_df["price_low"] + clean_df["price_high"]) / 2
    clean_df["width"] = (clean_df["price_high"] - clean_df["price_low"]).abs()
    clean_df["zone_label"] = clean_df.apply(
        lambda row: f"{row.get('zone_type', 'zone')} {row.get('direction', '')}".strip(),
        axis=1,
    )
    clean_df["strength_display"] = clean_df.get("strength", pd.Series(index=clean_df.index)).fillna(1)

    color_map = {
        "order_block": "#7a5af8",
        "fvg": "#f79009",
        "buy_side_liquidity": "#d92d20",
        "sell_side_liquidity": "#2e90fa",
        "supply": "#f04438",
        "demand": "#12b76a",
        "liquidity": "#2e90fa",
    }

    colors = [
        color_map.get(str(zone_type), "#667085")
        for zone_type in clean_df["zone_type"]
    ]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=clean_df["width"].where(clean_df["width"] > 0, 1),
            y=clean_df["zone_label"],
            base=clean_df["price_low"],
            orientation="h",
            marker_color=colors,
            text=clean_df["mid_price"].round(2),
            hovertemplate=(
                "Zone: %{y}<br>"
                "Low: %{base:.2f}<br>"
                "Width: %{x:.2f}<br>"
                "Mid: %{text}<extra></extra>"
            ),
            name="Zones",
        )
    )
    fig.update_xaxes(title_text="Price Zone")
    fig.update_yaxes(title_text="SMC / Liquidity Zone", automargin=True)
    return _base_layout(fig, "Liquidity and SMC Zones", height=520)


def render_liquidation_charts(orderbook_df=None):
    if orderbook_df is None or orderbook_df.empty:
        return None

    required = [
        "timestamp",
        "nearest_bid_wall_price",
        "nearest_bid_wall_size",
        "nearest_ask_wall_price",
        "nearest_ask_wall_size",
    ]

    if _empty_guard(orderbook_df, required):
        return None

    clean_df = _numeric(
        orderbook_df,
        [
            "nearest_bid_wall_price",
            "nearest_bid_wall_size",
            "nearest_ask_wall_price",
            "nearest_ask_wall_size",
        ],
    ).dropna(subset=["timestamp"])

    if clean_df.empty:
        return None

    latest = clean_df.sort_values("timestamp").tail(1).iloc[0]

    zones = pd.DataFrame(
        [
            {
                "zone": "Approx Long Liquidation / Bid Liquidity",
                "price": latest["nearest_bid_wall_price"],
                "size": latest["nearest_bid_wall_size"],
                "color": "#12b76a",
            },
            {
                "zone": "Approx Short Liquidation / Ask Liquidity",
                "price": latest["nearest_ask_wall_price"],
                "size": latest["nearest_ask_wall_size"],
                "color": "#f04438",
            },
        ]
    ).dropna(subset=["price", "size"])

    if zones.empty:
        return None

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=zones["size"],
            y=zones["price"],
            orientation="h",
            marker_color=zones["color"],
            text=zones["zone"],
            hovertemplate="Zone: %{text}<br>Price: %{y:.2f}<br>Size: %{x}<extra></extra>",
            name="Liquidation Zones",
        )
    )
    fig.update_xaxes(title_text="Approx Liquidity Size")
    fig.update_yaxes(title_text="Price")
    return _base_layout(fig, "Liquidation Zone Approximation", height=380)
