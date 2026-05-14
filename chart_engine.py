import plotly.graph_objects as go
from plotly.subplots import make_subplots


def create_eth_candlestick_chart(
    df,
    events_df=None,
    zones_df=None,
    title="ETH 5m Smart Money Chart",
):
    """
    ETH candlestick chart with:
    - volume
    - latest price line
    - BOS / CHoCH / swing points
    - FVG / order blocks / liquidity zones
    """

    if df is None or df.empty:
        return None

    required_cols = ["candle_time", "open", "high", "low", "close", "volume"]

    for col in required_cols:
        if col not in df.columns:
            print(f"Missing column for chart: {col}")
            return None

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
    )

    fig.add_trace(
        go.Candlestick(
            x=df["candle_time"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="ETH",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=df["candle_time"],
            y=df["volume"],
            name="Volume",
            opacity=0.45,
        ),
        row=2,
        col=1,
    )

    latest_close = df["close"].iloc[-1]

    fig.add_hline(
        y=latest_close,
        line_width=3,
        line_dash="solid",
        line_color="#00FFAA",
        annotation_text=f"ETH: {latest_close:.2f}",
        annotation_position="top left",
        annotation_font_size=14,
        annotation_font_color="#00FFAA",
        row=1,
        col=1,
    )

    # -----------------------------
    # Plot SMC zones
    # -----------------------------
    if zones_df is not None and not zones_df.empty:
        chart_start = df["candle_time"].min()
        chart_end = df["candle_time"].max()

        for _, zone in zones_df.iterrows():
            zone_type = zone.get("zone_type")
            direction = zone.get("direction")
            price_low = zone.get("price_low")
            price_high = zone.get("price_high")
            start_time = zone.get("start_time")
            end_time = chart_end

            if price_low is None or price_high is None:
                continue

            if zone_type == "fvg":
                color = "rgba(255, 193, 7, 0.18)"
                line_color = "rgba(255, 193, 7, 0.45)"
                label = "FVG"

            elif zone_type == "order_block":
                if direction == "bullish":
                    color = "rgba(0, 255, 170, 0.16)"
                    line_color = "rgba(0, 255, 170, 0.45)"
                    label = "Bullish OB"
                else:
                    color = "rgba(255, 77, 109, 0.16)"
                    line_color = "rgba(255, 77, 109, 0.45)"
                    label = "Bearish OB"

            elif zone_type == "buy_side_liquidity":
                color = "rgba(255, 77, 109, 0.12)"
                line_color = "rgba(255, 77, 109, 0.65)"
                label = "Buy-side Liquidity"

            elif zone_type == "sell_side_liquidity":
                color = "rgba(0, 191, 255, 0.12)"
                line_color = "rgba(0, 191, 255, 0.65)"
                label = "Sell-side Liquidity"

            else:
                color = "rgba(150, 150, 150, 0.12)"
                line_color = "rgba(150, 150, 150, 0.4)"
                label = zone_type

            fig.add_shape(
                type="rect",
                xref="x",
                yref="y",
                x0=max(start_time, chart_start),
                x1=end_time,
                y0=price_low,
                y1=price_high,
                fillcolor=color,
                line=dict(color=line_color, width=1),
                layer="below",
                row=1,
                col=1,
            )

            fig.add_annotation(
                x=chart_end,
                y=(price_low + price_high) / 2,
                text=label,
                showarrow=False,
                font=dict(size=10, color=line_color),
                xanchor="left",
                row=1,
                col=1,
            )

    # -----------------------------
    # Plot market events
    # -----------------------------
    if events_df is not None and not events_df.empty:
        for _, event in events_df.iterrows():
            event_type = event.get("event_type")
            direction = event.get("direction")
            event_time = event.get("event_time")
            price = event.get("price")

            if price is None:
                continue

            if event_type == "bos":
                marker_symbol = "triangle-up" if direction == "bullish" else "triangle-down"
                color = "#00FFAA" if direction == "bullish" else "#FF4D6D"
                label = "BOS"

            elif event_type == "choch":
                marker_symbol = "star"
                color = "#FFD166"
                label = "CHoCH"

            elif event_type == "swing_high":
                marker_symbol = "circle"
                color = "#FF4D6D"
                label = "SH"

            elif event_type == "swing_low":
                marker_symbol = "circle"
                color = "#00BFFF"
                label = "SL"

            else:
                marker_symbol = "circle"
                color = "#AAAAAA"
                label = event_type

            fig.add_trace(
                go.Scatter(
                    x=[event_time],
                    y=[price],
                    mode="markers+text",
                    marker=dict(
                        size=10,
                        color=color,
                        symbol=marker_symbol,
                        line=dict(width=1, color="white"),
                    ),
                    text=[label],
                    textposition="top center",
                    name=label,
                    showlegend=False,
                ),
                row=1,
                col=1,
            )

    fig.update_layout(
        title=title,
        height=720,
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        margin=dict(l=20, r=20, t=50, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
    )

    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    return fig


def create_volume_profile_chart(profile_df, title="ETH Volume Profile"):
    """
    Separate horizontal volume profile chart.
    """

    if profile_df is None or profile_df.empty:
        return None

    required_cols = ["price_level", "volume"]

    for col in required_cols:
        if col not in profile_df.columns:
            print(f"Missing column for volume profile: {col}")
            return None

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=profile_df["volume"],
            y=profile_df["price_level"],
            orientation="h",
            name="Volume Profile",
            opacity=0.75,
        )
    )

    poc_row = profile_df.loc[profile_df["volume"].idxmax()]
    poc_price = poc_row["price_level"]

    fig.add_hline(
        y=poc_price,
        line_width=3,
        line_dash="solid",
        line_color="#FFD166",
        annotation_text=f"POC: {poc_price:.2f}",
        annotation_position="top left",
        annotation_font_color="#FFD166",
    )

    fig.update_layout(
        title=title,
        height=600,
        template="plotly_dark",
        margin=dict(l=20, r=20, t=50, b=20),
        xaxis_title="Volume",
        yaxis_title="Price",
    )

    return fig