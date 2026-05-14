import requests
import pandas as pd
from datetime import datetime, timezone

DELTA_BASE_URL = "https://api.india.delta.exchange/v2"


def _to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def fetch_eth_orderbook(symbol="ETHUSD", depth=20):
    """
    Fetch ETH perpetual order book from Delta Exchange.
    Public endpoint, no API key required.
    """
    url = f"{DELTA_BASE_URL}/l2orderbook/{symbol}"

    response = requests.get(
        url,
        params={"depth": depth},
        headers={"Accept": "application/json"},
        timeout=10,
    )
    response.raise_for_status()

    data = response.json()

    if not data.get("success"):
        raise ValueError(f"Delta API error: {data}")

    result = data.get("result", {})

    bids = pd.DataFrame(result.get("buy", []))
    asks = pd.DataFrame(result.get("sell", []))

    for df in [bids, asks]:
        if not df.empty:
            df["price"] = df["price"].apply(_to_float)
            df["size"] = df["size"].apply(_to_float)
            df["depth"] = df["depth"].apply(_to_float)

    return {
        "symbol": result.get("symbol", symbol),
        "last_updated_at": result.get("last_updated_at"),
        "bids": bids,
        "asks": asks,
    }


def analyze_orderbook(orderbook):
    bids = orderbook["bids"]
    asks = orderbook["asks"]

    if bids.empty or asks.empty:
        return {
            "status": "error",
            "message": "Order book data unavailable",
        }

    best_bid = bids["price"].max()
    best_ask = asks["price"].min()
    mid_price = (best_bid + best_ask) / 2

    spread = best_ask - best_bid
    spread_pct = (spread / mid_price) * 100 if mid_price else 0

    bid_depth = bids["size"].sum()
    ask_depth = asks["size"].sum()

    imbalance_ratio = bid_depth / ask_depth if ask_depth else 0

    nearest_bid_wall = bids.sort_values("size", ascending=False).iloc[0]
    nearest_ask_wall = asks.sort_values("size", ascending=False).iloc[0]

    if imbalance_ratio > 1.25:
        bias = "Mild Bullish"
    elif imbalance_ratio < 0.80:
        bias = "Mild Bearish"
    else:
        bias = "Neutral"

    if spread_pct <= 0.03:
        spread_quality = "Good"
    elif spread_pct <= 0.08:
        spread_quality = "Average"
    else:
        spread_quality = "Poor"

    wall_gap_bid = ((mid_price - nearest_bid_wall["price"]) / mid_price) * 100
    wall_gap_ask = ((nearest_ask_wall["price"] - mid_price) / mid_price) * 100

    if spread_quality == "Poor":
        execution_signal = "Avoid market order"
    elif bias == "Mild Bullish" and wall_gap_bid < 0.30:
        execution_signal = "Buy-side support visible"
    elif bias == "Mild Bearish" and wall_gap_ask < 0.30:
        execution_signal = "Sell-side resistance visible"
    else:
        execution_signal = "Neutral execution zone"

    if (
        nearest_bid_wall["size"] > bid_depth * 0.35
        or nearest_ask_wall["size"] > ask_depth * 0.35
    ):
        trap_risk = "Medium / Watch Wall Spoofing"
    else:
        trap_risk = "Low"

    return {
        "status": "ok",
        "symbol": orderbook["symbol"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "last_updated_at": orderbook.get("last_updated_at"),

        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": round(mid_price, 2),

        "spread": round(spread, 4),
        "spread_pct": round(spread_pct, 4),
        "spread_quality": spread_quality,

        "bid_depth": round(bid_depth, 2),
        "ask_depth": round(ask_depth, 2),
        "imbalance_ratio": round(imbalance_ratio, 3),
        "bias": bias,

        "nearest_bid_wall_price": nearest_bid_wall["price"],
        "nearest_bid_wall_size": nearest_bid_wall["size"],
        "nearest_ask_wall_price": nearest_ask_wall["price"],
        "nearest_ask_wall_size": nearest_ask_wall["size"],

        "trap_risk": trap_risk,
        "execution_signal": execution_signal,
    }


def generate_orderbook_text_insights(insights):
    """
    Generate readable interpretation from order book analytics.
    """

    if insights.get("status") != "ok":
        return ["Order book data is currently unavailable."]

    text_insights = []

    bias = insights["bias"]
    imbalance = insights["imbalance_ratio"]
    spread_quality = insights["spread_quality"]
    spread_pct = insights["spread_pct"]
    trap_risk = insights["trap_risk"]
    execution_signal = insights["execution_signal"]

    bid_wall_price = insights["nearest_bid_wall_price"]
    bid_wall_size = insights["nearest_bid_wall_size"]
    ask_wall_price = insights["nearest_ask_wall_price"]
    ask_wall_size = insights["nearest_ask_wall_size"]

    if bias == "Mild Bullish":
        text_insights.append(
            f"Order book is mildly bullish. Bid-side liquidity is stronger than ask-side liquidity "
            f"with an imbalance ratio of {imbalance}. Buyers are showing more visible depth near spot."
        )
    elif bias == "Mild Bearish":
        text_insights.append(
            f"Order book is mildly bearish. Ask-side liquidity is heavier than bid-side liquidity "
            f"with an imbalance ratio of {imbalance}. Sellers are dominating visible depth."
        )
    else:
        text_insights.append(
            f"Order book is balanced. Bid and ask depth are almost evenly matched with an imbalance ratio of {imbalance}. "
            f"No strong short-term directional edge is visible from the order book alone."
        )

    if spread_quality == "Good":
        text_insights.append(
            f"Spread quality is good at {spread_pct}%. Execution conditions are acceptable, "
            f"but limit orders are still preferable."
        )
    elif spread_quality == "Average":
        text_insights.append(
            f"Spread quality is average at {spread_pct}%. Avoid aggressive entries unless the options setup is very strong."
        )
    else:
        text_insights.append(
            f"Spread quality is poor at {spread_pct}%. Market orders should be avoided because slippage risk is high."
        )

    text_insights.append(
        f"Nearest major bid wall is around {bid_wall_price} with size {bid_wall_size}. "
        f"This may act as a short-term support zone if the wall stays visible."
    )

    text_insights.append(
        f"Nearest major ask wall is around {ask_wall_price} with size {ask_wall_size}. "
        f"This may act as a short-term resistance zone if the wall does not disappear."
    )

    if "Medium" in trap_risk:
        text_insights.append(
            "Trap risk is medium. One side of the book has a large visible wall. "
            "Watch whether the wall remains stable or disappears as price approaches it. "
            "If the wall vanishes, it may indicate spoofing or liquidity trap behavior."
        )
    else:
        text_insights.append(
            "Trap risk is low. Liquidity appears relatively distributed instead of being concentrated in one suspicious wall."
        )

    text_insights.append(
        f"Execution read: {execution_signal}. Use this only as confirmation for the Strike Recommendation Engine, "
        f"not as an independent trade signal."
    )

    return text_insights


def get_eth_orderbook_insights(depth=20):
    """
    Main function to use in Streamlit app.
    """
    orderbook = fetch_eth_orderbook(symbol="ETHUSD", depth=depth)
    insights = analyze_orderbook(orderbook)
    text_insights = generate_orderbook_text_insights(insights)

    return {
        "orderbook": orderbook,
        "insights": insights,
        "text_insights": text_insights,
    }


if __name__ == "__main__":
    data = get_eth_orderbook_insights(depth=20)

    print(data["insights"])

    print("\nText Insights:")
    for insight in data["text_insights"]:
        print("-", insight)
