from delta_api import safe_float
from orderbook_engine import fetch_eth_orderbook
from rich_data.config import RICH_ORDERBOOK_VERSION
from rich_data.repositories import OrderbookAggregateRepository
from rich_data.time_utils import floor_time, utc_now


DEPTH_BUCKETS = {
    "10bp": 0.001,
    "25bp": 0.0025,
    "50bp": 0.005,
    "100bp": 0.01,
}


def build_orderbook_aggregate(orderbook, collected_at=None, version=RICH_ORDERBOOK_VERSION):
    collected_at = collected_at or utc_now()
    timestamp = floor_time(collected_at, 60)
    bids = orderbook.get("bids")
    asks = orderbook.get("asks")
    if bids is None or asks is None or bids.empty or asks.empty:
        return {
            "timestamp": timestamp.isoformat(),
            "symbol": orderbook.get("symbol", "ETHUSD"),
            "version": version,
            "source_status": "MISSING",
            "completeness": 0,
            "error_reason": "empty_orderbook",
            "metadata_json": {"sources": {"orderbook": "delta.l2orderbook"}},
        }

    best_bid = safe_float(bids["price"].max())
    best_ask = safe_float(asks["price"].min())
    mid = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
    spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
    spread_bps = (spread / mid) * 10000 if spread is not None and mid else None
    bid_depths = {}
    ask_depths = {}
    imbalances = {}

    for label, pct in DEPTH_BUCKETS.items():
        bid_floor = mid * (1 - pct) if mid else None
        ask_ceiling = mid * (1 + pct) if mid else None
        bid_depth = _depth_within(bids, lower=bid_floor)
        ask_depth = _depth_within(asks, upper=ask_ceiling)
        bid_depths[label] = bid_depth
        ask_depths[label] = ask_depth
        imbalances[label] = _imbalance(bid_depth, ask_depth)

    bid_wall = _major_wall(bids, mid, side="bid")
    ask_wall = _major_wall(asks, mid, side="ask")
    total_depth = bid_depths["100bp"] + ask_depths["100bp"]
    largest_wall = max(bid_wall["size"] or 0, ask_wall["size"] or 0)

    return {
        "timestamp": timestamp.isoformat(),
        "symbol": orderbook.get("symbol") or "ETHUSD",
        "version": version,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid,
        "spread": spread,
        "spread_bps": spread_bps,
        "bid_depth_10bp": bid_depths["10bp"],
        "ask_depth_10bp": ask_depths["10bp"],
        "bid_depth_25bp": bid_depths["25bp"],
        "ask_depth_25bp": ask_depths["25bp"],
        "bid_depth_50bp": bid_depths["50bp"],
        "ask_depth_50bp": ask_depths["50bp"],
        "bid_depth_100bp": bid_depths["100bp"],
        "ask_depth_100bp": ask_depths["100bp"],
        "imbalance_10bp": imbalances["10bp"],
        "imbalance_25bp": imbalances["25bp"],
        "imbalance_50bp": imbalances["50bp"],
        "imbalance_100bp": imbalances["100bp"],
        "weighted_book_imbalance": (
            imbalances["10bp"] * 0.4
            + imbalances["25bp"] * 0.3
            + imbalances["50bp"] * 0.2
            + imbalances["100bp"] * 0.1
        ),
        "microprice": _microprice(best_bid, best_ask, bid_depths["10bp"], ask_depths["10bp"]),
        "book_pressure": _book_pressure(imbalances["25bp"]),
        "nearest_major_bid_wall_distance": bid_wall["distance_pct"],
        "nearest_major_ask_wall_distance": ask_wall["distance_pct"],
        "major_bid_wall_size": bid_wall["size"],
        "major_ask_wall_size": ask_wall["size"],
        "liquidity_concentration": largest_wall / total_depth if total_depth else None,
        "source_status": "HEALTHY",
        "completeness": 1.0,
        "staleness_seconds": 0,
        "error_reason": None,
        "metadata_json": {
            "sources": {"orderbook": "delta.l2orderbook"},
            "depth_buckets": DEPTH_BUCKETS,
        },
    }


def _depth_within(frame, lower=None, upper=None):
    data = frame.copy()
    if lower is not None:
        data = data[data["price"] >= lower]
    if upper is not None:
        data = data[data["price"] <= upper]
    return float(data["size"].sum()) if not data.empty else 0.0


def _imbalance(bid_depth, ask_depth):
    total = bid_depth + ask_depth
    return (bid_depth - ask_depth) / total if total else 0.0


def _microprice(best_bid, best_ask, bid_size, ask_size):
    total = bid_size + ask_size
    if best_bid is None or best_ask is None or not total:
        return None
    return ((best_ask * bid_size) + (best_bid * ask_size)) / total


def _book_pressure(imbalance):
    if imbalance >= 0.15:
        return "BID_PRESSURE"
    if imbalance <= -0.15:
        return "ASK_PRESSURE"
    return "BALANCED"


def _major_wall(frame, mid, side):
    if frame is None or frame.empty or not mid:
        return {"size": None, "distance_pct": None}
    row = frame.sort_values("size", ascending=False).iloc[0]
    price = safe_float(row.get("price"))
    size = safe_float(row.get("size"))
    distance = abs(price - mid) / mid if price is not None else None
    return {"size": size, "distance_pct": distance}


class OrderbookCollector:
    def __init__(self, repository=None, orderbook_provider=None, version=RICH_ORDERBOOK_VERSION):
        self.repository = repository or OrderbookAggregateRepository()
        self.orderbook_provider = orderbook_provider or fetch_eth_orderbook
        self.version = version

    def collect(self, symbol="ETHUSD", depth=100):
        orderbook = self.orderbook_provider(symbol=symbol, depth=depth)
        row = build_orderbook_aggregate(orderbook, version=self.version)
        ok = self.repository.upsert_one(row)
        return {"ok": ok, "row_count": 1 if ok else 0, "timestamp": row["timestamp"], "source_status": row["source_status"]}

