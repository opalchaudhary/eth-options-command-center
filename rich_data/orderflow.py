from collections import defaultdict
from datetime import timedelta

from delta_api import safe_float
from rich_data import delta_public
from rich_data.config import RICH_ORDERFLOW_VERSION
from rich_data.repositories import OrderflowAggregateRepository
from rich_data.time_utils import floor_time, parse_delta_timestamp, utc_now


def aggressive_side(trade):
    buyer_role = str((trade or {}).get("buyer_role") or "").lower()
    seller_role = str((trade or {}).get("seller_role") or "").lower()
    if buyer_role == "taker":
        return "buy"
    if seller_role == "taker":
        return "sell"
    return None


def trade_dedupe_key(trade):
    return (
        trade.get("timestamp"),
        trade.get("symbol"),
        str(trade.get("price")),
        str(trade.get("size")),
        trade.get("buyer_role"),
        trade.get("seller_role"),
    )


def aggregate_trades(trades, now=None, version=RICH_ORDERFLOW_VERSION, previous_rows=None):
    now = now or utc_now()
    closed_bucket_cutoff = floor_time(now, 60)
    buckets = defaultdict(list)
    seen = set()

    for trade in trades or []:
        key = trade_dedupe_key(trade)
        if key in seen:
            continue
        seen.add(key)
        trade_time = parse_delta_timestamp(trade.get("timestamp"))
        if trade_time is None:
            continue
        bucket = floor_time(trade_time, 60)
        if bucket >= closed_bucket_cutoff:
            continue
        buckets[bucket].append(trade)

    previous_rows = sorted(previous_rows or [], key=lambda row: row.get("bucket_timestamp") or "")
    cvd_running = 0.0
    if previous_rows:
        cvd_running = safe_float(previous_rows[-1].get("cvd_running")) or 0.0

    results = []
    for bucket in sorted(buckets):
        bucket_trades = buckets[bucket]
        volumes = [safe_float(trade.get("size")) or 0.0 for trade in bucket_trades]
        sides = [aggressive_side(trade) for trade in bucket_trades]
        taker_buy_volume = sum(volume for volume, side in zip(volumes, sides) if side == "buy")
        taker_sell_volume = sum(volume for volume, side in zip(volumes, sides) if side == "sell")
        total_volume = sum(volumes)
        cvd_increment = taker_buy_volume - taker_sell_volume
        cvd_running += cvd_increment
        average_trade_size = (total_volume / len(volumes)) if volumes else None
        max_trade_size = max(volumes) if volumes else None
        large_threshold = _percentile(volumes, 0.95)
        large_trades = [
            (volume, side)
            for volume, side in zip(volumes, sides)
            if large_threshold is not None and volume >= large_threshold
        ]
        large_buy_volume = sum(volume for volume, side in large_trades if side == "buy")
        large_sell_volume = sum(volume for volume, side in large_trades if side == "sell")
        large_total = large_buy_volume + large_sell_volume

        row = {
            "bucket_timestamp": bucket.isoformat(),
            "symbol": bucket_trades[0].get("symbol") or "ETHUSD",
            "version": version,
            "trade_count": len(bucket_trades),
            "total_volume": total_volume,
            "taker_buy_volume": taker_buy_volume,
            "taker_sell_volume": taker_sell_volume,
            "taker_buy_ratio": taker_buy_volume / total_volume if total_volume else None,
            "taker_sell_ratio": taker_sell_volume / total_volume if total_volume else None,
            "net_taker_volume": cvd_increment,
            "cvd_increment": cvd_increment,
            "cvd_running": cvd_running,
            "average_trade_size": average_trade_size,
            "max_trade_size": max_trade_size,
            "large_trade_threshold": large_threshold,
            "large_buy_volume": large_buy_volume,
            "large_sell_volume": large_sell_volume,
            "large_trade_imbalance": (large_buy_volume - large_sell_volume) / large_total if large_total else None,
            "large_trade_count": len(large_trades),
            "source_status": "HEALTHY",
            "completeness": 1.0,
            "staleness_seconds": max(0, (now - (bucket + timedelta(minutes=1))).total_seconds()),
            "error_reason": None,
            "metadata_json": {
                "sources": {
                    "trades": "delta.public_trades",
                    "aggressive_side": "buyer_role=taker => aggressive buy; seller_role=taker => aggressive sell",
                    "dedupe": "timestamp,symbol,price,size,buyer_role,seller_role",
                }
            },
        }
        results.append(row)

    for row in results:
        bucket = row["bucket_timestamp"]
        history = [item for item in previous_rows if item.get("bucket_timestamp") < bucket] + [
            item for item in results if item["bucket_timestamp"] <= bucket
        ]
        row["cvd_5m"] = _rolling_sum(history, 5)
        row["cvd_15m"] = _rolling_sum(history, 15)
        row["cvd_1h"] = _rolling_sum(history, 60)

    return results


def _percentile(values, pct):
    clean = sorted(value for value in values if value is not None)
    if not clean:
        return None
    index = int(round((len(clean) - 1) * pct))
    return clean[index]


def _rolling_sum(rows, count):
    values = [safe_float(row.get("cvd_increment")) or 0.0 for row in rows[-count:]]
    return sum(values) if values else None


class OrderflowCollector:
    def __init__(self, repository=None, trades_provider=None, version=RICH_ORDERFLOW_VERSION):
        self.repository = repository or OrderflowAggregateRepository()
        self.trades_provider = trades_provider or delta_public.get_recent_public_trades
        self.version = version

    def collect(self, symbol="ETHUSD"):
        now = utc_now()
        previous = self.repository.recent_cvd(symbol=symbol, before_iso=floor_time(now, 60).isoformat(), limit=60)
        trades = self.trades_provider(symbol)
        rows = aggregate_trades(trades, now=now, version=self.version, previous_rows=previous)
        ok = self.repository.upsert_many(rows)
        return {
            "ok": bool(ok),
            "row_count": len(rows) if ok else 0,
            "trade_count": sum(row.get("trade_count", 0) for row in rows),
            "source_status": "HEALTHY" if rows else "NO_CLOSED_BUCKET",
        }

