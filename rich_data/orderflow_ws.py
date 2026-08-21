import json
import logging
import os
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from delta_api import safe_float
from rich_data.config import (
    RICH_ORDERFLOW_WS_CHANNEL,
    RICH_ORDERFLOW_WS_SYMBOL,
    RICH_ORDERFLOW_WS_URL,
    RICH_ORDERFLOW_WS_VERSION,
)
from rich_data.orderflow import aggressive_side
from rich_data.repositories import OrderflowAggregateRepository
from rich_data.time_utils import floor_time, parse_delta_timestamp, utc_now


logger = logging.getLogger(__name__)

DEDUPE_LIMIT = 20_000
HEARTBEAT_TIMEOUT_SECONDS = 35
LARGE_TRADE_NOTIONAL_USD = 100_000


def parse_delta_ws_trade(message, expected_symbol="ETHUSD"):
    payload = message
    if isinstance(message, str):
        payload = json.loads(message)
    if not isinstance(payload, dict):
        return None
    if payload.get("type") in {"heartbeat", "subscriptions", "pong"}:
        return None

    rows = payload.get("d")
    if rows is None and _looks_like_trade(payload):
        rows = [payload]
    elif isinstance(rows, dict):
        rows = [rows]
    if not rows:
        return None

    parsed = []
    for row in rows:
        trade = _parse_compact_trade(row, expected_symbol=expected_symbol)
        if trade:
            parsed.append(trade)
    return parsed


def _looks_like_trade(payload):
    return any(key in payload for key in ["sy", "symbol"]) and any(key in payload for key in ["p", "price"])


def _parse_compact_trade(row, expected_symbol="ETHUSD"):
    symbol = row.get("sy") or row.get("symbol") or row.get("s")
    if expected_symbol and symbol != expected_symbol:
        return None

    role = str(row.get("r") or row.get("buyer_role") or "").lower()
    buyer_role = None
    seller_role = None
    if role in {"t", "taker"}:
        buyer_role = "taker"
        seller_role = "maker"
    elif role in {"m", "maker"}:
        buyer_role = "maker"
        seller_role = "taker"

    timestamp = row.get("t") or row.get("timestamp")
    parsed_time = parse_delta_timestamp(timestamp)
    if parsed_time is None:
        parsed_time = parse_delta_timestamp(row.get("ts"))
    if parsed_time is None:
        return None

    price = safe_float(row.get("p") if "p" in row else row.get("price"))
    size = safe_float(row.get("s") if "s" in row else row.get("size"))
    if price is None or size is None:
        return None

    return {
        "timestamp": int(parsed_time.timestamp() * 1_000_000),
        "symbol": symbol,
        "price": price,
        "size": size,
        "buyer_role": buyer_role,
        "seller_role": seller_role,
        "server_timestamp": row.get("ts"),
        "raw_role": row.get("r"),
        "trade_id": row.get("id") or row.get("trade_id") or row.get("seq") or row.get("sequence"),
    }


def websocket_trade_dedupe_key(trade):
    stable_id = trade.get("trade_id")
    if stable_id not in [None, ""]:
        return ("id", trade.get("symbol"), str(stable_id))
    return (
        "composite",
        trade.get("timestamp"),
        trade.get("symbol"),
        str(trade.get("price")),
        str(trade.get("size")),
        trade.get("buyer_role"),
        trade.get("seller_role"),
    )


class BoundedDedupe:
    def __init__(self, limit=DEDUPE_LIMIT):
        self.limit = limit
        self._items = OrderedDict()

    def add(self, key):
        if key in self._items:
            self._items.move_to_end(key)
            return False
        self._items[key] = True
        while len(self._items) > self.limit:
            self._items.popitem(last=False)
        return True

    def __len__(self):
        return len(self._items)


class OrderflowBucket:
    def __init__(self, bucket_time, symbol, version):
        self.bucket_time = bucket_time
        self.symbol = symbol
        self.version = version
        self.trades = []
        self.incomplete = False
        self.gap_events = []

    def add_trade(self, trade):
        self.trades.append(trade)

    def mark_gap(self, start_at, end_at, reason):
        self.incomplete = True
        self.gap_events.append(
            {
                "start": start_at.isoformat() if start_at else None,
                "end": end_at.isoformat() if end_at else None,
                "reason": reason,
            }
        )

    def to_row(self, now=None, previous_rows=None):
        now = now or utc_now()
        previous_rows = previous_rows or []
        volumes = [safe_float(trade.get("size")) or 0.0 for trade in self.trades]
        sides = [aggressive_side(trade) for trade in self.trades]
        prices = [safe_float(trade.get("price")) for trade in self.trades]
        taker_buy_volume = sum(volume for volume, side in zip(volumes, sides) if side == "buy")
        taker_sell_volume = sum(volume for volume, side in zip(volumes, sides) if side == "sell")
        unclassified_volume = sum(volume for volume, side in zip(volumes, sides) if side is None)
        total_volume = sum(volumes)
        cvd_increment = taker_buy_volume - taker_sell_volume
        average_trade_size = total_volume / len(volumes) if volumes else 0.0
        max_trade_size = max(volumes) if volumes else 0.0
        large_trades = [
            (volume, side)
            for volume, side, price in zip(volumes, sides, prices)
            if price is not None and price * volume >= LARGE_TRADE_NOTIONAL_USD
        ]
        large_buy_volume = sum(volume for volume, side in large_trades if side == "buy")
        large_sell_volume = sum(volume for volume, side in large_trades if side == "sell")
        large_total = large_buy_volume + large_sell_volume
        previous_rows = sorted(previous_rows, key=lambda row: row.get("bucket_timestamp") or "")
        previous_cvd = safe_float(previous_rows[-1].get("cvd_running")) if previous_rows else 0.0
        cvd_running = (previous_cvd or 0.0) + cvd_increment
        status = "INCOMPLETE" if self.incomplete else ("NO_TRADES" if not self.trades else "COMPLETE")

        row = {
            "bucket_timestamp": self.bucket_time.isoformat(),
            "symbol": self.symbol,
            "version": self.version,
            "trade_count": len(self.trades),
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
            "large_trade_threshold": LARGE_TRADE_NOTIONAL_USD,
            "large_buy_volume": large_buy_volume,
            "large_sell_volume": large_sell_volume,
            "large_trade_imbalance": (large_buy_volume - large_sell_volume) / large_total if large_total else None,
            "large_trade_count": len(large_trades),
            "source_status": status,
            "completeness": 0.0 if self.incomplete else 1.0,
            "staleness_seconds": max(0, (now - (self.bucket_time + timedelta(minutes=1))).total_seconds()),
            "error_reason": "websocket_gap" if self.incomplete else None,
            "metadata_json": {
                "collector": "websocket_orderflow_v2",
                "status": status,
                "unclassified_volume": unclassified_volume,
                "large_trade_rule": {
                    "type": "temporary_notional_threshold",
                    "threshold_usd": LARGE_TRADE_NOTIONAL_USD,
                    "note": "Collector-only placeholder; whale threshold research remains future work.",
                },
                "gap_events": self.gap_events,
                "sources": {
                    "trades": "delta.websocket.trades",
                    "websocket_url": RICH_ORDERFLOW_WS_URL,
                    "channel": RICH_ORDERFLOW_WS_CHANNEL,
                    "aggressive_side": "Delta compact field r=t means buyer taker => aggressive buy; r=m means buyer maker/seller taker => aggressive sell",
                    "dedupe": "trade_id/sequence if present, otherwise timestamp,symbol,price,size,buyer_role,seller_role",
                },
            },
        }
        history = previous_rows + [row]
        row["cvd_5m"] = _rolling_sum(history, 5)
        row["cvd_15m"] = _rolling_sum(history, 15)
        row["cvd_1h"] = _rolling_sum(history, 60)
        return row


def _rolling_sum(rows, count):
    values = [safe_float(row.get("cvd_increment")) or 0.0 for row in rows[-count:]]
    return sum(values) if values else None


class WebsocketOrderflowAggregator:
    def __init__(self, symbol=RICH_ORDERFLOW_WS_SYMBOL, version=RICH_ORDERFLOW_WS_VERSION, dedupe=None):
        self.symbol = symbol
        self.version = version
        self.dedupe = dedupe or BoundedDedupe()
        self.buckets = {}
        self.connected_at = None
        self.disconnected_at = None
        self.duplicate_suppression_count = 0
        self.reconnect_count = 0
        self.last_message_at = None

    def mark_connected(self, at=None):
        at = at or utc_now()
        if self.disconnected_at:
            self._mark_gap(self.disconnected_at, at, "reconnect_gap")
            self.reconnect_count += 1
        self.connected_at = at
        self.disconnected_at = None

    def mark_disconnected(self, at=None):
        self.disconnected_at = at or utc_now()

    def ingest_message(self, message, received_at=None):
        received_at = received_at or utc_now()
        trades = parse_delta_ws_trade(message, expected_symbol=self.symbol) or []
        if trades:
            self.last_message_at = received_at
        accepted = 0
        for trade in trades:
            key = websocket_trade_dedupe_key(trade)
            if not self.dedupe.add(key):
                self.duplicate_suppression_count += 1
                continue
            trade_time = parse_delta_timestamp(trade.get("timestamp"))
            bucket_time = floor_time(trade_time, 60)
            bucket = self.buckets.setdefault(
                bucket_time,
                OrderflowBucket(bucket_time=bucket_time, symbol=self.symbol, version=self.version),
            )
            bucket.add_trade(trade)
            accepted += 1
        return accepted

    def close_ready_buckets(self, now=None, previous_rows=None):
        now = now or utc_now()
        cutoff = floor_time(now, 60)
        if self.disconnected_at:
            self._mark_gap(self.disconnected_at, now, "currently_disconnected")
        if self.connected_at:
            self._create_connected_empty_buckets(cutoff)
        rows = []
        for bucket_time in sorted(list(self.buckets)):
            if bucket_time >= cutoff:
                continue
            bucket = self.buckets.pop(bucket_time)
            row = bucket.to_row(now=now, previous_rows=(previous_rows or []) + rows)
            row["metadata_json"]["dedupe"] = {
                "recent_key_count": len(self.dedupe),
                "duplicate_suppression_count": self.duplicate_suppression_count,
            }
            row["metadata_json"]["connection"] = {
                "reconnect_count": self.reconnect_count,
                "last_message_at": self.last_message_at.isoformat() if self.last_message_at else None,
            }
            rows.append(row)
        if rows and self.connected_at and not self.disconnected_at:
            self.connected_at = cutoff
        return rows

    def _create_connected_empty_buckets(self, cutoff):
        if not self.connected_at:
            return
        start = floor_time(self.connected_at, 60)
        current = start
        while current < cutoff:
            self.buckets.setdefault(
                current,
                OrderflowBucket(bucket_time=current, symbol=self.symbol, version=self.version),
            )
            current += timedelta(minutes=1)

    def _mark_gap(self, start_at, end_at, reason):
        if not start_at or not end_at or end_at <= start_at:
            return
        current = floor_time(start_at, 60)
        last = floor_time(end_at, 60)
        while current <= last:
            bucket = self.buckets.setdefault(
                current,
                OrderflowBucket(bucket_time=current, symbol=self.symbol, version=self.version),
            )
            bucket.mark_gap(start_at, end_at, reason)
            current += timedelta(minutes=1)


class ConsumerProcessLock:
    def __init__(self, path=None):
        self.path = Path(path or os.getenv("RICH_ORDERFLOW_WS_LOCK_PATH", "logs/rich_orderflow_ws.lock"))
        self.acquired = False

    def acquire(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            fd = os.open(str(self.path), flags)
        except FileExistsError:
            if self._is_stale():
                self.path.unlink(missing_ok=True)
                fd = os.open(str(self.path), flags)
            else:
                return False
        with os.fdopen(fd, "w") as handle:
            handle.write(str(os.getpid()))
        self.acquired = True
        return True

    def release(self):
        if self.acquired:
            self.path.unlink(missing_ok=True)
            self.acquired = False

    def _is_stale(self):
        try:
            pid = int(self.path.read_text().strip())
        except Exception:
            return True
        if pid == os.getpid():
            return False
        if os.name == "nt":
            return False
        try:
            os.kill(pid, 0)
            return False
        except OSError:
            return True


class WebsocketOrderflowService:
    def __init__(self, repository=None, aggregator=None, lock=None, ws_app_factory=None):
        self.repository = repository or OrderflowAggregateRepository()
        self.aggregator = aggregator or WebsocketOrderflowAggregator()
        self.lock = lock or ConsumerProcessLock()
        self.ws_app_factory = ws_app_factory
        self.thread = None
        self.stop_event = threading.Event()
        self.status = {
            "enabled": False,
            "running": False,
            "connected": False,
            "last_error": None,
            "last_message_at": None,
            "last_persist_at": None,
            "persisted_rows": 0,
            "reconnect_count": 0,
            "duplicate_suppression_count": 0,
        }

    def start(self):
        if self.thread and self.thread.is_alive():
            return False
        if not self.lock.acquire():
            self.status["last_error"] = "Another websocket orderflow consumer lock is active."
            logger.warning("rich_orderflow_ws.single_consumer_lock_active")
            return False
        self.status.update({"enabled": True, "running": True})
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run_loop, name="rich-orderflow-ws", daemon=True)
        self.thread.start()
        return True

    def stop(self, timeout=10):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=timeout)
        self.lock.release()
        self.status.update({"running": False, "connected": False})

    def snapshot(self):
        return {
            **self.status,
            "reconnect_count": self.aggregator.reconnect_count,
            "duplicate_suppression_count": self.aggregator.duplicate_suppression_count,
            "buffered_bucket_count": len(self.aggregator.buckets),
            "version": self.aggregator.version,
            "symbol": self.aggregator.symbol,
        }

    def _run_loop(self):
        backoff = 1
        try:
            while not self.stop_event.is_set():
                try:
                    self._connect_once()
                    backoff = 1
                except Exception as exc:
                    self.status["last_error"] = str(exc)
                    logger.exception("rich_orderflow_ws.connection_failed")
                    self.aggregator.mark_disconnected()
                    self.status["connected"] = False
                    self.stop_event.wait(backoff)
                    backoff = min(backoff * 2, 60)
        finally:
            self.lock.release()
            self.status.update({"running": False, "connected": False})

    def _connect_once(self):
        websocket = _import_websocket()
        factory = self.ws_app_factory or websocket.WebSocketApp

        def on_open(ws):
            self.status["connected"] = True
            self.aggregator.mark_connected()
            ws.send(json.dumps({"type": "enable_heartbeat"}))
            ws.send(json.dumps(_subscribe_payload()))

        def on_message(ws, message):
            if _is_heartbeat(message):
                self.aggregator.last_message_at = utc_now()
                self._persist_closed()
                return
            accepted = self.aggregator.ingest_message(message)
            if accepted:
                self.status["last_message_at"] = utc_now().isoformat()
            self._persist_closed()

        def on_error(ws, error):
            self.status["last_error"] = str(error)
            logger.warning("rich_orderflow_ws.socket_error error=%s", error)

        def on_close(ws, close_status_code, close_msg):
            self.status["connected"] = False
            self.aggregator.mark_disconnected()
            logger.warning(
                "rich_orderflow_ws.socket_closed status=%s msg=%s",
                close_status_code,
                close_msg,
            )

        ws = factory(
            RICH_ORDERFLOW_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=30, ping_timeout=5)

    def _persist_closed(self):
        previous = self.repository.recent_cvd(
            symbol=self.aggregator.symbol,
            before_iso=floor_time(utc_now(), 60).isoformat(),
            limit=60,
        )
        rows = self.aggregator.close_ready_buckets(previous_rows=previous)
        if not rows:
            return
        ok = self.repository.upsert_many(rows)
        if ok:
            self.status["last_persist_at"] = utc_now().isoformat()
            self.status["persisted_rows"] += len(rows)


def _subscribe_payload():
    return {
        "type": "subscribe",
        "payload": {
            "channels": [
                {
                    "name": RICH_ORDERFLOW_WS_CHANNEL,
                    "symbols": [RICH_ORDERFLOW_WS_SYMBOL],
                }
            ]
        },
    }


def _is_heartbeat(message):
    try:
        payload = json.loads(message) if isinstance(message, str) else message
    except Exception:
        return False
    return isinstance(payload, dict) and payload.get("type") in {"heartbeat", "pong"}


def _import_websocket():
    try:
        import websocket
    except ImportError as exc:
        raise RuntimeError("websocket-client is required for rich orderflow websocket collection") from exc
    return websocket
