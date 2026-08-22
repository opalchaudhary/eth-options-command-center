from datetime import datetime, timedelta, timezone

from rich_data.config import RICH_ORDERFLOW_WS_VERSION
from rich_data.orderflow_ws import (
    ConsumerProcessLock,
    HEARTBEAT_TIMEOUT_SECONDS,
    OrderflowBucket,
    WebsocketOrderflowAggregator,
    WebsocketOrderflowService,
    parse_delta_ws_trade,
    utc_now,
    websocket_trade_dedupe_key,
)


def _trade(ts, price="2400", size="10", role="t", symbol="ETHUSD", trade_id=None):
    row = {"type": "trades", "d": [{"sy": symbol, "p": price, "s": size, "t": ts, "r": role}]}
    if trade_id:
        row["d"][0]["id"] = trade_id
    return row


def test_delta_websocket_trade_parsing_buyer_taker_is_aggressive_buy():
    ts = 1797792000000000
    trades = parse_delta_ws_trade(_trade(ts, role="t"))

    assert len(trades) == 1
    assert trades[0]["symbol"] == "ETHUSD"
    assert trades[0]["price"] == 2400
    assert trades[0]["size"] == 10
    assert trades[0]["buyer_role"] == "taker"
    assert trades[0]["seller_role"] == "maker"


def test_delta_websocket_trade_parsing_buyer_maker_is_aggressive_sell():
    ts = 1797792000000000
    trades = parse_delta_ws_trade(_trade(ts, role="m"))

    assert trades[0]["buyer_role"] == "maker"
    assert trades[0]["seller_role"] == "taker"


def test_delta_websocket_trade_parsing_unclassified_role_is_preserved():
    ts = 1797792000000000
    trades = parse_delta_ws_trade(_trade(ts, role="unknown"))

    assert trades[0]["buyer_role"] is None
    assert trades[0]["seller_role"] is None


def test_websocket_dedupe_prefers_stable_trade_id():
    trade = parse_delta_ws_trade(_trade(1797792000000000, trade_id="abc"))[0]

    assert websocket_trade_dedupe_key(trade) == ("id", "ETHUSD", "abc")


def test_websocket_aggregator_dedupes_and_closes_utc_minute_bucket():
    aggregator = WebsocketOrderflowAggregator()
    now = datetime(2026, 12, 20, 12, 2, 5, tzinfo=timezone.utc)
    ts = int(datetime(2026, 12, 20, 12, 1, 30, tzinfo=timezone.utc).timestamp() * 1_000_000)

    aggregator.mark_connected(datetime(2026, 12, 20, 12, 1, tzinfo=timezone.utc))
    assert aggregator.ingest_message(_trade(ts, price="2400", size="10", role="t")) == 1
    assert aggregator.ingest_message(_trade(ts, price="2400", size="10", role="t")) == 0
    aggregator.ingest_message(_trade(ts + 1, price="2401", size="4", role="m"))
    rows = aggregator.close_ready_buckets(now=now)

    trade_rows = [row for row in rows if row["bucket_timestamp"] == "2026-12-20T12:01:00+00:00"]
    assert len(trade_rows) == 1
    row = trade_rows[0]
    assert row["version"] == RICH_ORDERFLOW_WS_VERSION
    assert row["trade_count"] == 2
    assert row["taker_buy_volume"] == 10
    assert row["taker_sell_volume"] == 4
    assert row["total_volume"] == 14
    assert row["cvd_increment"] == 6
    assert row["source_status"] == "COMPLETE"
    assert row["metadata_json"]["dedupe"]["duplicate_suppression_count"] == 1


def test_bucket_keeps_unclassified_volume_out_of_cvd():
    bucket = OrderflowBucket(
        bucket_time=datetime(2026, 12, 20, 12, 1, tzinfo=timezone.utc),
        symbol="ETHUSD",
        version=RICH_ORDERFLOW_WS_VERSION,
    )
    bucket.add_trade({"timestamp": 1, "symbol": "ETHUSD", "price": 2400, "size": 5, "buyer_role": None, "seller_role": None})
    row = bucket.to_row(now=datetime(2026, 12, 20, 12, 2, tzinfo=timezone.utc))

    assert row["total_volume"] == 5
    assert row["taker_buy_volume"] == 0
    assert row["taker_sell_volume"] == 0
    assert row["cvd_increment"] == 0
    assert row["metadata_json"]["unclassified_volume"] == 5


def test_disconnected_gap_marks_bucket_incomplete_not_zero_complete():
    aggregator = WebsocketOrderflowAggregator()
    aggregator.mark_connected(datetime(2026, 12, 20, 12, 0, tzinfo=timezone.utc))
    aggregator.mark_disconnected(datetime(2026, 12, 20, 12, 1, 10, tzinfo=timezone.utc))
    rows = aggregator.close_ready_buckets(now=datetime(2026, 12, 20, 12, 3, tzinfo=timezone.utc))

    incomplete = [row for row in rows if row["source_status"] == "INCOMPLETE"]
    assert incomplete
    assert all(row["completeness"] == 0.0 for row in incomplete)
    assert all(row["error_reason"] == "websocket_gap" for row in incomplete)


def test_connected_empty_minute_is_explicit_no_trades():
    aggregator = WebsocketOrderflowAggregator()
    aggregator.mark_connected(datetime(2026, 12, 20, 12, 0, tzinfo=timezone.utc))
    rows = aggregator.close_ready_buckets(now=datetime(2026, 12, 20, 12, 1, 1, tzinfo=timezone.utc))

    assert rows[0]["source_status"] == "NO_TRADES"
    assert rows[0]["total_volume"] == 0
    assert rows[0]["completeness"] == 1.0


def test_repeated_flush_does_not_recreate_persisted_empty_minutes():
    aggregator = WebsocketOrderflowAggregator()
    aggregator.mark_connected(datetime(2026, 12, 20, 12, 0, tzinfo=timezone.utc))

    first = aggregator.close_ready_buckets(now=datetime(2026, 12, 20, 12, 1, 1, tzinfo=timezone.utc))
    second = aggregator.close_ready_buckets(now=datetime(2026, 12, 20, 12, 1, 30, tzinfo=timezone.utc))

    assert len(first) == 1
    assert second == []


def test_cvd_rolling_sums_reconcile():
    previous = [
        {"bucket_timestamp": "2026-12-20T11:58:00+00:00", "cvd_increment": 2, "cvd_running": 2},
        {"bucket_timestamp": "2026-12-20T11:59:00+00:00", "cvd_increment": 3, "cvd_running": 5},
    ]
    bucket = OrderflowBucket(
        bucket_time=datetime(2026, 12, 20, 12, 0, tzinfo=timezone.utc),
        symbol="ETHUSD",
        version=RICH_ORDERFLOW_WS_VERSION,
    )
    bucket.add_trade({"timestamp": 1, "symbol": "ETHUSD", "price": 2400, "size": 9, "buyer_role": "taker", "seller_role": "maker"})
    bucket.add_trade({"timestamp": 2, "symbol": "ETHUSD", "price": 2400, "size": 4, "buyer_role": "maker", "seller_role": "taker"})
    row = bucket.to_row(now=datetime(2026, 12, 20, 12, 1, tzinfo=timezone.utc), previous_rows=previous)

    assert row["cvd_increment"] == 5
    assert row["cvd_running"] == 10
    assert row["cvd_5m"] == 10


def test_single_consumer_lock_prevents_duplicate_process(tmp_path):
    path = tmp_path / "consumer.lock"
    first = ConsumerProcessLock(path)
    second = ConsumerProcessLock(path)

    assert first.acquire() is True
    assert second.acquire() is False
    first.release()
    assert second.acquire() is True
    second.release()


def test_service_uses_delta_heartbeat_without_websocket_client_auto_ping():
    instances = []

    class FakeRepository:
        def recent_cvd(self, **kwargs):
            return []

        def upsert_many(self, rows):
            return True

    class FakeWebSocketApp:
        def __init__(self, url, on_open, on_message, on_error, on_close):
            self.url = url
            self.on_open = on_open
            self.on_message = on_message
            self.on_error = on_error
            self.on_close = on_close
            self.sent = []
            self.run_kwargs = None
            instances.append(self)

        def send(self, payload):
            self.sent.append(payload)

        def run_forever(self, **kwargs):
            self.run_kwargs = kwargs
            self.on_open(self)
            self.on_message(self, '{"type":"subscriptions"}')
            self.on_message(self, '{"type":"heartbeat"}')
            ts = int(datetime(2026, 12, 20, 12, 1, 30, tzinfo=timezone.utc).timestamp() * 1_000_000)
            self.on_message(self, _trade(ts, price="2400", size="3", role="t"))

    service = WebsocketOrderflowService(repository=FakeRepository(), ws_app_factory=FakeWebSocketApp)

    service._connect_once()

    assert instances[0].run_kwargs == {"ping_interval": 0}
    assert '{"type": "enable_heartbeat"}' in instances[0].sent
    assert service.status["heartbeat_messages_received_total"] == 1
    assert service.status["trade_messages_received_total"] == 1
    assert service.status["trade_messages_parsed_total"] == 1
    assert service.status["trade_messages_accepted_total"] == 1
    assert service.status["trade_volume_accepted_total"] == 3
    assert service.status["current_subscription_state"] == "subscribed"


def test_heartbeat_watchdog_closes_stale_connection():
    class FakeWebSocket:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    service = WebsocketOrderflowService()
    fake_ws = FakeWebSocket()
    heartbeat_state = {
        "last_seen_at": utc_now() - timedelta(seconds=HEARTBEAT_TIMEOUT_SECONDS + 1),
        "ws": fake_ws,
    }

    service._heartbeat_watchdog(heartbeat_state, heartbeat_stop=type("Stop", (), {"wait": lambda self, seconds: False})())

    assert fake_ws.closed is True
    assert service.status["last_error"] == "heartbeat timed out"


def test_trade_stream_watchdog_closes_socket_and_marks_stale_bucket_incomplete():
    class FakeWebSocket:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    service = WebsocketOrderflowService()
    now = utc_now()
    service.status["connected"] = True
    service.status["connection_started_at"] = (now - timedelta(seconds=130)).isoformat()
    fake_ws = FakeWebSocket()
    trade_state = {"last_trade_at": now - timedelta(seconds=130), "stale_since": None, "ws": fake_ws}

    service._trade_stream_watchdog(trade_state, trade_stop=type("Stop", (), {"wait": lambda self, seconds: False})())

    assert fake_ws.closed is True
    assert service.status["last_error"] == "trade stream stale"
    assert service.status["stale_trade_detections"] == 1
    assert service.status["forced_recoveries"] == 1
    assert any(bucket.incomplete for bucket in service.aggregator.buckets.values())


def test_trade_stream_watchdog_does_not_close_fresh_trade_stream():
    class FakeWebSocket:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    class OnePassStop:
        def __init__(self):
            self.calls = 0

        def wait(self, seconds):
            self.calls += 1
            return self.calls > 1

    service = WebsocketOrderflowService()
    service.status["connected"] = True
    service.status["connection_started_at"] = utc_now().isoformat()
    fake_ws = FakeWebSocket()
    trade_state = {"last_trade_at": utc_now(), "stale_since": None, "ws": fake_ws}

    service._trade_stream_watchdog(trade_state, trade_stop=OnePassStop())

    assert fake_ws.closed is False
    assert service.status["stale_trade_detections"] == 0
