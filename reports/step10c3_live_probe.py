import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import requests
import websocket

from rich_data.orderflow_ws import WebsocketOrderflowAggregator, parse_delta_ws_trade


def load_env(path=".env"):
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key, value.strip().strip('"').strip("'"))


def floor_minute(value):
    return value.replace(second=0, microsecond=0)


def iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_ts(value):
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def supabase_get(table, params):
    base_url = os.environ["SUPABASE_URL"].rstrip("/")
    key = os.environ["SUPABASE_KEY"]
    response = requests.get(
        f"{base_url}/rest/v1/{table}",
        headers={"apikey": key, "Authorization": f"Bearer {key}"},
        params=params,
        timeout=30,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"{table} {response.status_code}: {response.text[:300]}")
    return response.json()


def delta_candles(start, end):
    response = requests.get(
        "https://api.india.delta.exchange/v2/history/candles",
        params={
            "resolution": "1m",
            "symbol": "ETHUSD",
            "start": int(start.timestamp()),
            "end": int(end.timestamp()),
        },
        headers={"Accept": "application/json"},
        timeout=30,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"delta candles {response.status_code}: {response.text[:300]}")
    rows = response.json().get("result") or []
    return {datetime.fromtimestamp(row["time"], timezone.utc): float(row.get("volume") or 0) for row in rows}


def main():
    load_env()
    now = datetime.now(timezone.utc)
    start = floor_minute(now + timedelta(minutes=1))
    end = start + timedelta(minutes=3)
    hard_stop = end + timedelta(seconds=8)

    ws = websocket.create_connection("wss://public-socket.india.delta.exchange", timeout=8)
    ws.settimeout(8)
    ws.send(json.dumps({"type": "enable_heartbeat"}))
    ws.send(
        json.dumps(
            {
                "type": "subscribe",
                "payload": {"channels": [{"name": "trades", "symbols": ["ETHUSD"]}]},
            }
        )
    )

    aggregator = WebsocketOrderflowAggregator()
    aggregator.mark_connected(start)
    raw_volume = defaultdict(float)
    raw_count = Counter()
    parsed_volume = defaultdict(float)
    parsed_count = Counter()
    message_types = Counter()
    heartbeats = 0
    errors = []
    last_message_at = None

    while datetime.now(timezone.utc) < hard_stop:
        try:
            message = ws.recv()
        except Exception as exc:
            if datetime.now(timezone.utc) >= hard_stop:
                break
            errors.append(f"recv:{type(exc).__name__}:{exc}")
            continue
        received_at = datetime.now(timezone.utc)
        last_message_at = received_at
        try:
            payload = json.loads(message)
        except Exception as exc:
            errors.append(f"json:{exc}")
            continue
        message_types[str(payload.get("type"))] += 1
        if payload.get("type") == "heartbeat":
            heartbeats += 1
            continue
        trades = parse_delta_ws_trade(payload, expected_symbol="ETHUSD") or []
        for trade in trades:
            trade_time = datetime.fromtimestamp(trade["timestamp"] / 1_000_000, timezone.utc)
            bucket = floor_minute(trade_time)
            if start <= bucket < end:
                parsed_volume[bucket] += float(trade["size"])
                parsed_count[bucket] += 1
        rows = payload.get("d")
        if rows is None and payload.get("type") == "trades":
            rows = [payload]
        elif isinstance(rows, dict):
            rows = [rows]
        for row in rows or []:
            if row.get("sy") != "ETHUSD":
                continue
            trade_time = datetime.fromtimestamp(int(row["t"]) / 1_000_000, timezone.utc)
            bucket = floor_minute(trade_time)
            if start <= bucket < end:
                raw_volume[bucket] += float(row["s"])
                raw_count[bucket] += 1
        aggregator.ingest_message(payload, received_at=received_at)

    ws.close()
    agg_rows = aggregator.close_ready_buckets(now=hard_stop)
    agg_by_min = {parse_ts(row["bucket_timestamp"]): row for row in agg_rows}
    reference = delta_candles(start, end)
    persisted_rows = supabase_get(
        "orderflow_aggregates",
        {
            "select": "bucket_timestamp,total_volume,trade_count,source_status,metadata_json",
            "symbol": "eq.ETHUSD",
            "version": "eq.rich_data_v2_orderflow_ws",
            "bucket_timestamp": f"gte.{iso(start)}",
            "order": "bucket_timestamp.asc",
            "limit": "20",
        },
    )
    persisted = {
        parse_ts(row["bucket_timestamp"]): row
        for row in persisted_rows
        if parse_ts(row["bucket_timestamp"]) < end
    }

    buckets = []
    cursor = start
    while cursor < end:
        ref = reference.get(cursor)
        raw = raw_volume.get(cursor, 0.0)
        parsed = parsed_volume.get(cursor, 0.0)
        agg = float((agg_by_min.get(cursor) or {}).get("total_volume") or 0)
        prod = float((persisted.get(cursor) or {}).get("total_volume") or 0)
        buckets.append(
            {
                "bucket": iso(cursor),
                "raw_volume": raw,
                "raw_count": raw_count.get(cursor, 0),
                "parsed_volume": parsed,
                "parsed_count": parsed_count.get(cursor, 0),
                "local_aggregator_volume": agg,
                "prod_persisted_volume": prod,
                "prod_status": (persisted.get(cursor) or {}).get("source_status"),
                "reference_1m_volume": ref,
                "raw_to_reference": raw / ref if ref else None,
                "prod_to_reference": prod / ref if ref else None,
            }
        )
        cursor += timedelta(minutes=1)

    print(
        json.dumps(
            {
                "sample": {"start": iso(start), "end": iso(end), "last_message_at": iso(last_message_at)},
                "message_types": dict(message_types),
                "heartbeats": heartbeats,
                "errors": errors,
                "buckets": buckets,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
