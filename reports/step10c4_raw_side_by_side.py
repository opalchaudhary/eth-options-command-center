import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import requests
import websocket


def load_env(path=".env"):
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key, value)


def floor_minute(value):
    return value.replace(second=0, microsecond=0)


def iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_ts(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def status():
    return requests.get("https://deltaforge.in/api/system/status", timeout=10).json()["rich_orderflow_ws"]


def supabase_rows(start, end):
    base_url = os.environ["SUPABASE_URL"].rstrip("/")
    key = os.environ["SUPABASE_KEY"]
    response = requests.get(
        f"{base_url}/rest/v1/orderflow_aggregates",
        headers={"apikey": key, "Authorization": f"Bearer {key}"},
        params={
            "select": "bucket_timestamp,total_volume,trade_count,source_status,metadata_json",
            "symbol": "eq.ETHUSD",
            "version": "eq.rich_data_v2_orderflow_ws",
            "bucket_timestamp": f"gte.{iso(start)}",
            "order": "bucket_timestamp.asc",
            "limit": "20",
        },
        timeout=20,
    )
    response.raise_for_status()
    return {
        parse_ts(row["bucket_timestamp"]): row
        for row in response.json()
        if start <= parse_ts(row["bucket_timestamp"]) < end
    }


def main():
    load_env()
    start = floor_minute(datetime.now(timezone.utc) + timedelta(minutes=1))
    end = start + timedelta(minutes=3)
    before = status()

    raw_volume = defaultdict(float)
    raw_count = Counter()
    message_types = Counter()
    ws = websocket.create_connection("wss://public-socket.india.delta.exchange", timeout=5)
    ws.settimeout(5)
    ws.send(json.dumps({"type": "enable_heartbeat"}))
    ws.send(json.dumps({"type": "subscribe", "payload": {"channels": [{"name": "trades", "symbols": ["ETHUSD"]}]}}))
    while datetime.now(timezone.utc) < end + timedelta(seconds=8):
        try:
            payload = json.loads(ws.recv())
        except Exception:
            continue
        message_types[str(payload.get("type"))] += 1
        rows = payload.get("d")
        if rows is None and payload.get("type") == "trades":
            rows = [payload]
        if isinstance(rows, dict):
            rows = [rows]
        for row in rows or []:
            if row.get("sy") != "ETHUSD":
                continue
            bucket = floor_minute(datetime.fromtimestamp(int(row["t"]) / 1_000_000, timezone.utc))
            if start <= bucket < end:
                raw_volume[bucket] += float(row["s"])
                raw_count[bucket] += 1
    ws.close()

    time.sleep(3)
    after = status()
    persisted = supabase_rows(start, end)
    buckets = []
    cursor = start
    while cursor < end:
        row = persisted.get(cursor) or {}
        buckets.append(
            {
                "bucket": iso(cursor),
                "raw_count": raw_count.get(cursor, 0),
                "raw_volume": raw_volume.get(cursor, 0.0),
                "persisted_count": row.get("trade_count"),
                "persisted_volume": row.get("total_volume"),
                "persisted_status": row.get("source_status"),
            }
        )
        cursor += timedelta(minutes=1)

    fields = [
        "trade_messages_received_total",
        "trade_messages_parsed_total",
        "trade_messages_accepted_total",
        "trade_messages_deduped_total",
        "trade_messages_late_total",
        "trade_messages_out_of_order_total",
        "trade_volume_received_total",
        "trade_volume_accepted_total",
        "stale_trade_detections",
        "forced_recoveries",
        "reconnect_count",
    ]
    deltas = {field: (after.get(field) or 0) - (before.get(field) or 0) for field in fields}
    print(
        json.dumps(
            {
                "window": {"start": iso(start), "end": iso(end)},
                "raw_message_type_counts": dict(message_types),
                "production_counter_deltas": deltas,
                "production_after": after,
                "buckets": buckets,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
