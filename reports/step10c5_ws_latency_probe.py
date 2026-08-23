import json
import time
from datetime import datetime, timezone

import websocket


def parse_epoch_micro(value):
    if value is None:
        return None
    value = int(value)
    if value > 10_000_000_000_000:
        return datetime.fromtimestamp(value / 1_000_000, timezone.utc)
    if value > 10_000_000_000:
        return datetime.fromtimestamp(value / 1_000, timezone.utc)
    return datetime.fromtimestamp(value, timezone.utc)


def pct(values, q):
    values = sorted(values)
    if not values:
        return None
    pos = (len(values) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(values) - 1)
    if lo == hi:
        return values[lo]
    return values[lo] * (1 - (pos - lo)) + values[hi] * (pos - lo)


def main():
    started = time.monotonic()
    samples = []
    fields = []
    ws = websocket.create_connection("wss://public-socket.india.delta.exchange", timeout=8)
    ws.settimeout(8)
    ws.send(json.dumps({"type": "enable_heartbeat"}))
    ws.send(json.dumps({"type": "subscribe", "payload": {"channels": [{"name": "trades", "symbols": ["ETHUSD"]}]}}))
    while time.monotonic() - started < 180 and len(samples) < 500:
        try:
            payload = json.loads(ws.recv())
        except Exception:
            continue
        rows = payload.get("d")
        if rows is None and payload.get("type") == "trades":
            rows = [payload]
        if isinstance(rows, dict):
            rows = [rows]
        received = datetime.now(timezone.utc)
        for row in rows or []:
            if row.get("sy") != "ETHUSD":
                continue
            event_at = parse_epoch_micro(row.get("t"))
            publish_at = parse_epoch_micro(row.get("ts"))
            fields.append(sorted(row.keys()))
            samples.append(
                {
                    "receive_minus_t_seconds": (received - event_at).total_seconds() if event_at else None,
                    "receive_minus_ts_seconds": (received - publish_at).total_seconds() if publish_at else None,
                    "ts_minus_t_seconds": (publish_at - event_at).total_seconds() if event_at and publish_at else None,
                    "has_id": row.get("id") is not None,
                    "has_seq": row.get("seq") is not None or row.get("sequence") is not None,
                }
            )
    ws.close()
    report = {"sample_count": len(samples), "field_examples": fields[:5]}
    for key in ["receive_minus_t_seconds", "receive_minus_ts_seconds", "ts_minus_t_seconds"]:
        values = [item[key] for item in samples if item[key] is not None]
        report[key] = {
            "median": pct(values, 0.5),
            "p75": pct(values, 0.75),
            "p90": pct(values, 0.90),
            "p95": pct(values, 0.95),
            "p99": pct(values, 0.99),
            "max": max(values) if values else None,
            "le_1s_pct": sum(1 for value in values if value <= 1) / len(values) * 100 if values else None,
            "le_2s_pct": sum(1 for value in values if value <= 2) / len(values) * 100 if values else None,
            "le_5s_pct": sum(1 for value in values if value <= 5) / len(values) * 100 if values else None,
            "le_10s_pct": sum(1 for value in values if value <= 10) / len(values) * 100 if values else None,
            "le_30s_pct": sum(1 for value in values if value <= 30) / len(values) * 100 if values else None,
            "gt_30s_pct": sum(1 for value in values if value > 30) / len(values) * 100 if values else None,
        }
    report["id_presence"] = {
        "has_id": sum(1 for item in samples if item["has_id"]),
        "has_seq": sum(1 for item in samples if item["has_seq"]),
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
