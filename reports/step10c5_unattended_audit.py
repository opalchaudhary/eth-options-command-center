import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import mean, median

import requests
from dotenv import load_dotenv


API_BASE = "https://deltaforge.in/api"
SYMBOL = "ETHUSD"
WS_VERSION = "rich_data_v2_orderflow_ws"
REST_VERSION = "rich_data_v1_orderflow"


def parse_ts(value):
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def floor_minute(value):
    return value.replace(second=0, microsecond=0)


def floor_5m(value):
    minute = (value.minute // 5) * 5
    return value.replace(minute=minute, second=0, microsecond=0)


def pct(numerator, denominator):
    return (float(numerator) / float(denominator) * 100.0) if denominator else None


def quantile(values, q):
    clean = sorted(float(v) for v in values if v is not None)
    if not clean:
        return None
    pos = (len(clean) - 1) * q
    lower = int(pos)
    upper = min(lower + 1, len(clean) - 1)
    if lower == upper:
        return clean[lower]
    weight = pos - lower
    return clean[lower] * (1 - weight) + clean[upper] * weight


def stats(values):
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return {}
    return {
        "count": len(clean),
        "overall": None,
        "mean": mean(clean),
        "median": median(clean),
        "p10": quantile(clean, 0.10),
        "p25": quantile(clean, 0.25),
        "p75": quantile(clean, 0.75),
        "p90": quantile(clean, 0.90),
        "min": min(clean),
        "max": max(clean),
    }


def get_json(url, params=None, headers=None):
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def supabase_get(table, params):
    base = os.environ["SUPABASE_URL"].rstrip("/")
    key = os.environ["SUPABASE_KEY"]
    return get_json(
        f"{base}/rest/v1/{table}",
        params=params,
        headers={"apikey": key, "Authorization": f"Bearer {key}"},
    )


def fetch_all(table, base_params, order_col, page_size=1000, max_pages=100):
    rows = []
    cursor = base_params.get(order_col)
    params = {**base_params, "order": f"{order_col}.asc", "limit": str(page_size)}
    for _ in range(max_pages):
        if cursor:
            params[order_col] = cursor
        batch = supabase_get(table, params)
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < page_size:
            break
        last = batch[-1][order_col]
        cursor = f"gt.{last}"
    return rows


def status_snapshot():
    return get_json(f"{API_BASE}/system/status")


def infer_scheduler_start(status):
    candidates = []
    for job_name, state in status.get("job_state", {}).items():
        last_started = parse_ts(state.get("last_started_at"))
        run_count = int(state.get("run_count") or 0)
        if not last_started or run_count <= 1:
            continue
        trigger_seconds = None
        for job in status.get("jobs", []):
            if job.get("id") != job_name:
                continue
            trigger = str(job.get("trigger"))
            if "0:01:00" in trigger:
                trigger_seconds = 60
            elif "0:05:00" in trigger:
                trigger_seconds = 300
            elif "0:15:00" in trigger:
                trigger_seconds = 900
            elif "0:30:00" in trigger:
                trigger_seconds = 1800
            elif "1:00:00" in trigger:
                trigger_seconds = 3600
        if trigger_seconds:
            candidates.append(last_started - timedelta(seconds=trigger_seconds * (run_count - 1)))
    return min(candidates) if candidates else None


def coverage(rows, start, end):
    by_bucket = defaultdict(list)
    for row in rows:
        ts = parse_ts(row["bucket_timestamp"])
        if start <= ts < end:
            by_bucket[ts].append(row)
    expected = int((end - start).total_seconds() // 60)
    missing = []
    duplicate_semantic = {}
    largest_gap_minutes = 0
    previous_seen = None
    cursor = start
    while cursor < end:
        count = len(by_bucket.get(cursor, []))
        if count == 0:
            missing.append(iso(cursor))
        elif count > 1:
            duplicate_semantic[iso(cursor)] = count
        if count:
            if previous_seen:
                largest_gap_minutes = max(largest_gap_minutes, int((cursor - previous_seen).total_seconds() // 60) - 1)
            previous_seen = cursor
        cursor += timedelta(minutes=1)
    statuses = Counter(row.get("source_status") for row in rows if start <= parse_ts(row["bucket_timestamp"]) < end)
    gaps = []
    streak_start = None
    streak_end = None
    for item in missing:
        ts = parse_ts(item)
        if streak_start is None:
            streak_start = streak_end = ts
        elif ts == streak_end + timedelta(minutes=1):
            streak_end = ts
        else:
            gaps.append({"start": iso(streak_start), "end": iso(streak_end), "minutes": int((streak_end - streak_start).total_seconds() // 60) + 1})
            streak_start = streak_end = ts
    if streak_start is not None:
        gaps.append({"start": iso(streak_start), "end": iso(streak_end), "minutes": int((streak_end - streak_start).total_seconds() // 60) + 1})
    return {
        "expected_1m_buckets": expected,
        "persisted_buckets": sum(1 for rows_for_bucket in by_bucket.values() if rows_for_bucket),
        "coverage_pct": pct(sum(1 for rows_for_bucket in by_bucket.values() if rows_for_bucket), expected),
        "source_status_counts": dict(statuses),
        "missing_bucket_count": len(missing),
        "missing_bucket_ranges": gaps[:20],
        "duplicate_semantic_buckets": duplicate_semantic,
        "largest_timestamp_gap_minutes": largest_gap_minutes,
        "abnormal_gap_count_gt_1m": sum(1 for gap in gaps if gap["minutes"] > 1),
    }


def aggregate_5m_orderflow(rows, start, end):
    groups = defaultdict(lambda: {"volume": 0.0, "trade_count": 0, "statuses": []})
    for row in rows:
        ts = parse_ts(row["bucket_timestamp"])
        if not (start <= ts < end):
            continue
        key = floor_5m(ts)
        groups[key]["volume"] += float(row.get("total_volume") or 0)
        groups[key]["trade_count"] += int(row.get("trade_count") or 0)
        groups[key]["statuses"].append(row.get("source_status"))
    return groups


def aggregate_5m_ohlcv(rows, start, end):
    groups = {}
    for row in rows:
        ts = parse_ts(row["candle_time"])
        if start <= ts < end:
            groups[ts] = float(row.get("volume") or 0)
    return groups


def compare_to_ohlcv(orderflow_groups, ohlcv_groups, complete_only=False):
    ratios = []
    total_of = 0.0
    total_ref = 0.0
    window_count = 0
    for window, ref_volume in sorted(ohlcv_groups.items()):
        group = orderflow_groups.get(window)
        if not group or not ref_volume:
            continue
        if complete_only and (len(group["statuses"]) != 5 or any(status != "COMPLETE" for status in group["statuses"])):
            continue
        ratios.append(group["volume"] / ref_volume * 100.0)
        total_of += group["volume"]
        total_ref += ref_volume
        window_count += 1
    result = stats(ratios)
    result["overall"] = pct(total_of, total_ref)
    result["window_count"] = window_count
    result["orderflow_volume"] = total_of
    result["reference_volume"] = total_ref
    return result


def cvd_checks(rows):
    bad = []
    volume_bad = []
    for row in rows:
        buy = float(row.get("taker_buy_volume") or 0)
        sell = float(row.get("taker_sell_volume") or 0)
        cvd = float(row.get("cvd_increment") or 0)
        total = float(row.get("total_volume") or 0)
        metadata = row.get("metadata_json") or {}
        unclassified = float(metadata.get("unclassified_volume") or 0)
        if abs((buy - sell) - cvd) > 1e-9:
            bad.append(row["bucket_timestamp"])
        if abs((buy + sell + unclassified) - total) > 1e-9:
            volume_bad.append(row["bucket_timestamp"])
    return {"cvd_arithmetic_mismatches": len(bad), "volume_component_mismatches": len(volume_bad)}


def segment(rows, ohlcv, start, end):
    duration = end - start
    blocks = []
    for index, label in enumerate(["early", "middle", "late"]):
        block_start = start + duration * index / 3
        block_end = start + duration * (index + 1) / 3
        block_start = floor_5m(block_start)
        block_end = floor_5m(block_end)
        groups = aggregate_5m_orderflow(rows, block_start, block_end)
        refs = aggregate_5m_ohlcv(ohlcv, block_start, block_end)
        blocks.append({"label": label, "start": iso(block_start), "end": iso(block_end), "ws_vs_ohlcv": compare_to_ohlcv(groups, refs)})
    return blocks


def main():
    load_dotenv()
    status = status_snapshot()
    health = get_json(f"{API_BASE}/health")
    now = datetime.now(timezone.utc)
    audit_end = floor_minute(now) - timedelta(minutes=1)
    scheduler_start = infer_scheduler_start(status)
    ws_current = status.get("rich_orderflow_ws", {})
    current_process_start = parse_ts(ws_current.get("connection_started_at"))

    min_rows = supabase_get(
        "orderflow_aggregates",
        {
            "select": "bucket_timestamp",
            "symbol": f"eq.{SYMBOL}",
            "version": f"eq.{WS_VERSION}",
            "order": "bucket_timestamp.asc",
            "limit": "1",
        },
    )
    first_ws_row = parse_ts(min_rows[0]["bucket_timestamp"]) if min_rows else None
    audit_start = max(dt for dt in [scheduler_start, first_ws_row] if dt is not None)
    audit_start = floor_minute(audit_start)

    ws_rows = fetch_all(
        "orderflow_aggregates",
        {
            "select": "bucket_timestamp,total_volume,trade_count,taker_buy_volume,taker_sell_volume,cvd_increment,cvd_5m,cvd_15m,cvd_1h,source_status,completeness,error_reason,metadata_json,version,symbol",
            "symbol": f"eq.{SYMBOL}",
            "version": f"eq.{WS_VERSION}",
            "bucket_timestamp": f"gte.{iso(audit_start)}",
        },
        "bucket_timestamp",
    )
    rest_rows = fetch_all(
        "orderflow_aggregates",
        {
            "select": "bucket_timestamp,total_volume,trade_count,source_status,version,symbol",
            "symbol": f"eq.{SYMBOL}",
            "version": f"eq.{REST_VERSION}",
            "bucket_timestamp": f"gte.{iso(audit_start)}",
        },
        "bucket_timestamp",
    )
    ohlcv_rows = fetch_all(
        "eth_ohlcv",
        {
            "select": "candle_time,volume,symbol,resolution",
            "symbol": f"eq.{SYMBOL}",
            "resolution": "eq.5m",
            "candle_time": f"gte.{iso(floor_5m(audit_start))}",
        },
        "candle_time",
    )

    ws_groups = aggregate_5m_orderflow(ws_rows, audit_start, audit_end)
    rest_groups = aggregate_5m_orderflow(rest_rows, audit_start, audit_end)
    ohlcv_groups = aggregate_5m_ohlcv(ohlcv_rows, floor_5m(audit_start), floor_5m(audit_end))

    report = {
        "generated_at": iso(now),
        "health": health,
        "audit_window": {
            "audit_start": iso(audit_start),
            "audit_end": iso(audit_end),
            "elapsed_hours": round((audit_end - audit_start).total_seconds() / 3600, 3),
            "scheduler_start_inferred": iso(scheduler_start) if scheduler_start else None,
            "first_ws_db_row": iso(first_ws_row) if first_ws_row else None,
            "current_ws_connection_started_at": iso(current_process_start) if current_process_start else None,
            "basis": "max(inferred FastAPI scheduler start, first WS V2 DB bucket); current process counters are separated because websocket connection restarted later",
        },
        "current_runtime": {
            "scheduler": {
                "enabled": status.get("scheduler_enabled"),
                "running": status.get("scheduler_running"),
                "running_jobs": status.get("running_jobs"),
                "skipped_cycles": status.get("skipped_cycles"),
                "selected_job_state": {
                    name: status.get("job_state", {}).get(name)
                    for name in [
                        "probability_prediction_v1",
                        "probability_outcome_evaluator_v1",
                        "rich_derivatives_v1",
                        "rich_orderflow_v1",
                        "rich_orderbook_v1",
                    ]
                },
            },
            "rich_orderflow_ws": ws_current,
        },
        "database_full_interval": {
            "ws_v2_coverage": coverage(ws_rows, audit_start, audit_end),
            "ws_v2_volume": sum(float(row.get("total_volume") or 0) for row in ws_rows if audit_start <= parse_ts(row["bucket_timestamp"]) < audit_end),
            "ws_v2_trade_count": sum(int(row.get("trade_count") or 0) for row in ws_rows if audit_start <= parse_ts(row["bucket_timestamp"]) < audit_end),
            "ws_v2_cvd_checks": cvd_checks([row for row in ws_rows if audit_start <= parse_ts(row["bucket_timestamp"]) < audit_end]),
            "rest_v1_coverage": coverage(rest_rows, audit_start, audit_end),
            "rest_v1_volume": sum(float(row.get("total_volume") or 0) for row in rest_rows if audit_start <= parse_ts(row["bucket_timestamp"]) < audit_end),
            "rest_v1_trade_count": sum(int(row.get("trade_count") or 0) for row in rest_rows if audit_start <= parse_ts(row["bucket_timestamp"]) < audit_end),
        },
        "external_quality": {
            "ws_v2_vs_ohlcv_5m": compare_to_ohlcv(ws_groups, ohlcv_groups),
            "ws_v2_complete_only_vs_ohlcv_5m": compare_to_ohlcv(ws_groups, ohlcv_groups, complete_only=True),
            "rest_v1_vs_ohlcv_5m": compare_to_ohlcv(rest_groups, ohlcv_groups),
            "chronological_thirds": segment(ws_rows, ohlcv_rows, audit_start, audit_end),
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
