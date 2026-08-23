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


def parse_ts(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc) if value else None


def iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def floor_minute(value):
    return value.replace(second=0, microsecond=0)


def floor_5m(value):
    return value.replace(minute=(value.minute // 5) * 5, second=0, microsecond=0)


def pct(numerator, denominator):
    return (float(numerator) / float(denominator) * 100.0) if denominator else None


def quantile(values, q):
    clean = sorted(float(v) for v in values if v is not None)
    if not clean:
        return None
    pos = (len(clean) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(clean) - 1)
    if lo == hi:
        return clean[lo]
    weight = pos - lo
    return clean[lo] * (1 - weight) + clean[hi] * weight


def distribution(values):
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return {}
    return {
        "count": len(clean),
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


def fetch_all(table, base_params, order_col, page_size=1000, max_pages=20):
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
        cursor = f"gt.{batch[-1][order_col]}"
    return rows


def coverage(rows, start, end):
    by_bucket = defaultdict(list)
    for row in rows:
        ts = parse_ts(row["bucket_timestamp"])
        if start <= ts < end:
            by_bucket[ts].append(row)
    expected = int((end - start).total_seconds() // 60)
    statuses = Counter(row.get("source_status") for row in rows if start <= parse_ts(row["bucket_timestamp"]) < end)
    missing = 0
    cursor = start
    while cursor < end:
        if cursor not in by_bucket:
            missing += 1
        cursor += timedelta(minutes=1)
    return {
        "expected_1m_buckets": expected,
        "persisted_buckets": len(by_bucket),
        "coverage_pct": pct(len(by_bucket), expected),
        "missing_buckets": missing,
        "source_status_counts": dict(statuses),
    }


def cvd_checks(rows):
    mismatches = 0
    component_mismatches = 0
    for row in rows:
        buy = float(row.get("taker_buy_volume") or 0)
        sell = float(row.get("taker_sell_volume") or 0)
        cvd = float(row.get("cvd_increment") or 0)
        total = float(row.get("total_volume") or 0)
        metadata = row.get("metadata_json") or {}
        unclassified = float(metadata.get("unclassified_volume") or 0)
        if abs((buy - sell) - cvd) > 1e-9:
            mismatches += 1
        if abs((buy + sell + unclassified) - total) > 1e-9:
            component_mismatches += 1
    return {"cvd_arithmetic_mismatches": mismatches, "volume_component_mismatches": component_mismatches}


def aggregate_5m_orderflow(rows, start, end):
    groups = defaultdict(lambda: {"volume": 0.0, "statuses": []})
    for row in rows:
        ts = parse_ts(row["bucket_timestamp"])
        if start <= ts < end:
            key = floor_5m(ts)
            groups[key]["volume"] += float(row.get("total_volume") or 0)
            groups[key]["statuses"].append(row.get("source_status"))
    return groups


def aggregate_5m_ohlcv(rows, start, end):
    return {
        parse_ts(row["candle_time"]): float(row.get("volume") or 0)
        for row in rows
        if start <= parse_ts(row["candle_time"]) < end
    }


def compare(orderflow, ohlcv, complete_only=False):
    ratios = []
    total_of = 0.0
    total_ref = 0.0
    for window, ref_volume in sorted(ohlcv.items()):
        group = orderflow.get(window)
        if not group or not ref_volume:
            continue
        if complete_only and (len(group["statuses"]) != 5 or any(status != "COMPLETE" for status in group["statuses"])):
            continue
        ratios.append(group["volume"] / ref_volume * 100)
        total_of += group["volume"]
        total_ref += ref_volume
    result = distribution(ratios)
    result["overall"] = pct(total_of, total_ref)
    result["orderflow_volume"] = total_of
    result["reference_volume"] = total_ref
    return result


def main():
    load_dotenv()
    generated_at = datetime.now(timezone.utc)
    status = get_json(f"{API_BASE}/system/status")
    ws = status["rich_orderflow_ws"]
    start = floor_minute(parse_ts(ws["connection_started_at"]))
    end = floor_minute(generated_at) - timedelta(minutes=1)
    rows = fetch_all(
        "orderflow_aggregates",
        {
            "select": "bucket_timestamp,total_volume,trade_count,taker_buy_volume,taker_sell_volume,cvd_increment,source_status,metadata_json",
            "symbol": f"eq.{SYMBOL}",
            "version": f"eq.{WS_VERSION}",
            "bucket_timestamp": f"gte.{iso(start)}",
        },
        "bucket_timestamp",
    )
    ohlcv = fetch_all(
        "eth_ohlcv",
        {
            "select": "candle_time,volume",
            "symbol": f"eq.{SYMBOL}",
            "resolution": "eq.5m",
            "candle_time": f"gte.{iso(floor_5m(start))}",
        },
        "candle_time",
    )
    final_rows = [row for row in rows if start <= parse_ts(row["bucket_timestamp"]) < end]
    represented_count = sum(int(row.get("trade_count") or 0) for row in final_rows)
    represented_volume = sum(float(row.get("total_volume") or 0) for row in final_rows)
    accepted_finalized_count = int(ws.get("trade_messages_accepted_total") or 0) - int(ws.get("buffered_trade_count") or 0)
    accepted_finalized_volume = float(ws.get("trade_volume_accepted_total") or 0) - float(ws.get("buffered_volume") or 0)
    groups = aggregate_5m_orderflow(final_rows, start, end)
    refs = aggregate_5m_ohlcv(ohlcv, floor_5m(start), floor_5m(end))
    print(
        json.dumps(
            {
                "generated_at": iso(generated_at),
                "window": {
                    "start": iso(start),
                    "end": iso(end),
                    "elapsed_minutes": int((end - start).total_seconds() // 60),
                },
                "runtime": ws,
                "internal_reconciliation": {
                    "accepted_finalized_trade_count": accepted_finalized_count,
                    "accepted_finalized_volume": accepted_finalized_volume,
                    "final_represented_trade_count": represented_count,
                    "final_represented_volume": represented_volume,
                    "trade_count_reconciliation_pct": pct(represented_count, accepted_finalized_count),
                    "volume_reconciliation_pct": pct(represented_volume, accepted_finalized_volume),
                    "unaccounted_trade_count": accepted_finalized_count - represented_count,
                    "unaccounted_volume": accepted_finalized_volume - represented_volume,
                },
                "bucket_completeness": coverage(final_rows, start, end),
                "cvd_checks_1m": cvd_checks(final_rows),
                "external_quality": {
                    "ws_v2_vs_ohlcv_5m": compare(groups, refs),
                    "ws_v2_complete_only_vs_ohlcv_5m": compare(groups, refs, complete_only=True),
                },
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
