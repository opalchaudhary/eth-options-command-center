import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from rich_data.options_surface import TARGET_BUCKETS, build_options_surface_rows, normalize_eth_options_surface_chain


API_BASE = "https://deltaforge.in/api"
VERSION = "rich_data_v1_options_surface"


def parse_ts(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc) if value else None


def iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def supabase_get(table, params):
    base = os.environ["SUPABASE_URL"].rstrip("/")
    key = os.environ["SUPABASE_KEY"]
    response = requests.get(
        f"{base}/rest/v1/{table}",
        headers={"apikey": key, "Authorization": f"Bearer {key}"},
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def fetch_all(table, params, order_column, max_pages=20):
    rows = []
    cursor = params.get(order_column)
    query = {**params, "order": f"{order_column}.asc", "limit": "1000"}
    for _ in range(max_pages):
        if cursor:
            query[order_column] = cursor
        batch = supabase_get(table, query)
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < 1000:
            break
        cursor = f"gt.{batch[-1][order_column]}"
    return rows


def cadence_minutes(rows):
    stamps = sorted({parse_ts(row["snapshot_timestamp"]) for row in rows})
    return [round((b - a).total_seconds() / 60, 3) for a, b in zip(stamps, stamps[1:])]


def row_size(row):
    return len(json.dumps(row, separators=(",", ":"), default=str).encode("utf-8"))


def internal_checks(rows):
    bad = Counter()
    for row in rows:
        call = row.get("atm_call_mark")
        put = row.get("atm_put_mark")
        straddle = row.get("atm_straddle_mark")
        spot = row.get("spot_price")
        move_pct = row.get("implied_move_pct")
        if call is not None and put is not None and straddle is not None and abs((call + put) - straddle) > 1e-9:
            bad["straddle_sum"] += 1
        if straddle is not None and spot and move_pct is not None and abs((straddle / spot * 100) - move_pct) > 1e-9:
            bad["implied_move_pct"] += 1
        if row.get("atm_iv") is not None and row["atm_iv"] < 0:
            bad["negative_atm_iv"] += 1
        if row.get("put_25d_iv") is not None and row["put_25d_iv"] < 0:
            bad["negative_put_25d_iv"] += 1
        if row.get("call_25d_iv") is not None and row["call_25d_iv"] < 0:
            bad["negative_call_25d_iv"] += 1
        if row.get("atm_strike") is not None and spot and abs(row["atm_strike"] - spot) / spot > 0.05:
            bad["atm_not_near_spot_gt_5pct"] += 1
    return dict(bad)


def duplicate_groups(rows):
    counts = Counter((row["symbol"], row["logical_expiry_bucket"], row["snapshot_timestamp"], row["version"]) for row in rows)
    return [
        {"symbol": key[0], "bucket": key[1], "snapshot_timestamp": key[2], "version": key[3], "count": count}
        for key, count in counts.items()
        if count > 1
    ]


def main():
    load_dotenv()
    status = requests.get(f"{API_BASE}/system/status", timeout=30).json()
    rows = fetch_all(
        "options_surface_snapshots",
        {
            "select": "*",
            "version": f"eq.{VERSION}",
            "snapshot_timestamp": "gte.2026-08-23T17:55:00Z",
        },
        "snapshot_timestamp",
    )
    by_bucket = defaultdict(list)
    for row in rows:
        by_bucket[row["logical_expiry_bucket"]].append(row)

    latest = {}
    for bucket, bucket_rows in by_bucket.items():
        latest[bucket] = sorted(bucket_rows, key=lambda row: row["snapshot_timestamp"])[-1]

    live_df = normalize_eth_options_surface_chain()
    live_rows = build_options_surface_rows(live_df, now=datetime.now(timezone.utc), buckets=TARGET_BUCKETS, realized_volatility_reference=None)
    live_latest = {row["logical_expiry_bucket"]: row for row in live_rows}
    plausibility = {}
    for bucket, row in latest.items():
        live = live_latest.get(bucket)
        plausibility[bucket] = {
            "atm_near_spot_pct": abs(row["atm_strike"] - row["spot_price"]) / row["spot_price"] * 100 if row.get("atm_strike") and row.get("spot_price") else None,
            "live_actual_expiry": live.get("actual_expiry") if live else None,
            "latest_actual_expiry": row.get("actual_expiry"),
            "live_total_call_oi": live.get("total_call_oi") if live else None,
            "latest_total_call_oi": row.get("total_call_oi"),
            "live_total_put_oi": live.get("total_put_oi") if live else None,
            "latest_total_put_oi": row.get("total_put_oi"),
            "live_call_wall": live.get("largest_call_oi_strike") if live else None,
            "latest_call_wall": row.get("largest_call_oi_strike"),
            "live_put_wall": live.get("largest_put_oi_strike") if live else None,
            "latest_put_wall": row.get("largest_put_oi_strike"),
        }

    sizes = [row_size(row) for row in rows]
    row_count_available = sum(144 if bucket.startswith("D") else 48 if bucket.startswith("W") else 24 for bucket in by_bucket)
    avg_size = mean(sizes) if sizes else 0
    report = {
        "generated_at": iso(datetime.now(timezone.utc)),
        "system": {
            "scheduler_running": status.get("scheduler_running"),
            "running_jobs": status.get("running_jobs"),
            "options_job": status.get("job_state", {}).get("rich_options_surface_v1"),
            "options_job_registered_count": sum(1 for job in status.get("jobs", []) if job.get("id") == "rich_options_surface_v1"),
            "options_job_trigger": [job.get("trigger") for job in status.get("jobs", []) if job.get("id") == "rich_options_surface_v1"],
        },
        "row_count": len(rows),
        "buckets_seen": sorted(by_bucket),
        "rows_by_bucket": {
            bucket: {
                "count": len(bucket_rows),
                "actual_expiry": latest[bucket].get("actual_expiry"),
                "latest_dte_days": latest[bucket].get("dte_days"),
                "source_status_counts": dict(Counter(row.get("source_status") for row in bucket_rows)),
                "avg_completeness": mean([row.get("completeness") or 0 for row in bucket_rows]),
                "avg_contracts_seen": mean([row.get("contracts_seen") or 0 for row in bucket_rows]),
                "avg_valid_iv_contracts": mean([row.get("valid_iv_contracts") or 0 for row in bucket_rows]),
                "avg_valid_quote_contracts": mean([row.get("valid_quote_contracts") or 0 for row in bucket_rows]),
                "avg_valid_oi_contracts": mean([row.get("valid_oi_contracts") or 0 for row in bucket_rows]),
                "cadence_minutes": cadence_minutes(bucket_rows),
            }
            for bucket, bucket_rows in sorted(by_bucket.items())
        },
        "duplicate_semantic_groups": duplicate_groups(rows),
        "internal_plausibility_failures": internal_checks(rows),
        "latest_plausibility_vs_live": plausibility,
        "row_size_bytes": {
            "count": len(sizes),
            "average": avg_size,
            "min": min(sizes) if sizes else None,
            "max": max(sizes) if sizes else None,
        },
        "storage_projection": {
            "rows_per_day_current_available_buckets": row_count_available,
            "rows_per_day_theoretical_max": 744,
            "mb_per_day_current_json": avg_size * row_count_available / 1024 / 1024,
            "mb_per_month_current_json": avg_size * row_count_available * 30 / 1024 / 1024,
            "gb_per_year_current_json": avg_size * row_count_available * 365 / 1024 / 1024 / 1024,
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
