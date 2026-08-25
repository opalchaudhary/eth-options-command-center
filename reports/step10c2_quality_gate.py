import json
import math
import os
import statistics
from collections import Counter
from datetime import datetime, timedelta, timezone

import requests


def load_env(path=".env"):
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key, value.strip().strip('"').strip("'"))


def parse_ts(value):
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def floor5(value):
    return value.replace(minute=(value.minute // 5) * 5, second=0, microsecond=0)


def percentile(values, pct):
    if not values:
        return None
    xs = sorted(values)
    idx = (len(xs) - 1) * pct / 100
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return xs[int(idx)]
    return xs[lo] * (hi - idx) + xs[hi] * (idx - lo)


def stats(values):
    return {
        "mean": statistics.mean(values) if values else None,
        "median": statistics.median(values) if values else None,
        "p10": percentile(values, 10),
        "p25": percentile(values, 25),
        "p75": percentile(values, 75),
        "p90": percentile(values, 90),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }


def number(value):
    return 0.0 if value is None else float(value)


def main():
    load_env()
    base_url = os.environ["SUPABASE_URL"].rstrip("/")
    key = os.environ["SUPABASE_KEY"]
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}

    def get(table, params):
        response = requests.get(
            f"{base_url}/rest/v1/{table}",
            headers=headers,
            params=params,
            timeout=30,
        )
        if response.status_code >= 400:
            raise SystemExit(
                f"GET {table} failed {response.status_code}: {response.text[:500]} params={params}"
            )
        return response.json()

    select_cols = (
        "bucket_timestamp,symbol,version,trade_count,total_volume,taker_buy_volume,"
        "taker_sell_volume,taker_buy_ratio,taker_sell_ratio,net_taker_volume,"
        "cvd_increment,cvd_running,average_trade_size,max_trade_size,"
        "large_trade_threshold,large_buy_volume,large_sell_volume,"
        "large_trade_imbalance,large_trade_count,source_status,completeness,"
        "staleness_seconds,error_reason,metadata_json"
    )

    v2 = []
    offset = 0
    since_iso = os.getenv("QUALITY_GATE_SINCE_ISO")
    while True:
        params = {
            "select": select_cols,
            "symbol": "eq.ETHUSD",
            "version": "eq.rich_data_v2_orderflow_ws",
            "order": "bucket_timestamp.asc",
            "limit": "1000",
            "offset": str(offset),
        }
        if since_iso:
            params["bucket_timestamp"] = f"gte.{since_iso}"
        batch = get(
            "orderflow_aggregates",
            params,
        )
        v2.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    if not v2:
        raise SystemExit("No V2 rows found")

    first = parse_ts(v2[0]["bucket_timestamp"])
    latest = parse_ts(v2[-1]["bucket_timestamp"])
    start5 = floor5(first)
    end5 = floor5(latest)

    v1 = []
    offset = 0
    while True:
        batch = get(
            "orderflow_aggregates",
            {
                "select": select_cols,
                "symbol": "eq.ETHUSD",
                "version": "eq.rich_data_v1_orderflow",
                "bucket_timestamp": f"gte.{iso(start5)}",
                "order": "bucket_timestamp.asc",
                "limit": "1000",
                "offset": str(offset),
            },
        )
        batch = [row for row in batch if parse_ts(row["bucket_timestamp"]) <= latest]
        v1.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000

    sample = get("eth_ohlcv", {"select": "*", "limit": "1"})
    cols = set(sample[0].keys()) if sample else set()
    time_col = "candle_time" if "candle_time" in cols else "timestamp" if "timestamp" in cols else "time" if "time" in cols else None
    vol_col = "volume" if "volume" in cols else "volume_base" if "volume_base" in cols else None
    symbol_col = "symbol" if "symbol" in cols else None
    res_col = "resolution" if "resolution" in cols else "timeframe" if "timeframe" in cols else None
    if not time_col or not vol_col:
        raise SystemExit(f"Cannot identify OHLCV schema cols={sorted(cols)}")

    params = {"select": ",".join(filter(None, [time_col, vol_col, symbol_col]))}
    if symbol_col:
        params[symbol_col] = "eq.ETHUSD"
    if res_col:
        params[res_col] = "eq.5m"
    params[time_col] = f"gte.{iso(start5)}"
    params["order"] = f"{time_col}.asc"
    params["limit"] = "1000"
    ohlc = get("eth_ohlcv", params)
    ohlc = [row for row in ohlc if parse_ts(row[time_col]) <= end5]

    v2_by_min = {
        parse_ts(row["bucket_timestamp"]).replace(second=0, microsecond=0): row for row in v2
    }
    v1_by_min = {
        parse_ts(row["bucket_timestamp"]).replace(second=0, microsecond=0): row for row in v1
    }
    ohlc_by_5 = {
        parse_ts(row[time_col]).replace(second=0, microsecond=0): row for row in ohlc
    }

    expected = int(
        (
            latest.replace(second=0, microsecond=0)
            - first.replace(second=0, microsecond=0)
        ).total_seconds()
        / 60
    ) + 1
    minutes = [first.replace(second=0, microsecond=0) + timedelta(minutes=i) for i in range(expected)]
    missing = [minute for minute in minutes if minute not in v2_by_min]
    statuses = Counter(row.get("source_status") for row in v2)
    largest_gap = 0
    prev = None
    for row in v2:
        current = parse_ts(row["bucket_timestamp"]).replace(second=0, microsecond=0)
        if prev:
            largest_gap = max(largest_gap, int((current - prev).total_seconds() / 60) - 1)
        prev = current

    windows = []
    cursor = start5
    while cursor <= end5:
        mins = [cursor + timedelta(minutes=i) for i in range(5)]
        if all(minute in v2_by_min for minute in mins) and cursor in ohlc_by_5:
            windows.append(cursor)
        cursor += timedelta(minutes=5)

    per_window = []
    per_window_complete_only = []
    v2_ratios = []
    v1_ratios = []
    v2_complete_only_ratios = []
    v1_complete_only_ratios = []
    for window in windows:
        mins = [window + timedelta(minutes=i) for i in range(5)]
        ref = number(ohlc_by_5[window][vol_col])
        if ref <= 0:
            continue
        v2_volume = sum(number(v2_by_min[minute].get("total_volume")) for minute in mins)
        v1_volume = sum(number(v1_by_min.get(minute, {}).get("total_volume")) for minute in mins)
        v2_ratios.append(v2_volume / ref)
        v1_ratios.append(v1_volume / ref)
        per_window.append(
            {
                "window": iso(window),
                "reference_volume": ref,
                "v2_volume": v2_volume,
                "v1_volume": v1_volume,
                "v2_ratio": v2_volume / ref,
                "v1_ratio": v1_volume / ref,
            }
        )
        if all(v2_by_min[minute].get("source_status") == "COMPLETE" for minute in mins):
            v2_complete_only_ratios.append(v2_volume / ref)
            v1_complete_only_ratios.append(v1_volume / ref)
            per_window_complete_only.append(per_window[-1])

    cvd_mismatches = []
    side_mismatches = []
    for row in v2:
        total = number(row.get("total_volume"))
        buy = number(row.get("taker_buy_volume"))
        sell = number(row.get("taker_sell_volume"))
        cvd = number(row.get("cvd_increment"))
        metadata = row.get("metadata_json") or {}
        unclassified = number(metadata.get("unclassified_volume"))
        if abs((buy - sell) - cvd) > 1e-9:
            cvd_mismatches.append(row["bucket_timestamp"])
        if abs((buy + sell + unclassified) - total) > 1e-9:
            side_mismatches.append(row["bucket_timestamp"])

    aggregate_checks = {}
    for span in [5, 15, 30, 60]:
        checks = 0
        mismatches = 0
        base = (
            floor5(first)
            if span == 5
            else first.replace(minute=(first.minute // span) * span, second=0, microsecond=0)
        )
        cursor = base
        while cursor <= latest:
            mins = [cursor + timedelta(minutes=i) for i in range(span)]
            present = [minute for minute in mins if minute in v2_by_min]
            if present:
                checks += 1
                buy = sum(number(v2_by_min[minute].get("taker_buy_volume")) for minute in present)
                sell = sum(number(v2_by_min[minute].get("taker_sell_volume")) for minute in present)
                cvd = sum(number(v2_by_min[minute].get("cvd_increment")) for minute in present)
                if abs((buy - sell) - cvd) > 1e-9:
                    mismatches += 1
            cursor += timedelta(minutes=span)
        aggregate_checks[f"{span}m"] = {"checks": checks, "mismatches": mismatches}

    buy_ratios = [number(row.get("taker_buy_ratio")) for row in v2 if row.get("taker_buy_ratio") is not None]
    sell_ratios = [number(row.get("taker_sell_ratio")) for row in v2 if row.get("taker_sell_ratio") is not None]
    cvds = [number(row.get("cvd_increment")) for row in v2]
    large_count = sum(int(row.get("large_trade_count") or 0) for row in v2)
    large_buy = sum(number(row.get("large_buy_volume")) for row in v2)
    large_sell = sum(number(row.get("large_sell_volume")) for row in v2)
    total_volume = sum(number(row.get("total_volume")) for row in v2)

    semantic = Counter((row["symbol"], row["bucket_timestamp"], row["version"]) for row in v2)
    invalid_ts = []
    for row in v2:
        try:
            parse_ts(row["bucket_timestamp"])
        except Exception:
            invalid_ts.append(row.get("bucket_timestamp"))
    unexpected = [
        row.get("source_status")
        for row in v2
        if row.get("source_status") not in {"COMPLETE", "NO_TRADES", "INCOMPLETE"}
    ]
    json_sizes = [len(json.dumps(row, separators=(",", ":"))) for row in v2]
    avg_size = statistics.mean(json_sizes) if json_sizes else 0
    mb_day = avg_size * 1440 / 1024 / 1024
    latest_meta = v2[-1].get("metadata_json") or {}

    print(
        json.dumps(
            {
                "window": {
                    "first": iso(first),
                    "latest": iso(latest),
                    "start5": iso(start5),
                    "end5": iso(end5),
                },
                "coverage": {
                    "expected_closed_1m": expected,
                    "persisted_v2": len(v2),
                    "complete": statuses.get("COMPLETE", 0),
                    "no_trades": statuses.get("NO_TRADES", 0),
                    "incomplete": statuses.get("INCOMPLETE", 0),
                    "missing": len(missing),
                    "coverage_pct": 100 * len(v2) / expected if expected else None,
                    "largest_gap_minutes": largest_gap,
                    "missing_sample": [iso(minute) for minute in missing[:10]],
                },
                "volume_capture": {
                    "matched_5m_windows": len(v2_ratios),
                    "v2_overall_ratio": (
                        sum(row["v2_volume"] for row in per_window)
                        / sum(row["reference_volume"] for row in per_window)
                        if per_window
                        else None
                    ),
                    "v2_stats": stats(v2_ratios),
                    "rest_v1_overall_ratio": (
                        sum(row["v1_volume"] for row in per_window)
                        / sum(row["reference_volume"] for row in per_window)
                        if per_window
                        else None
                    ),
                    "rest_v1_stats": stats(v1_ratios),
                    "improvement_overall": (
                        (
                            sum(row["v2_volume"] for row in per_window)
                            / sum(row["reference_volume"] for row in per_window)
                        )
                        / (
                            sum(row["v1_volume"] for row in per_window)
                            / sum(row["reference_volume"] for row in per_window)
                        )
                        if per_window and sum(row["v1_volume"] for row in per_window) > 0
                        else None
                    ),
                    "improvement_median": (
                        statistics.median(v2_ratios) / statistics.median(v1_ratios)
                        if v1_ratios and statistics.median(v1_ratios) > 0
                        else None
                    ),
                    "sample_windows": per_window[:5],
                    "complete_only_matched_5m_windows": len(v2_complete_only_ratios),
                    "complete_only_v2_overall_ratio": (
                        sum(row["v2_volume"] for row in per_window_complete_only)
                        / sum(row["reference_volume"] for row in per_window_complete_only)
                        if per_window_complete_only
                        else None
                    ),
                    "complete_only_v2_stats": stats(v2_complete_only_ratios),
                    "complete_only_rest_v1_overall_ratio": (
                        sum(row["v1_volume"] for row in per_window_complete_only)
                        / sum(row["reference_volume"] for row in per_window_complete_only)
                        if per_window_complete_only
                        else None
                    ),
                    "complete_only_improvement_overall": (
                        (
                            sum(row["v2_volume"] for row in per_window_complete_only)
                            / sum(row["reference_volume"] for row in per_window_complete_only)
                        )
                        / (
                            sum(row["v1_volume"] for row in per_window_complete_only)
                            / sum(row["reference_volume"] for row in per_window_complete_only)
                        )
                        if per_window_complete_only
                        and sum(row["v1_volume"] for row in per_window_complete_only) > 0
                        else None
                    ),
                },
                "cvd": {
                    "mismatches_1m": len(cvd_mismatches),
                    "side_volume_mismatches_1m": len(side_mismatches),
                    "aggregates": aggregate_checks,
                },
                "taker": {
                    "buy_ratio_stats": stats(buy_ratios),
                    "sell_ratio_stats": stats(sell_ratios),
                    "cvd_increment_stats": stats(cvds),
                },
                "large_trades": {
                    "count": large_count,
                    "large_buy_volume": large_buy,
                    "large_sell_volume": large_sell,
                    "large_volume_share": (large_buy + large_sell) / total_volume if total_volume else None,
                },
                "integrity": {
                    "duplicate_semantic_buckets": sum(1 for count in semantic.values() if count > 1),
                    "invalid_timestamps": len(invalid_ts),
                    "unexpected_status_count": len(unexpected),
                    "version_set": sorted({row.get("version") for row in v2}),
                    "source_statuses": dict(statuses),
                },
                "storage": {
                    "avg_row_json_bytes": avg_size,
                    "mb_per_day": mb_day,
                    "mb_per_month_30d": mb_day * 30,
                    "gb_per_year": mb_day * 365 / 1024,
                },
                "latest_metadata": {
                    "duplicate_suppression_count": latest_meta.get("duplicate_suppression_count"),
                    "recent_key_count": latest_meta.get("recent_key_count"),
                    "unclassified_volume": latest_meta.get("unclassified_volume"),
                    "gap_events": latest_meta.get("gap_events"),
                },
                "incomplete_rows": [
                    {
                        "bucket_timestamp": row["bucket_timestamp"],
                        "total_volume": row.get("total_volume"),
                        "trade_count": row.get("trade_count"),
                        "gap_events": (row.get("metadata_json") or {}).get("gap_events"),
                    }
                    for row in v2
                    if row.get("source_status") == "INCOMPLETE"
                ],
                "ohlcv_schema": {
                    "time_col": time_col,
                    "vol_col": vol_col,
                    "symbol_col": symbol_col,
                    "resolution_col": res_col,
                    "ohlcv_rows": len(ohlc),
                    "v1_rows_window": len(v1),
                },
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
