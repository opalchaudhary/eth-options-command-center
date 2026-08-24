from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import database_reader
from probability_engine.services.v2_shadow_service import (
    DIRECT_FEATURES,
    INTERACTION_FEATURES,
    MANIFEST_PATH,
    compute_v2_features_for_timestamps,
    load_manifest,
)
from probability_engine.research.step15_spec import file_sha256


REPORT_DIR = ROOT / "reports"
TABLES = [
    "probability_v2_feature_snapshots",
    "probability_v2_shadow_predictions",
    "probability_v2_shadow_outcomes",
]


def load_step11_module():
    path = ROOT / "reports" / "step11_indicator_feature_discovery.py"
    spec = importlib.util.spec_from_file_location("step11_indicator_feature_discovery", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def table_exists(table: str) -> bool:
    if not database_reader.SUPABASE_URL or not database_reader.SUPABASE_KEY:
        return False
    url = f"{database_reader.SUPABASE_URL}/rest/v1/{table}"
    response = requests.get(
        url,
        headers=database_reader.HEADERS,
        params={"select": "id", "limit": "1"},
        timeout=15,
    )
    return response.status_code == 200


def parity_check() -> dict:
    step11 = load_step11_module()
    dataset = pd.read_parquet(REPORT_DIR / "step13_probability_v2_dataset.parquet")
    dataset["prediction_timestamp"] = pd.to_datetime(dataset["prediction_timestamp"], utc=True)
    timestamps = pd.Series(sorted(dataset["prediction_timestamp"].drop_duplicates()))
    # Use deterministic representative timestamps after threshold warmup.
    selected = pd.concat(
        [
            timestamps.iloc[350:355],
            timestamps.iloc[len(timestamps) // 2 : len(timestamps) // 2 + 5],
            timestamps.iloc[-5:],
        ],
        ignore_index=True,
    )
    parity_timestamps = timestamps[timestamps <= selected.max()].reset_index(drop=True)
    read_start = parity_timestamps.min() - pd.Timedelta(days=4)
    read_end = parity_timestamps.max() + pd.Timedelta(days=1)
    rows = step11.fetch_all(
        "eth_ohlcv",
        {
            "select": "symbol,resolution,candle_time,epoch_time,open,high,low,close,volume",
            "symbol": "eq.ETHUSD",
            "resolution": "eq.5m",
            "candle_time": f"gte.{read_start.isoformat()}",
        },
        "candle_time",
        max_pages=80,
    )
    ohlcv = pd.DataFrame(rows)
    ohlcv["candle_time"] = pd.to_datetime(ohlcv["candle_time"], utc=True)
    ohlcv = ohlcv[ohlcv["candle_time"] <= read_end].copy()
    rebuilt_all = compute_v2_features_for_timestamps(ohlcv, parity_timestamps)
    rebuilt = rebuilt_all[rebuilt_all["prediction_timestamp"].isin(selected)].copy()
    expected = (
        dataset[dataset["prediction_timestamp"].isin(selected)]
        .drop_duplicates("prediction_timestamp")
        .sort_values("prediction_timestamp")
        [["prediction_timestamp", "feature_source_timestamp"] + DIRECT_FEATURES + INTERACTION_FEATURES]
        .reset_index(drop=True)
    )
    rebuilt = rebuilt.sort_values("prediction_timestamp").reset_index(drop=True)
    features = DIRECT_FEATURES + INTERACTION_FEATURES
    diffs = {}
    mismatch_count = 0
    max_abs = 0.0
    for feature in features:
        left = pd.to_numeric(expected[feature], errors="coerce")
        right = pd.to_numeric(rebuilt[feature], errors="coerce")
        delta = (left - right).abs()
        feature_max = float(delta.max(skipna=True) or 0.0)
        feature_mismatches = int(((delta > 1e-10) & ~(left.isna() & right.isna())).sum())
        max_abs = max(max_abs, feature_max)
        mismatch_count += feature_mismatches
        if feature_mismatches:
            diffs[feature] = {"max_abs_diff": feature_max, "mismatch_count": feature_mismatches}
    return {
        "timestamps_checked": int(len(selected)),
        "features_checked": len(features),
        "max_abs_difference": max_abs,
        "mismatch_count": mismatch_count,
        "mismatches": diffs,
        "passed": mismatch_count == 0 and max_abs <= 1e-10,
    }


def main() -> None:
    load_dotenv(dotenv_path=ROOT / ".env")
    manifest = load_manifest()
    schema = {table: table_exists(table) for table in TABLES}
    parity = parity_check()
    manifest_hash = file_sha256(MANIFEST_PATH)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "manifest_path": str(MANIFEST_PATH.relative_to(ROOT)).replace("\\", "/"),
        "manifest_hash": manifest_hash,
        "direct_model_count": len(manifest.get("models", [])),
        "derived_output_count": len(manifest.get("derived_outputs", [])),
        "feature_contract": manifest.get("feature_contract"),
        "spec_version": manifest.get("spec_version"),
        "schema_status": schema,
        "manual_sql_required": not all(schema.values()),
        "manual_migration": "migrations/probability_v2_shadow_schema.sql",
        "step13_feature_parity": parity,
        "decision_gate": "STEP 16 CODE READY — MANUAL SUPABASE SQL REQUIRED" if not all(schema.values()) else "STEP 16 CODE READY — SHADOW TABLES PRESENT",
    }
    (REPORT_DIR / "step16_shadow_readiness.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
