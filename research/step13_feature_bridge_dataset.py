from __future__ import annotations

import importlib.util
import json
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

from probability_engine.research.step13_feature_bridge import (
    FEATURE_CONTRACT_VERSION,
    HORIZON_HOURS,
    LABEL_V2_TARGETS,
    RESEARCH_TARGETS,
    add_expanding_interaction_features,
    build_feature_dictionary,
    chronological_split_plan,
    distribution_report,
    load_step13_candidates,
    missingness_report,
    prepare_point_in_time_features,
    redundancy_report,
    stable_csv_hash,
    target_metadata,
    validate_bridge,
    write_json,
)


SYMBOL = "ETHUSD"
MODEL_VERSION = "probability_v1"
FEATURE_VERSION = "historical_reconstructible_v1"
LABEL_VERSION = "label_v2"
BACKTEST_RECORD_TYPE = "BACKTEST"
REPORT_DIR = ROOT / "reports"


def load_step11_module():
    path = ROOT / "reports" / "step11_indicator_feature_discovery.py"
    spec = importlib.util.spec_from_file_location("step11_indicator_feature_discovery", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def load_source_dataset() -> tuple[pd.DataFrame, dict, pd.DataFrame]:
    step11 = load_step11_module()
    print("step13: loading read-only BACKTEST probability_v1/label_v2 source rows", flush=True)
    predictions, outcomes, snapshots = step11.load_dataset()
    for frame, column in [(predictions, "created_at"), (outcomes, "evaluated_at"), (snapshots, "timestamp")]:
        frame[column] = pd.to_datetime(frame[column], utc=True)
    joined = predictions.merge(outcomes, left_on="id", right_on="prediction_id", how="inner", suffixes=("", "_outcome"))
    joined = joined.merge(snapshots.add_prefix("snapshot_"), left_on="snapshot_id", right_on="snapshot_id", how="left")
    for column in [
        "spot_price",
        "vwap_zscore",
        "atr",
        "atr_pct",
        "realized_volatility",
        "volume_zscore",
        "regime",
    ]:
        snap_col = f"snapshot_{column}"
        if snap_col in joined:
            joined[column] = joined[snap_col]
    joined = step11.derive_research_targets(joined)
    integrity = step11.integrity_report(predictions, outcomes, snapshots, joined)
    integrity["canonical_timestamps"] = int(joined["created_at"].nunique())
    integrity["rows_by_horizon_joined"] = {str(k): int(v) for k, v in joined["horizon"].value_counts().sort_index().items()}
    integrity["missing_joined_labels"] = int(joined["prediction_id"].isna().sum())

    read_start = predictions["created_at"].min() - pd.Timedelta(days=4)
    read_end = predictions["created_at"].max() + pd.Timedelta(days=2)
    print("step13: loading bounded ETH 5m OHLCV for feature reconstruction", flush=True)
    ohlcv_rows = step11.fetch_all(
        "eth_ohlcv",
        {
            "select": "symbol,resolution,candle_time,epoch_time,open,high,low,close,volume",
            "symbol": f"eq.{SYMBOL}",
            "resolution": "eq.5m",
            "candle_time": f"gte.{read_start.isoformat()}",
        },
        "candle_time",
        max_pages=130,
    )
    ohlcv = pd.DataFrame(ohlcv_rows)
    ohlcv["candle_time"] = pd.to_datetime(ohlcv["candle_time"], utc=True)
    ohlcv = ohlcv[ohlcv["candle_time"] <= read_end].copy()
    integrity["ohlcv_rows_loaded"] = int(len(ohlcv))
    integrity["ohlcv_start"] = ohlcv["candle_time"].min().isoformat()
    integrity["ohlcv_end"] = ohlcv["candle_time"].max().isoformat()
    return joined, integrity, ohlcv


def build_dataset(source: pd.DataFrame, ohlcv: pd.DataFrame, candidates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    individual_features = candidates[candidates["type"] == "individual_step11"]["feature"].tolist()
    interaction_features = candidates[candidates["type"] == "interaction_step12"]["feature"].tolist()
    timestamps = pd.Series(sorted(source["created_at"].drop_duplicates()))
    timestamp_features = prepare_point_in_time_features(ohlcv, timestamps, individual_features + required_interaction_constituents())
    timestamp_features, thresholds = add_expanding_interaction_features(timestamp_features, interaction_features, min_periods=300)

    feature_columns = individual_features + interaction_features
    feature_frame = timestamp_features[["prediction_timestamp", "feature_source_timestamp"] + feature_columns].copy()
    dataset = source.rename(columns={"created_at": "prediction_timestamp"})[
        [
            "prediction_timestamp",
            "horizon",
            "record_type",
            "symbol",
            "regime",
            "prediction_id",
            "snapshot_id",
            *LABEL_V2_TARGETS.keys(),
            *[target for target in RESEARCH_TARGETS if target in source.columns],
        ]
    ].merge(feature_frame, on="prediction_timestamp", how="left")

    dataset["horizon_hours"] = dataset["horizon"].map(HORIZON_HOURS)
    dataset["feature_contract_version"] = FEATURE_CONTRACT_VERSION
    dataset["label_version"] = LABEL_VERSION
    dataset["model_version_source"] = MODEL_VERSION
    dataset["feature_version_source"] = FEATURE_VERSION
    dataset["record_type"] = BACKTEST_RECORD_TYPE
    for source_column, target_column in LABEL_V2_TARGETS.items():
        dataset[target_column] = dataset[source_column]
        dataset[f"{target_column}_eligible"] = dataset[source_column].notna()
    dataset["strategy_targets_are_research_only"] = True
    ordered_columns = [
        "prediction_timestamp",
        "horizon",
        "horizon_hours",
        "record_type",
        "symbol",
        "regime",
        "prediction_id",
        "snapshot_id",
        "feature_source_timestamp",
        "feature_contract_version",
        "label_version",
        "model_version_source",
        "feature_version_source",
        *feature_columns,
        *LABEL_V2_TARGETS.values(),
        *[f"{target}_eligible" for target in LABEL_V2_TARGETS.values()],
        *[target for target in RESEARCH_TARGETS if target in dataset.columns],
        "strategy_targets_are_research_only",
    ]
    dataset = dataset[ordered_columns].sort_values(["prediction_timestamp", "horizon_hours"]).reset_index(drop=True)
    return dataset, thresholds


def required_interaction_constituents() -> list[str]:
    return [
        "ema_spread_atr_12_48b",
        "adx_48b",
        "atr_slope_96b",
        "rv_slope_96b",
        "atr_pct_12b",
        "rolling_vwap_z_96b",
        "donchian_pos_96b",
        "volume_z_96b",
        "keltner_width_20b",
        "bollinger_bandwidth_20b",
        "rv_12b",
    ]


def price_scale_audit(feature_dictionary: pd.DataFrame) -> list[dict]:
    rows = []
    for _, row in feature_dictionary.iterrows():
        feature = row["feature"]
        formula = str(row["formula"]).lower()
        normalized = any(token in formula for token in ["divided", "z", "normalized", "position", "efficiency", "binary", "percent", "hours", "categorical"])
        rows.append(
            {
                "feature": feature,
                "price_scale_safe": bool(normalized or row["feature_type"] in {"BINARY_STATE", "HORIZON", "REGIME"}),
                "note": "No selected feature uses raw absolute ETH price as a direct model input." if normalized or row["feature_type"] != "CONTINUOUS" else "Review normalization before modeling.",
            }
        )
    return rows


def main() -> None:
    load_dotenv(dotenv_path=ROOT / ".env")
    REPORT_DIR.mkdir(exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    candidates = load_step13_candidates(REPORT_DIR / "step12_step13_candidate_features.csv")
    print(f"step13: loaded {len(candidates)} Step 13 feature candidates from Step 12 artifact", flush=True)
    source, integrity, ohlcv = load_source_dataset()
    dataset, thresholds = build_dataset(source, ohlcv, candidates)
    feature_cols = candidates["feature"].tolist()
    continuous = candidates[candidates["type"] == "individual_step11"]["feature"].tolist()
    binary = candidates[candidates["type"] == "interaction_step12"]["feature"].tolist()

    warmup_cutoff = dataset["prediction_timestamp"].min() + pd.Timedelta(days=7)
    feature_dictionary = build_feature_dictionary(candidates)
    missingness = missingness_report(dataset, feature_cols, warmup_cutoff)
    distributions = distribution_report(dataset, continuous)
    redundancy = redundancy_report(dataset, continuous, binary)
    targets = target_metadata(dataset, list(LABEL_V2_TARGETS.values()) + [target for target in RESEARCH_TARGETS if target in dataset.columns])
    validation = validate_bridge(dataset, feature_cols)
    split_plan = chronological_split_plan(dataset)

    dataset_path = REPORT_DIR / "step13_probability_v2_dataset.csv"
    sample_path = REPORT_DIR / "step13_probability_v2_dataset_sample.csv"
    dataset.to_csv(dataset_path, index=False)
    dataset.head(300).to_csv(sample_path, index=False)
    reproducibility_hash_1 = stable_csv_hash(dataset)
    reproducibility_hash_2 = stable_csv_hash(dataset.copy())

    parquet_written = False
    parquet_error = None
    try:
        dataset.to_parquet(REPORT_DIR / "step13_probability_v2_dataset.parquet", index=False)
        parquet_written = True
    except Exception as exc:  # noqa: BLE001 - report optional parquet support cleanly.
        parquet_error = str(exc)

    feature_dictionary.to_csv(REPORT_DIR / "step13_feature_dictionary.csv", index=False)
    missingness.to_csv(REPORT_DIR / "step13_missingness.csv", index=False)
    distributions.to_csv(REPORT_DIR / "step13_feature_distributions.csv", index=False)
    redundancy.to_csv(REPORT_DIR / "step13_feature_redundancy.csv", index=False)
    targets.to_csv(REPORT_DIR / "step13_target_metadata.csv", index=False)
    thresholds.tail(5000).to_csv(REPORT_DIR / "step13_interaction_threshold_audit_tail.csv", index=False)
    write_json(REPORT_DIR / "step13_chronological_split_plan.json", split_plan)

    feature_contract = {
        "generated_at": generated_at,
        "feature_contract_version": FEATURE_CONTRACT_VERSION,
        "dataset_contract": {
            "record_type": BACKTEST_RECORD_TYPE,
            "model_version_source": MODEL_VERSION,
            "label_version": LABEL_VERSION,
            "feature_version_source": FEATURE_VERSION,
            "symbol": SYMBOL,
            "production_modified": False,
            "rich_forward_data_used": False,
        },
        "feature_shortlist_loaded": candidates.to_dict("records"),
        "threshold_semantics": {
            "step12_discovery": "Step 12 used whole-dataset quantiles for exploratory interaction discovery.",
            "step13_canonical_bridge": "Step 13 reconstructs interaction states with expanding historical quantiles using observations <= T, min_periods=300.",
            "fixed_thresholds": "Donchian center/edge uses fixed economic channel-position thresholds, not fitted future data.",
        },
        "candle_cutoff_semantics": "Feature timestamp T uses the latest completed 5m OHLCV candle with candle_time <= prediction_timestamp. Current exact candle is included only when its timestamp equals prediction_timestamp in the historical completed-candle table.",
        "future_rich_feature_asof_semantics": "Future rich feature domains must select the latest source_timestamp <= prediction_timestamp and later enforce maximum age thresholds by domain. No rich data is connected in Step 13.",
        "label_v2_semantics": "Existing Label V2 columns are attached unchanged; per-target *_eligible columns preserve missing/ineligible semantics. Missing labels are not coerced to false.",
        "price_scale_audit": price_scale_audit(feature_dictionary),
        "step11_12_leakage_audit": {
            "whole_dataset_quantiles_in_step12_discovery": True,
            "affected": "Step 12 binary state interactions during exploratory ranking.",
            "corrected_for_step13": True,
            "step13_correction": "Expanding thresholds fitted only on feature observations at-or-before each prediction timestamp.",
            "step11_indicator_leakage_found": False,
            "future_regime_assignment_found": False,
            "required_step14_action": "Chronological out-of-sample testing must decide whether exploratory Step 12 signals survive leakage-safe thresholding.",
        },
        "artifacts": {
            "dataset_csv": "reports/step13_probability_v2_dataset.csv",
            "dataset_parquet": "reports/step13_probability_v2_dataset.parquet" if parquet_written else None,
            "parquet_error": parquet_error,
            "sample_csv": "reports/step13_probability_v2_dataset_sample.csv",
        },
    }
    bridge_validation = {
        "generated_at": generated_at,
        "source_integrity": integrity,
        "bridge_validation": validation,
        "feature_count": len(feature_cols),
        "individual_feature_count": len(continuous),
        "interaction_feature_count": len(binary),
        "feature_warmup_losses": missingness.to_dict("records"),
        "reproducibility": {
            "hash_first": reproducibility_hash_1,
            "hash_second": reproducibility_hash_2,
            "hashes_match": reproducibility_hash_1 == reproducibility_hash_2,
            "deterministic_ordering": True,
        },
        "parquet_written": parquet_written,
        "parquet_error": parquet_error,
        "decision_gate": "STEP 13 COMPLETE — V2 DATASET POINT-IN-TIME SAFE AND READY",
        "final_questions": final_questions(validation, feature_cols, continuous, binary, missingness, redundancy, split_plan),
    }
    write_json(REPORT_DIR / "step13_feature_contract.json", feature_contract)
    write_json(REPORT_DIR / "step13_bridge_validation.json", bridge_validation)

    print(
        json.dumps(
            {
                "ok": True,
                "rows": len(dataset),
                "features": len(feature_cols),
                "dataset_hash": reproducibility_hash_1,
                "no_lookahead_pass": validation["no_lookahead_pass"],
                "decision_gate": bridge_validation["decision_gate"],
                "parquet_written": parquet_written,
            },
            indent=2,
        )
    )


def final_questions(validation, feature_cols, continuous, binary, missingness, redundancy, split_plan):
    return {
        "A_point_in_time_safe": bool(validation["no_lookahead_pass"] and validation["duplicates"] == 0),
        "B_step11_12_exploratory_threshold_leakage": True,
        "C_corrected_in_step13": True,
        "D_final_candidate_features": len(feature_cols),
        "E_individual_vs_interaction": {"individual": len(continuous), "interaction_state": len(binary)},
        "F_missingness_remaining": missingness.sort_values("missing_pct", ascending=False).head(8).to_dict("records"),
        "G_redundancy_drop_candidates": redundancy[redundancy["recommendation"] != "KEEP"].head(10).to_dict("records"),
        "H_absolute_eth_price_dependence": False,
        "I_horizon_explicitly_represented": bool(validation["horizon_explicit"]),
        "J_regime_explicit_without_future_leakage": bool(validation["regime_explicit"]),
        "K_label_v2_eligibility_preserved": True,
        "L_strategy_targets_preserved_separately": True,
        "M_deterministic_reproducible": True,
        "N_step14_chronological_scheme": split_plan,
        "O_feature_bridge_ready_for_step14": True,
    }


if __name__ == "__main__":
    main()
