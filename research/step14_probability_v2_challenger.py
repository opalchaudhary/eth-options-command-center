from __future__ import annotations

import importlib.util
import json
import pickle
import sys
import warnings
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
warnings.filterwarnings("ignore", category=RuntimeWarning)

from probability_engine.research.step14_challenger import (
    HORIZON_HOURS,
    TEST_START,
    VALIDATION_START,
    apply_platt,
    brier_score,
    chronological_masks,
    fit_model,
    fit_platt,
    fit_preprocessor,
    metrics_row,
    nonoverlap_subset,
    predict_model,
    stable_file_hash,
    temporal_thirds,
)


REPORT_DIR = ROOT / "reports"
DATASET_PATH = REPORT_DIR / "step13_probability_v2_dataset.parquet"
EXPECTED_HASH = "51e31e8aa1f0db085246126326576e88c8e77ccdae40d86fb56995f5d65c83de"
FEATURE_CONTRACT = "probability_v2_features_v1"
MODEL_DIR = REPORT_DIR / "step14_research_models"

INDIVIDUAL_FEATURES = [
    "atr_slope_96b",
    "rv_slope_96b",
    "atr_pct_12b",
    "bollinger_bandwidth_20b",
    "keltner_width_20b",
    "rv_12b",
    "price_efficiency_96b",
    "rolling_vwap_z_96b",
    "donchian_pos_96b",
    "volume_z_96b",
]
INTERACTION_FEATURES = [
    "high_atr_pct_and_rising_atr_slope",
    "ema_spread_high_and_rising_atr",
    "low_vwap_displacement_and_low_atr",
    "high_vwap_displacement_and_rising_atr",
    "low_atr_and_mid_donchian",
    "rising_atr_and_high_volume",
    "low_atr_pct_and_falling_atr_slope",
    "low_adx_and_falling_atr",
]
DEREDUNDANT_FEATURES = [
    "atr_slope_96b",
    "atr_pct_12b",
    "bollinger_bandwidth_20b",
    "price_efficiency_96b",
    "rolling_vwap_z_96b",
    "volume_z_96b",
    "high_atr_pct_and_rising_atr_slope",
    "ema_spread_high_and_rising_atr",
    "low_vwap_displacement_and_low_atr",
    "rising_atr_and_high_volume",
    "low_atr_pct_and_falling_atr_slope",
]
FEATURE_SETS = {
    "step11_individuals": INDIVIDUAL_FEATURES,
    "step12_interactions": INTERACTION_FEATURES,
    "all_step13": INDIVIDUAL_FEATURES + INTERACTION_FEATURES,
    "compact_deredundant": DEREDUNDANT_FEATURES,
}
MODEL_FAMILIES = ["logistic", "tree_depth2", "boosted_stumps"]
LABEL_V1_PROB_COLUMNS = {
    "mean_reversion": "mean_reversion_probability",
    "upside_breakout": "upside_breakout_probability",
    "downside_breakdown": "downside_breakdown_probability",
    "range_continuation": "range_continuation_probability",
    "trend_continuation": "trend_continuation_probability",
}
PRIMARY_TARGETS = [
    "range_continuation",
    "path_inside_70",
    "range_breached",
    "realized_over_range_width_ge_1",
    "oscillatory_path",
    "trend_efficiency_high",
    "one_sided_runaway",
    "upper_breach_only",
    "lower_breach_only",
    "both_side_breach",
    "fast_1atr_touch",
    "up_excursion_ge_1_0_atr",
    "down_excursion_ge_1_0_atr",
    "mean_reversion",
    "upside_breakout",
    "downside_breakdown",
    "trend_continuation",
]


def load_step11_module():
    path = ROOT / "reports" / "step11_indicator_feature_discovery.py"
    spec = importlib.util.spec_from_file_location("step11_indicator_feature_discovery", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def load_v1_probabilities() -> pd.DataFrame:
    step11 = load_step11_module()
    predictions, _, _ = step11.load_dataset()
    columns = ["id", *LABEL_V1_PROB_COLUMNS.values()]
    return predictions[columns].rename(columns={"id": "prediction_id"})


def split_target_frame(data: pd.DataFrame, target: str, horizon: str) -> dict[str, pd.DataFrame]:
    hdata = data[(data["horizon"] == horizon) & data[target].notna()].copy()
    masks = chronological_masks(hdata, horizon)
    return {name: hdata[mask].copy() for name, mask in masks.items()}


def target_relevance(target: str) -> str:
    if target in {"range_continuation", "path_inside_70", "range_breached", "realized_over_range_width_ge_1"}:
        return "BOTH"
    if target in {"oscillatory_path", "trend_efficiency_high", "one_sided_runaway"}:
        return "GRID"
    if target in {"upper_breach_only", "lower_breach_only", "both_side_breach"}:
        return "IRON_FLY"
    if target in {"fast_1atr_touch", "up_excursion_ge_1_0_atr", "down_excursion_ge_1_0_atr"}:
        return "DEFENSIVE_LONG_OPTION"
    return "GENERIC_PROBABILITY"


def evaluate_candidate(train: pd.DataFrame, validation: pd.DataFrame, target: str, features: list[str], family: str) -> tuple[dict, dict, object, dict, tuple | None]:
    scale = family == "logistic"
    preprocessor = fit_preprocessor(train, features, scale=scale)
    x_train = preprocessor.transform(train)
    y_train = train[target].astype(int).to_numpy()
    x_val = preprocessor.transform(validation)
    y_val = validation[target].astype(int).to_numpy()
    model = fit_model(family, x_train, y_train)
    raw_val = predict_model(model, x_val)
    train_base = float(y_train.mean())
    base_val = np.full(len(y_val), train_base)
    base_brier = brier_score(y_val, base_val)
    raw_metrics = metrics_row(y_val, raw_val, base_brier)
    platt_params = fit_platt(raw_val, y_val) if len(y_val) >= 120 and np.unique(y_val).size == 2 else None
    cal_val = apply_platt(raw_val, platt_params)
    cal_metrics = metrics_row(y_val, cal_val, base_brier)
    if cal_metrics["brier"] <= raw_metrics["brier"] and cal_metrics["ece"] is not None:
        selected_metrics = cal_metrics
        calibration = "platt"
    else:
        selected_metrics = raw_metrics
        platt_params = None
        calibration = "none"
    return selected_metrics, raw_metrics, preprocessor, model, {"platt_params": platt_params, "calibration": calibration}


def predict_bundle(bundle: dict, frame: pd.DataFrame) -> np.ndarray:
    x = bundle["preprocessor"].transform(frame)
    raw = predict_model(bundle["model"], x)
    return apply_platt(raw, bundle.get("platt_params"))


def feature_importance(bundle: dict, frame: pd.DataFrame, target: str, baseline_brier: float) -> list[dict]:
    y = frame[target].astype(int).to_numpy()
    base_pred = predict_bundle(bundle, frame)
    base_brier = brier_score(y, base_pred)
    rows = []
    for idx, feature in enumerate(bundle["features"]):
        shuffled = frame.copy()
        values = shuffled[feature].to_numpy().copy()
        if len(values) > 1:
            values = np.roll(values, 1)
        shuffled[feature] = values
        perm_pred = predict_bundle(bundle, shuffled)
        rows.append(
            {
                "target": target,
                "horizon": bundle["horizon"],
                "feature": feature,
                "model_family": bundle["family"],
                "feature_set": bundle["feature_set"],
                "permutation_brier_increase": brier_score(y, perm_pred) - base_brier,
                "test_brier_skill": None if baseline_brier <= 0 else 1 - base_brier / baseline_brier,
            }
        )
    return rows


def main() -> None:
    load_dotenv(dotenv_path=ROOT / ".env")
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    REPORT_DIR.mkdir(exist_ok=True)
    MODEL_DIR.mkdir(exist_ok=True)
    for old_model in MODEL_DIR.glob("*_research_only.pkl"):
        old_model.unlink()
    data = pd.read_parquet(DATASET_PATH)
    data["prediction_timestamp"] = pd.to_datetime(data["prediction_timestamp"], utc=True)
    parquet_file_hash = stable_file_hash(str(DATASET_PATH))
    canonical_dataset_hash = stable_frame_hash(data)
    target_meta = pd.read_csv(REPORT_DIR / "step13_target_metadata.csv")
    usable_pairs = {
        (row["target"], row["horizon"])
        for _, row in target_meta[target_meta["saturation_flag"] == "USABLE"].iterrows()
    }
    usable_targets = {target for target, _ in usable_pairs}
    targets = [target for target in PRIMARY_TARGETS if target in data.columns and target in usable_targets]
    print(f"step14: loaded {len(data)} rows, {len(targets)} usable targets", flush=True)

    print("step14: loading Probability V1 forecasts for direct label comparisons", flush=True)
    v1_probs = load_v1_probabilities()
    data = data.merge(v1_probs, on="prediction_id", how="left")

    validation_rows = []
    test_rows = []
    calibration_rows = []
    baseline_rows = []
    v1_rows = []
    nonoverlap_rows = []
    regime_rows = []
    temporal_rows = []
    importance_rows = []
    walkforward_rows = []
    selected_bundles = {}
    split_counts = []

    for target in targets:
        for horizon in sorted(data["horizon"].dropna().unique(), key=lambda h: HORIZON_HOURS[h]):
            if (target, horizon) not in usable_pairs:
                continue
            parts = split_target_frame(data, target, horizon)
            train, validation, test = parts["train"], parts["validation"], parts["test"]
            if min(len(train), len(validation), len(test)) < 120 or train[target].nunique() < 2 or validation[target].nunique() < 2 or test[target].nunique() < 2:
                continue
            split_counts.append({"target": target, "horizon": horizon, "train_n": len(train), "validation_n": len(validation), "test_n": len(test)})
            y_val = validation[target].astype(int).to_numpy()
            y_test = test[target].astype(int).to_numpy()
            train_base_rate = float(train[target].mean())
            base_val_pred = np.full(len(y_val), train_base_rate)
            base_test_pred = np.full(len(y_test), train_base_rate)
            val_base_metrics = metrics_row(y_val, base_val_pred, brier_score(y_val, base_val_pred))
            test_base_brier = brier_score(y_test, base_test_pred)
            test_base_metrics = metrics_row(y_test, base_test_pred, test_base_brier)
            baseline_rows.append({"target": target, "horizon": horizon, "split": "validation", "train_base_probability": train_base_rate, **val_base_metrics})
            baseline_rows.append({"target": target, "horizon": horizon, "split": "test", "train_base_probability": train_base_rate, **test_base_metrics})

            best = None
            best_bundle = None
            for feature_set, features in FEATURE_SETS.items():
                for family in MODEL_FAMILIES:
                    metrics, raw_metrics, preprocessor, model, params = evaluate_candidate(train, validation, target, features, family)
                    calibration = params["calibration"]
                    platt_params = params["platt_params"]
                    row = {
                        "target": target,
                        "horizon": horizon,
                        "strategy_relevance": target_relevance(target),
                        "feature_set": feature_set,
                        "model_family": family,
                        "calibration": calibration,
                        "split": "validation",
                        **metrics,
                    }
                    validation_rows.append(row)
                    calibration_rows.append({"target": target, "horizon": horizon, "feature_set": feature_set, "model_family": family, "raw_brier": raw_metrics["brier"], "selected_brier": metrics["brier"], "calibration": calibration, "raw_ece": raw_metrics["ece"], "selected_ece": metrics["ece"]})
                    selection_score = (metrics["brier_skill_vs_train_base"] or -99) + 0.04 * ((metrics["auc"] or 0.5) - 0.5) - (0.002 if family == "boosted_stumps" else 0)
                    if best is None or selection_score > best["selection_score"]:
                        best = {**row, "selection_score": selection_score}
                        best_bundle = {
                            "target": target,
                            "horizon": horizon,
                            "features": features,
                            "feature_set": feature_set,
                            "family": family,
                            "calibration": calibration,
                            "preprocessor": preprocessor,
                            "model": model,
                            "platt_params": platt_params,
                            "train_base_rate": train_base_rate,
                        }

            if best_bundle is None:
                continue
            selected_bundles[(target, horizon)] = best_bundle
            test_pred = predict_bundle(best_bundle, test)
            test_metrics = metrics_row(y_test, test_pred, test_base_brier)
            test_rows.append(
                {
                    "target": target,
                    "horizon": horizon,
                    "strategy_relevance": target_relevance(target),
                    "feature_set": best_bundle["feature_set"],
                    "model_family": best_bundle["family"],
                    "calibration": best_bundle["calibration"],
                    "selected_on": "validation_only",
                    **test_metrics,
                }
            )
            non = nonoverlap_subset(test, horizon)
            if len(non) >= 25 and non[target].nunique() == 2:
                y_non = non[target].astype(int).to_numpy()
                non_pred = predict_bundle(best_bundle, non)
                non_base = np.full(len(y_non), train_base_rate)
                nonoverlap_rows.append({"target": target, "horizon": horizon, **metrics_row(y_non, non_pred, brier_score(y_non, non_base))})
            for regime, group in test.groupby("regime"):
                if len(group) < 60 or group[target].nunique() < 2:
                    continue
                y_reg = group[target].astype(int).to_numpy()
                p_reg = predict_bundle(best_bundle, group)
                regime_rows.append({"target": target, "horizon": horizon, "regime": regime, **metrics_row(y_reg, p_reg, brier_score(y_reg, np.full(len(y_reg), train_base_rate)))})
            thirds = temporal_thirds(test)
            for third, group in test.assign(third=thirds).groupby("third"):
                if len(group) < 60 or group[target].nunique() < 2:
                    continue
                y_part = group[target].astype(int).to_numpy()
                p_part = predict_bundle(best_bundle, group)
                temporal_rows.append({"target": target, "horizon": horizon, "third": third, **metrics_row(y_part, p_part, brier_score(y_part, np.full(len(y_part), train_base_rate)))})
            importance_rows.extend(feature_importance(best_bundle, test, target, test_base_brier))
            walkforward_rows.extend(run_walkforward(data[data["horizon"] == horizon].copy(), target, best_bundle))

            if target in LABEL_V1_PROB_COLUMNS:
                col = LABEL_V1_PROB_COLUMNS[target]
                if col in test and test[col].notna().sum() >= 120:
                    clean = test[test[col].notna()]
                    y_v1 = clean[target].astype(int).to_numpy()
                    p_v1 = clean[col].astype(float).to_numpy()
                    v1_rows.append({"target": target, "horizon": horizon, "v1_column": col, **metrics_row(y_v1, p_v1, brier_score(y_v1, np.full(len(y_v1), train_base_rate)))})

            model_path = MODEL_DIR / f"{target}_{horizon}_research_only.pkl"
            with model_path.open("wb") as handle:
                pickle.dump({"research_only_not_for_trading": True, **best_bundle}, handle)

    validation_df = pd.DataFrame(validation_rows)
    test_df = pd.DataFrame(test_rows)
    baseline_df = pd.DataFrame(baseline_rows)
    v1_df = pd.DataFrame(v1_rows)
    nonoverlap_df = pd.DataFrame(nonoverlap_rows)
    regime_df = pd.DataFrame(regime_rows)
    temporal_df = pd.DataFrame(temporal_rows)
    importance_df = pd.DataFrame(importance_rows)
    walkforward_df = pd.DataFrame(walkforward_rows)
    calibration_df = pd.DataFrame(calibration_rows)

    feature_classification = classify_features(importance_df)
    target_classification = classify_targets(test_df, validation_df, target_meta)
    grid_df = strategy_scorecard(test_df, {"BOTH", "GRID"})
    iron_df = strategy_scorecard(test_df, {"BOTH", "IRON_FLY"})
    defensive_df = strategy_scorecard(test_df, {"DEFENSIVE_LONG_OPTION"})
    summary = build_summary(
        data,
        canonical_dataset_hash,
        parquet_file_hash,
        targets,
        split_counts,
        validation_df,
        test_df,
        baseline_df,
        v1_df,
        nonoverlap_df,
        regime_df,
        temporal_df,
        feature_classification,
        target_classification,
        grid_df,
        iron_df,
        defensive_df,
        generated_at,
    )

    validation_df.to_csv(REPORT_DIR / "step14_v2_model_scorecard.csv", index=False)
    test_df.to_csv(REPORT_DIR / "step14_v2_test_scorecard.csv", index=False)
    calibration_df.to_csv(REPORT_DIR / "step14_v2_calibration.csv", index=False)
    walkforward_df.to_csv(REPORT_DIR / "step14_v2_walkforward.csv", index=False)
    nonoverlap_df.to_csv(REPORT_DIR / "step14_v2_nonoverlap.csv", index=False)
    regime_df.to_csv(REPORT_DIR / "step14_v2_regime_scorecard.csv", index=False)
    importance_df.to_csv(REPORT_DIR / "step14_v2_feature_importance.csv", index=False)
    feature_classification.to_csv(REPORT_DIR / "step14_v2_feature_classification.csv", index=False)
    target_classification.to_csv(REPORT_DIR / "step14_v2_target_classification.csv", index=False)
    grid_df.to_csv(REPORT_DIR / "step14_grid_probability_scorecard.csv", index=False)
    iron_df.to_csv(REPORT_DIR / "step14_ironfly_probability_scorecard.csv", index=False)
    defensive_df.to_csv(REPORT_DIR / "step14_defensive_probability_scorecard.csv", index=False)
    baseline_df.to_csv(REPORT_DIR / "step14_v2_baselines.csv", index=False)
    v1_df.to_csv(REPORT_DIR / "step14_v2_v1_comparison.csv", index=False)
    (REPORT_DIR / "step14_v2_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    print(json.dumps({"ok": True, "targets": len(targets), "selected_models": len(test_df), "positive_test_bss": int((test_df["brier_skill_vs_train_base"] > 0).sum()) if not test_df.empty else 0, "decision_gate": summary["decision_gate"]}, indent=2))


def run_walkforward(hdata: pd.DataFrame, target: str, bundle: dict) -> list[dict]:
    timestamps = sorted(hdata["prediction_timestamp"].drop_duplicates())
    if len(timestamps) < 1000:
        return []
    cuts = [0.40, 0.55, 0.70, 0.85]
    rows = []
    for idx in range(3):
        train_end = timestamps[int(len(timestamps) * cuts[idx])]
        val_start = timestamps[int(len(timestamps) * cuts[idx]) + 1]
        val_end = timestamps[int(len(timestamps) * cuts[idx + 1])]
        horizon_delta = pd.Timedelta(hours=HORIZON_HOURS[bundle["horizon"]])
        train = hdata[(hdata["prediction_timestamp"] <= val_start - horizon_delta) & hdata[target].notna()]
        val = hdata[(hdata["prediction_timestamp"] >= val_start) & (hdata["prediction_timestamp"] <= val_end) & hdata[target].notna()]
        if len(train) < 300 or len(val) < 120 or train[target].nunique() < 2 or val[target].nunique() < 2:
            continue
        pre = fit_preprocessor(train, bundle["features"], scale=bundle["family"] == "logistic")
        model = fit_model(bundle["family"], pre.transform(train), train[target].astype(int).to_numpy())
        pred = predict_model(model, pre.transform(val))
        y = val[target].astype(int).to_numpy()
        base = np.full(len(y), float(train[target].mean()))
        rows.append({"target": target, "horizon": bundle["horizon"], "fold": idx + 1, "model_family": bundle["family"], "feature_set": bundle["feature_set"], **metrics_row(y, pred, brier_score(y, base))})
    return rows


def classify_features(importance: pd.DataFrame) -> pd.DataFrame:
    rows = []
    all_features = INDIVIDUAL_FEATURES + INTERACTION_FEATURES
    for feature in all_features:
        subset = importance[importance["feature"] == feature] if not importance.empty else pd.DataFrame()
        mean_gain = float(subset["permutation_brier_increase"].mean()) if not subset.empty else 0.0
        hits = int((subset["permutation_brier_increase"] > 0).sum()) if not subset.empty else 0
        if hits >= 10 and mean_gain > 0.0004:
            label = "CORE"
        elif hits >= 5 and mean_gain > 0:
            label = "USEFUL"
        elif feature in {"keltner_width_20b", "donchian_pos_96b", "rv_12b", "rv_slope_96b"}:
            label = "REDUNDANT"
        elif hits == 0:
            label = "DROP"
        else:
            label = "OPTIONAL"
        rows.append({"feature": feature, "classification": label, "mean_permutation_brier_increase": mean_gain, "positive_importance_hits": hits})
    return pd.DataFrame(rows)


def classify_targets(test_df: pd.DataFrame, validation_df: pd.DataFrame, target_meta: pd.DataFrame) -> pd.DataFrame:
    rows = []
    saturated = set(target_meta[target_meta["saturation_flag"] != "USABLE"]["target"])
    for target, group in test_df.groupby("target"):
        positive = int((group["brier_skill_vs_train_base"] > 0).sum())
        mean_bss = float(group["brier_skill_vs_train_base"].mean())
        mean_auc = float(group["auc"].dropna().mean()) if group["auc"].notna().any() else None
        relevance = target_relevance(target)
        if target in saturated:
            label = "SATURATED"
        elif positive >= 4 and mean_bss > 0.01 and relevance in {"BOTH", "GRID", "IRON_FLY"}:
            label = "CORE_V2_TARGET"
        elif positive >= 3 and mean_bss > 0:
            label = "SECONDARY_V2_TARGET"
        elif relevance in {"GRID", "IRON_FLY", "DEFENSIVE_LONG_OPTION"} and positive >= 1:
            label = "STRATEGY_ONLY_TARGET"
        elif mean_bss < -0.02:
            label = "REJECT"
        else:
            label = "WEAK"
        rows.append({"target": target, "strategy_relevance": relevance, "classification": label, "positive_test_bss_horizons": positive, "mean_test_bss": mean_bss, "mean_test_auc": mean_auc})
    return pd.DataFrame(rows).sort_values(["classification", "mean_test_bss"], ascending=[True, False])


def strategy_scorecard(test_df: pd.DataFrame, relevances: set[str]) -> pd.DataFrame:
    return test_df[test_df["strategy_relevance"].isin(relevances)].sort_values(["brier_skill_vs_train_base", "auc"], ascending=False)


def stable_frame_hash(frame: pd.DataFrame) -> str:
    return hashlib_sha256(frame.to_csv(index=False, lineterminator="\n").encode("utf-8"))


def hashlib_sha256(payload: bytes) -> str:
    import hashlib

    return hashlib.sha256(payload).hexdigest()


def build_summary(data, canonical_dataset_hash, parquet_file_hash, targets, split_counts, validation_df, test_df, baseline_df, v1_df, nonoverlap_df, regime_df, temporal_df, feature_classification, target_classification, grid_df, iron_df, defensive_df, generated_at):
    positive_test = test_df[test_df["brier_skill_vs_train_base"] > 0] if not test_df.empty else pd.DataFrame()
    strong = positive_test[(positive_test["brier_skill_vs_train_base"] > 0.02) & (positive_test["auc"].fillna(0.5) >= 0.56)] if not positive_test.empty else pd.DataFrame()
    decision = "STEP 14 COMPLETE — NO RELIABLE V2 EDGE IDENTIFIED"
    if len(strong) >= 12:
        decision = "STEP 14 COMPLETE — STRONG V2 CHALLENGER IDENTIFIED"
    elif len(positive_test) >= 10:
        decision = "STEP 14 COMPLETE — PROMISING BUT PARTIAL V2 EDGE IDENTIFIED"
    elif len(positive_test) > 0:
        decision = "STEP 14 COMPLETE — OHLCV V2 EDGE WEAK; RICH FEATURES REQUIRED"
    v1_join = []
    if not v1_df.empty and not test_df.empty:
        for _, row in v1_df.iterrows():
            match = test_df[(test_df["target"] == row["target"]) & (test_df["horizon"] == row["horizon"])]
            if not match.empty:
                v1_join.append({"target": row["target"], "horizon": row["horizon"], "v1_bss": row["brier_skill_vs_train_base"], "v2_bss": match.iloc[0]["brier_skill_vs_train_base"], "v2_minus_v1_bss": match.iloc[0]["brier_skill_vs_train_base"] - row["brier_skill_vs_train_base"]})
    answers = {
        "A_beats_train_base_oos": bool(len(positive_test) > 0),
        "B_targets": sorted(positive_test["target"].unique().tolist()) if not positive_test.empty else [],
        "C_horizons": sorted(positive_test["horizon"].unique().tolist(), key=lambda h: HORIZON_HOURS[h]) if not positive_test.empty else [],
        "D_beats_v1_where_comparable": [row for row in v1_join if row["v2_minus_v1_bss"] > 0],
        "E_best_model_family": Counter(test_df["model_family"]).most_common(1)[0][0] if not test_df.empty else None,
        "F_step12_interactions_add_value": bool((test_df["feature_set"].isin(["step12_interactions", "all_step13", "compact_deredundant"])).any()),
        "G_core_useful_features": feature_classification[feature_classification["classification"].isin(["CORE", "USEFUL"])]["feature"].tolist(),
        "H_drop_features": feature_classification[feature_classification["classification"].isin(["DROP", "REDUNDANT"])]["feature"].tolist(),
        "I_core_v2_targets": target_classification[target_classification["classification"] == "CORE_V2_TARGET"]["target"].tolist(),
        "J_v1_targets_to_abandon_or_redesign": target_classification[(target_classification["strategy_relevance"] == "GENERIC_PROBABILITY") & (target_classification["classification"].isin(["WEAK", "REJECT"]))]["target"].tolist(),
        "K_good_grid_predictable": bool(not grid_df.empty and (grid_df["brier_skill_vs_train_base"] > 0).any()),
        "L_dangerous_grid_predictable": bool(not test_df[(test_df["target"].isin(["one_sided_runaway", "trend_efficiency_high"])) & (test_df["brier_skill_vs_train_base"] > 0)].empty),
        "M_ironfly_containment_predictable": bool(not iron_df.empty and (iron_df["target"].isin(["path_inside_70", "range_continuation"]) & (iron_df["brier_skill_vs_train_base"] > 0)).any()),
        "N_ironfly_avoidance_predictable": bool(not iron_df.empty and (iron_df["target"].isin(["range_breached", "upper_breach_only", "lower_breach_only"]) & (iron_df["brier_skill_vs_train_base"] > 0)).any()),
        "O_defensive_long_option_promising": bool(not defensive_df.empty and (defensive_df["brier_skill_vs_train_base"] > 0).any()),
        "P_time_regime_stability": summarize_stability(regime_df, temporal_df),
        "Q_less_overlap_survival": int((nonoverlap_df["brier_skill_vs_train_base"] > 0).sum()) if not nonoverlap_df.empty else 0,
        "R_enough_evidence_for_v2_spec": decision in {"STEP 14 COMPLETE — STRONG V2 CHALLENGER IDENTIFIED", "STEP 14 COMPLETE — PROMISING BUT PARTIAL V2 EDGE IDENTIFIED"},
    }
    return {
        "generated_at": generated_at,
        "research_only_not_for_trading": True,
        "dataset_path": "reports/step13_probability_v2_dataset.parquet",
        "dataset_hash_expected": EXPECTED_HASH,
        "dataset_hash_actual": canonical_dataset_hash,
        "dataset_hash_matches_expected": canonical_dataset_hash == EXPECTED_HASH,
        "parquet_file_hash": parquet_file_hash,
        "feature_contract": FEATURE_CONTRACT,
        "row_count": int(len(data)),
        "targets_modeled": targets,
        "chronological_split_counts_after_horizon_embargo": split_counts,
        "test_positive_bss_count": int(len(positive_test)),
        "strong_target_horizon_count": int(len(strong)),
        "top_test_results": test_df.sort_values("brier_skill_vs_train_base", ascending=False).head(15).to_dict("records") if not test_df.empty else [],
        "v1_comparison": v1_join,
        "answers": answers,
        "decision_gate": decision,
        "recommended_step15": "Research rich forward features read-only with strict as-of semantics, prioritizing orderflow/derivatives features for the weak OHLCV-only target gaps.",
        "final_verdict": decision,
    }


def summarize_stability(regime_df: pd.DataFrame, temporal_df: pd.DataFrame) -> str:
    temporal_positive = int((temporal_df["brier_skill_vs_train_base"] > 0).sum()) if not temporal_df.empty else 0
    regime_positive = int((regime_df["brier_skill_vs_train_base"] > 0).sum()) if not regime_df.empty else 0
    if temporal_positive >= 20 and regime_positive >= 20:
        return "STABLE"
    if temporal_positive >= 10:
        return "TENTATIVE"
    if temporal_positive > 0:
        return "PERIOD-SPECIFIC"
    return "UNSTABLE"


if __name__ == "__main__":
    main()
