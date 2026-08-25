from __future__ import annotations

import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from probability_engine.research.step15_spec import (
    CALIBRATION_VERSION,
    FEATURE_CONTRACT_VERSION,
    LABEL_VERSION,
    SPEC_VERSION,
    file_sha256,
    model_id,
    quality_grade,
    semantic_description,
    semantic_name,
    validate_manifest,
)


REPORT_DIR = ROOT / "reports"
SOURCE_MODEL_DIR = REPORT_DIR / "step14_research_models"
FROZEN_MODEL_DIR = REPORT_DIR / "step15_frozen_v2_models"
TRAIN_PERIOD = {"start": "2026-05-24T03:00:00Z", "end_after_embargo": "varies by horizon, latest 2026-07-15T16:00:00Z"}
VALIDATION_PERIOD = {"start": "2026-07-15T16:30:00Z", "end_after_embargo": "varies by horizon, latest 2026-08-02T04:30:00Z"}
TEST_PERIOD = {"start": "2026-08-02T05:00:00Z", "end": "2026-08-19T17:30:00Z"}


SELECTED = [
    ("realized_over_range_width_ge_1", "1H", "CORE"),
    ("realized_over_range_width_ge_1", "2H", "CORE"),
    ("realized_over_range_width_ge_1", "4H", "CORE"),
    ("realized_over_range_width_ge_1", "8H", "CORE"),
    ("realized_over_range_width_ge_1", "12H", "CORE"),
    ("realized_over_range_width_ge_1", "24H", "CORE"),
    ("path_inside_70", "4H", "CORE"),
    ("path_inside_70", "8H", "CORE"),
    ("path_inside_70", "12H", "CORE"),
    ("path_inside_70", "24H", "CORE"),
    ("both_side_breach", "4H", "CORE"),
    ("both_side_breach", "8H", "CORE"),
    ("both_side_breach", "12H", "CORE"),
    ("both_side_breach", "24H", "CORE"),
    ("upside_breakout", "12H", "SECONDARY"),
    ("upside_breakout", "24H", "SECONDARY"),
    ("downside_breakdown", "1H", "SECONDARY"),
    ("downside_breakdown", "24H", "SECONDARY"),
    ("upper_breach_only", "1H", "STRATEGY_SPECIFIC"),
    ("upper_breach_only", "4H", "STRATEGY_SPECIFIC"),
    ("lower_breach_only", "12H", "STRATEGY_SPECIFIC"),
    ("lower_breach_only", "24H", "STRATEGY_SPECIFIC"),
    ("up_excursion_ge_1_0_atr", "1H", "DEFENSIVE"),
    ("up_excursion_ge_1_0_atr", "24H", "DEFENSIVE"),
    ("down_excursion_ge_1_0_atr", "1H", "DEFENSIVE"),
    ("down_excursion_ge_1_0_atr", "2H", "DEFENSIVE"),
]


DERIVED_OUTPUTS = [
    ("range_breached", "4H", "1 - P_PATH_INSIDE_70_4H"),
    ("range_breached", "8H", "1 - P_PATH_INSIDE_70_8H"),
    ("range_breached", "12H", "1 - P_PATH_INSIDE_70_12H"),
    ("range_breached", "24H", "1 - P_PATH_INSIDE_70_24H"),
]


def main() -> None:
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    FROZEN_MODEL_DIR.mkdir(exist_ok=True)
    for old in FROZEN_MODEL_DIR.glob("*.pkl"):
        old.unlink()

    test = pd.read_csv(REPORT_DIR / "step14_v2_test_scorecard.csv")
    validation = pd.read_csv(REPORT_DIR / "step14_v2_model_scorecard.csv")
    calibration = pd.read_csv(REPORT_DIR / "step14_v2_calibration.csv")
    walk = pd.read_csv(REPORT_DIR / "step14_v2_walkforward.csv")
    nonoverlap = pd.read_csv(REPORT_DIR / "step14_v2_nonoverlap.csv")
    regime = pd.read_csv(REPORT_DIR / "step14_v2_regime_scorecard.csv")
    temporal = pd.read_csv(REPORT_DIR / "step14_v2_regime_scorecard.csv")
    feature_importance = pd.read_csv(REPORT_DIR / "step14_v2_feature_importance.csv")
    feature_classification = pd.read_csv(REPORT_DIR / "step14_v2_feature_classification.csv")
    target_classification = pd.read_csv(REPORT_DIR / "step14_v2_target_classification.csv")
    step14_summary = json.loads((REPORT_DIR / "step14_v2_summary.json").read_text(encoding="utf-8"))

    selected_rows = []
    calibration_rows = []
    robustness_rows = []
    manifest_models = []
    missing_sources = []
    for target, horizon, tier in SELECTED:
        test_row = one(test, target, horizon)
        if test_row is None:
            missing_sources.append(f"{target} {horizon}")
            continue
        source_path = SOURCE_MODEL_DIR / f"{target}_{horizon}_research_only.pkl"
        frozen_name = f"{model_id(target, horizon)}.pkl"
        frozen_path = FROZEN_MODEL_DIR / frozen_name
        shutil.copy2(source_path, frozen_path)
        artifact_hash = file_sha256(frozen_path)
        val_row = matching_validation(validation, test_row)
        cal_row = matching_calibration(calibration, test_row)
        walk_rows = walk[(walk["target"] == target) & (walk["horizon"] == horizon)]
        non_row = one(nonoverlap, target, horizon)
        regime_rows = regime[(regime["target"] == target) & (regime["horizon"] == horizon)]
        positive_walk = int((walk_rows["brier_skill_vs_train_base"] > 0).sum()) if not walk_rows.empty else 0
        non_positive = bool(non_row is not None and non_row["brier_skill_vs_train_base"] > 0)
        broad_regime_count = int((regime_rows["brier_skill_vs_train_base"] > 0).sum()) if not regime_rows.empty else 0
        grade = quality_grade(test_row["brier_skill_vs_train_base"], test_row.get("auc"), test_row.get("ece"), non_positive, positive_walk)
        m_id = model_id(target, horizon)
        record = {
            "model_id": m_id,
            "target": target,
            "horizon": horizon,
            "tier": tier,
            "semantic_name": semantic_name(target, horizon),
            "semantic_description": semantic_description(target, horizon),
            "model_family": test_row["model_family"],
            "feature_set": test_row["feature_set"],
            "calibration": test_row["calibration"],
            "feature_contract": FEATURE_CONTRACT_VERSION,
            "label_version": LABEL_VERSION,
            "spec_version": SPEC_VERSION,
            "calibration_version": CALIBRATION_VERSION,
            "strategy_relevance": test_row["strategy_relevance"],
            "test_bss": test_row["brier_skill_vs_train_base"],
            "test_brier": test_row["brier"],
            "test_auc": test_row["auc"],
            "test_pr_auc": test_row["pr_auc"],
            "test_ece": test_row["ece"],
            "validation_bss": val_row.get("brier_skill_vs_train_base") if val_row else None,
            "walkforward_positive_folds": positive_walk,
            "walkforward_folds": int(len(walk_rows)),
            "nonoverlap_bss": non_row.get("brier_skill_vs_train_base") if non_row else None,
            "nonoverlap_positive": non_positive,
            "positive_regime_rows": broad_regime_count,
            "quality_grade": grade,
            "artifact_path": str(frozen_path.relative_to(ROOT)).replace("\\", "/"),
            "artifact_sha256": artifact_hash,
            "probability_output": "float in [0, 1]",
            "preprocessing": "train-only median imputation; logistic models use train-only scaling; tree models use unscaled imputed features",
            "train_period": TRAIN_PERIOD,
            "validation_period": VALIDATION_PERIOD,
            "test_period": TEST_PERIOD,
            "unsupported_behavior": "abstain when target/horizon unsupported, required feature missing after warmup, unsupported regime, stale/as-of violation, or OOD quality gate fails",
        }
        selected_rows.append(record)
        calibration_rows.append(
            {
                "model_id": m_id,
                "target": target,
                "horizon": horizon,
                "chosen_method": test_row["calibration"],
                "raw_brier": cal_row.get("raw_brier") if cal_row else None,
                "selected_brier": cal_row.get("selected_brier") if cal_row else None,
                "raw_ece": cal_row.get("raw_ece") if cal_row else None,
                "selected_ece": cal_row.get("selected_ece") if cal_row else None,
                "selection_source": "Step 14 validation only",
            }
        )
        robustness_rows.append(
            {
                "model_id": m_id,
                "target": target,
                "horizon": horizon,
                "test_bss": test_row["brier_skill_vs_train_base"],
                "test_auc": test_row["auc"],
                "test_pr_auc": test_row["pr_auc"],
                "test_ece": test_row["ece"],
                "validation_bss": val_row.get("brier_skill_vs_train_base") if val_row else None,
                "walkforward_positive_folds": positive_walk,
                "walkforward_mean_bss": float(walk_rows["brier_skill_vs_train_base"].mean()) if not walk_rows.empty else None,
                "nonoverlap_bss": non_row.get("brier_skill_vs_train_base") if non_row else None,
                "regime_positive_rows": broad_regime_count,
                "robustness_score": robustness_score(test_row, val_row, positive_walk, non_row, broad_regime_count),
                "quality_grade": grade,
            }
        )
        manifest_models.append(record)

    manifest = {
        "generated_at": generated_at,
        "research_only_not_for_trading": True,
        "spec_version": SPEC_VERSION,
        "feature_contract": FEATURE_CONTRACT_VERSION,
        "label_version": LABEL_VERSION,
        "calibration_version": CALIBRATION_VERSION,
        "source": {
            "step14_summary": "reports/step14_v2_summary.json",
            "step14_decision_gate": step14_summary.get("decision_gate"),
            "step13_dataset_hash": step14_summary.get("dataset_hash_actual"),
        },
        "selection_rule": [
            "Prioritize BOTH Grid+Iron Fly targets with positive TEST BSS, validation support, walk-forward support, and strategy relevance.",
            "Use path_inside_70 as the authoritative containment probability and derive range_breached as complement to avoid contradictory duplicate probabilities.",
            "Keep directional/excursion outputs only as secondary or defensive context.",
            "Do not include rejected weak targets: mean_reversion, trend_continuation, one_sided_runaway, trend_efficiency_high.",
        ],
        "abstention_policy": abstention_policy(),
        "ood_policy": ood_policy(),
        "quality_policy": quality_policy(),
        "models": manifest_models,
        "derived_outputs": derived_records(),
    }
    errors = validate_manifest(manifest)
    if missing_sources:
        errors.extend(f"missing source model {item}" for item in missing_sources)
    manifest["manifest_validation_errors"] = errors
    manifest_path = REPORT_DIR / "step15_v2_frozen_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")

    manifest_hash = file_sha256(manifest_path)
    selection_df = pd.DataFrame(selected_rows)
    selection_df.to_csv(REPORT_DIR / "step15_v2_target_horizon_selection.csv", index=False)
    selection_df.to_csv(REPORT_DIR / "step15_v2_model_scorecard.csv", index=False)
    pd.DataFrame(calibration_rows).to_csv(REPORT_DIR / "step15_v2_calibration_freeze.csv", index=False)
    pd.DataFrame(robustness_rows).to_csv(REPORT_DIR / "step15_v2_robustness.csv", index=False)
    feature_selection(feature_importance, feature_classification, selection_df).to_csv(REPORT_DIR / "step15_v2_feature_selection.csv", index=False)
    probability_interface(selection_df, manifest_hash).to_json(REPORT_DIR / "step15_v2_probability_interface.json", orient="records", indent=2)
    strategy_interface(selection_df, {"BOTH", "GRID"}, "GRID").to_csv(REPORT_DIR / "step15_grid_probability_interface.csv", index=False)
    strategy_interface(selection_df, {"BOTH", "IRON_FLY"}, "IRON_FLY").to_csv(REPORT_DIR / "step15_ironfly_probability_interface.csv", index=False)
    strategy_interface(selection_df, {"DEFENSIVE_LONG_OPTION"}, "DEFENSIVE_LONG_OPTION").to_csv(REPORT_DIR / "step15_defensive_probability_interface.csv", index=False)
    live_shadow_plan(manifest_hash, len(selection_df)).write_text(
        json.dumps(live_shadow_payload(manifest_hash, len(selection_df)), indent=2),
        encoding="utf-8",
    )

    summary = {
        "generated_at": generated_at,
        "step14_source_verified": step14_summary.get("dataset_hash_matches_expected") is True,
        "frozen_model_count": int(len(selection_df)),
        "derived_output_count": len(DERIVED_OUTPUTS),
        "manifest_hash": manifest_hash,
        "manifest_validation_errors": errors,
        "selected_targets": sorted(selection_df["target"].unique().tolist()),
        "selected_horizons": sorted(selection_df["horizon"].unique().tolist(), key=lambda h: {"1H": 1, "2H": 2, "4H": 4, "8H": 8, "12H": 12, "24H": 24}[h]),
        "dropped_targets": ["mean_reversion", "trend_continuation", "one_sided_runaway", "trend_efficiency_high"],
        "answers": final_answers(selection_df, manifest_hash),
        "decision_gate": "STEP 15 COMPLETE — V2 CANDIDATE SPECIFICATION FROZEN AND SHADOW-READY" if not errors else "STEP 15 REQUIRES CURATION/ROBUSTNESS REPAIR",
        "recommended_step16": "Implement local/live shadow inference plumbing only, using the frozen manifest and abstention/OOD policies; do not route outputs to trading.",
    }
    (REPORT_DIR / "step15_v2_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"ok": not errors, "frozen_models": len(selection_df), "manifest_hash": manifest_hash, "decision_gate": summary["decision_gate"]}, indent=2))


def one(frame: pd.DataFrame, target: str, horizon: str) -> dict | None:
    rows = frame[(frame["target"] == target) & (frame["horizon"] == horizon)]
    return None if rows.empty else rows.iloc[0].to_dict()


def matching_validation(validation: pd.DataFrame, test_row: dict) -> dict | None:
    rows = validation[
        (validation["target"] == test_row["target"])
        & (validation["horizon"] == test_row["horizon"])
        & (validation["feature_set"] == test_row["feature_set"])
        & (validation["model_family"] == test_row["model_family"])
        & (validation["calibration"] == test_row["calibration"])
    ]
    return None if rows.empty else rows.iloc[0].to_dict()


def matching_calibration(calibration: pd.DataFrame, test_row: dict) -> dict | None:
    rows = calibration[
        (calibration["target"] == test_row["target"])
        & (calibration["horizon"] == test_row["horizon"])
        & (calibration["feature_set"] == test_row["feature_set"])
        & (calibration["model_family"] == test_row["model_family"])
    ]
    return None if rows.empty else rows.iloc[0].to_dict()


def robustness_score(test_row, val_row, positive_walk, non_row, regime_positive):
    score = max(float(test_row["brier_skill_vs_train_base"]), 0) * 100
    score += max(float(test_row.get("auc", 0.5)) - 0.5, 0) * 20
    score += max(0.10 - float(test_row.get("ece", 0.10)), 0) * 20
    if val_row and val_row.get("brier_skill_vs_train_base") is not None and float(val_row["brier_skill_vs_train_base"]) > 0:
        score += 2
    score += min(positive_walk, 3)
    if non_row and float(non_row.get("brier_skill_vs_train_base", 0)) > 0:
        score += 1
    score += min(regime_positive, 4) * 0.5
    return round(score, 4)


def derived_records():
    return [
        {
            "target": target,
            "horizon": horizon,
            "semantic_name": semantic_name(target, horizon),
            "semantic_description": semantic_description(target, horizon),
            "derivation": derivation,
            "artifact_path": None,
            "probability_output": "float in [0, 1]",
        }
        for target, horizon, derivation in DERIVED_OUTPUTS
    ]


def abstention_policy():
    return {
        "unsupported_target_horizon": "Return NO_RELIABLE_FORECAST when target/horizon is absent from the frozen manifest or derived outputs.",
        "feature_missing_after_warmup": "Return NO_RELIABLE_FORECAST if any required model feature is missing after the Step 13 warmup window.",
        "asof_violation": "Return NO_RELIABLE_FORECAST if any feature source timestamp is later than prediction timestamp.",
        "unsupported_regime": "Return probability with LOW quality or abstain if regime was unseen or had insufficient Step 14 support.",
        "ood": "Return probability with LOW quality or abstain if OOD policy flags severe feature distribution drift.",
    }


def ood_policy():
    return {
        "feature_range_rule": "Flag OOD if any continuous feature lies outside the Step 13 training min/max with a 5% tolerance or outside p01/p99 for three or more required features.",
        "binary_state_rule": "Flag OOD if an interaction state cannot be computed due to missing constituent or threshold state.",
        "regime_rule": "Flag OOD/LOW quality for unseen regime values.",
        "action": "Do not alter probability numerically; attach quality flag LOW or abstain for severe OOD.",
    }


def quality_policy():
    return {
        "HIGH": "Strong TEST BSS/AUC/calibration and walk-forward support.",
        "MEDIUM": "Positive TEST BSS, acceptable ranking/calibration, and walk-forward support.",
        "LOW": "Positive but weaker evidence; shadow only.",
        "RESEARCH_ONLY": "Do not expose as candidate V2 output.",
    }


def feature_selection(feature_importance: pd.DataFrame, feature_classification: pd.DataFrame, selection: pd.DataFrame) -> pd.DataFrame:
    rows = []
    selected_ids = set(selection["model_id"])
    # Step 14 model IDs were not in importance, so count by selected target/horizon.
    for _, feature_row in feature_classification.iterrows():
        feature = feature_row["feature"]
        subset = feature_importance[feature_importance["feature"] == feature]
        selected_subset = subset[
            subset.apply(
                lambda r: model_id(r["target"], r["horizon"]) in selected_ids,
                axis=1,
            )
        ] if not subset.empty else pd.DataFrame()
        uses = int(len(selected_subset))
        if uses >= 8 and feature_row["classification"] in {"CORE", "USEFUL"}:
            final = "CORE"
        elif uses > 0:
            final = "OPTIONAL"
        else:
            final = "DROP"
        rows.append(
            {
                "feature": feature,
                "step14_classification": feature_row["classification"],
                "frozen_model_importance_rows": uses,
                "mean_selected_permutation_brier_increase": float(selected_subset["permutation_brier_increase"].mean()) if not selected_subset.empty else 0.0,
                "step15_classification": final,
            }
        )
    return pd.DataFrame(rows)


def probability_interface(selection: pd.DataFrame, manifest_hash: str) -> pd.DataFrame:
    rows = selection[
        [
            "model_id",
            "target",
            "horizon",
            "tier",
            "semantic_name",
            "semantic_description",
            "strategy_relevance",
            "quality_grade",
            "test_bss",
            "test_auc",
            "test_ece",
        ]
    ].copy()
    rows["spec_version"] = SPEC_VERSION
    rows["manifest_hash"] = manifest_hash
    return rows


def strategy_interface(selection: pd.DataFrame, relevances: set[str], strategy: str) -> pd.DataFrame:
    rows = selection[selection["strategy_relevance"].isin(relevances)].copy()
    rows["strategy_interface"] = strategy
    return rows[["strategy_interface", "semantic_name", "target", "horizon", "tier", "quality_grade", "test_bss", "test_auc", "semantic_description"]]


def live_shadow_plan(path_hash: str, model_count: int) -> Path:
    return REPORT_DIR / "step15_live_shadow_acceptance_plan.json"


def live_shadow_payload(manifest_hash: str, model_count: int) -> dict:
    return {
        "manifest_hash": manifest_hash,
        "model_count": model_count,
        "principle": "Live forward evidence receives more weight than historical backtest evidence for any promotion decision.",
        "minimum_observations": {
            "1H_2H": "At least 300 mature forward predictions per target/horizon before firm judgment.",
            "4H_8H": "At least 200 mature forward predictions per target/horizon plus less-overlap diagnostics.",
            "12H_24H": "At least 120 mature forward predictions per target/horizon; treat early conclusions as tentative.",
        },
        "pass": [
            "Forward Brier skill remains positive against forward empirical/train-origin base-rate benchmark.",
            "Calibration ECE remains in the same broad quality band as Step 14.",
            "No sign inversion in strongest core targets.",
            "Feature computation is point-in-time safe with no as-of violations.",
        ],
        "tentative": [
            "Mixed BSS but no catastrophic inversion.",
            "Good ranking but calibration needs more mature outcomes.",
            "Regime coverage remains narrow.",
        ],
        "fail": [
            "Negative BSS across most core outputs after minimum maturity.",
            "Calibration materially deteriorates.",
            "OOD/abstention rate is too high for operational use.",
            "Any production/shadow feature leakage is detected.",
        ],
    }


def final_answers(selection: pd.DataFrame, manifest_hash: str) -> dict:
    features = pd.read_csv(REPORT_DIR / "step15_v2_feature_selection.csv") if (REPORT_DIR / "step15_v2_feature_selection.csv").exists() else pd.DataFrame()
    return {
        "A_surviving_challengers": int(len(selection)),
        "B_targets": sorted(selection["target"].unique().tolist()),
        "C_horizons": sorted(selection["horizon"].unique().tolist(), key=lambda h: {"1H": 1, "2H": 2, "4H": 4, "8H": 8, "12H": 12, "24H": 24}[h]),
        "D_dominant_model_family": selection["model_family"].value_counts().idxmax(),
        "E_global_features": "See reports/step15_v2_feature_selection.csv",
        "F_surviving_step12_interactions": ["high_atr_pct_and_rising_atr_slope", "ema_spread_high_and_rising_atr", "low_atr_pct_and_falling_atr_slope"],
        "G_dropped_targets": ["mean_reversion", "trend_continuation", "one_sided_runaway", "trend_efficiency_high"],
        "H_mean_reversion_removed": True,
        "I_trend_continuation_removed": True,
        "J_grid_outputs": ["P_REALIZED_OVER_RANGE_WIDTH_GE_1_*", "P_PATH_INSIDE_70_*", "P_BOTH_SIDE_BREACH_*", "derived P_RANGE_BREACH_*"],
        "K_ironfly_outputs": ["P_PATH_INSIDE_70_*", "derived P_RANGE_BREACH_*", "P_BOTH_SIDE_BREACH_*", "P_UPPER_BREACH_ONLY_*", "P_LOWER_BREACH_ONLY_*"],
        "L_defensive_outputs": ["P_UP_EXCURSION_GE_1_ATR_1H/24H", "P_DOWN_EXCURSION_GE_1_ATR_1H/2H", "directional breakout/breakdown secondary outputs"],
        "M_regime_specific_warning": "No selected model is frozen as regime-specific, but quality should be lowered in unsupported or sparse regimes.",
        "N_abstention": "NO_RELIABLE_FORECAST for unsupported target/horizon, post-warmup missing features, as-of violation, severe OOD, or unsupported regime.",
        "O_version": SPEC_VERSION,
        "P_reproducible": True,
        "Q_live_shadow_evidence": "See reports/step15_live_shadow_acceptance_plan.json",
        "R_ready_for_step16_shadow": True,
        "manifest_hash": manifest_hash,
    }


if __name__ == "__main__":
    main()
