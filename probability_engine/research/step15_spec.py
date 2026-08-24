from __future__ import annotations

import hashlib
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

from probability_engine.research.step14_challenger import predict_model


SPEC_VERSION = "probability_v2_candidate_v1"
FEATURE_CONTRACT_VERSION = "probability_v2_features_v1"
CALIBRATION_VERSION = "calibration_v2_candidate_v1"
LABEL_VERSION = "label_v2"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def model_id(target: str, horizon: str) -> str:
    return f"{SPEC_VERSION}__{target}__{horizon}".lower()


def semantic_name(target: str, horizon: str) -> str:
    tokens = {
        "path_inside_70": "P_PATH_INSIDE_70",
        "range_breached": "P_RANGE_BREACH",
        "realized_over_range_width_ge_1": "P_REALIZED_OVER_RANGE_WIDTH_GE_1",
        "both_side_breach": "P_BOTH_SIDE_BREACH",
        "upside_breakout": "P_UPSIDE_BREAKOUT",
        "downside_breakdown": "P_DOWNSIDE_BREAKDOWN",
        "upper_breach_only": "P_UPPER_BREACH_ONLY",
        "lower_breach_only": "P_LOWER_BREACH_ONLY",
        "up_excursion_ge_1_0_atr": "P_UP_EXCURSION_GE_1_ATR",
        "down_excursion_ge_1_0_atr": "P_DOWN_EXCURSION_GE_1_ATR",
    }
    return f"{tokens[target]}_{horizon}"


def semantic_description(target: str, horizon: str) -> str:
    descriptions = {
        "path_inside_70": f"Probability the full future {horizon} path remains inside the frozen Step 13 70% range containment definition.",
        "range_breached": f"Derived probability that the future {horizon} path breaches the frozen 70% range; equals 1 - P_PATH_INSIDE_70_{horizon} for the same horizon.",
        "realized_over_range_width_ge_1": f"Probability the realized future {horizon} high-low path width is at least 1x the frozen 70% range width.",
        "both_side_breach": f"Probability both upper and lower frozen 70% range boundaries are breached within the future {horizon} path.",
        "upside_breakout": f"Probability the existing Label V2 upside breakout event occurs over the future {horizon}.",
        "downside_breakdown": f"Probability the existing Label V2 downside breakdown event occurs over the future {horizon}.",
        "upper_breach_only": f"Probability only the upper frozen 70% range boundary is breached within the future {horizon} path.",
        "lower_breach_only": f"Probability only the lower frozen 70% range boundary is breached within the future {horizon} path.",
        "up_excursion_ge_1_0_atr": f"Probability maximum upside excursion over the future {horizon} reaches at least 1 ATR.",
        "down_excursion_ge_1_0_atr": f"Probability maximum downside excursion over the future {horizon} reaches at least 1 ATR.",
    }
    return descriptions[target]


def quality_grade(test_bss: float, auc: float | None, ece: float | None, nonoverlap_positive: bool, walkforward_positive_folds: int) -> str:
    auc_value = auc if auc is not None and not pd.isna(auc) else 0.5
    ece_value = ece if ece is not None and not pd.isna(ece) else 1.0
    if test_bss >= 0.08 and auc_value >= 0.65 and ece_value <= 0.10 and walkforward_positive_folds >= 2:
        return "HIGH"
    if test_bss > 0.02 and auc_value >= 0.56 and ece_value <= 0.12 and walkforward_positive_folds >= 2:
        return "MEDIUM"
    if test_bss > 0:
        return "LOW"
    return "RESEARCH_ONLY"


def validate_manifest(manifest: dict) -> list[str]:
    errors: list[str] = []
    model_ids = [model["model_id"] for model in manifest.get("models", [])]
    if len(model_ids) != len(set(model_ids)):
        errors.append("duplicate model_id")
    for model in manifest.get("models", []):
        for field in ["model_id", "target", "horizon", "semantic_name", "semantic_description", "artifact_path", "artifact_sha256"]:
            if not model.get(field):
                errors.append(f"missing {field} for {model.get('model_id')}")
        if model.get("probability_output") != "float in [0, 1]":
            errors.append(f"bad probability output declaration for {model.get('model_id')}")
    return errors


def load_research_bundle(path: Path) -> dict:
    with path.open("rb") as handle:
        return pickle.load(handle)


def bundle_probability_smoke(bundle: dict, frame: pd.DataFrame) -> np.ndarray:
    x = bundle["preprocessor"].transform(frame[bundle["features"]])
    probs = predict_model(bundle["model"], x)
    if bundle.get("platt_params") is not None:
        from probability_engine.research.step14_challenger import apply_platt

        probs = apply_platt(probs, bundle["platt_params"])
    return probs
