from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass

import numpy as np
import pandas as pd


EPS = 1e-9
HORIZON_HOURS = {"1H": 1.0, "2H": 2.0, "4H": 4.0, "8H": 8.0, "12H": 12.0, "24H": 24.0}
TRAIN_START = pd.Timestamp("2026-05-24T03:00:00Z")
TRAIN_END = pd.Timestamp("2026-07-15T16:00:00Z")
VALIDATION_START = pd.Timestamp("2026-07-15T16:30:00Z")
VALIDATION_END = pd.Timestamp("2026-08-02T04:30:00Z")
TEST_START = pd.Timestamp("2026-08-02T05:00:00Z")
TEST_END = pd.Timestamp("2026-08-19T17:30:00Z")


@dataclass(frozen=True)
class Preprocessor:
    features: list[str]
    medians: np.ndarray
    means: np.ndarray
    stds: np.ndarray
    scale: bool

    def transform(self, frame: pd.DataFrame) -> np.ndarray:
        x = frame[self.features].astype(float).to_numpy()
        x = np.where(np.isnan(x), self.medians, x)
        if self.scale:
            x = (x - self.means) / self.stds
        return x


def stable_file_hash(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def chronological_masks(frame: pd.DataFrame, horizon: str) -> dict[str, pd.Series]:
    horizon_delta = pd.Timedelta(hours=HORIZON_HOURS[horizon])
    ts = frame["prediction_timestamp"]
    return {
        "train": (ts >= TRAIN_START) & (ts <= VALIDATION_START - horizon_delta),
        "validation": (ts >= VALIDATION_START) & (ts <= TEST_START - horizon_delta),
        "test": (ts >= TEST_START) & (ts <= TEST_END),
    }


def fit_preprocessor(train: pd.DataFrame, features: list[str], scale: bool) -> Preprocessor:
    x = train[features].astype(float)
    medians = x.median(skipna=True).fillna(0.0).to_numpy()
    filled = x.fillna(dict(zip(features, medians)))
    means = filled.mean().to_numpy()
    stds = filled.std(ddof=0).replace(0, 1.0).fillna(1.0).to_numpy()
    return Preprocessor(features=features, medians=medians, means=means, stds=stds, scale=scale)


def sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(z, -35, 35)))


def fit_logistic(x: np.ndarray, y: np.ndarray, l2: float = 0.25, lr: float = 0.08, iterations: int = 450) -> dict:
    beta = np.zeros(x.shape[1] + 1)
    xb = np.column_stack([np.ones(len(x)), x])
    for _ in range(iterations):
        pred = sigmoid(xb @ beta)
        grad = (xb.T @ (pred - y)) / len(y)
        grad[1:] += l2 * beta[1:] / len(y)
        beta -= lr * grad
    return {"family": "logistic", "beta": beta}


def predict_logistic(model: dict, x: np.ndarray) -> np.ndarray:
    xb = np.column_stack([np.ones(len(x)), x])
    return sigmoid(xb @ model["beta"])


def best_stump(x: np.ndarray, residual: np.ndarray, min_leaf: int = 40) -> tuple[int, float, float, float]:
    best = (0, 0.0, float(residual.mean()), float(residual.mean()))
    best_loss = float("inf")
    for feature_idx in range(x.shape[1]):
        values = x[:, feature_idx]
        if np.unique(values).size < 3:
            continue
        for threshold in np.quantile(values, [0.25, 0.5, 0.75]):
            left = values <= threshold
            right = ~left
            if left.sum() < min_leaf or right.sum() < min_leaf:
                continue
            left_value = float(residual[left].mean())
            right_value = float(residual[right].mean())
            pred = np.where(left, left_value, right_value)
            loss = float(((residual - pred) ** 2).mean())
            if loss < best_loss:
                best_loss = loss
                best = (feature_idx, float(threshold), left_value, right_value)
    return best


def fit_tree(x: np.ndarray, y: np.ndarray) -> dict:
    base = float(np.clip(y.mean(), EPS, 1 - EPS))
    root_residual = y - base
    root = best_stump(x, root_residual)
    root_pred = apply_stump(x, root)
    left_mask = x[:, root[0]] <= root[1]
    right_mask = ~left_mask
    left = best_stump(x[left_mask], y[left_mask] - np.clip(base + root_pred[left_mask], EPS, 1 - EPS)) if left_mask.sum() >= 100 else None
    right = best_stump(x[right_mask], y[right_mask] - np.clip(base + root_pred[right_mask], EPS, 1 - EPS)) if right_mask.sum() >= 100 else None
    return {"family": "tree_depth2", "base": base, "root": root, "left": left, "right": right}


def apply_stump(x: np.ndarray, stump: tuple[int, float, float, float]) -> np.ndarray:
    return np.where(x[:, stump[0]] <= stump[1], stump[2], stump[3])


def predict_tree(model: dict, x: np.ndarray) -> np.ndarray:
    pred = np.full(len(x), model["base"]) + apply_stump(x, model["root"])
    left_mask = x[:, model["root"][0]] <= model["root"][1]
    if model.get("left") is not None and left_mask.any():
        pred[left_mask] += apply_stump(x[left_mask], model["left"])
    if model.get("right") is not None and (~left_mask).any():
        pred[~left_mask] += apply_stump(x[~left_mask], model["right"])
    return np.clip(pred, EPS, 1 - EPS)


def fit_boosted_stumps(x: np.ndarray, y: np.ndarray, n_estimators: int = 24, learning_rate: float = 0.08) -> dict:
    base_logit = math.log(np.clip(y.mean(), EPS, 1 - EPS) / np.clip(1 - y.mean(), EPS, 1 - EPS))
    raw = np.full(len(y), base_logit)
    stumps = []
    for _ in range(n_estimators):
        pred = sigmoid(raw)
        residual = y - pred
        stump = best_stump(x, residual)
        raw += learning_rate * apply_stump(x, stump)
        stumps.append(stump)
    return {"family": "boosted_stumps", "base_logit": base_logit, "stumps": stumps, "learning_rate": learning_rate}


def predict_boosted_stumps(model: dict, x: np.ndarray) -> np.ndarray:
    raw = np.full(len(x), model["base_logit"])
    for stump in model["stumps"]:
        raw += model["learning_rate"] * apply_stump(x, stump)
    return sigmoid(raw)


def fit_model(family: str, x: np.ndarray, y: np.ndarray) -> dict:
    if family == "logistic":
        return fit_logistic(x, y)
    if family == "tree_depth2":
        return fit_tree(x, y)
    if family == "boosted_stumps":
        return fit_boosted_stumps(x, y)
    raise ValueError(f"Unknown family: {family}")


def predict_model(model: dict, x: np.ndarray) -> np.ndarray:
    if model["family"] == "logistic":
        return predict_logistic(model, x)
    if model["family"] == "tree_depth2":
        return predict_tree(model, x)
    if model["family"] == "boosted_stumps":
        return predict_boosted_stumps(model, x)
    raise ValueError(f"Unknown family: {model['family']}")


def fit_platt(probs: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    logits = np.log(np.clip(probs, EPS, 1 - EPS) / np.clip(1 - probs, EPS, 1 - EPS))
    beta = np.array([0.0, 1.0])
    xb = np.column_stack([np.ones(len(logits)), logits])
    for _ in range(250):
        pred = sigmoid(xb @ beta)
        grad = (xb.T @ (pred - y)) / len(y)
        beta -= 0.05 * grad
    return float(beta[0]), float(beta[1])


def apply_platt(probs: np.ndarray, params: tuple[float, float] | None) -> np.ndarray:
    if params is None:
        return probs
    logits = np.log(np.clip(probs, EPS, 1 - EPS) / np.clip(1 - probs, EPS, 1 - EPS))
    return sigmoid(params[0] + params[1] * logits)


def brier_score(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))


def log_loss(y: np.ndarray, p: np.ndarray) -> float:
    p = np.clip(p, EPS, 1 - EPS)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def auc_score(y: np.ndarray, p: np.ndarray) -> float | None:
    if np.unique(y).size < 2:
        return None
    order = np.argsort(p, kind="mergesort")
    ranks = np.empty(len(p), dtype=float)
    ranks[order] = np.arange(1, len(p) + 1)
    n_pos = int(y.sum())
    n_neg = len(y) - n_pos
    if n_pos == 0 or n_neg == 0:
        return None
    rank_sum_pos = ranks[y == 1].sum()
    return float((rank_sum_pos - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def average_precision(y: np.ndarray, p: np.ndarray) -> float | None:
    if np.unique(y).size < 2 or y.sum() == 0:
        return None
    order = np.argsort(-p, kind="mergesort")
    y_sorted = y[order]
    precision = np.cumsum(y_sorted) / np.arange(1, len(y_sorted) + 1)
    return float((precision * y_sorted).sum() / y.sum())


def calibration_metrics(y: np.ndarray, p: np.ndarray, bins: int = 5) -> dict:
    if len(y) < 30:
        return {"ece": None, "calibration_intercept": None, "calibration_slope": None}
    edges = np.linspace(0, 1, bins + 1)
    ece = 0.0
    for idx in range(bins):
        mask = (p >= edges[idx]) & (p <= edges[idx + 1] if idx == bins - 1 else p < edges[idx + 1])
        if not mask.any():
            continue
        ece += mask.mean() * abs(float(y[mask].mean()) - float(p[mask].mean()))
    logits = np.log(np.clip(p, EPS, 1 - EPS) / np.clip(1 - p, EPS, 1 - EPS))
    if np.unique(y).size < 2 or np.std(logits) < EPS:
        return {"ece": float(ece), "calibration_intercept": None, "calibration_slope": None}
    a, b = fit_platt(p, y)
    return {"ece": float(ece), "calibration_intercept": a, "calibration_slope": b}


def metrics_row(y: np.ndarray, p: np.ndarray, baseline_brier: float | None = None) -> dict:
    brier = brier_score(y, p)
    cal = calibration_metrics(y, p)
    return {
        "n": int(len(y)),
        "positives": int(y.sum()),
        "base_rate": float(y.mean()) if len(y) else None,
        "brier": brier,
        "brier_skill_vs_train_base": None if not baseline_brier or baseline_brier <= 0 else float(1 - brier / baseline_brier),
        "log_loss": log_loss(y, p),
        "auc": auc_score(y, p),
        "pr_auc": average_precision(y, p),
        **cal,
    }


def nonoverlap_subset(frame: pd.DataFrame, horizon: str) -> pd.DataFrame:
    step = max(1, int(HORIZON_HOURS[horizon] * 12))
    return frame.sort_values("prediction_timestamp").iloc[::step].copy()


def temporal_thirds(frame: pd.DataFrame) -> pd.Series:
    ordered = frame.sort_values("prediction_timestamp")
    edges = np.linspace(0, len(ordered), 4, dtype=int)
    labels = pd.Series(index=ordered.index, dtype="object")
    for idx, label in enumerate(["early", "middle", "late"]):
        labels.iloc[edges[idx] : edges[idx + 1]] = label
    return labels.reindex(frame.index)
