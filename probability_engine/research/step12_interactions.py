from __future__ import annotations

import math

import numpy as np
import pandas as pd


HORIZON_ORDER = {"1H": 60, "2H": 120, "4H": 240, "8H": 480, "12H": 720, "24H": 1440}


def auc_score(y_true: pd.Series, values: pd.Series) -> float | None:
    data = pd.DataFrame({"y": y_true, "x": values}).dropna()
    if data.empty or data["y"].nunique() < 2:
        return None
    y = data["y"].astype(int).to_numpy()
    x = data["x"].astype(float).to_numpy()
    order = np.argsort(x, kind="mergesort")
    ranks = np.empty(len(x), dtype=float)
    ranks[order] = np.arange(1, len(x) + 1)
    n_pos = int(y.sum())
    n_neg = len(y) - n_pos
    if not n_pos or not n_neg:
        return None
    rank_sum_pos = ranks[y == 1].sum()
    return float((rank_sum_pos - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def average_precision(y_true: pd.Series, values: pd.Series) -> float | None:
    data = pd.DataFrame({"y": y_true, "x": values}).dropna().sort_values("x", ascending=False)
    if data.empty or data["y"].nunique() < 2:
        return None
    positives = int(data["y"].astype(int).sum())
    if not positives:
        return None
    cumulative_tp = data["y"].astype(int).cumsum()
    precision = cumulative_tp / np.arange(1, len(data) + 1)
    return float((precision * data["y"].astype(int)).sum() / positives)


def quantile_threshold(series: pd.Series, q: float) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return float("nan")
    return float(clean.quantile(q))


def state_from_quantiles(series: pd.Series, low_q: float = 0.30, high_q: float = 0.70) -> pd.Series:
    low = quantile_threshold(series, low_q)
    high = quantile_threshold(series, high_q)
    values = pd.to_numeric(series, errors="coerce")
    state = pd.Series("MID", index=series.index, dtype="object")
    state = state.where(values > low, "LOW")
    state = state.where(values < high, "HIGH")
    state[values.isna()] = None
    return state


def binary_state(series: pd.Series, op: str, threshold: float) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    if op == "<=":
        result = values <= threshold
    elif op == ">=":
        result = values >= threshold
    elif op == "<":
        result = values < threshold
    elif op == ">":
        result = values > threshold
    else:
        raise ValueError(f"Unsupported op: {op}")
    return result.where(values.notna(), np.nan)


def chronological_split(frame: pd.DataFrame, time_column: str = "created_at") -> pd.Series:
    ordered = frame.sort_values(time_column).copy()
    edges = np.linspace(0, len(ordered), 4, dtype=int)
    split = pd.Series(index=ordered.index, dtype="object")
    labels = ["early", "middle", "late"]
    for idx, label in enumerate(labels):
        split.iloc[edges[idx] : edges[idx + 1]] = label
    return split.reindex(frame.index)


def classify_horizon(horizons: list[str]) -> str:
    values = set(horizons)
    if len(values) >= 5:
        return "MULTI-HORIZON"
    if values and values <= {"1H", "2H"}:
        return "SHORT-HORIZON"
    if values and values <= {"4H", "8H", "12H"}:
        return "MEDIUM-HORIZON"
    if values and values <= {"12H", "24H"}:
        return "LONG-HORIZON"
    if values:
        return "HORIZON-SPECIFIC"
    return "UNSTABLE"


def fit_logistic_metrics(frame: pd.DataFrame, target: str, features: list[str]) -> dict[str, float | None]:
    data = frame[[target] + features].replace([np.inf, -np.inf], np.nan).dropna()
    if len(data) < 120 or data[target].nunique() < 2:
        return {"n": int(len(data)), "brier": None, "base_brier": None, "auc": None, "pr_auc": None}
    y = data[target].astype(float).to_numpy()
    x = data[features].astype(float).to_numpy()
    mean = x.mean(axis=0)
    std = x.std(axis=0)
    std[std == 0] = 1.0
    x = (x - mean) / std
    x = np.column_stack([np.ones(len(x)), x])
    beta = np.zeros(x.shape[1])
    lr = 0.08
    l2 = 0.05
    for _ in range(250):
        z = np.clip(x @ beta, -35, 35)
        p = 1 / (1 + np.exp(-z))
        grad = (x.T @ (p - y)) / len(y)
        grad[1:] += l2 * beta[1:] / len(y)
        beta -= lr * grad
    pred = 1 / (1 + np.exp(-np.clip(x @ beta, -35, 35)))
    base = float(y.mean())
    return {
        "n": int(len(data)),
        "brier": float(np.mean((pred - y) ** 2)),
        "base_brier": float(np.mean((base - y) ** 2)),
        "auc": auc_score(pd.Series(y), pd.Series(pred)),
        "pr_auc": average_precision(pd.Series(y), pd.Series(pred)),
    }


def safe_rate(y: pd.Series) -> float | None:
    clean = y.dropna()
    if clean.empty:
        return None
    return float(clean.astype(float).mean())


def state_row(frame: pd.DataFrame, state_column: str, target: str) -> dict:
    clean = frame[[state_column, target]].dropna()
    active = clean[clean[state_column].astype(bool)]
    base = safe_rate(clean[target])
    rate = safe_rate(active[target])
    rel = None if base in (None, 0) or rate is None else float(rate / base)
    return {
        "n": int(len(active)),
        "base_n": int(len(clean)),
        "base_rate": base,
        "conditioned_rate": rate,
        "absolute_lift": None if base is None or rate is None else float(rate - base),
        "relative_lift": rel,
    }


def point_biserial_auc_edge(frame: pd.DataFrame, state_column: str, target: str) -> float | None:
    auc = auc_score(frame[target], frame[state_column].astype(float))
    if auc is None:
        return None
    return float(abs(auc - 0.5))


def phi_corr(a: pd.Series, b: pd.Series) -> float | None:
    data = pd.DataFrame({"a": a, "b": b}).dropna()
    if len(data) < 30 or data["a"].nunique() < 2 or data["b"].nunique() < 2:
        return None
    value = data["a"].astype(float).corr(data["b"].astype(float))
    if value is None or math.isnan(value):
        return None
    return float(value)
