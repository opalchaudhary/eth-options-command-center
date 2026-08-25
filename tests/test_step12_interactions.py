from __future__ import annotations

import numpy as np
import pandas as pd

from probability_engine.research.step12_interactions import (
    binary_state,
    chronological_split,
    fit_logistic_metrics,
    quantile_threshold,
    state_from_quantiles,
)


def test_quantile_threshold_is_deterministic():
    series = pd.Series([1, 2, 3, 4, 5])

    assert quantile_threshold(series, 0.30) == 2.2
    assert quantile_threshold(series, 0.70) == 3.8


def test_state_from_quantiles_classifies_low_mid_high_without_dropping_nulls():
    series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0, np.nan])

    result = state_from_quantiles(series)

    assert result.tolist() == ["LOW", "LOW", "MID", "HIGH", "HIGH", None]


def test_binary_state_preserves_missing_values():
    result = binary_state(pd.Series([1.0, 3.0, np.nan]), ">", 2.0)

    assert result.iloc[0] == False
    assert result.iloc[1] == True
    assert pd.isna(result.iloc[2])


def test_chronological_split_uses_ordered_thirds():
    frame = pd.DataFrame({"created_at": pd.date_range("2026-01-01", periods=9, freq="h")[::-1]})

    split = chronological_split(frame)
    ordered = frame.assign(split=split).sort_values("created_at")

    assert ordered["split"].tolist() == ["early"] * 3 + ["middle"] * 3 + ["late"] * 3


def test_fit_logistic_metrics_is_reproducible_and_improves_base_brier():
    x = np.linspace(-2, 2, 240)
    y = (x > 0).astype(int)
    frame = pd.DataFrame({"target": y, "x": x})

    first = fit_logistic_metrics(frame, "target", ["x"])
    second = fit_logistic_metrics(frame, "target", ["x"])

    assert first == second
    assert first["brier"] < first["base_brier"]
    assert first["auc"] > 0.95
