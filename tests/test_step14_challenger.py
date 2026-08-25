from __future__ import annotations

import pandas as pd

from probability_engine.research.step14_challenger import (
    TEST_START,
    VALIDATION_START,
    brier_score,
    chronological_masks,
    fit_preprocessor,
    metrics_row,
)


def test_chronological_masks_apply_horizon_embargo_to_train_and_validation():
    frame = pd.DataFrame(
        {
            "prediction_timestamp": pd.to_datetime(
                [
                    "2026-07-14T16:30:00Z",
                    "2026-07-14T17:00:00Z",
                    "2026-07-15T16:30:00Z",
                    "2026-08-01T05:00:00Z",
                    "2026-08-01T06:00:00Z",
                    "2026-08-02T05:00:00Z",
                ],
                utc=True,
            )
        }
    )

    masks = chronological_masks(frame, "24H")

    assert masks["train"].tolist() == [True, False, False, False, False, False]
    assert masks["validation"].tolist() == [False, False, True, True, False, False]
    assert masks["test"].tolist() == [False, False, False, False, False, True]


def test_preprocessor_fits_medians_on_train_only_and_preserves_feature_order():
    train = pd.DataFrame({"a": [1.0, None, 3.0], "b": [10.0, 12.0, None]})
    validation = pd.DataFrame({"a": [None], "b": [1000.0]})

    prep = fit_preprocessor(train, ["a", "b"], scale=False)
    transformed = prep.transform(validation)

    assert prep.features == ["a", "b"]
    assert transformed[0, 0] == 2.0
    assert transformed[0, 1] == 1000.0


def test_brier_skill_is_positive_when_model_beats_baseline():
    y = pd.Series([0, 0, 1, 1]).to_numpy()
    model = pd.Series([0.1, 0.2, 0.8, 0.9]).to_numpy()
    baseline = pd.Series([0.5, 0.5, 0.5, 0.5]).to_numpy()

    row = metrics_row(y, model, brier_score(y, baseline))

    assert row["brier_skill_vs_train_base"] > 0


def test_split_constants_are_exact_step13_boundaries():
    assert VALIDATION_START == pd.Timestamp("2026-07-15T16:30:00Z")
    assert TEST_START == pd.Timestamp("2026-08-02T05:00:00Z")
