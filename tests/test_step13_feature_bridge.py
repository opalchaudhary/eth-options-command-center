from __future__ import annotations

import numpy as np
import pandas as pd

from probability_engine.research.step13_feature_bridge import (
    HORIZON_HOURS,
    expanding_binary_state,
    stable_csv_hash,
    validate_bridge,
)


def test_expanding_binary_state_uses_only_history_at_or_before_row():
    values = pd.Series([10.0, 20.0, 30.0, 1000.0])

    state, threshold = expanding_binary_state(values, 0.50, "ge", min_periods=2)

    assert pd.isna(state.iloc[0])
    assert threshold.iloc[1] == 15.0
    assert state.iloc[1] == 1.0
    assert threshold.iloc[2] == 20.0
    assert state.iloc[2] == 1.0
    assert threshold.iloc[3] == 25.0


def test_dataset_hash_is_deterministic_for_same_ordered_values():
    frame = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    assert stable_csv_hash(frame) == stable_csv_hash(frame.copy())


def test_horizon_hours_contract_contains_all_step13_horizons():
    assert HORIZON_HOURS == {"1H": 1.0, "2H": 2.0, "4H": 4.0, "8H": 8.0, "12H": 12.0, "24H": 24.0}


def test_validate_bridge_rejects_future_feature_timestamp():
    dataset = pd.DataFrame(
        {
            "prediction_timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"], utc=True),
            "feature_source_timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z"], utc=True),
            "horizon": ["1H", "1H"],
            "symbol": ["ETHUSD", "ETHUSD"],
            "record_type": ["BACKTEST", "BACKTEST"],
            "feature_x": [1.0, 2.0],
        }
    )

    validation = validate_bridge(dataset, ["feature_x"])

    assert validation["no_lookahead_pass"] is False
    assert validation["max_feature_source_after_prediction_seconds"] == 300.0


def test_validate_bridge_detects_duplicate_prediction_horizon_rows():
    dataset = pd.DataFrame(
        {
            "prediction_timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"], utc=True),
            "feature_source_timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"], utc=True),
            "horizon": ["1H", "1H"],
            "symbol": ["ETHUSD", "ETHUSD"],
            "record_type": ["BACKTEST", "BACKTEST"],
            "feature_x": [1.0, np.nan],
        }
    )

    validation = validate_bridge(dataset, ["feature_x"])

    assert validation["duplicates"] == 1
