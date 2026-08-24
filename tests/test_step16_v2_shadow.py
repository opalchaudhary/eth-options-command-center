from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from probability_engine.services.v2_shadow_outcome import is_mature
from probability_engine.services.v2_shadow_service import (
    clamp_probability,
    compute_v2_features_for_timestamps,
    load_manifest,
    requires_range_reference,
)


def _ohlcv(rows=420):
    timestamps = pd.date_range("2026-01-01T00:00:00Z", periods=rows, freq="5min")
    return pd.DataFrame(
        {
            "candle_time": timestamps,
            "open": [100 + i * 0.01 for i in range(rows)],
            "high": [101 + i * 0.01 for i in range(rows)],
            "low": [99 + i * 0.01 for i in range(rows)],
            "close": [100.5 + i * 0.01 for i in range(rows)],
            "volume": [10 + (i % 7) for i in range(rows)],
        }
    )


def test_packaged_manifest_loads_and_has_frozen_counts():
    manifest = load_manifest()

    assert manifest["spec_version"] == "probability_v2_candidate_v1"
    assert len(manifest["models"]) == 26
    assert len(manifest["derived_outputs"]) == 4


def test_historical_feature_mode_is_deterministic_for_supplied_timestamp_grid():
    ohlcv = _ohlcv()
    timestamps = pd.Series(pd.date_range("2026-01-01T12:00:00Z", periods=6, freq="30min"))

    first = compute_v2_features_for_timestamps(ohlcv, timestamps)
    second = compute_v2_features_for_timestamps(ohlcv, timestamps)

    pd.testing.assert_frame_equal(first, second)
    assert (first["feature_source_timestamp"] <= first["prediction_timestamp"]).all()


def test_probability_clamp_rejects_bad_values_and_bounds_good_values():
    assert clamp_probability(-0.1) == 0.0
    assert clamp_probability(1.1) == 1.0
    assert clamp_probability(float("nan")) is None
    assert clamp_probability(0.42) == 0.42


def test_range_dependent_targets_require_range_reference():
    assert requires_range_reference("path_inside_70") is True
    assert requires_range_reference("up_excursion_ge_1_0_atr") is False


def test_v2_shadow_outcome_maturity_respects_horizon():
    prediction = {"prediction_timestamp": "2026-01-01T00:00:00Z", "horizon": "2H"}

    assert is_mature(prediction, now=datetime(2026, 1, 1, 1, 59, tzinfo=timezone.utc)) is False
    assert is_mature(prediction, now=datetime(2026, 1, 1, 2, 0, tzinfo=timezone.utc)) is True
