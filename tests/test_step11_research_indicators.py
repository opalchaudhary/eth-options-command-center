from __future__ import annotations

import pandas as pd

from probability_engine.research.step11_indicators import add_indicators, consecutive_count


def _ohlcv(rows=40):
    timestamps = pd.date_range("2026-01-01T00:00:00Z", periods=rows, freq="5min")
    return pd.DataFrame(
        {
            "candle_time": timestamps,
            "open": [100 + i for i in range(rows)],
            "high": [101 + i for i in range(rows)],
            "low": [99 + i for i in range(rows)],
            "close": [100.5 + i for i in range(rows)],
            "volume": [10 + (i % 5) for i in range(rows)],
        }
    )


def test_indicators_preserve_warmup_nulls():
    features = add_indicators(_ohlcv(30))

    assert pd.isna(features.loc[0, "sma_dist_24b"])
    assert pd.notna(features.loc[23, "sma_dist_24b"])
    assert pd.isna(features.loc[10, "rsi_14b"])
    assert pd.notna(features.loc[14, "rsi_14b"])


def test_asof_join_uses_feature_timestamp_at_or_before_prediction():
    features = add_indicators(_ohlcv(10))[["timestamp", "return_3b"]]
    predictions = pd.DataFrame(
        {
            "created_at": pd.to_datetime(
                [
                    "2026-01-01T00:17:00Z",
                    "2026-01-01T00:20:00Z",
                ],
                utc=True,
            )
        }
    )

    joined = pd.merge_asof(
        predictions.sort_values("created_at"),
        features.sort_values("timestamp"),
        left_on="created_at",
        right_on="timestamp",
        direction="backward",
        allow_exact_matches=True,
    )

    assert joined.loc[0, "timestamp"] == pd.Timestamp("2026-01-01T00:15:00Z")
    assert joined.loc[1, "timestamp"] == pd.Timestamp("2026-01-01T00:20:00Z")
    assert (joined["timestamp"] <= joined["created_at"]).all()


def test_consecutive_count_resets_on_false_values():
    result = consecutive_count(pd.Series([True, True, False, True, True, True, False]))

    assert result.tolist() == [1.0, 2.0, 0.0, 1.0, 2.0, 3.0, 0.0]

