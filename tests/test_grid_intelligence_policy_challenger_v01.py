from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from research.grid_intelligence_policy_challenger_v01 import (
    champion_policy,
    evaluate_policy,
    expansion_widen_policy,
    stress_no_grid_policy,
    v2_range_floor_policy,
    v2_volatility_buffer_policy,
)


START = datetime(2026, 9, 8, 0, 0, tzinfo=timezone.utc)


def _row(**updates):
    row = {
        "recommendation_id": "rec-1",
        "requested_at": START.isoformat(),
        "recommended_lower_price": "95",
        "recommended_upper_price": "105",
        "range_70_lower": "96",
        "range_70_upper": "104",
        "path_inside_70": "0.52",
        "realized_over_range_width_ge_1": "0.75",
        "upside_probability": "0.50",
        "downside_probability": "0.46",
        "recommender_confidence": "0.58",
    }
    row.update(updates)
    return row


def _outcome(**updates):
    row = {
        "recommendation_id": "rec-1",
        "evaluation_start": START.isoformat(),
        "actual_forward_low": "94",
        "actual_forward_high": "107",
    }
    row.update(updates)
    return row


def _candle(minutes, open_price, high, low, close):
    return {
        "candle_time": (START + timedelta(minutes=minutes)).isoformat(),
        "open": str(open_price),
        "high": str(high),
        "low": str(low),
        "close": str(close),
    }


def test_expansion_widen_uses_point_in_time_expansion_probability():
    policy = expansion_widen_policy(_row(realized_over_range_width_ge_1="0.75"))

    assert policy.policy == "expansion_widen"
    assert policy.lower == pytest.approx(93.25)
    assert policy.upper == pytest.approx(106.75)
    assert policy.width == pytest.approx(13.5)


def test_v2_range_floor_uses_only_available_range70_without_shrinking_champion():
    policy = v2_range_floor_policy(_row(range_70_lower="90", range_70_upper="104"))

    assert policy.lower == 90
    assert policy.upper == 105


def test_trailing_buffer_uses_pre_recommendation_width_and_expansion():
    policy = v2_volatility_buffer_policy(_row(realized_over_range_width_ge_1="0.50"), {"trailing_12h_path_width": 20})

    assert policy.lower == pytest.approx(91.5)
    assert policy.upper == pytest.approx(108.5)


def test_stress_filter_returns_no_grid_for_low_containment_high_expansion():
    policy = stress_no_grid_policy(_row(path_inside_70="0.30", realized_over_range_width_ge_1="0.80"))

    assert policy.no_grid is True
    assert policy.reason == "containment <= 0.38 and expansion >= 0.72"


def test_evaluate_policy_recomputes_breach_timing_for_challenger_range():
    policy = champion_policy(_row())
    result = evaluate_policy(
        _row(),
        _outcome(),
        policy,
        [
            _candle(5, 100, 103, 98, 101),
            _candle(10, 101, 108, 100, 107),
        ],
    )

    assert result["stayed_inside"] is False
    assert result["breached"] is True
    assert result["first_breach_side"] == "upper"
    assert result["minutes_to_first_breach"] == 10
    assert result["actual_recommended_width_ratio"] == pytest.approx(1.3)
    assert result["range_utilization"] == pytest.approx(1.0)
