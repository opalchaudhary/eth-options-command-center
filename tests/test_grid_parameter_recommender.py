from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from grid_bot.models import GridConfig, GridType, SpacingType
from grid_bot.recommendation import (
    CurrentGridSnapshot,
    GridProbabilityInputs,
    HorizonProbability,
    RecommendationAction,
    recommend_grid_parameters,
)


NOW = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)


def _prediction(
    horizon="12H",
    inside=0.68,
    expansion=0.35,
    lower="4400",
    upper="4600",
    up=None,
    down=None,
    timestamp=None,
    **updates,
):
    payload = {
        "horizon": horizon,
        "path_inside_70": inside,
        "realized_over_range_width_ge_1": expansion,
        "range_70_lower": Decimal(lower),
        "range_70_upper": Decimal(upper),
        "upside_breakout": up,
        "downside_breakdown": down,
        "prediction_timestamp": timestamp or NOW - timedelta(minutes=5),
        "model_version": "probability_v2_candidate_v1",
        "manifest_hash": "aa59ecc",
    }
    payload.update(updates)
    return HorizonProbability(**payload)


def _inputs(prediction=None, spot="4500", as_of=NOW):
    prediction = prediction or _prediction()
    return GridProbabilityInputs(
        spot_price=Decimal(spot),
        as_of=as_of,
        predictions={prediction.horizon: prediction},
    )


def _current(**updates):
    payload = {
        "grid_type": GridType.NEUTRAL,
        "lower_price": Decimal("4400"),
        "upper_price": Decimal("4600"),
        "grid_count": 9,
        "spacing_type": SpacingType.ARITHMETIC,
    }
    payload.update(updates)
    return CurrentGridSnapshot(**payload)


def test_stable_range_bound_market_recommends_neutral_grid():
    rec = recommend_grid_parameters(_inputs(_prediction(inside=0.7, expansion=0.3, up=0.42, down=0.38)))

    assert rec.grid_type == GridType.NEUTRAL
    assert rec.action == RecommendationAction.CONSIDER_EDIT
    assert "HIGH_PATH_CONTAINMENT" in rec.reason_codes
    assert rec.lower_price < Decimal("4500") < rec.upper_price


def test_clear_bullish_directional_evidence_recommends_long_bias():
    rec = recommend_grid_parameters(_inputs(_prediction(up=0.72, down=0.35)))

    assert rec.grid_type == GridType.LONG_BIAS
    assert "DIRECTIONAL_UPSIDE_BIAS" in rec.reason_codes


def test_clear_bearish_directional_evidence_recommends_short_bias():
    rec = recommend_grid_parameters(_inputs(_prediction(up=0.31, down=0.66)))

    assert rec.grid_type == GridType.SHORT_BIAS
    assert "DIRECTIONAL_DOWNSIDE_BIAS" in rec.reason_codes


def test_weak_direction_remains_neutral():
    rec = recommend_grid_parameters(_inputs(_prediction(up=0.53, down=0.45)))

    assert rec.grid_type == GridType.NEUTRAL
    assert "DIRECTIONAL_SIGNAL_WEAK" in rec.reason_codes


def test_high_breakout_risk_widens_and_sparsifies_grid():
    normal = recommend_grid_parameters(_inputs(_prediction(expansion=0.4)))
    risky = recommend_grid_parameters(_inputs(_prediction(expansion=0.78)))

    assert risky.lower_price < normal.lower_price
    assert risky.upper_price > normal.upper_price
    assert risky.grid_count <= normal.grid_count
    assert "RANGE_EXPANSION_RISK" in risky.reason_codes


def test_extreme_expansion_and_low_containment_returns_no_grid():
    rec = recommend_grid_parameters(_inputs(_prediction(inside=0.25, expansion=0.9)))

    assert rec.action == RecommendationAction.NO_GRID
    assert rec.grid_type is None


def test_strong_containment_returns_reasonable_neutral_grid():
    rec = recommend_grid_parameters(_inputs(_prediction(inside=0.82, expansion=0.22)))

    assert rec.grid_type == GridType.NEUTRAL
    assert rec.confidence > 0.6
    assert rec.grid_count >= 5


def test_missing_critical_v2_signal_returns_conservative_result():
    rec = recommend_grid_parameters(_inputs(_prediction(inside=None)))

    assert rec.action == RecommendationAction.NO_GRID
    assert "MISSING_CRITICAL_V2_SIGNAL" in rec.reason_codes


def test_abstained_prediction_returns_no_grid():
    rec = recommend_grid_parameters(_inputs(_prediction(abstained=True, abstention_reason="MISSING_RANGE_REFERENCE")))

    assert rec.action == RecommendationAction.NO_GRID
    assert "V2_ABSTAINED" in rec.reason_codes


def test_ood_prediction_returns_no_grid():
    rec = recommend_grid_parameters(_inputs(_prediction(ood_status="FLAGGED", ood_reason="MISSING_OR_EXTREME_FEATURES")))

    assert rec.action == RecommendationAction.NO_GRID
    assert "V2_OOD" in rec.reason_codes


def test_stale_prediction_returns_no_grid():
    rec = recommend_grid_parameters(_inputs(_prediction(timestamp=NOW - timedelta(minutes=30))))

    assert rec.action == RecommendationAction.NO_GRID
    assert "V2_STALE" in rec.reason_codes


def test_arithmetic_selection_for_normal_eth_range():
    rec = recommend_grid_parameters(_inputs(_prediction(lower="4400", upper="4600")))

    assert rec.spacing_type == SpacingType.ARITHMETIC


def test_geometric_selection_for_broad_relative_range():
    rec = recommend_grid_parameters(_inputs(_prediction(lower="3800", upper="5200")))

    assert rec.spacing_type == SpacingType.GEOMETRIC


def test_grid_count_and_spacing_are_consistent():
    rec = recommend_grid_parameters(_inputs(_prediction(lower="4400", upper="4600")))

    expected = (rec.upper_price - rec.lower_price) / Decimal(rec.grid_count - 1)
    assert abs(expected - rec.step) <= Decimal("0.05")


def test_current_grid_within_tolerance_keeps_current():
    rec = recommend_grid_parameters(_inputs(_prediction()), current_grid=_current())

    assert rec.action == RecommendationAction.KEEP_CURRENT
    assert "CURRENT_GRID_WITHIN_TOLERANCE" in rec.reason_codes


def test_mild_current_grid_drift_consider_edit():
    rec = recommend_grid_parameters(
        _inputs(_prediction()),
        current_grid=_current(lower_price=Decimal("4370"), upper_price=Decimal("4630")),
    )

    assert rec.action == RecommendationAction.CONSIDER_EDIT


def test_material_current_grid_mismatch_regrid():
    rec = recommend_grid_parameters(_inputs(_prediction(up=0.72, down=0.35)), current_grid=_current())

    assert rec.action == RecommendationAction.REGRID
    assert "GRID_TYPE_MISMATCH" in rec.reason_codes


def test_spot_near_current_grid_edge_regrids():
    rec = recommend_grid_parameters(
        _inputs(_prediction()),
        current_grid=_current(spot_price=Decimal("4597")),
    )

    assert rec.action == RecommendationAction.REGRID
    assert "SPOT_NEAR_CURRENT_RANGE_EDGE" in rec.reason_codes


def test_no_current_grid_still_returns_complete_parameter_recommendation():
    rec = recommend_grid_parameters(_inputs(_prediction()))

    assert rec.action == RecommendationAction.CONSIDER_EDIT
    assert rec.lower_price is not None
    assert rec.upper_price is not None
    assert rec.grid_count is not None
    assert rec.step is not None


def test_recommended_prices_and_spacing_are_valid():
    rec = recommend_grid_parameters(_inputs(_prediction()))

    assert rec.lower_price < rec.upper_price
    assert rec.step > 0
    assert rec.grid_count >= 2


def test_recommendation_is_deterministic_for_same_input():
    inputs = _inputs(_prediction(up=0.41, down=0.38))

    first = recommend_grid_parameters(inputs)
    second = recommend_grid_parameters(inputs)

    assert asdict(first) == asdict(second)


def test_fallback_horizon_uses_8h_when_12h_missing():
    pred = _prediction(horizon="8H", inside=0.66, expansion=0.4)
    rec = recommend_grid_parameters(GridProbabilityInputs(spot_price=Decimal("4500"), predictions={"8H": pred}, as_of=NOW))

    assert rec.metadata["selected_operating_horizon"] == "8H"


def test_unsafe_12h_falls_back_to_valid_8h():
    stale_12h = _prediction(horizon="12H", timestamp=NOW - timedelta(hours=1))
    valid_8h = _prediction(horizon="8H")
    rec = recommend_grid_parameters(GridProbabilityInputs(spot_price=Decimal("4500"), predictions={"12H": stale_12h, "8H": valid_8h}, as_of=NOW))

    assert rec.metadata["selected_operating_horizon"] == "8H"


def test_grid_config_can_be_used_as_current_snapshot():
    config = GridConfig(
        bot_id="bot",
        config_version=1,
        bot_name="Test",
        product_symbol="ETHUSD",
        grid_type=GridType.NEUTRAL,
        lower_price=Decimal("4400"),
        upper_price=Decimal("4600"),
        grid_count=9,
        spacing_type=SpacingType.ARITHMETIC,
        lot_size=Decimal("1"),
        max_inventory_lots=Decimal("5"),
        allocated_capital=Decimal("100"),
        risk_capital=Decimal("50"),
    )

    rec = recommend_grid_parameters(_inputs(_prediction()), current_grid=config)

    assert rec.action == RecommendationAction.KEEP_CURRENT


def test_supplied_tick_size_is_used_for_range_and_step_quantization():
    pred = _prediction(lower="4400.03", upper="4600.07")
    rec = recommend_grid_parameters(
        GridProbabilityInputs(
            spot_price=Decimal("4500"),
            predictions={"12H": pred},
            as_of=NOW,
            tick_size=Decimal("0.10"),
        )
    )

    assert rec.lower_price % Decimal("0.10") == 0
    assert rec.upper_price % Decimal("0.10") == 0
    assert rec.step % Decimal("0.10") == 0


def test_grid_config_current_snapshot_uses_geometric_step_for_geometric_grid():
    config = GridConfig(
        bot_id="bot",
        config_version=1,
        bot_name="Test",
        product_symbol="ETHUSD",
        grid_type=GridType.NEUTRAL,
        lower_price=Decimal("3800"),
        upper_price=Decimal("5200"),
        grid_count=11,
        spacing_type=SpacingType.GEOMETRIC,
        lot_size=Decimal("1"),
        max_inventory_lots=Decimal("5"),
        allocated_capital=Decimal("100"),
        risk_capital=Decimal("50"),
    )

    snapshot = CurrentGridSnapshot.from_grid_config(config, Decimal("4500"))

    assert snapshot.step != (config.upper_price - config.lower_price) / Decimal(config.grid_count - 1)
    assert snapshot.step > 0


def test_recommender_has_no_exchange_supabase_network_or_order_side_effects(monkeypatch):
    def fail(*_args, **_kwargs):
        raise AssertionError("side effect attempted")

    monkeypatch.setattr("requests.get", fail)
    monkeypatch.setattr("requests.post", fail)

    rec = recommend_grid_parameters(_inputs(_prediction()))

    assert rec.grid_type == GridType.NEUTRAL
