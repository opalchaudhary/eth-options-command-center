from __future__ import annotations

from datetime import datetime, timedelta, timezone

from backend.routers import grid as grid_router
from grid_bot.recommendation_outcome_evaluator import (
    HORIZON_MINUTES,
    OUTCOME_TABLE,
    GridRecommendationOutcomeEvaluator,
)


NOW = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)
START = datetime(2026, 9, 8, 8, 0, tzinfo=timezone.utc)


def _recommendation(**updates):
    row = {
        "recommendation_id": "rec-1",
        "requested_at": START.isoformat(),
        "created_at": START.isoformat(),
        "symbol": "ETHUSD",
        "recommender_version": "grid_parameter_recommender_v0_1",
        "selected_operating_horizon": "12H",
        "recommended_lower_price": "95",
        "recommended_upper_price": "105",
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
        "volume": "1",
        "resolution": "5m",
    }


class FakeDb:
    enabled = True

    def __init__(self, *, recommendations=None, candles=None, outcomes=None, latest=None):
        self.recommendations = recommendations if recommendations is not None else [_recommendation()]
        self.candles = candles if candles is not None else []
        self.outcomes = outcomes if outcomes is not None else []
        self.latest = latest or (START + timedelta(hours=24))
        self.inserts = []
        self.selects = []

    def select(self, table, params=None):
        params = params or {}
        self.selects.append({"table": table, "params": params})
        if table == "grid_parameter_recommendations":
            return self.recommendations[: int(params.get("limit") or len(self.recommendations))]
        if table == OUTCOME_TABLE:
            return self.outcomes
        if table == "eth_ohlcv":
            if params.get("order") == "candle_time.desc":
                resolution = str(params.get("resolution", "eq.5m")).replace("eq.", "")
                if resolution == "1m":
                    return []
                return [{"candle_time": self.latest.isoformat(), "resolution": "5m"}]
            resolution = str(params.get("resolution", "eq.5m")).replace("eq.", "")
            if resolution == "1m":
                return []
            return self.candles[: int(params.get("limit") or len(self.candles))]
        return []

    def insert_once(self, table, payload, on_conflict=None):
        self.inserts.append({"table": table, "payload": payload, "on_conflict": on_conflict})
        return True


def test_evaluator_calculates_breach_metrics_and_persists_idempotently():
    db = FakeDb(
        candles=[
            _candle(5, 100, 103, 98, 102),
            _candle(10, 102, 108, 101, 107),
            _candle(15, 107, 109, 93, 94),
        ]
    )
    evaluator = GridRecommendationOutcomeEvaluator(db=db, now_fn=lambda: NOW)

    result = evaluator.evaluate_pending(persist=True)

    one_hour = next(row for row in result.rows if row["horizon"] == "1H")
    assert one_hour["upper_breached"] is True
    assert one_hour["lower_breached"] is True
    assert one_hour["both_sides_breached"] is True
    assert one_hour["first_breach_side"] == "upper"
    assert one_hour["minutes_to_first_breach"] == 10
    assert one_hour["max_excursion_above_upper"] == 4
    assert one_hour["max_excursion_below_lower"] == 2
    assert one_hour["actual_recommended_width_ratio"] == 1.6
    assert db.inserts[0]["table"] == OUTCOME_TABLE
    assert db.inserts[0]["on_conflict"] == "recommendation_id,horizon"


def test_evaluator_skips_unmatured_horizons():
    db = FakeDb(latest=START + timedelta(hours=4))
    evaluator = GridRecommendationOutcomeEvaluator(db=db, now_fn=lambda: NOW)

    result = evaluator.evaluate_pending(persist=False)

    assert result.mature_counts["1H"] == 1
    assert result.mature_counts["4H"] == 1
    assert result.mature_counts["8H"] == 0
    assert result.skipped_unmatured["8H"] == 1
    assert len(result.rows) == 2


def test_evaluator_skips_existing_outcome_keys_when_persisting():
    db = FakeDb(
        outcomes=[{"recommendation_id": "rec-1", "horizon": "1H"}],
        candles=[_candle(minutes, 100, 104, 96, 101) for minutes in range(5, HORIZON_MINUTES["24H"] + 1, 5)],
    )
    evaluator = GridRecommendationOutcomeEvaluator(db=db, now_fn=lambda: NOW)

    result = evaluator.evaluate_pending(persist=True)

    assert result.skipped_existing == 1
    assert all(insert["payload"]["horizon"] != "1H" for insert in db.inserts)


def test_summary_aggregates_by_horizon_and_version():
    db = FakeDb(
        outcomes=[
            {
                "horizon": "1H",
                "recommender_version": "v1",
                "stayed_inside_recommended_range": True,
                "upper_breached": False,
                "lower_breached": False,
                "minutes_to_first_breach": None,
                "actual_recommended_width_ratio": "0.5",
            },
            {
                "horizon": "1H",
                "recommender_version": "v1",
                "stayed_inside_recommended_range": False,
                "upper_breached": True,
                "lower_breached": False,
                "minutes_to_first_breach": "15",
                "actual_recommended_width_ratio": "1.2",
            },
        ]
    )
    evaluator = GridRecommendationOutcomeEvaluator(db=db, now_fn=lambda: NOW)

    payload = evaluator.summary()

    assert payload["rows"] == [
        {
            "horizon": "1H",
            "recommender_version": "v1",
            "total_evaluated": 2,
            "containment_rate": 0.5,
            "breach_rate": 0.5,
            "median_time_to_breach_minutes": 15.0,
            "median_actual_recommended_width_ratio": 0.85,
        }
    ]


def test_summary_endpoint_is_registered(monkeypatch):
    class FakeEvaluator:
        def summary(self, limit=5000):
            assert limit == 123
            return {"ok": True, "rows": []}

    monkeypatch.setattr(grid_router, "GridRecommendationOutcomeEvaluator", lambda: FakeEvaluator())

    assert grid_router.grid_v01_recommendation_outcomes_summary(limit=123) == {"ok": True, "rows": []}
    routes = {
        route.path
        for route in grid_router.public_grid_router.routes
        if "GET" in getattr(route, "methods", set())
    }
    assert "/grid/v01/recommendation-outcomes/summary" in routes
