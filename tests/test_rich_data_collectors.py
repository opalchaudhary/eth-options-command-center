from datetime import datetime, timedelta, timezone

import pandas as pd

from rich_data.derivatives import build_derivatives_snapshot
from rich_data.orderbook import build_orderbook_aggregate
from rich_data.orderflow import aggregate_trades, aggressive_side


def test_derivatives_snapshot_parses_funding_oi_and_basis():
    row = build_derivatives_snapshot(
        {
            "symbol": "ETHUSD",
            "spot_price": "2300",
            "mark_price": "2302.3",
            "oi": "15000",
            "funding_rate": "0.01",
        },
        previous={"open_interest": "14900"},
        funding_samples=[0.001] * 19 + [0.02],
        collected_at=datetime(2026, 8, 20, 19, 3, tzinfo=timezone.utc),
    )

    assert row["timestamp"] == "2026-08-20T19:00:00+00:00"
    assert row["mark_premium"] == 2.300000000000182
    assert round(row["mark_premium_pct"], 6) == 0.001
    assert row["open_interest"] == 15000
    assert row["oi_delta_5m"] == 100
    assert round(row["oi_delta_pct_5m"], 6) == 0.006711
    assert row["funding_rate"] == 0.01
    assert row["funding_percentile"] == 0.95
    assert row["version"] == "rich_data_v1_derivatives"
    assert row["metadata_json"]["sources"]["funding_rate"] == "delta.ticker.funding_rate"


def test_derivatives_snapshot_does_not_fabricate_missing_values():
    row = build_derivatives_snapshot(
        {"symbol": "ETHUSD", "spot_price": "2300"},
        previous={},
        funding_samples=[],
        collected_at=datetime(2026, 8, 20, 19, 0, tzinfo=timezone.utc),
    )

    assert row["mark_price"] is None
    assert row["mark_premium"] is None
    assert row["open_interest"] is None
    assert row["funding_zscore"] is None
    assert row["source_status"] == "PARTIAL"


def test_trade_aggressor_classification_from_delta_roles():
    assert aggressive_side({"buyer_role": "taker", "seller_role": "maker"}) == "buy"
    assert aggressive_side({"buyer_role": "maker", "seller_role": "taker"}) == "sell"
    assert aggressive_side({"buyer_role": "maker", "seller_role": "maker"}) is None


def test_orderflow_aggregates_cvd_and_deduplicates_closed_buckets():
    base = datetime(2026, 8, 20, 19, 2, tzinfo=timezone.utc)
    timestamp = int(base.timestamp() * 1_000_000)
    trades = [
        {"timestamp": timestamp, "symbol": "ETHUSD", "price": "2300", "size": "10", "buyer_role": "taker", "seller_role": "maker"},
        {"timestamp": timestamp, "symbol": "ETHUSD", "price": "2300", "size": "10", "buyer_role": "taker", "seller_role": "maker"},
        {"timestamp": timestamp + 1, "symbol": "ETHUSD", "price": "2301", "size": "4", "buyer_role": "maker", "seller_role": "taker"},
    ]

    rows = aggregate_trades(
        trades,
        now=datetime(2026, 8, 20, 19, 4, tzinfo=timezone.utc),
        previous_rows=[{"bucket_timestamp": "2026-08-20T19:01:00+00:00", "cvd_increment": 3, "cvd_running": 3}],
    )

    assert len(rows) == 1
    assert rows[0]["trade_count"] == 2
    assert rows[0]["taker_buy_volume"] == 10
    assert rows[0]["taker_sell_volume"] == 4
    assert rows[0]["cvd_increment"] == 6
    assert rows[0]["cvd_running"] == 9
    assert rows[0]["cvd_5m"] == 9
    assert rows[0]["metadata_json"]["sources"]["aggressive_side"].startswith("buyer_role=taker")


def test_orderflow_skips_open_current_bucket_to_avoid_double_counting():
    now = datetime(2026, 8, 20, 19, 4, 30, tzinfo=timezone.utc)
    trades = [
        {
            "timestamp": int(now.timestamp() * 1_000_000),
            "symbol": "ETHUSD",
            "price": "2300",
            "size": "10",
            "buyer_role": "taker",
            "seller_role": "maker",
        }
    ]

    assert aggregate_trades(trades, now=now) == []


def test_orderbook_depth_imbalance_microprice_and_walls():
    orderbook = {
        "symbol": "ETHUSD",
        "bids": pd.DataFrame(
            [
                {"price": 99.9, "size": 10, "depth": 10},
                {"price": 99.75, "size": 30, "depth": 40},
                {"price": 99.0, "size": 100, "depth": 140},
            ]
        ),
        "asks": pd.DataFrame(
            [
                {"price": 100.1, "size": 20, "depth": 20},
                {"price": 100.25, "size": 10, "depth": 30},
                {"price": 101.0, "size": 80, "depth": 110},
            ]
        ),
    }

    row = build_orderbook_aggregate(
        orderbook,
        collected_at=datetime(2026, 8, 20, 19, 4, 30, tzinfo=timezone.utc),
    )

    assert row["timestamp"] == "2026-08-20T19:04:00+00:00"
    assert round(row["spread_bps"], 6) == 20
    assert row["bid_depth_25bp"] == 40
    assert row["ask_depth_25bp"] == 30
    assert round(row["imbalance_25bp"], 6) == 0.142857
    assert row["major_bid_wall_size"] == 100
    assert row["major_ask_wall_size"] == 80
    assert row["liquidity_concentration"] == 100 / 250
    assert row["version"] == "rich_data_v1_orderbook"


def test_rich_data_scheduler_jobs_are_opt_in(monkeypatch):
    import backend.services.scheduler_service as scheduler_service
    from probability_engine.config import ProbabilityEngineConfig

    added = []
    monkeypatch.setattr(scheduler_service.config, "BACKEND_SCHEDULER_ENABLED", True)
    monkeypatch.setattr(scheduler_service.config, "RICH_DATA_COLLECTION_ENABLED", False)
    monkeypatch.setattr(scheduler_service, "get_probability_config", lambda: ProbabilityEngineConfig(enabled=False))

    class FakeScheduler:
        running = False

        def add_job(self, *args, **kwargs):
            added.append(kwargs["id"])

        def start(self):
            self.running = True

        def get_jobs(self):
            return []

    monkeypatch.setattr(scheduler_service, "BackgroundScheduler", lambda timezone: FakeScheduler())
    scheduler_service._scheduler = None
    scheduler_service.start_scheduler()

    assert "rich_derivatives_v1" not in added
    assert "rich_orderflow_v1" not in added
    assert "rich_orderbook_v1" not in added


def test_rich_data_scheduler_registers_when_enabled(monkeypatch):
    import backend.services.scheduler_service as scheduler_service
    from probability_engine.config import ProbabilityEngineConfig

    added = []
    monkeypatch.setattr(scheduler_service.config, "BACKEND_SCHEDULER_ENABLED", True)
    monkeypatch.setattr(scheduler_service.config, "RICH_DATA_COLLECTION_ENABLED", True)
    monkeypatch.setattr(scheduler_service.config, "RICH_ORDERFLOW_REST_ENABLED", True)
    monkeypatch.setattr(scheduler_service.config, "RICH_DERIVATIVES_INTERVAL_SECONDS", 300)
    monkeypatch.setattr(scheduler_service.config, "RICH_ORDERFLOW_INTERVAL_SECONDS", 60)
    monkeypatch.setattr(scheduler_service.config, "RICH_ORDERBOOK_INTERVAL_SECONDS", 60)
    monkeypatch.setattr(scheduler_service, "get_probability_config", lambda: ProbabilityEngineConfig(enabled=False))

    class FakeScheduler:
        running = False

        def add_job(self, *args, **kwargs):
            added.append(kwargs["id"])

        def start(self):
            self.running = True

        def get_jobs(self):
            return []

    monkeypatch.setattr(scheduler_service, "BackgroundScheduler", lambda timezone: FakeScheduler())
    scheduler_service._scheduler = None
    scheduler_service.start_scheduler()

    assert "rich_derivatives_v1" in added
    assert "rich_orderflow_v1" in added
    assert "rich_orderbook_v1" in added
