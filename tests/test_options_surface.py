from datetime import datetime, timezone

import pandas as pd

from rich_data.config import RICH_OPTIONS_SURFACE_VERSION
from rich_data.options_surface import (
    OptionsSurfaceCollector,
    build_options_surface_rows,
    due_buckets,
    map_target_expiries,
    normalize_eth_options_surface_chain,
    realized_volatility_24h,
)
from rich_data.repositories import OptionsSurfaceSnapshotRepository


NOW = datetime(2026, 8, 23, 15, 0, tzinfo=timezone.utc)


def _expiry(day):
    return datetime(2026, 8, day, 12, 0, tzinfo=timezone.utc)


def _row(expiry, option_type, strike, mark, iv, delta, bid=None, ask=None, oi=10, volume=1):
    return {
        "contract_symbol": f"{'C' if option_type == 'call_options' else 'P'}-ETH-{strike}",
        "expiry": expiry,
        "strike": strike,
        "type": option_type,
        "spot_price": 2400,
        "index_price": 2400,
        "mark_price": mark,
        "bid": bid if bid is not None else max(0, mark - 1),
        "ask": ask if ask is not None else mark + 1,
        "oi": oi,
        "volume": volume,
        "mark_iv": iv,
        "bid_iv": iv - 2 if iv is not None else None,
        "ask_iv": iv + 2 if iv is not None else None,
        "delta": delta,
        "gamma": 0.01,
        "theta": -1,
        "vega": 2,
    }


def _surface_df(expiries=None):
    expiries = expiries or [
        _expiry(24),
        _expiry(25),
        _expiry(26),
        _expiry(28),
        datetime(2026, 9, 4, 12, tzinfo=timezone.utc),
        datetime(2026, 9, 11, 12, tzinfo=timezone.utc),
        datetime(2026, 9, 25, 12, tzinfo=timezone.utc),
        datetime(2026, 10, 30, 12, tzinfo=timezone.utc),
    ]
    rows = []
    for expiry in expiries:
        rows.extend(
            [
                _row(expiry, "put_options", 2300, 12, 62, -0.25, oi=70, volume=7),
                _row(expiry, "put_options", 2350, 24, 58, -0.40, oi=20, volume=3),
                _row(expiry, "call_options", 2400, 50, 50, 0.52, oi=30, volume=4),
                _row(expiry, "put_options", 2400, 45, 54, -0.48, oi=40, volume=5),
                _row(expiry, "call_options", 2450, 26, 56, 0.38, oi=25, volume=2),
                _row(expiry, "call_options", 2500, 15, 60, 0.25, oi=80, volume=9),
            ]
        )
    return pd.DataFrame(rows)


def test_expiry_bucket_mapping_uses_calendar_slots_and_missing_d0():
    mapping = map_target_expiries(_surface_df()["expiry"].unique(), now=NOW)

    assert "D0" not in mapping
    assert mapping["D1"] == _expiry(24)
    assert mapping["D2"] == _expiry(25)
    assert mapping["D3"] == _expiry(26)
    assert mapping["W1"] == _expiry(28)
    assert mapping["W2"] == datetime(2026, 9, 4, 12, tzinfo=timezone.utc)
    assert mapping["W3"] == datetime(2026, 9, 11, 12, tzinfo=timezone.utc)
    assert mapping["M1"] == datetime(2026, 9, 25, 12, tzinfo=timezone.utc)


def test_due_bucket_cadence_10m_30m_60m():
    assert due_buckets(datetime(2026, 8, 23, 15, 10, tzinfo=timezone.utc)) == ["D0", "D1", "D2", "D3"]
    assert due_buckets(datetime(2026, 8, 23, 15, 30, tzinfo=timezone.utc)) == ["D0", "D1", "D2", "D3", "W1", "W2", "W3"]
    assert due_buckets(datetime(2026, 8, 23, 16, 0, tzinfo=timezone.utc)) == ["D0", "D1", "D2", "D3", "W1", "W2", "W3", "M1"]


def test_surface_row_core_atm_iv_implied_move_skew_oi_volume_liquidity_and_greeks():
    rows = build_options_surface_rows(_surface_df(), now=NOW, buckets=["D1"], realized_volatility_reference=40)

    assert len(rows) == 1
    row = rows[0]
    assert row["version"] == RICH_OPTIONS_SURFACE_VERSION
    assert row["logical_expiry_bucket"] == "D1"
    assert row["atm_strike"] == 2400
    assert row["atm_call_mark"] == 50
    assert row["atm_put_mark"] == 45
    assert row["atm_iv"] == 52
    assert row["atm_straddle_mark"] == 95
    assert round(row["implied_move_pct"], 6) == round(95 / 2400 * 100, 6)
    assert row["iv_rv_spread"] == 12
    assert row["iv_rv_ratio"] == 1.3
    assert row["call_25d_iv"] == 60
    assert row["put_25d_iv"] == 62
    assert row["risk_reversal_25d"] == -2
    assert row["butterfly_25d"] == 9
    assert row["total_call_oi"] == 135
    assert row["total_put_oi"] == 130
    assert round(row["put_call_oi_ratio"], 6) == round(130 / 135, 6)
    assert row["largest_call_oi_strike"] == 2500
    assert row["largest_put_oi_strike"] == 2300
    assert row["total_call_volume"] == 15
    assert row["total_put_volume"] == 15
    assert row["largest_call_volume_strike"] == 2500
    assert row["largest_put_volume_strike"] == 2300
    assert row["atm_call_spread"] == 2
    assert row["atm_put_spread"] == 2
    assert row["valid_quoted_calls"] == 3
    assert row["valid_quoted_puts"] == 3
    assert row["atm_gamma"] == 0.02
    assert row["atm_theta"] == -2
    assert row["atm_vega"] == 4
    assert row["source_status"] == "COMPLETE"


def test_missing_target_expiry_is_not_substituted():
    df = _surface_df(expiries=[_expiry(24)])
    rows = build_options_surface_rows(df, now=NOW, buckets=["D0", "D1", "W1", "M1"], realized_volatility_reference=None)

    assert [row["logical_expiry_bucket"] for row in rows] == ["D1"]


def test_normalize_surface_chain_confirms_delta_quote_field_mapping():
    products = [
        {
            "symbol": "C-ETH-2400-240826",
            "contract_type": "call_options",
            "strike_price": "2400",
            "settlement_time": "2026-08-24T12:00:00Z",
        }
    ]
    tickers = [
        {
            "symbol": "C-ETH-2400-240826",
            "mark_price": "50",
            "spot_price": "2400",
            "oi": "10",
            "volume": "3",
            "quotes": {"best_bid": "49", "best_ask": "51", "bid_iv": "0.48", "ask_iv": "0.52", "mark_iv": "0.50"},
            "greeks": {"delta": "0.5", "gamma": "0.01", "theta": "-1", "vega": "2", "spot": "2400"},
        }
    ]

    df = normalize_eth_options_surface_chain(products=products, tickers=tickers)

    assert df.iloc[0]["bid"] == 49
    assert df.iloc[0]["ask"] == 51
    assert df.iloc[0]["bid_iv"] == 48
    assert df.iloc[0]["ask_iv"] == 52
    assert df.iloc[0]["mark_iv"] == 50


def test_realized_volatility_reference_uses_5m_close_returns():
    closes = [100, 101, 100.5, 102]
    df = pd.DataFrame({"close": closes})

    rv = realized_volatility_24h(ohlcv_provider=lambda **kwargs: df)

    assert rv is not None
    assert rv > 0


def test_collector_reuses_one_master_chain_fetch_and_persists_idempotent_rows():
    calls = {"chain": 0, "rv": 0}

    class FakeRepository:
        def __init__(self):
            self.rows = []

        def upsert_many(self, rows):
            self.rows.extend(rows)
            return True

    repo = FakeRepository()

    def chain_provider():
        calls["chain"] += 1
        return _surface_df()

    def rv_provider(**kwargs):
        calls["rv"] += 1
        return 40

    collector = OptionsSurfaceCollector(repository=repo, chain_provider=chain_provider, realized_vol_provider=rv_provider)
    result = collector.collect(now=datetime(2026, 8, 23, 16, 0, tzinfo=timezone.utc))

    assert result["ok"] is True
    assert calls == {"chain": 1, "rv": 1}
    assert set(result["persisted_buckets"]) == {"D1", "D2", "D3", "W1", "W2", "W3", "M1"}
    assert OptionsSurfaceSnapshotRepository.conflict_columns == "symbol,logical_expiry_bucket,snapshot_timestamp,version"


def test_per_expiry_failure_isolation(monkeypatch):
    import rich_data.options_surface as surface

    original = surface.build_expiry_surface_row

    def flaky(expiry_df, logical_expiry_bucket, *args, **kwargs):
        if logical_expiry_bucket == "D1":
            raise ValueError("bad expiry")
        return original(expiry_df, logical_expiry_bucket, *args, **kwargs)

    monkeypatch.setattr(surface, "build_expiry_surface_row", flaky)
    rows = build_options_surface_rows(_surface_df(), now=NOW, buckets=["D1", "D2"], realized_volatility_reference=40)

    assert rows[0]["logical_expiry_bucket"] == "D1"
    assert rows[0]["source_status"] == "UNAVAILABLE"
    assert rows[1]["logical_expiry_bucket"] == "D2"
    assert rows[1]["source_status"] == "COMPLETE"
