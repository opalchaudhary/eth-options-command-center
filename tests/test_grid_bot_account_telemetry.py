from decimal import Decimal

from grid_bot.account_telemetry import (
    AccountTelemetryCache,
    TelemetrySync,
    normalize_account_risk_state,
    risk_increasing_action_allowed,
    risk_reducing_action_allowed,
)
from grid_bot.models import ProductSpec


def _product():
    return ProductSpec(
        product_id=1699,
        symbol="ETHUSD",
        contract_type="perpetual_futures",
        contract_multiplier=Decimal("0.01"),
        lot_size=Decimal("1"),
        min_quantity=Decimal("1"),
        tick_size=Decimal("0.05"),
        price_precision=2,
        quantity_precision=0,
        mark_price=Decimal("2500"),
    )


def _syncs(age=0, account_ok=True, position_ok=True, orders_ok=True, market_ok=True):
    return {
        "account": TelemetrySync("2026-08-27T00:00:00+00:00" if account_ok else None, age if account_ok else None, account_ok),
        "position": TelemetrySync("2026-08-27T00:00:00+00:00" if position_ok else None, age if position_ok else None, position_ok),
        "orders": TelemetrySync("2026-08-27T00:00:00+00:00" if orders_ok else None, age if orders_ok else None, orders_ok),
        "market": TelemetrySync("2026-08-27T00:00:00+00:00" if market_ok else None, age if market_ok else None, market_ok),
    }


def _wallet(balance="100", available="80", blocked="5", order_margin="10", position_margin="5", equity="100"):
    return {
        "success": True,
        "meta": {"net_equity": equity},
        "result": [
            {
                "asset_symbol": "USD",
                "balance": balance,
                "available_balance": available,
                "blocked_margin": blocked,
                "order_margin": order_margin,
                "position_margin": position_margin,
            }
        ],
    }


MISSING = object()


def _state(wallet=None, positions=MISSING, orders=None, ticker=None, syncs=None, stale_after=60):
    return normalize_account_risk_state(
        product=_product(),
        wallet_payload=wallet if wallet is not None else _wallet(),
        positions_payload={"success": True, "result": []} if positions is MISSING else positions,
        orders_payload=orders if orders is not None else {"success": True, "result": []},
        ticker_payload=ticker if ticker is not None else {"success": True, "result": {"mark_price": "2500"}},
        syncs=syncs if syncs is not None else _syncs(),
        stale_after_seconds=stale_after,
    )


def test_zero_values_remain_distinct_from_unknown_values():
    zero = _state(
        wallet=_wallet(balance="0", available="0", blocked="0", order_margin="0", position_margin="0", equity="0"),
        positions={"success": True, "result": [{"product_id": 1699, "size": "0", "unrealized_pnl": "0"}]},
    )
    unknown = _state(wallet={"success": True, "meta": {}, "result": []}, positions=None, syncs=_syncs(position_ok=False))

    assert zero.position_lots == Decimal("0")
    assert zero.unrealized_pnl == Decimal("0")
    assert zero.available_margin == Decimal("0")
    assert unknown.position_lots is None
    assert unknown.unrealized_pnl is None
    assert unknown.available_margin is None


def test_telemetry_status_fresh_missing_greeks_stale_and_unavailable():
    fresh = _state()
    stale = _state(syncs=_syncs(age=120), stale_after=60)
    no_account = _state(syncs=_syncs(account_ok=False))
    never_position = _state(positions=None, syncs=_syncs(position_ok=False))

    assert fresh.telemetry_status == "HEALTHY"
    assert fresh.portfolio_gamma is None
    assert stale.telemetry_status == "STALE"
    assert no_account.telemetry_status == "DEGRADED"
    assert never_position.telemetry_status == "UNAVAILABLE"


def test_position_sign_and_notional_use_ethusd_multiplier():
    long_state = _state(positions={"success": True, "result": [{"product_id": 1699, "size": "10", "entry_price": "2400"}]})
    short_state = _state(positions={"success": True, "result": [{"product_id": 1699, "size": "-10"}]})
    flat_state = _state(positions={"success": True, "result": [{"product_id": 1699, "size": "0"}]})

    assert long_state.position_side == "LONG"
    assert long_state.position_base_quantity == Decimal("0.10")
    assert long_state.position_notional == Decimal("250.00")
    assert long_state.average_entry_price == Decimal("2400")
    assert short_state.position_side == "SHORT"
    assert short_state.position_notional == Decimal("250.00")
    assert flat_state.position_side == "FLAT"


def test_margin_calculation_handles_normal_zero_missing_and_invalid_values():
    normal = _state(wallet=_wallet(equity="100", available="75", blocked="5", order_margin="10", position_margin="10"))
    zero_equity = _state(wallet=_wallet(equity="0", available="0", blocked="0", order_margin="0", position_margin="0"))
    missing = _state(wallet={"success": True, "meta": {}, "result": [{"asset_symbol": "USD"}]})
    invalid = _state(wallet=_wallet(equity="bad", available="75", blocked="-5", order_margin="0", position_margin="0"))

    assert normal.used_margin == Decimal("25")
    assert normal.margin_utilisation_pct == Decimal("25.00")
    assert zero_equity.margin_utilisation_pct is None
    assert missing.account_equity is None
    assert missing.available_margin is None
    assert invalid.account_equity is None
    assert invalid.used_margin == Decimal("-5")


def test_open_order_exposure_counts_lots_and_notional_by_side():
    state = _state(
        orders={
            "success": True,
            "result": [
                {"side": "buy", "size": "2", "unfilled_size": "1.5", "limit_price": "2400"},
                {"side": "buy", "size": "1", "unfilled_size": "1", "limit_price": "2450"},
                {"side": "sell", "size": "3", "unfilled_size": "2", "limit_price": "2600"},
            ],
        }
    )
    empty = _state(orders={"success": True, "result": []})

    assert state.open_buy_order_count == 2
    assert state.open_sell_order_count == 1
    assert state.open_buy_quantity_lots == Decimal("2.5")
    assert state.open_sell_quantity_lots == Decimal("2")
    assert state.open_buy_notional == Decimal("60.500")
    assert state.open_sell_notional == Decimal("52.00")
    assert empty.open_order_count == 0


def test_telemetry_action_gates_fail_closed_for_risk_increasing_only():
    healthy = _state()
    unavailable_account = _state(wallet={"success": True, "meta": {}, "result": []}, syncs=_syncs(account_ok=False))
    no_position = _state(positions=None, syncs=_syncs(position_ok=False))

    assert risk_increasing_action_allowed(healthy) == (True, [])
    assert risk_increasing_action_allowed(unavailable_account)[0] is False
    assert risk_reducing_action_allowed(unavailable_account) == (True, [])
    assert risk_reducing_action_allowed(no_position)[0] is False


def test_cache_advances_age_without_delta_reads_until_refresh_interval():
    class Client:
        def __init__(self):
            self.calls = 0

        def product_spec(self, symbol):
            self.calls += 1
            return _product()

        def wallet(self):
            return _wallet()

        def positions(self, underlying_asset_symbol="ETH"):
            return {"success": True, "result": []}

        def open_orders(self, product_id=None):
            return {"success": True, "result": []}

        def ticker(self, symbol):
            return {"success": True, "result": {"mark_price": "2500"}}

    cache = AccountTelemetryCache(Client(), refresh_interval_seconds=1000, stale_after_seconds=1000)
    first = cache.get()
    second = cache.get()
    cache._last_refresh_monotonic = 0
    third = cache.get()

    assert first.request_counts == {"wallet": 1, "positions": 1, "orders": 1, "ticker": 1, "product_spec": 1}
    assert second.request_counts == first.request_counts
    assert second.account_age_seconds is not None
    assert third.request_counts == {"wallet": 2, "positions": 2, "orders": 2, "ticker": 2, "product_spec": 2}


def test_cache_retains_last_known_values_after_endpoint_timeout_and_marks_stale():
    class FlakyClient:
        def __init__(self):
            self.fail_positions = False

        def product_spec(self, symbol):
            return _product()

        def wallet(self):
            return _wallet(equity="123", available="100")

        def positions(self, underlying_asset_symbol="ETH"):
            if self.fail_positions:
                raise TimeoutError("position timeout")
            return {"success": True, "result": [{"product_id": 1699, "size": "10"}]}

        def open_orders(self, product_id=None):
            return {"success": True, "result": []}

        def ticker(self, symbol):
            return {"success": True, "result": {"mark_price": "2500"}}

    client = FlakyClient()
    cache = AccountTelemetryCache(client, refresh_interval_seconds=0, stale_after_seconds=0)
    fresh = cache.get()
    cache._last_syncs["position"] = TelemetrySync("2026-08-27T00:00:00+00:00", 999, True)
    client.fail_positions = True
    stale = cache.get(force=True)

    assert fresh.position_lots == Decimal("10")
    assert stale.position_lots == Decimal("10")
    assert stale.telemetry_status == "STALE"
    assert stale.errors == ["positions: TimeoutError: position timeout"]
    assert stale.position_age_seconds is not None


def test_serialized_state_exposes_normalized_live_state_sections():
    state = _state().as_dict()

    assert set(state["sections"]) >= {"account", "margin", "position", "orders", "inventory", "risk", "telemetry_health"}
    assert state["sections"]["account"]["account_equity"] == "100"
    assert state["sections"]["margin"]["available_margin"] == "80"
    assert state["sections"]["position"]["side"] == "FLAT"
    assert state["sections"]["orders"]["open_order_count"] == 0
    assert state["sections"]["telemetry_health"]["status"] == "HEALTHY"
