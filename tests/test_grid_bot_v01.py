from decimal import Decimal
import time

import pytest

from grid_bot.accounting import ExchangeCost, gross_cycle_pnl, summarize_pnl
from grid_bot.config import REST_URL, TestnetEndpointConfig, validate_testnet_endpoints
from grid_bot.continuous_worker import ContinuousGridBotWorker
from grid_bot.delta_testnet_client import DeltaTestnetClient
from grid_bot.durable_lifecycle import DurableGridBotLifecycle
from grid_bot.engine import DeltaGridBotEngine
from grid_bot.execution import make_client_order_id
from grid_bot.exchange_truth import inventory_from_fills, reconcile_exchange_truth
from grid_bot.grid_builder import build_grid_levels, generate_prices
from grid_bot.models import FillRecord, GridConfig, GridStatus, GridType, ProductSpec, Side, SpacingType
from grid_bot.reconciliation import reconcile_orders
from grid_bot.repository import InMemoryGridRepository
from grid_bot.rest_fallback import RestFallbackPoller, RestFallbackState
from grid_bot.risk import GridRiskController, RiskInputs, RiskState, grid_risk_ratio, inventory_utilisation
from grid_bot.semantics import evaluate_order_semantics, round_price_for_side, validate_post_only_price
from grid_bot.supabase_repository import SupabaseGridRepository, SupabasePersistenceError


def _config(bot_id="bot_a", grid_type=GridType.NEUTRAL, spacing=SpacingType.ARITHMETIC):
    return GridConfig(
        bot_id=bot_id,
        config_version=1,
        bot_name="Test Grid",
        product_symbol="ETHUSD",
        grid_type=grid_type,
        lower_price=Decimal("2800"),
        upper_price=Decimal("3200"),
        grid_count=5,
        spacing_type=spacing,
        lot_size=Decimal("10"),
        max_inventory_lots=Decimal("50"),
        allocated_capital=Decimal("1000"),
        risk_capital=Decimal("500"),
    )


def test_hard_testnet_endpoint_guard_accepts_only_india_testnet():
    validate_testnet_endpoints(TestnetEndpointConfig())
    assert REST_URL == "https://cdn-ind.testnet.deltaex.org"

    bad_urls = [
        "https://api.india.delta.exchange",
        "https://api.delta.exchange",
        "https://testnet-api.delta.exchange",
    ]
    for url in bad_urls:
        with pytest.raises(ValueError):
            validate_testnet_endpoints(TestnetEndpointConfig(rest_url=url))


def test_client_construct_rejects_live_execution_hosts():
    with pytest.raises(ValueError):
        DeltaTestnetClient(endpoints=TestnetEndpointConfig(rest_url="https://api.india.delta.exchange"))


def test_private_post_sends_exact_compact_json_that_was_signed():
    class Response:
        def json(self):
            return {"success": True}

        def raise_for_status(self):
            return None

    class Session:
        def __init__(self):
            self.sent = None

        def post(self, url, headers=None, data=None, timeout=None):
            self.sent = {"url": url, "headers": headers, "data": data, "timeout": timeout}
            return Response()

    session = Session()
    client = DeltaTestnetClient(session=session)
    client.private_post("/orders", {"product_id": 1699, "post_only": False, "size": 1})
    assert session.sent["data"] == '{"product_id":1699,"post_only":false,"size":1}'


def test_websocket_auth_uses_key_auth_not_deprecated_auth():
    client = DeltaTestnetClient()
    payload = client.websocket_auth_payload()
    assert payload["type"] == "key-auth"
    assert payload["type"] != "auth"
    assert "signature" in payload["payload"]


def test_arithmetic_grid_generation_and_sides():
    config = _config()
    prices = generate_prices(config, Decimal("0.1"))
    assert prices == [Decimal("2800.0"), Decimal("2900.0"), Decimal("3000.0"), Decimal("3100.0"), Decimal("3200.0")]
    levels = build_grid_levels(config, Decimal("3000"), Decimal("0.1"))
    assert [level.side for level in levels] == [Side.BUY, Side.BUY, Side.SELL, Side.SELL, Side.SELL]


def test_geometric_grid_generation_is_deterministic_and_unique():
    config = _config(spacing=SpacingType.GEOMETRIC)
    prices = generate_prices(config, Decimal("0.1"))
    assert len(prices) == 5
    assert prices[0] == Decimal("2800.0")
    assert prices[-1] == Decimal("3200.0")
    assert len(set(prices)) == 5


def test_grid_count_is_total_price_levels_for_arithmetic_and_geometric():
    for spacing in [SpacingType.ARITHMETIC, SpacingType.GEOMETRIC]:
        for count in [4, 10, 20, 30]:
            config = GridConfig(**{**_config(spacing=spacing).__dict__, "grid_count": count})
            assert len(generate_prices(config, Decimal("0.05"))) == count


def test_narrow_range_duplicate_levels_are_rejected_after_tick_rounding():
    config = GridConfig(
        **{
            **_config().__dict__,
            "lower_price": Decimal("2500"),
            "upper_price": Decimal("2500.5"),
            "grid_count": 30,
        }
    )
    with pytest.raises(ValueError, match="not unique"):
        generate_prices(config, Decimal("0.1"))


def test_grid_nature_does_not_change_arithmetic_or_geometric_prices():
    for spacing in [SpacingType.ARITHMETIC, SpacingType.GEOMETRIC]:
        neutral = generate_prices(_config(grid_type=GridType.NEUTRAL, spacing=spacing), Decimal("0.05"))
        long = generate_prices(_config(grid_type=GridType.LONG_BIAS, spacing=spacing), Decimal("0.05"))
        short = generate_prices(_config(grid_type=GridType.SHORT_BIAS, spacing=spacing), Decimal("0.05"))
        assert long == neutral
        assert short == neutral


@pytest.mark.parametrize(
    "grid_type,current,side,qty,allowed",
    [
        (GridType.NEUTRAL, "0", Side.BUY, "5", True),
        (GridType.NEUTRAL, "0", Side.SELL, "5", True),
        (GridType.NEUTRAL, "50", Side.BUY, "5", False),
        (GridType.NEUTRAL, "50", Side.SELL, "5", True),
        (GridType.NEUTRAL, "-50", Side.SELL, "5", False),
        (GridType.NEUTRAL, "-50", Side.BUY, "5", True),
        (GridType.NEUTRAL, "45", Side.BUY, "10", False),
        (GridType.LONG_BIAS, "0", Side.BUY, "5", True),
        (GridType.LONG_BIAS, "0", Side.SELL, "5", False),
        (GridType.LONG_BIAS, "5", Side.SELL, "5", True),
        (GridType.LONG_BIAS, "5", Side.SELL, "10", False),
        (GridType.LONG_BIAS, "45", Side.BUY, "5", True),
        (GridType.LONG_BIAS, "45", Side.BUY, "10", False),
        (GridType.LONG_BIAS, "50", Side.BUY, "5", False),
        (GridType.LONG_BIAS, "50", Side.SELL, "5", True),
        (GridType.SHORT_BIAS, "0", Side.SELL, "5", True),
        (GridType.SHORT_BIAS, "0", Side.BUY, "5", False),
        (GridType.SHORT_BIAS, "-5", Side.BUY, "5", True),
        (GridType.SHORT_BIAS, "-5", Side.BUY, "10", False),
        (GridType.SHORT_BIAS, "-45", Side.SELL, "5", True),
        (GridType.SHORT_BIAS, "-45", Side.SELL, "10", False),
        (GridType.SHORT_BIAS, "-50", Side.SELL, "5", False),
        (GridType.SHORT_BIAS, "-50", Side.BUY, "5", True),
    ],
)
def test_nature_specific_projected_inventory_limits(grid_type, current, side, qty, allowed):
    decision = evaluate_order_semantics(grid_type, Decimal(current), Decimal("50"), side, Decimal(qty))
    assert decision.allowed is allowed


def test_outstanding_opening_order_reservation_blocks_only_new_openers():
    long_reserved = [{"side": "buy", "remaining_quantity": "5", "status": "open", "opens_inventory": True} for _ in range(10)]
    assert not evaluate_order_semantics(GridType.LONG_BIAS, Decimal("0"), Decimal("50"), Side.BUY, Decimal("5"), long_reserved).allowed
    assert evaluate_order_semantics(GridType.LONG_BIAS, Decimal("5"), Decimal("50"), Side.SELL, Decimal("5"), long_reserved).allowed

    short_reserved = [{"side": "sell", "remaining_quantity": "5", "status": "open", "opens_inventory": True} for _ in range(10)]
    assert not evaluate_order_semantics(GridType.SHORT_BIAS, Decimal("0"), Decimal("50"), Side.SELL, Decimal("5"), short_reserved).allowed
    assert evaluate_order_semantics(GridType.SHORT_BIAS, Decimal("-5"), Decimal("50"), Side.BUY, Decimal("5"), short_reserved).allowed

    neutral_buys = [{"side": "buy", "remaining_quantity": "5", "status": "open", "opens_inventory": True} for _ in range(10)]
    assert not evaluate_order_semantics(GridType.NEUTRAL, Decimal("0"), Decimal("50"), Side.BUY, Decimal("5"), neutral_buys).allowed
    assert evaluate_order_semantics(GridType.NEUTRAL, Decimal("0"), Decimal("50"), Side.SELL, Decimal("5"), neutral_buys).allowed


def test_side_aware_tick_rounding_and_post_only_guard():
    assert round_price_for_side(Decimal("2500.074"), Decimal("0.05"), Side.BUY) == Decimal("2500.05")
    assert round_price_for_side(Decimal("2500.076"), Decimal("0.05"), Side.BUY) == Decimal("2500.05")
    assert round_price_for_side(Decimal("2500.074"), Decimal("0.05"), Side.SELL) == Decimal("2500.10")
    assert round_price_for_side(Decimal("2500.075"), Decimal("0.05"), Side.SELL) == Decimal("2500.10")
    assert validate_post_only_price(Side.BUY, Decimal("2500.00"), Decimal("2499.95"), Decimal("2500.05")).allowed
    assert not validate_post_only_price(Side.BUY, Decimal("2500.05"), Decimal("2499.95"), Decimal("2500.05")).allowed
    assert validate_post_only_price(Side.SELL, Decimal("2500.05"), Decimal("2499.95"), Decimal("2500.05")).allowed
    assert not validate_post_only_price(Side.SELL, Decimal("2499.95"), Decimal("2499.95"), Decimal("2500.05")).allowed


def test_grid_validation_rejects_invalid_range_and_count():
    with pytest.raises(ValueError):
        generate_prices(_config(), Decimal("0"))
    with pytest.raises(ValueError):
        generate_prices(
            GridConfig(
                **{
                    **_config().__dict__,
                    "lower_price": Decimal("3200"),
                    "upper_price": Decimal("2800"),
                }
            ),
            Decimal("0.1"),
        )


def test_basic_risk_formulas_and_state_transitions():
    assert inventory_utilisation(Decimal("-25"), Decimal("50")) == Decimal("0.5")
    assert grid_risk_ratio(Decimal("250"), Decimal("500")) == Decimal("0.5")
    controller = GridRiskController()
    decision = controller.evaluate(
        RiskInputs(
            net_inventory=Decimal("49"),
            max_inventory=Decimal("50"),
            risk_capital=Decimal("500"),
            projected_adverse_grid_exposure=Decimal("600"),
        )
    )
    assert decision.risk_state == RiskState.RED
    assert not decision.allowed
    assert "GRR_RED" in decision.reason_codes


def test_accounting_uses_delta_reported_costs_without_gst():
    entry = FillRecord("f1", "o1", "r1", Side.BUY, Decimal("3000"), Decimal("10"), "taker", Decimal("1.5"))
    exit = FillRecord("f2", "o2", "r1", Side.SELL, Decimal("3010"), Decimal("10"), "maker", Decimal("0.5"))
    gross = gross_cycle_pnl(entry, exit, Decimal("1"))
    costs = [
        ExchangeCost("r1", None, None, "funding", Decimal("2"), "USD", "credit"),
        ExchangeCost("r1", None, None, "settlement", Decimal("1"), "USD", "debit"),
    ]
    summary = summarize_pnl(gross, [entry, exit], costs, Decimal("60000"))
    assert gross == Decimal("100")
    assert summary.total_exchange_fees == Decimal("2.0")
    assert summary.net_funding == Decimal("2")
    assert summary.other_exchange_costs == Decimal("1")
    assert summary.net_trading_pnl_before_income_tax == Decimal("99.0")
    assert not hasattr(summary, "gst")


def test_reconciliation_detects_missing_and_orphan_orders():
    result = reconcile_orders(
        [{"exchange_order_id": "1"}, {"exchange_order_id": "local-orphan"}],
        [{"id": "1"}, {"id": "2"}],
    )
    assert not result.ok
    assert {"type": "LOCAL_ORDER_MISSING", "exchange_order_id": "2"} in result.mismatches
    assert {"type": "EXCHANGE_ORDER_MISSING", "exchange_order_id": "local-orphan"} in result.mismatches


def test_single_active_grid_guard_and_stopped_allows_next_start():
    repo = InMemoryGridRepository()
    engine = DeltaGridBotEngine(repo)
    bot_a = engine.create_bot(_config("bot_a"))
    bot_b = engine.create_bot(_config("bot_b"))
    first = engine.start(bot_a.bot_id, Decimal("3000"))
    assert first["ok"]
    with pytest.raises(RuntimeError):
        repo.start_run(bot_b.bot_id, Decimal("3000"))
    engine.stop(first["run"]["run_id"])
    second = engine.start(bot_b.bot_id, Decimal("3000"))
    assert second["ok"]


def test_grid_run_summary_is_generated_after_stop_and_immutable():
    repo = InMemoryGridRepository()
    engine = DeltaGridBotEngine(repo)
    bot = engine.create_bot(_config("bot_summary"))
    started = engine.start(bot.bot_id, Decimal("3000"))
    stopped = engine.stop(started["run"]["run_id"], "manual", "test stop")
    summary = stopped["summary"]
    assert summary["immutable"] is True
    assert summary["gridbot_version"] == "0.1"
    assert summary["NET_TRADING_PNL_BEFORE_INCOME_TAX"] == "0"
    assert repo.runs[started["run"]["run_id"]].status == GridStatus.STOPPED
    with pytest.raises(RuntimeError):
        repo.update_summary(started["run"]["run_id"], {"operator_notes": "edit"})


def test_client_order_id_is_unique_and_bounded():
    first = make_client_order_id("run_abcdef1234567890", "L001", Side.BUY, 1)
    second = make_client_order_id("run_abcdef1234567890", "L001", Side.BUY, 2)
    assert first != second
    assert len(first) <= 32


class _FakeClient:
    def __init__(self, fills=None, orders=None, positions=None, margin=None, error=None):
        self._fills = fills or []
        self._orders = orders or []
        self._positions = positions or {"result": []}
        self._margin = margin or {"success": True, "result": {}}
        self.error = error
        self.fills_calls = []

    def fills(self, product_id, start_time=None, page_size=50, **_kwargs):
        self.fills_calls.append({"product_id": product_id, "start_time": start_time, "page_size": page_size})
        if self.error:
            raise self.error
        return {"success": True, "result": self._fills}

    def open_orders(self, product_id=None):
        if self.error:
            raise self.error
        return {"success": True, "result": self._orders}

    def positions(self, underlying_asset_symbol="ETH"):
        if self.error:
            raise self.error
        return self._positions

    def account_margin(self):
        if self.error:
            raise self.error
        return self._margin


def test_rest_fallback_activation_persists_degraded_mode_and_events():
    repo = InMemoryGridRepository()
    poller = RestFallbackPoller(_FakeClient(), repo)
    state = poller.activate("run_1", 1699, "ETHUSD")
    saved = repo.rest_fallback_state["run_1"]
    assert saved["execution_event_mode"] == "REST_FALLBACK"
    assert saved["operational_state"] == "DEGRADED"
    assert saved["private_ws_status"] == "BLOCKED_403"
    assert {event["event_type"] for event in repo.events} >= {"PRIVATE_WS_UNAVAILABLE", "REST_FALLBACK_ENABLED", "EXECUTION_MODE_CHANGED"}


def test_rest_fallback_fill_deduplication_across_overlap_polls():
    fill = {"id": "fill-1", "side": "buy", "size": "10", "created_at": 1_800_000_000_000_000}
    repo = InMemoryGridRepository()
    poller = RestFallbackPoller(_FakeClient(fills=[fill], positions={"result": [{"product_id": 1699, "size": "10"}]}), repo)
    state = RestFallbackState("run_1", 1699, "ETHUSD")
    first = poller.poll_once(state)
    second = poller.poll_once(state)
    assert first["fills"]["processed"] == 1
    assert second["fills"]["processed"] == 0
    assert second["fills"]["duplicates"] == 1
    assert state.local_inventory == Decimal("10")
    assert state.metrics.duplicate_fills_ignored == 1


def test_rest_fallback_lookback_uses_overlap_from_last_confirmed_fill():
    fill = {"id": "fill-1", "side": "buy", "size": "1", "created_at": 1_800_000_000_000_000}
    client = _FakeClient(fills=[fill], positions={"result": [{"product_id": 1699, "size": "1"}]})
    poller = RestFallbackPoller(client, InMemoryGridRepository())
    state = RestFallbackState("run_1", 1699, "ETHUSD")
    poller.poll_once(state)
    poller.poll_once(state)
    assert client.fills_calls[0]["start_time"] is None
    assert client.fills_calls[1]["start_time"] == 1_800_000_000_000_000 - state.fill_lookback_seconds * 1_000_000


def test_rest_fallback_position_mismatch_enters_degraded_reconciliation():
    fill = {"id": "fill-1", "side": "buy", "size": "2", "created_at": 1_800_000_000_000_000}
    client = _FakeClient(fills=[fill], positions={"result": [{"product_id": 1699, "size": "1"}]})
    repo = InMemoryGridRepository()
    poller = RestFallbackPoller(client, repo)
    state = RestFallbackState("run_1", 1699, "ETHUSD")
    result = poller.poll_once(state)
    assert result["position"]["mismatch"] is True
    assert result["state"]["operational_state"] == "DEGRADED_RECONCILIATION"
    assert state.metrics.position_mismatches == 1
    assert any(event["event_type"] == "POSITION_MISMATCH" for event in repo.events)


def test_rest_fallback_reconciles_only_gridbot_orders():
    orders = [
        {"id": 1, "client_order_id": "DGB01-run-L001-B-1"},
        {"id": 2, "client_order_id": "manual-order"},
    ]
    poller = RestFallbackPoller(_FakeClient(orders=orders), InMemoryGridRepository())
    state = RestFallbackState("run_1", 1699, "ETHUSD")
    result = poller.poll_once(state)
    assert result["orders"]["open_exchange_orders"] == 2
    assert result["orders"]["open_gridbot_orders"] == 1


def test_rest_fallback_429_backoff_and_event():
    import requests

    response = requests.Response()
    response.status_code = 429
    error = requests.HTTPError("rate limited", response=response)
    repo = InMemoryGridRepository()
    poller = RestFallbackPoller(_FakeClient(error=error), repo)
    state = RestFallbackState("run_1", 1699, "ETHUSD")
    result = poller.poll_once(state)
    assert result["ok"] is False
    assert state.metrics.rate_limit_429_count == 1
    assert state.metrics.backoff_seconds >= 2
    assert any(event["event_type"] == "REST_RATE_LIMITED" for event in repo.events)


def test_grid_run_summary_includes_rest_fallback_metrics():
    repo = InMemoryGridRepository()
    engine = DeltaGridBotEngine(repo)
    bot = engine.create_bot(_config("bot_rest_summary"))
    started = engine.start(bot.bot_id, Decimal("3000"))
    repo.set_rest_fallback_state(
        started["run"]["run_id"],
        {
            "execution_event_mode": "REST_FALLBACK",
            "private_ws_status": "BLOCKED_403",
            "rest_poll_count": 3,
            "fills_detected_through_REST": 1,
            "duplicate_fills_ignored": 2,
        },
    )
    stopped = engine.stop(started["run"]["run_id"])
    summary = stopped["summary"]
    assert summary["execution_event_mode"] == "REST_FALLBACK"
    assert summary["private_ws_available"] is False
    assert summary["rest_poll_count"] == 3
    assert summary["fills_detected_through_REST"] == 1


class _FakeLifecycleClient:
    def __init__(self):
        self.next_order_id = 100
        self.orders = []
        self.fill_rows = []
        self.cancelled = []

    def product_spec(self, symbol):
        assert symbol == "ETHUSD"
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
            last_price=Decimal("2500"),
            best_bid=Decimal("2499.95"),
            best_ask=Decimal("2500.05"),
        )

    def place_order(self, payload):
        row = {
            "id": str(self.next_order_id),
            "client_order_id": payload["client_order_id"],
            "side": payload["side"],
            "size": payload["size"],
            "unfilled_size": payload["size"],
            "limit_price": payload["limit_price"],
            "state": "open",
        }
        self.next_order_id += 1
        self.orders.append(row)
        return {"success": True, "result": row}

    def cancel_order(self, product_id, order_id):
        self.cancelled.append(str(order_id))
        for row in self.orders:
            if str(row["id"]) == str(order_id):
                row["state"] = "cancelled"
                row["unfilled_size"] = "0"
        return {"success": True, "result": {"id": order_id, "state": "cancelled"}}

    def open_orders(self, product_id=None):
        return {"success": True, "result": [row for row in self.orders if row["state"] == "open"]}

    def order_history(self, product_id=None, start_time=None, end_time=None, after=None, page_size=50):
        return {"success": True, "result": [row for row in self.orders if row["state"] != "open"]}

    def fills(self, product_id, start_time=None, page_size=50, **_kwargs):
        return {"success": True, "result": self.fill_rows}

    def positions(self, underlying_asset_symbol="ETH"):
        return {"success": True, "result": [{"product_id": 1699, "size": "0"}]}

    def account_margin(self):
        return {"success": True, "result": {"portfolio_margin": True}}


def _wait_for(predicate, timeout=3):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    return predicate()


def test_durable_lifecycle_active_run_survives_restart_and_blocks_second_start(tmp_path):
    client = _FakeLifecycleClient()
    path = tmp_path / "grid_state.json"
    lifecycle = DurableGridBotLifecycle(client, path)
    started = lifecycle.start_tiny_grid()
    assert started["run"]["status"] == "RUNNING"
    assert len(started["run"]["orders"]) == 4

    restarted = DurableGridBotLifecycle(client, path)
    assert restarted.status()["active_run_id"] == started["run"]["run_id"]
    with pytest.raises(RuntimeError):
        restarted.start_tiny_grid()


def test_durable_lifecycle_fill_dedupe_places_one_replacement_for_partial_fill(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "grid_state.json")
    started = lifecycle.start_tiny_grid()
    first_order = next(iter(started["run"]["orders"].values()))
    client.fill_rows = [
        {
            "id": "fill-1",
            "order_id": first_order["exchange_order_id"],
            "client_order_id": first_order["client_order_id"],
            "side": first_order["side"],
            "price": first_order["price"],
            "size": "0.5",
            "commission": "0.01",
        }
    ]
    first = lifecycle.reconcile(started["run"]["run_id"])
    second = lifecycle.reconcile(started["run"]["run_id"])
    replacement_orders = [order for order in first["run"]["orders"].values() if order["order_kind"] == "replacement"]
    deferred_replacements = [order for order in first["run"].get("deferred_orders", {}).values() if order["order_kind"] == "replacement"]
    assert first["new_fills"] == 1
    assert second["new_fills"] == 0
    assert replacement_orders == []
    assert deferred_replacements == []


def test_durable_lifecycle_pause_resume_regrid_stop_summary(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "grid_state.json")
    run_id = lifecycle.start_tiny_grid()["run"]["run_id"]

    paused = lifecycle.pause(run_id)
    assert paused["run"]["status"] == "PAUSED"
    assert client.cancelled

    resumed = lifecycle.resume(run_id)
    assert resumed["run"]["status"] == "RUNNING"

    regridded = lifecycle.regrid(run_id)
    assert regridded["run"]["status"] == "RUNNING"
    assert int(regridded["run"]["config"]["config_version"]) == 2

    stopped = lifecycle.stop(run_id, "unit_test")
    summary = stopped["summary"]
    assert stopped["run"]["status"] == "STOPPED"
    assert summary["immutable"] is True
    assert summary["stray_gridbot_orders"] == 0
    assert DurableGridBotLifecycle(client, lifecycle.state_path).status()["active_run_id"] is None


def test_operator_preview_derives_reference_tick_and_account_risk(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "grid_state.json", use_supabase=False)
    preview = lifecycle.preview_operator_grid(
        {
            "bot_name": "Operator Grid",
            "product_symbol": "ETHUSD",
            "grid_type": "neutral",
            "lower_price": "2400.01",
            "upper_price": "2500.01",
            "grid_count": 4,
            "spacing_type": "arithmetic",
            "lot_size": "1",
            "max_inventory_lots": "2",
        }
    )
    assert preview["preview"]["reference_price"] == "2500.00"
    assert preview["product"]["tick_size"] == "0.05"
    assert preview["config"]["lower_price"] == "2400.00"
    assert preview["risk"]["version"] == "gridbot_v01_account_health_grr_v1"
    assert preview["risk"]["formula"] == "projected_grid_exposure / account_equity"
    assert "allocated_capital" in preview["config"]


def test_bias_initial_ladder_uses_nature_specific_opening_orders(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "grid_state.json", use_supabase=False)
    base = {
        "bot_name": "Operator Grid",
        "product_symbol": "ETHUSD",
        "lower_price": "2400",
        "upper_price": "2600",
        "grid_count": 30,
        "spacing_type": "arithmetic",
        "lot_size": "5",
        "max_inventory_lots": "50",
    }

    neutral = lifecycle.preview_operator_grid({**base, "grid_type": "neutral"})["preview"]
    long = lifecycle.preview_operator_grid({**base, "grid_type": "long_bias"})["preview"]
    short = lifecycle.preview_operator_grid({**base, "grid_type": "short_bias"})["preview"]

    assert neutral["total_grid_levels"] == 30
    assert neutral["opening_buy_orders_eligible"] == 10
    assert neutral["opening_sell_orders_eligible"] == 10
    assert long["opening_buy_orders_eligible"] == 10
    assert long["opening_sell_orders_eligible"] == 0
    assert short["opening_buy_orders_eligible"] == 0
    assert short["opening_sell_orders_eligible"] == 10
    assert len(long["deferred_levels"]) == 20
    assert len(short["deferred_levels"]) == 20


def test_durable_start_defers_inventory_capped_orders_without_placing_them(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "grid_state.json", use_supabase=False)
    started = lifecycle.start_operator_grid(
        {
            "bot_name": "Operator Grid",
            "product_symbol": "ETHUSD",
            "grid_type": "long_bias",
            "lower_price": "2400",
            "upper_price": "2600",
            "grid_count": 30,
            "spacing_type": "arithmetic",
            "lot_size": "5",
            "max_inventory_lots": "50",
        }
    )

    assert len(started["run"]["levels"]) == 30
    assert len(started["run"]["orders"]) == 10
    assert len(client.orders) == 10
    assert all(order["side"] == "buy" for order in started["run"]["orders"].values())
    assert len(started["run"]["deferred_orders"]) == 20


def test_operator_start_uses_supplied_grid_without_manual_capital_fields(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "grid_state.json", use_supabase=False)
    started = lifecycle.start_operator_grid(
        {
            "bot_name": "Operator Grid",
            "product_symbol": "ETHUSD",
            "grid_type": "neutral",
            "lower_price": "2400",
            "upper_price": "2600",
            "grid_count": 4,
            "spacing_type": "arithmetic",
            "lot_size": "1",
            "max_inventory_lots": "2",
        }
    )
    assert started["ok"] is True
    assert started["run"]["status"] == "RUNNING"
    assert len(started["run"]["levels"]) == 4
    assert len(started["run"]["orders"]) == 4
    assert all(order["config_version"] == 1 for order in started["run"]["orders"].values())


def test_operator_background_start_returns_starting_then_reaches_running(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "grid_state.json", use_supabase=False)
    started = lifecycle.start_operator_grid_background(
        {
            "bot_name": "Operator Grid",
            "product_symbol": "ETHUSD",
            "grid_type": "neutral",
            "lower_price": "2400",
            "upper_price": "2600",
            "grid_count": 4,
            "spacing_type": "arithmetic",
            "lot_size": "1",
            "max_inventory_lots": "2",
        }
    )
    assert started["run"]["status"] == "STARTING"
    assert started["start_stage"] == "PERSISTING"

    def running_status():
        active_run = DurableGridBotLifecycle(client, lifecycle.state_path, use_supabase=False).status()["active_run"] or {}
        return active_run if active_run.get("status") == "RUNNING" else None

    status = _wait_for(running_status)
    assert status["status"] == "RUNNING"
    assert status["startup"]["orders_expected"] == 4
    assert status["startup"]["orders_submitted"] == 4


def test_operator_grid_start_does_not_resurrect_run_when_stop_races_placement(tmp_path):
    class StopDuringPlacementClient(_FakeLifecycleClient):
        def __init__(self):
            super().__init__()
            self.on_place = None
            self._stopped_once = False

        def place_order(self, payload):
            if self.on_place and not self._stopped_once:
                self._stopped_once = True
                self.on_place()
            return super().place_order(payload)

    client = StopDuringPlacementClient()
    path = tmp_path / "state.json"
    lifecycle = DurableGridBotLifecycle(client, path, use_supabase=False)
    begun = lifecycle.begin_operator_grid_start(
        {
            "bot_name": "Operator Grid",
            "product_symbol": "ETHUSD",
            "grid_type": "neutral",
            "lower_price": "2400",
            "upper_price": "2600",
            "grid_count": 4,
            "spacing_type": "arithmetic",
            "lot_size": "1",
            "max_inventory_lots": "2",
        }
    )
    run_id = begun["run"]["run_id"]
    client.on_place = lambda: DurableGridBotLifecycle(client, path, use_supabase=False).stop(run_id, "race_stop")

    result = lifecycle.complete_operator_grid_start(run_id)
    final_status = DurableGridBotLifecycle(client, path, use_supabase=False).status()

    assert result["run"]["status"] == GridStatus.STOPPED.value
    assert final_status["active_run_id"] is None
    assert client.open_orders(1699)["result"] == []
    assert client.cancelled == ["100"]


def test_duplicate_background_start_attaches_to_starting_run(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "grid_state.json", use_supabase=False)
    payload = {
        "bot_name": "Operator Grid",
        "product_symbol": "ETHUSD",
        "grid_type": "neutral",
        "lower_price": "2400",
        "upper_price": "2600",
        "grid_count": 4,
        "spacing_type": "arithmetic",
        "lot_size": "1",
        "max_inventory_lots": "2",
    }
    first = lifecycle.begin_operator_grid_start(payload)
    second = lifecycle.start_operator_grid_background(payload)
    assert second["attached"] is True
    assert second["run"]["run_id"] == first["run"]["run_id"]
    lifecycle.complete_operator_grid_start(first["run"]["run_id"])
    third = lifecycle.start_operator_grid_background(payload)
    assert third["attached"] is True
    assert third["run"]["run_id"] == first["run"]["run_id"]
    assert len(client.orders) == 4


def test_background_start_failure_persists_start_failed(tmp_path):
    class FailingClient(_FakeLifecycleClient):
        def place_order(self, payload):
            raise RuntimeError("exchange unavailable")

    client = FailingClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "grid_state.json", use_supabase=False)
    begun = lifecycle.begin_operator_grid_start(
        {
            "bot_name": "Operator Grid",
            "product_symbol": "ETHUSD",
            "grid_type": "neutral",
            "lower_price": "2400",
            "upper_price": "2600",
            "grid_count": 4,
            "spacing_type": "arithmetic",
            "lot_size": "1",
            "max_inventory_lots": "2",
        }
    )
    with pytest.raises(RuntimeError):
        lifecycle.complete_operator_grid_start(begun["run"]["run_id"])
    status = DurableGridBotLifecycle(client, lifecycle.state_path, use_supabase=False).status()
    failed = status["runs"][0]
    assert failed["status"] == "START_FAILED"
    assert status["active_run_id"] is None
    assert failed["startup"]["start_stage"] == "START_FAILED"


def test_manual_exchange_cancellation_reconciliation_marks_orders_terminal(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "grid_state.json", use_supabase=False)
    run_id = lifecycle.start_tiny_grid()["run"]["run_id"]
    for row in client.orders:
        row["state"] = "cancelled"
        row["unfilled_size"] = "0"

    reconciled = lifecycle.reconcile(run_id)
    assert reconciled["open_gridbot_orders"] == 0
    assert {order["status"] for order in reconciled["run"]["orders"].values()} == {"manual_cancelled"}
    stopped = lifecycle.stop(run_id, "manual_exchange_cancel_reconciled")
    assert stopped["summary"]["stray_gridbot_orders"] == 0


def _truth_run(grid_type="neutral", quantity="5", status="open"):
    return {
        "run_id": "run_truth",
        "bot_id": "bot_truth",
        "config": {
            "bot_id": "bot_truth",
            "config_version": 3,
            "grid_type": grid_type,
            "product_symbol": "ETHUSD",
            "max_inventory_lots": "50",
        },
        "product": {"product_id": 1699, "symbol": "ETHUSD", "contract_multiplier": "1"},
        "orders": {
            "DGB01-truth-L001-B-1": {
                "order_key": "DGB01-truth-L001-B-1",
                "run_id": "run_truth",
                "level_id": "L001",
                "side": "buy",
                "price": "2400",
                "requested_quantity": quantity,
                "filled_quantity": "0",
                "remaining_quantity": quantity,
                "client_order_id": "DGB01-truth-L001-B-1",
                "exchange_order_id": "ex-1",
                "status": status,
                "order_kind": "initial_grid",
                "config_version": 3,
            }
        },
        "fills": {},
        "risk_snapshots": [],
    }


class _TruthClient:
    def __init__(self, open_orders=None, order_pages=None, fill_pages=None, position="0"):
        self._open_orders = open_orders or []
        self._order_pages = order_pages if order_pages is not None else [[]]
        self._fill_pages = fill_pages if fill_pages is not None else [[]]
        self._position = position

    def open_orders(self, product_id=None):
        return {"success": True, "result": self._open_orders}

    def order_history(self, product_id=None, start_time=None, end_time=None, after=None, page_size=50):
        index = int(after or 0)
        next_after = str(index + 1) if index + 1 < len(self._order_pages) else None
        return {"success": True, "result": self._order_pages[index], "meta": {"after": next_after}}

    def fills(self, product_id=None, start_time=None, end_time=None, after=None, page_size=50):
        index = int(after or 0)
        next_after = str(index + 1) if index + 1 < len(self._fill_pages) else None
        return {"success": True, "result": self._fill_pages[index], "meta": {"after": next_after}}

    def positions(self, underlying_asset_symbol="ETH"):
        return {"success": True, "result": [{"product_id": 1699, "symbol": "ETHUSD", "size": self._position}]}


def _open_exchange_order(unfilled="5", state="open"):
    return {
        "id": "ex-1",
        "client_order_id": "DGB01-truth-L001-B-1",
        "side": "buy",
        "size": "5",
        "unfilled_size": unfilled,
        "limit_price": "2400",
        "state": state,
    }


def _fill(fill_id="fill-1", size="5", side="buy", price="2400", order_id="ex-1"):
    return {
        "id": fill_id,
        "order_id": order_id,
        "client_order_id": "DGB01-truth-L001-B-1",
        "side": side,
        "price": price,
        "size": size,
        "created_at": "1800000000000000",
        "commission": "0",
    }


def _replacement_run(grid_type="neutral", source_level="L002", source_side="buy", fill_id="fill-1", fill_size="1"):
    client_order_id = f"DGB01-truth-{source_level}-{source_side[0].upper()}-1"
    exchange_order_id = f"ex-{source_level}"
    return {
        "run_id": "run_truth",
        "bot_id": "bot_truth",
        "status": "RUNNING",
        "config": {
            "bot_id": "bot_truth",
            "config_version": 3,
            "grid_type": grid_type,
            "product_symbol": "ETHUSD",
            "lower_price": "2490",
            "upper_price": "2510",
            "max_inventory_lots": "2",
        },
        "product": {"product_id": 1699, "symbol": "ETHUSD", "contract_multiplier": "1"},
        "levels": [
            {"level_id": "L001", "index": 1, "side": "buy", "price": "2490", "quantity": "1", "state": "active"},
            {"level_id": "L002", "index": 2, "side": "buy", "price": "2495", "quantity": "1", "state": "active"},
            {"level_id": "L003", "index": 3, "side": "sell", "price": "2505", "quantity": "1", "state": "active"},
            {"level_id": "L004", "index": 4, "side": "sell", "price": "2510", "quantity": "1", "state": "active"},
        ],
        "orders": {
            client_order_id: {
                "order_key": client_order_id,
                "run_id": "run_truth",
                "level_id": source_level,
                "side": source_side,
                "price": "2495" if source_level == "L002" else "2505",
                "requested_quantity": fill_size,
                "filled_quantity": fill_size,
                "remaining_quantity": "0",
                "client_order_id": client_order_id,
                "exchange_order_id": exchange_order_id,
                "status": "filled",
                "order_kind": "initial_grid",
                "config_version": 3,
                "opens_inventory": True,
            }
        },
        "fills": {
            fill_id: {
                "id": fill_id,
                "order_id": exchange_order_id,
                "client_order_id": client_order_id,
                "side": source_side,
                "price": "2495" if source_side == "buy" else "2505",
                "size": fill_size,
                "commission": "0",
            }
        },
        "deferred_orders": {},
        "replacement_keys": {},
        "risk_snapshots": [],
        "sequence": 2,
    }


def test_exchange_truth_open_order_remains_open():
    run = _truth_run()
    result = reconcile_exchange_truth(run, _TruthClient(open_orders=[_open_exchange_order()], position="0"))
    assert result["new_fills"] == 0
    assert run["orders"]["DGB01-truth-L001-B-1"]["status"] == "open"
    assert result["unresolved_orders"] == 0


def test_exchange_truth_full_fill_is_persisted_once_with_config_attribution():
    run = _truth_run()
    client = _TruthClient(order_pages=[[{"id": "ex-1", "client_order_id": "DGB01-truth-L001-B-1", "state": "filled"}]], fill_pages=[[_fill()]], position="5")
    first = reconcile_exchange_truth(run, client)
    second = reconcile_exchange_truth(run, client)
    third = reconcile_exchange_truth(run, client)
    assert first["new_fills"] == 1
    assert second["new_fills"] == 0
    assert third["new_fills"] == 0
    assert len(run["fills"]) == 1
    assert run["orders"]["DGB01-truth-L001-B-1"]["status"] == "filled"
    assert run["orders"]["DGB01-truth-L001-B-1"]["remaining_quantity"] == "0"
    assert first["gridbot_inventory"] == "5"
    assert first["position_mismatches"] == 0


def test_exchange_truth_partial_and_multiple_partial_fills():
    run = _truth_run(quantity="10")
    client = _TruthClient(open_orders=[_open_exchange_order(unfilled="5")], fill_pages=[[_fill("fill-1", "3"), _fill("fill-2", "2")]], position="5")
    result = reconcile_exchange_truth(run, client)
    assert result["new_fills"] == 2
    assert result["partial_fills"] == 1
    assert run["orders"]["DGB01-truth-L001-B-1"]["status"] == "partially_filled"
    assert run["orders"]["DGB01-truth-L001-B-1"]["filled_quantity"] == "5"
    assert run["orders"]["DGB01-truth-L001-B-1"]["remaining_quantity"] == "5"


def test_exchange_truth_partial_then_cancel_preserves_fill():
    run = _truth_run(quantity="10")
    history = [{"id": "ex-1", "client_order_id": "DGB01-truth-L001-B-1", "state": "cancelled"}]
    result = reconcile_exchange_truth(run, _TruthClient(order_pages=[history], fill_pages=[[_fill("fill-1", "4")]], position="4"))
    assert result["new_fills"] == 1
    assert run["orders"]["DGB01-truth-L001-B-1"]["status"] == "cancelled"
    assert run["orders"]["DGB01-truth-L001-B-1"]["filled_quantity"] == "4"
    assert run["orders"]["DGB01-truth-L001-B-1"]["remaining_quantity"] == "6"


def test_exchange_truth_manual_cancel_no_fill_and_disappeared_unresolved():
    run = _truth_run()
    history = [{"id": "ex-1", "client_order_id": "DGB01-truth-L001-B-1", "state": "cancelled"}]
    cancelled = reconcile_exchange_truth(run, _TruthClient(order_pages=[history], position="0"))
    assert cancelled["new_fills"] == 0
    assert run["orders"]["DGB01-truth-L001-B-1"]["status"] == "manual_cancelled"

    unresolved_run = _truth_run()
    unresolved = reconcile_exchange_truth(unresolved_run, _TruthClient(position="0"))
    assert unresolved_run["orders"]["DGB01-truth-L001-B-1"]["status"] == "unresolved"
    assert unresolved["manual_cancelled_orders"] == 0
    assert unresolved["unresolved_orders"] == 1


def test_exchange_truth_position_and_fill_ledger_mismatches():
    run = _truth_run()
    position_mismatch = reconcile_exchange_truth(run, _TruthClient(fill_pages=[[_fill()]], position="10"))
    assert position_mismatch["position_mismatches"] == 1
    assert any(event["event_type"] == "POSITION_MISMATCH" for event in position_mismatch["events"])

    bad_run = _truth_run()
    bad = reconcile_exchange_truth(bad_run, _TruthClient(fill_pages=[[_fill("fill-big", "10")]], position="10"))
    assert bad["fill_ledger_mismatches"] == 1
    assert any(event["event_type"] == "FILL_LEDGER_MISMATCH" for event in bad["events"])


def test_exchange_truth_grid_nature_sign_violations():
    long_run = _truth_run(grid_type="long_bias")
    long_result = reconcile_exchange_truth(long_run, _TruthClient(fill_pages=[[_fill(side="sell")]], position="-5"))
    assert any(event["event_type"] == "GRID_NATURE_INVENTORY_VIOLATION" for event in long_result["events"])

    short_run = _truth_run(grid_type="short_bias")
    short_result = reconcile_exchange_truth(short_run, _TruthClient(fill_pages=[[_fill(side="buy")]], position="5"))
    assert any(event["event_type"] == "GRID_NATURE_INVENTORY_VIOLATION" for event in short_result["events"])


def test_exchange_truth_discovers_fill_on_later_page_once():
    run = _truth_run()
    client = _TruthClient(fill_pages=[[], [_fill()]], order_pages=[[], [{"id": "ex-1", "client_order_id": "DGB01-truth-L001-B-1", "state": "filled"}]], position="5")
    result = reconcile_exchange_truth(run, client)
    assert result["new_fills"] == 1
    assert len(run["fills"]) == 1


def test_inventory_from_fills_uses_signed_fill_ledger():
    assert inventory_from_fills([_fill("f1", "5", "buy"), _fill("f2", "5", "buy"), _fill("f3", "5", "sell")]) == Decimal("5")


def test_deferred_replacement_without_exchange_id_does_not_become_unresolved():
    run = _replacement_run(source_level="L002", source_side="buy", fill_id="fill-deferred")
    deferred = {
        "order_key": "DGB01-truth-L003-S-Rdeferred",
        "run_id": run["run_id"],
        "level_id": "L003",
        "side": "sell",
        "price": "2505",
        "requested_quantity": "1",
        "filled_quantity": "0",
        "remaining_quantity": "0",
        "client_order_id": "DGB01-truth-L003-S-Rdeferred",
        "exchange_order_id": "",
        "status": "deferred",
        "order_kind": "replacement",
        "source_fill_id": "fill-deferred",
    }
    run["orders"][deferred["client_order_id"]] = deferred
    run["deferred_orders"][deferred["client_order_id"]] = deferred

    result = reconcile_exchange_truth(run, _TruthClient(position="1"))

    assert result["unresolved_orders"] == 0
    assert not [event for event in result["events"] if event["type"] == "ORDER_UNRESOLVED"]
    assert run["orders"][deferred["client_order_id"]]["status"] == "deferred"


def test_replacement_semantics_neutral_buy_to_adjacent_sell_and_idempotent(tmp_path):
    client = _FakeLifecycleClient()
    run = _replacement_run(source_level="L002", source_side="buy", fill_id="fill-buy")
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "state.json", use_supabase=False)

    first = lifecycle.process_replacements(run, {"gridbot_inventory": "1"})
    second = lifecycle.process_replacements(run, {"gridbot_inventory": "1"})
    replacements = [order for order in run["orders"].values() if order.get("order_kind") == "replacement"]

    assert first["created"] == 1
    assert second["existing"] == 1
    assert len(replacements) == 1
    assert replacements[0]["level_id"] == "L003"
    assert replacements[0]["side"] == "sell"
    assert replacements[0]["requested_quantity"] == "1"
    assert replacements[0]["source_fill_id"] == "fill-buy"


def test_replacement_semantics_neutral_sell_to_adjacent_buy(tmp_path):
    client = _FakeLifecycleClient()
    run = _replacement_run(source_level="L003", source_side="sell", fill_id="fill-sell")
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "state.json", use_supabase=False)

    result = lifecycle.process_replacements(run, {"gridbot_inventory": "-1"})
    replacement = [order for order in run["orders"].values() if order.get("order_kind") == "replacement"][0]

    assert result["created"] == 1
    assert replacement["level_id"] == "L002"
    assert replacement["side"] == "buy"
    assert replacement["source_fill_id"] == "fill-sell"


def test_deferred_replacement_submits_when_post_only_becomes_eligible(tmp_path):
    class MovingBookClient(_FakeLifecycleClient):
        def __init__(self):
            super().__init__()
            self.best_bid = Decimal("2505")

        def product_spec(self, symbol):
            spec = super().product_spec(symbol)
            return ProductSpec(**{**spec.__dict__, "best_bid": self.best_bid})

    client = MovingBookClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "state.json", use_supabase=False)
    run = _replacement_run(source_level="L002", source_side="buy", fill_id="fill-retry")

    deferred = lifecycle.process_replacements(run, {"gridbot_inventory": "1"})
    client.best_bid = Decimal("2499.95")
    submitted = lifecycle.process_replacements(run, {"gridbot_inventory": "1"})
    replacement = [order for order in run["orders"].values() if order.get("order_kind") == "replacement"][0]

    assert deferred["deferred"] == 1
    assert submitted["created"] == 1
    assert replacement["status"] == "open"
    assert replacement["exchange_order_id"]
    assert replacement["client_order_id"] == client.orders[0]["client_order_id"]
    assert run["deferred_orders"] == {}


def test_stop_terminalizes_never_submitted_deferred_replacement_without_unresolved_noise(tmp_path):
    class CrossingBookClient(_FakeLifecycleClient):
        def product_spec(self, symbol):
            spec = super().product_spec(symbol)
            return ProductSpec(**{**spec.__dict__, "best_bid": Decimal("2505")})

    client = CrossingBookClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "state.json", use_supabase=False)
    run = _replacement_run(source_level="L002", source_side="buy", fill_id="fill-stop")
    result = lifecycle.process_replacements(run, {"gridbot_inventory": "1"})
    deferred_id = next(iter(run["deferred_orders"]))
    state = {"runs": {run["run_id"]: run}, "active_run_id": run["run_id"], "events": []}
    lifecycle._save(state)

    stopped = lifecycle.stop(run["run_id"], "test_stop_deferred")
    order = stopped["run"]["orders"][deferred_id]

    assert result["deferred"] == 1
    assert order["status"] == "abandoned_by_stop"
    assert order["exchange_order_id"] == ""
    assert not [event for event in stopped["run"].get("events", []) if event.get("event_type") == "ORDER_UNRESOLVED"]
    assert client.cancelled == []


def test_terminalized_deferred_replacement_does_not_reserve_inventory(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "state.json", use_supabase=False)
    run = _replacement_run(grid_type="long_bias", source_level="L003", source_side="sell", fill_id="fill-terminal")
    order = {
        "client_order_id": "deferred-buy",
        "order_key": "deferred-buy",
        "level_id": "L002",
        "side": "buy",
        "requested_quantity": "1",
        "remaining_quantity": "0",
        "exchange_order_id": "",
        "status": "abandoned_by_stop",
        "opens_inventory": True,
    }
    run["orders"][order["client_order_id"]] = order

    semantic = evaluate_order_semantics(
        GridType.LONG_BIAS,
        Decimal("0"),
        Decimal("1"),
        Side.BUY,
        Decimal("1"),
        lifecycle._open_order_records(run),
    )

    assert lifecycle._open_order_records(run) == []
    assert semantic.allowed
    assert semantic.projected_inventory == Decimal("1")


def test_long_and_short_replacements_are_risk_reducing_and_restore_opening_opportunity(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "state.json", use_supabase=False)
    long_run = _replacement_run(grid_type="long_bias", source_level="L002", source_side="buy", fill_id="long-entry")
    short_run = _replacement_run(grid_type="short_bias", source_level="L003", source_side="sell", fill_id="short-entry")

    long_result = lifecycle.process_replacements(long_run, {"gridbot_inventory": "1"})
    short_result = lifecycle.process_replacements(short_run, {"gridbot_inventory": "-1"})

    long_replacement = [order for order in long_run["orders"].values() if order.get("order_kind") == "replacement"][0]
    short_replacement = [order for order in short_run["orders"].values() if order.get("order_kind") == "replacement"][0]
    assert long_result["created"] == 1
    assert long_replacement["side"] == "sell"
    assert long_replacement["opens_inventory"] is False
    assert short_result["created"] == 1
    assert short_replacement["side"] == "buy"
    assert short_replacement["opens_inventory"] is False

    long_run["fills"]["long-close"] = {
        "id": "long-close",
        "order_id": long_replacement["exchange_order_id"],
        "client_order_id": long_replacement["client_order_id"],
        "side": "sell",
        "price": long_replacement["price"],
        "size": "1",
        "commission": "0",
    }
    long_replacement["status"] = "filled"
    long_replacement["filled_quantity"] = "1"
    long_replacement["remaining_quantity"] = "0"
    long_cycle = lifecycle.process_replacements(long_run, {"gridbot_inventory": "0"})
    restored = [order for order in long_run["orders"].values() if order.get("source_fill_id") == "long-close"][0]
    assert long_cycle["created"] == 1
    assert restored["side"] == "buy"
    assert restored["level_id"] == "L002"
    assert restored["opens_inventory"] is True


def test_boundary_fill_has_no_out_of_range_replacement(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "state.json", use_supabase=False)
    buy_at_top = _replacement_run(source_level="L004", source_side="buy", fill_id="edge-buy")
    sell_at_bottom = _replacement_run(source_level="L001", source_side="sell", fill_id="edge-sell")

    assert lifecycle.process_replacements(buy_at_top, {"gridbot_inventory": "1"})["skipped"] == 1
    assert lifecycle.process_replacements(sell_at_bottom, {"gridbot_inventory": "-1"})["skipped"] == 1
    assert buy_at_top["replacement_keys"]["edge-buy:replacement"]["reason"] == "edge_level"
    assert sell_at_bottom["replacement_keys"]["edge-sell:replacement"]["reason"] == "edge_level"


def test_inventory_cap_blocks_risk_increasing_but_allows_risk_reducing_replacement(tmp_path):
    client = _FakeLifecycleClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "state.json", use_supabase=False)
    blocked = _replacement_run(grid_type="long_bias", source_level="L002", source_side="buy", fill_id="bad-short")
    allowed = _replacement_run(grid_type="long_bias", source_level="L002", source_side="buy", fill_id="good-close")

    blocked_result = lifecycle.process_replacements(blocked, {"gridbot_inventory": "0"})
    allowed_result = lifecycle.process_replacements(allowed, {"gridbot_inventory": "1"})

    assert blocked_result["deferred"] == 1
    assert "LONG_BIAS_CANNOT_OPEN_NET_SHORT" in next(iter(blocked["deferred_orders"].values()))["rejection_reason"]
    assert allowed_result["created"] == 1
    assert [order for order in allowed["orders"].values() if order.get("order_kind") == "replacement"][0]["side"] == "sell"


def test_ambiguous_replacement_submission_recovers_existing_exchange_order(tmp_path):
    class RecoveringClient(_FakeLifecycleClient):
        def place_order(self, payload):
            raise AssertionError("duplicate submission should not occur")

    client = RecoveringClient()
    client.orders.append(
        {
            "id": "accepted-before-crash",
            "client_order_id": "DGB01-un_truth-L003-S-Rillcrash",
            "side": "sell",
            "size": "1",
            "unfilled_size": "1",
            "limit_price": "2505",
            "state": "open",
        }
    )
    run = _replacement_run(source_level="L002", source_side="buy", fill_id="fill-crash")
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "state.json", use_supabase=False)

    result = lifecycle.process_replacements(run, {"gridbot_inventory": "1"})
    replacement = [order for order in run["orders"].values() if order.get("order_kind") == "replacement"][0]

    assert result["created"] == 1
    assert replacement["exchange_order_id"] == "accepted-before-crash"
    assert replacement["client_order_id"] == "DGB01-un_truth-L003-S-Rillcrash"


def test_out_of_range_blocks_opening_replenishment_but_allows_closing(tmp_path):
    class OutOfRangeClient(_FakeLifecycleClient):
        def product_spec(self, symbol):
            spec = super().product_spec(symbol)
            return ProductSpec(**{**spec.__dict__, "mark_price": Decimal("2605")})

    client = OutOfRangeClient()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "state.json", use_supabase=False)
    opening = _replacement_run(grid_type="long_bias", source_level="L003", source_side="sell", fill_id="cycle-close")
    closing = _replacement_run(grid_type="long_bias", source_level="L002", source_side="buy", fill_id="entry-fill")

    opening_result = lifecycle.process_replacements(opening, {"gridbot_inventory": "0"})
    closing_result = lifecycle.process_replacements(closing, {"gridbot_inventory": "1"})

    assert opening_result["deferred"] == 1
    assert "MARKET_OUTSIDE_CONFIGURED_GRID_RANGE" in next(iter(opening["deferred_orders"].values()))["rejection_reason"]
    assert closing_result["created"] == 1


class _CountingSupabaseGridRepository(SupabaseGridRepository):
    def __init__(self):
        self.enabled = True
        self.tables = {}
        self.request_counts = {
            "select": 0,
            "upsert_rows": 0,
            "insert_once": 0,
            "patch": 0,
            "delete": 0,
            "by_table": {},
        }
        self.write_counts = {"upsert": 0, "insert_once": 0, "patch": 0}
        self.read_count = 0

    def select(self, table, params=None):
        self._count("select", table)
        self.read_count += 1
        rows = list(self.tables.get(table, {}).values())
        params = params or {}
        for key, value in params.items():
            if key in {"select", "order", "limit"}:
                continue
            if isinstance(value, str) and value.startswith("eq."):
                expected = value[3:]
                rows = [row for row in rows if str(row.get(key)) == expected]
            elif isinstance(value, str) and value.startswith("in."):
                allowed = set(value[3:].strip("()").split(","))
                rows = [row for row in rows if str(row.get(key)) in allowed]
        if "order" in params:
            key = params["order"].split(".")[0]
            reverse = params["order"].endswith(".desc")
            rows = sorted(rows, key=lambda row: str(row.get(key) or ""), reverse=reverse)
        if "limit" in params:
            rows = rows[: int(params["limit"])]
        return rows

    def upsert(self, table, payload, on_conflict=None):
        self._count("upsert_rows", table, len(payload if isinstance(payload, list) else [payload]))
        self.write_counts["upsert"] += len(payload if isinstance(payload, list) else [payload])
        rows = payload if isinstance(payload, list) else [payload]
        self.tables.setdefault(table, {})
        for row in rows:
            if on_conflict:
                key = tuple(str(row.get(col)) for col in on_conflict.split(","))
            else:
                key = (str(row.get("id") or row.get(f"{table[:-1]}_id") or len(self.tables[table])),)
            self.tables[table][key] = {**self.tables[table].get(key, {}), **row}

    def insert_once(self, table, payload, on_conflict=None):
        self._count("insert_once", table)
        if on_conflict:
            key = tuple(str(payload.get(col)) for col in on_conflict.split(","))
        else:
            key = (str(payload.get("id") or payload.get(f"{table[:-1]}_id") or len(self.tables.get(table, {}))),)
        self.tables.setdefault(table, {})
        inserted = key not in self.tables[table]
        if inserted:
            self.tables[table][key] = dict(payload)
            self.write_counts["insert_once"] += 1
        return inserted

    def insert_once_with_optional_config_version(self, table, payload, on_conflict=None):
        return self.insert_once(table, payload, on_conflict)

    def patch(self, table, filters, payload):
        self._count("patch", table)
        self.write_counts["patch"] += 1
        for key, row in self.tables.get(table, {}).items():
            if all(str(row.get(name)) == str(value) for name, value in filters.items()):
                self.tables[table][key] = {**row, **payload}

    def _request(self, method, table, *, params=None, json=None, prefer=None):
        if method == "DELETE":
            return None
        raise AssertionError(method)


def test_continuous_worker_no_change_polls_do_not_write_per_loop(tmp_path):
    client = _FakeLifecycleClient()
    db = _CountingSupabaseGridRepository()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "state.json", db=db, use_supabase=True)
    started = lifecycle.start_tiny_grid()
    worker = ContinuousGridBotWorker(client=client, db=db, poll_interval_seconds=0.01, snapshot_interval_seconds=3600)
    worker._run = started["run"]
    before = dict(db.write_counts)

    for _ in range(5):
        worker._poll_once(worker._run)

    after = db.write_counts
    assert after["insert_once"] == before["insert_once"] + 2
    assert after["upsert"] == before["upsert"]
    assert after["patch"] == before["patch"]


def test_continuous_worker_default_snapshot_cadence_is_five_minutes():
    worker = ContinuousGridBotWorker(client=_FakeLifecycleClient(), db=_CountingSupabaseGridRepository())
    assert worker.state()["snapshot_interval_seconds"] == 300.0
    assert worker.state()["supabase_write_policy"]["snapshots"] == "approximately every 300 seconds while running"


def test_continuous_worker_exposes_supabase_request_counts():
    db = _CountingSupabaseGridRepository()
    db.select("grid_runs", {"select": "*", "limit": 1})
    worker = ContinuousGridBotWorker(client=_FakeLifecycleClient(), db=db)
    counts = worker.state()["supabase_request_counts"]
    assert counts["select"] == 1
    assert counts["by_table"]["grid_runs"]["select"] == 1


def test_continuous_worker_active_refresh_does_not_reload_same_running_run(tmp_path):
    client = _FakeLifecycleClient()
    db = _CountingSupabaseGridRepository()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "state.json", db=db, use_supabase=True)
    started = lifecycle.start_tiny_grid()
    db.request_counts = {"select": 0, "upsert_rows": 0, "insert_once": 0, "patch": 0, "delete": 0, "by_table": {}}
    worker = ContinuousGridBotWorker(client=client, db=db)
    worker._run = started["run"]

    refreshed = worker._refresh_active_run_if_due()

    assert refreshed["run_id"] == started["run"]["run_id"]
    assert db.stats()["select"] == 1
    assert db.stats()["by_table"] == {"grid_runs": {"select": 1}}


def test_supabase_order_source_fill_fallback_preserves_raw_link():
    class MissingSourceFillColumnRepository(_MemorySupabaseGridRepository):
        def upsert(self, table, payload, on_conflict=None):
            rows = payload if isinstance(payload, list) else [payload]
            if table == "grid_orders" and any("source_fill_id" in row for row in rows):
                raise SupabasePersistenceError(
                    "Supabase POST grid_orders failed: 400 "
                    '{"code":"PGRST204","message":"Could not find the source_fill_id column"}'
                )
            return super().upsert(table, payload, on_conflict)

    db = MissingSourceFillColumnRepository()
    run = {"run_id": "run-source", "bot_id": "bot-source", "config": {"config_version": 1}}
    db.persist_order(
        run,
        {
            "order_key": "order-source",
            "client_order_id": "order-source",
            "exchange_order_id": "exchange-source",
            "level_id": "L003",
            "side": "sell",
            "price": "2501",
            "requested_quantity": "1",
            "remaining_quantity": "1",
            "status": "open",
            "order_kind": "replacement",
            "source_fill_id": "fill-source",
            "raw": {"id": "exchange-source"},
        },
    )

    row = next(iter(db.tables["grid_orders"].values()))
    assert row["raw"]["gridbot"]["source_fill_id"] == "fill-source"
    assert "source_fill_id" not in row


class _MemorySupabaseGridRepository(SupabaseGridRepository):
    def __init__(self):
        self.enabled = True
        self.tables = {}
        self.request_counts = {
            "select": 0,
            "upsert_rows": 0,
            "insert_once": 0,
            "patch": 0,
            "delete": 0,
            "by_table": {},
        }

    def select(self, table, params=None):
        self._count("select", table)
        rows = list(self.tables.get(table, {}).values())
        params = params or {}
        for key, value in params.items():
            if key in {"select", "order", "limit"}:
                continue
            if isinstance(value, str) and value.startswith("eq."):
                expected = value[3:]
                rows = [row for row in rows if str(row.get(key)) == expected]
            elif isinstance(value, str) and value.startswith("in."):
                allowed = set(value[3:].strip("()").split(","))
                rows = [row for row in rows if str(row.get(key)) in allowed]
        if "order" in params:
            key = params["order"].split(".")[0]
            reverse = params["order"].endswith(".desc")
            rows = sorted(rows, key=lambda row: str(row.get(key) or ""), reverse=reverse)
        if "limit" in params:
            rows = rows[: int(params["limit"])]
        return rows

    def upsert(self, table, payload, on_conflict=None):
        self._count("upsert_rows", table, len(payload if isinstance(payload, list) else [payload]))
        rows = payload if isinstance(payload, list) else [payload]
        self.tables.setdefault(table, {})
        for row in rows:
            if on_conflict:
                key = tuple(str(row.get(col)) for col in on_conflict.split(","))
            else:
                key = (str(row.get("id") or row.get(f"{table[:-1]}_id") or len(self.tables[table])),)
            self.tables[table][key] = {**self.tables[table].get(key, {}), **row}

    def insert_once(self, table, payload, on_conflict=None):
        self._count("insert_once", table)
        if on_conflict:
            key = tuple(str(payload.get(col)) for col in on_conflict.split(","))
        else:
            key = (str(payload.get("id") or payload.get(f"{table[:-1]}_id") or len(self.tables.get(table, {}))),)
        self.tables.setdefault(table, {})
        if key in self.tables[table]:
            return False
        self.tables[table][key] = dict(payload)
        return True

    def patch(self, table, filters, payload):
        self._count("patch", table)
        for key, row in self.tables.get(table, {}).items():
            if all(str(row.get(name)) == str(value) for name, value in filters.items()):
                self.tables[table][key] = {**row, **payload}

    def _request(self, method, table, *, params=None, json=None, prefer=None):
        if method == "DELETE":
            params = params or {}
            kept = {}
            for key, row in self.tables.get(table, {}).items():
                matched = True
                for name, value in params.items():
                    if name in {"select", "order", "limit"}:
                        continue
                    expected = value[3:] if isinstance(value, str) and value.startswith("eq.") else value
                    if str(row.get(name)) != str(expected):
                        matched = False
                if not matched:
                    kept[key] = row
            self.tables[table] = kept
            return None
        raise AssertionError(method)


def test_supabase_recovery_without_json_preserves_run_and_orders(tmp_path):
    client = _FakeLifecycleClient()
    db = _MemorySupabaseGridRepository()
    path = tmp_path / "grid_state.json"
    lifecycle = DurableGridBotLifecycle(client, path, db=db, use_supabase=True)
    started = lifecycle.start_tiny_grid()
    run_id = started["run"]["run_id"]
    config_version = started["run"]["config"]["config_version"]
    order_ids = set(started["run"]["orders"])
    path.unlink()

    recovered = DurableGridBotLifecycle(client, path, db=db, use_supabase=True).status()["active_run"]
    assert recovered["run_id"] == run_id
    assert recovered["config"]["config_version"] == config_version
    assert set(recovered["orders"]) == order_ids
    assert len(client.open_orders()["result"]) == len(order_ids)


def test_supabase_fill_uniqueness_and_restart_does_not_duplicate_orders(tmp_path):
    client = _FakeLifecycleClient()
    db = _MemorySupabaseGridRepository()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "grid_state.json", db=db, use_supabase=True)
    started = lifecycle.start_tiny_grid()
    first_order = next(iter(started["run"]["orders"].values()))
    client.fill_rows = [
        {
            "id": "fill-unique",
            "order_id": first_order["exchange_order_id"],
            "client_order_id": first_order["client_order_id"],
            "side": first_order["side"],
            "price": first_order["price"],
            "size": "0.5",
            "commission": "0.01",
        }
    ]
    first = lifecycle.reconcile(started["run"]["run_id"])
    second = DurableGridBotLifecycle(client, lifecycle.state_path, db=db, use_supabase=True).reconcile(started["run"]["run_id"])
    fills = list(db.tables["grid_fills"].values())
    orders = list(db.tables["grid_orders"].values())
    assert first["new_fills"] == 1
    assert second["new_fills"] == 0
    assert len([fill for fill in fills if fill["exchange_fill_id"] == "fill-unique"]) == 1
    assert len({order["client_order_id"] for order in orders}) == len(orders)


def test_supabase_idempotent_stop_summary_and_active_guard_release(tmp_path):
    client = _FakeLifecycleClient()
    db = _MemorySupabaseGridRepository()
    lifecycle = DurableGridBotLifecycle(client, tmp_path / "grid_state.json", db=db, use_supabase=True)
    run_id = lifecycle.start_tiny_grid()["run"]["run_id"]
    first = lifecycle.stop(run_id, "unit_test")
    second = lifecycle.stop(run_id, "unit_test")
    summaries = list(db.tables["grid_run_summaries"].values())
    assert first["summary"]["summary_id"] == second["summary"]["summary_id"]
    assert len(summaries) == 1
    assert db.select("grid_active_run_locks") == []
