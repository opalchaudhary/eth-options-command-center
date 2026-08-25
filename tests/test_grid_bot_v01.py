from decimal import Decimal

import pytest

from grid_bot.accounting import ExchangeCost, gross_cycle_pnl, summarize_pnl
from grid_bot.config import REST_URL, TestnetEndpointConfig, validate_testnet_endpoints
from grid_bot.delta_testnet_client import DeltaTestnetClient
from grid_bot.durable_lifecycle import DurableGridBotLifecycle
from grid_bot.engine import DeltaGridBotEngine
from grid_bot.execution import make_client_order_id
from grid_bot.grid_builder import build_grid_levels, generate_prices
from grid_bot.models import FillRecord, GridConfig, GridStatus, GridType, ProductSpec, Side, SpacingType
from grid_bot.reconciliation import reconcile_orders
from grid_bot.repository import InMemoryGridRepository
from grid_bot.rest_fallback import RestFallbackPoller, RestFallbackState
from grid_bot.risk import GridRiskController, RiskInputs, RiskState, grid_risk_ratio, inventory_utilisation
from grid_bot.supabase_repository import SupabaseGridRepository


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

    def fills(self, product_id, start_time=None, page_size=50, **_kwargs):
        return {"success": True, "result": self.fill_rows}

    def positions(self, underlying_asset_symbol="ETH"):
        return {"success": True, "result": [{"product_id": 1699, "size": "0"}]}

    def account_margin(self):
        return {"success": True, "result": {"portfolio_margin": True}}


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
    assert first["new_fills"] == 1
    assert second["new_fills"] == 0
    assert len(replacement_orders) == 1
    assert replacement_orders[0]["requested_quantity"] == "0.5"


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


class _MemorySupabaseGridRepository(SupabaseGridRepository):
    def __init__(self):
        self.enabled = True
        self.tables = {}

    def select(self, table, params=None):
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
        rows = payload if isinstance(payload, list) else [payload]
        self.tables.setdefault(table, {})
        for row in rows:
            if on_conflict:
                key = tuple(str(row.get(col)) for col in on_conflict.split(","))
            else:
                key = (str(row.get("id") or row.get(f"{table[:-1]}_id") or len(self.tables[table])),)
            self.tables[table][key] = {**self.tables[table].get(key, {}), **row}

    def insert_once(self, table, payload, on_conflict=None):
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
