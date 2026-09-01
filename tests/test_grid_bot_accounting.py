from decimal import Decimal

from grid_bot.accounting import (
    ExchangeCost,
    FEE_CONFIRMED,
    FEE_PENDING,
    FEE_UNAVAILABLE,
    build_cycle_ledger,
    build_run_accounting,
    calculate_unrealized_pnl,
    extract_fee,
    normalize_maker_taker_role,
)


def _run(fills, multiplier="1", status="RUNNING"):
    orders = {}
    for fill in fills:
        client_id = fill["client_order_id"]
        orders[client_id] = {
            "order_key": client_id,
            "client_order_id": client_id,
            "exchange_order_id": fill["order_id"],
            "level_id": fill.get("level_id"),
            "side": fill["side"],
            "price": fill["price"],
            "requested_quantity": fill["size"],
            "filled_quantity": fill["size"],
            "remaining_quantity": "0",
            "status": "filled",
            "order_kind": fill.get("order_kind", "initial_grid"),
            "config_version": fill.get("config_version", 1),
            "source_fill_id": fill.get("source_fill_id"),
        }
    return {
        "run_id": "run-accounting",
        "bot_id": "bot-accounting",
        "status": status,
        "product": {"product_id": 1699, "symbol": "ETHUSD", "contract_multiplier": multiplier},
        "config": {"config_version": 1, "grid_type": "neutral", "product_symbol": "ETHUSD"},
        "orders": orders,
        "fills": {fill["id"]: fill for fill in fills},
    }


def _fill(fill_id, side, price, size="10", fee="0.5", role="maker", config_version=1):
    order_hint = {
        "entry": 1,
        "older": 1,
        "newer": 2,
        "exit": 3,
        "exit1": 4,
        "exit2": 5,
        "exit3": 6,
    }.get(fill_id, len(fill_id))
    return {
        "id": fill_id,
        "order_id": f"order-{fill_id}",
        "client_order_id": f"client-{fill_id}",
        "side": side,
        "price": price,
        "size": size,
        "commission": fee,
        "commission_asset": "USD",
        "role": role,
        "created_at": f"2026-08-28T00:00:{order_hint:02d}+00:00",
        "level_id": f"L{len(fill_id)}",
        "config_version": config_version,
    }


def test_fee_extraction_distinguishes_confirmed_zero_pending_and_unavailable():
    zero = extract_fee({"id": "z", "commission": "0"})
    pending = extract_fee({"id": "p", "price": "2500"})
    unavailable = extract_fee({})

    assert zero.status == FEE_CONFIRMED
    assert zero.amount == Decimal("0")
    assert pending.status == FEE_PENDING
    assert pending.amount is None
    assert unavailable.status == FEE_UNAVAILABLE


def test_maker_taker_role_normalization():
    assert normalize_maker_taker_role({"role": "m"}) == "maker"
    assert normalize_maker_taker_role({"liquidity": "taker"}) == "taker"
    assert normalize_maker_taker_role({"role": ""}) == "unknown"


def test_long_cycle_profit_and_fees():
    run = _run([_fill("entry", "buy", "3000", fee="1.5", role="taker"), _fill("exit", "sell", "3010", fee="0.5")])
    accounting = build_run_accounting(run, mark_price=Decimal("3010"), account_position_lots=Decimal("0"))

    assert accounting.cycles_completed == 1
    assert accounting.cycles[0].direction == "LONG_CYCLE"
    assert accounting.gross_realized_pnl == Decimal("100")
    assert accounting.trading_fees == Decimal("2.0")
    assert accounting.net_realized_pnl == Decimal("98.0")
    assert accounting.fee_to_gross_ratio == Decimal("0.02")
    assert accounting.taker_fees == Decimal("1.5")


def test_short_cycle_profit_and_neutral_sell_to_buy():
    run = _run([_fill("entry", "sell", "3010", fee="1"), _fill("exit", "buy", "3000", fee="1")])
    accounting = build_run_accounting(run, mark_price=Decimal("3000"), account_position_lots=Decimal("0"))

    assert accounting.cycles[0].direction == "SHORT_CYCLE"
    assert accounting.gross_realized_pnl == Decimal("100")
    assert accounting.net_realized_pnl == Decimal("98")


def test_neutral_buy_to_sell_cycle():
    run = _run([_fill("entry", "buy", "3000", fee="0"), _fill("exit", "sell", "3005", fee="0")])

    assert build_run_accounting(run).cycles[0].direction == "LONG_CYCLE"


def test_partial_close_leaves_remaining_inventory_basis():
    run = _run([_fill("entry", "buy", "3000", size="10", fee="1"), _fill("exit", "sell", "3010", size="4", fee="0.4")])
    accounting = build_run_accounting(run, mark_price=Decimal("3020"), account_position_lots=Decimal("6"))

    assert accounting.cycles_completed == 1
    assert accounting.cycles[0].quantity_lots == Decimal("4")
    assert accounting.gross_realized_pnl == Decimal("40")
    assert accounting.realized_trading_fees == Decimal("0.8")
    assert accounting.open_inventory_trading_fees == Decimal("0.6")
    assert accounting.net_realized_pnl == Decimal("39.2")
    assert accounting.remaining_inventory_lots == Decimal("6")
    assert accounting.unrealized_pnl == Decimal("120")
    assert accounting.live_net_pnl == Decimal("158.6")


def test_open_inventory_fee_is_not_reported_as_realized_loss():
    run = _run([_fill("entry", "buy", "3000", size="2", fee="0.8")])
    accounting = build_run_accounting(run, mark_price=Decimal("3010"), account_position_lots=Decimal("2"))

    assert accounting.gross_realized_pnl == Decimal("0")
    assert accounting.realized_trading_fees == Decimal("0")
    assert accounting.open_inventory_trading_fees == Decimal("0.8")
    assert accounting.net_realized_pnl == Decimal("0")
    assert accounting.unrealized_pnl == Decimal("20")
    assert accounting.live_net_pnl == Decimal("19.2")


def test_fifo_uses_fill_timestamp_not_dictionary_insertion_order():
    older = _fill("older", "buy", "3000", size="1", fee="0", config_version=1)
    older["created_at"] = "2026-08-28T00:00:01+00:00"
    newer = _fill("newer", "buy", "3010", size="1", fee="0", config_version=1)
    newer["created_at"] = "2026-08-28T00:00:02+00:00"
    exit_fill = _fill("exit", "sell", "3020", size="1", fee="0", config_version=1)
    exit_fill["created_at"] = "2026-08-28T00:00:03+00:00"
    run = _run([newer, older, exit_fill])

    accounting = build_run_accounting(run, mark_price=Decimal("3020"), account_position_lots=Decimal("1"))

    assert accounting.cycles[0].entry_fill_id == "older"
    assert accounting.cycles[0].gross_pnl == Decimal("20")
    assert accounting.remaining_inventory_basis == Decimal("3010")


def test_multiple_closing_fills_do_not_double_count():
    run = _run(
        [
            _fill("entry", "buy", "3000", size="10", fee="1"),
            _fill("exit1", "sell", "3003", size="3", fee="0.3"),
            _fill("exit2", "sell", "3004", size="2", fee="0.2"),
            _fill("exit3", "sell", "3005", size="5", fee="0.5"),
        ]
    )
    cycles, _, remaining_lots, _ = build_cycle_ledger(run)

    assert [cycle.quantity_lots for cycle in cycles] == [Decimal("3"), Decimal("2"), Decimal("5")]
    assert sum(cycle.gross_pnl for cycle in cycles) == Decimal("42")
    assert remaining_lots == Decimal("0")


def test_cycle_spanning_config_versions_keeps_entry_and_exit_versions():
    run = _run([_fill("entry", "buy", "3000", config_version=1), _fill("exit", "sell", "3010", config_version=2)])
    cycle = build_run_accounting(run).cycles[0]

    assert cycle.entry_config_version == 1
    assert cycle.exit_config_version == 2


def test_fee_pending_marks_accounting_partial_without_zeroing_fee():
    pending = _fill("entry", "buy", "3000")
    pending.pop("commission")
    run = _run([pending, _fill("exit", "sell", "3010", fee="0")])
    accounting = build_run_accounting(run)

    assert accounting.trading_fees == Decimal("0")
    assert accounting.accounting_status == "PARTIAL"
    assert "FILL_FEE_PENDING" in accounting.warnings


def test_fee_unavailable_marks_accounting_partial():
    fill = _fill("entry", "buy", "3000", size="1")
    fill.pop("commission")
    fill["fee_status"] = FEE_UNAVAILABLE
    run = _run([fill, _fill("exit", "sell", "3010", size="1", fee="0")])

    assert "FILL_FEE_UNAVAILABLE" in build_run_accounting(run).warnings


def test_zero_gross_fee_ratio_is_unknown():
    run = _run([_fill("entry", "buy", "3000", fee="1"), _fill("exit", "sell", "3000", fee="1")])

    assert build_run_accounting(run).fee_to_gross_ratio is None


def test_unrealized_long_short_flat_and_ambiguous_attribution():
    assert calculate_unrealized_pnl(Decimal("2"), Decimal("6000"), Decimal("3010"), Decimal("1"), True) == Decimal("20")
    assert calculate_unrealized_pnl(Decimal("-2"), Decimal("-6020"), Decimal("3000"), Decimal("1"), True) == Decimal("20")
    assert calculate_unrealized_pnl(Decimal("0"), Decimal("0"), None, Decimal("1"), False) == Decimal("0")
    assert calculate_unrealized_pnl(Decimal("1"), Decimal("3000"), Decimal("3010"), Decimal("1"), False) is None


def test_funding_and_other_costs_are_signed_without_double_counting_trading_fee_cost_rows():
    run = _run([_fill("entry", "buy", "3000", fee="1"), _fill("exit", "sell", "3010", fee="1")])
    costs = [
        ExchangeCost("run-accounting", None, None, "funding", Decimal("3"), "USD", "credit"),
        ExchangeCost("run-accounting", None, None, "funding", Decimal("2"), "USD", "debit"),
        ExchangeCost("run-accounting", None, None, "settlement", Decimal("4"), "USD", "debit"),
        ExchangeCost("run-accounting", None, None, "rebate", Decimal("1"), "USD", "credit"),
        ExchangeCost("run-accounting", None, "entry", "trading_fee", Decimal("99"), "USD", "debit"),
    ]
    accounting = build_run_accounting(run, costs=costs)

    assert accounting.funding_received == Decimal("3")
    assert accounting.funding_paid == Decimal("2")
    assert accounting.funding_net == Decimal("1")
    assert accounting.other_costs == Decimal("4")
    assert accounting.other_credits == Decimal("1")
    assert accounting.trading_fees == Decimal("2")
    assert accounting.realized_trading_fees == Decimal("2")
    assert accounting.net_realized_pnl == Decimal("96")


def test_duplicate_mirrored_trading_fee_cost_warns_without_economic_double_count():
    run = _run([_fill("entry", "buy", "3000", fee="1"), _fill("exit", "sell", "3010", fee="1")])
    costs = [
        ExchangeCost("run-accounting", None, "entry", "trading_fee", Decimal("1"), "USD", "debit", exchange_transaction_id="fill-entry"),
        ExchangeCost("run-accounting", None, "entry", "trading_fee", Decimal("1"), "USD", "debit", exchange_transaction_id="fill-entry"),
    ]

    accounting = build_run_accounting(run, costs=costs)

    assert accounting.trading_fees == Decimal("2")
    assert accounting.net_realized_pnl == Decimal("98")
    assert "DUPLICATE_EXCHANGE_COST" in accounting.warnings
    assert accounting.accounting_status == "PARTIAL"


def test_external_position_resolution_keeps_accounting_partial():
    run = _run([_fill("entry", "buy", "3000", size="2", fee="1")], status="STOPPED")
    run["external_position_resolution"] = {"status": "EXTERNALLY_RESOLVED"}

    accounting = build_run_accounting(run, mark_price=Decimal("3010"), account_position_lots=Decimal("0"))

    assert accounting.unrealized_pnl is None
    assert accounting.live_net_pnl is None
    assert accounting.accounting_status == "PARTIAL"
    assert "EXTERNAL_POSITION_CLOSE_UNATTRIBUTED" in accounting.warnings


def test_external_position_resolution_from_summary_keeps_historical_accounting_partial():
    run = _run([_fill("entry", "buy", "3000", size="2", fee="1")], status="STOPPED")
    run["summary"] = {"external_position_resolution": {"status": "EXTERNALLY_RESOLVED"}}

    accounting = build_run_accounting(run, mark_price=Decimal("3010"), account_position_lots=Decimal("0"))

    assert accounting.accounting_status == "PARTIAL"
    assert accounting.funding_attribution_status == "PARTIALLY_ATTRIBUTED"
    assert "EXTERNAL_POSITION_CLOSE_UNATTRIBUTED" in accounting.warnings


def test_external_position_adjustment_keeps_accounting_partial_without_fake_close():
    run = _run([_fill("entry", "buy", "3000", size="2", fee="1")], status="PAUSED")
    run["external_position_adjustment"] = {"classification": "MANUAL_PARTIAL_REDUCTION_OR_EXTERNAL_REDUCTION"}

    accounting = build_run_accounting(run, mark_price=Decimal("3010"), account_position_lots=Decimal("1"))

    assert accounting.remaining_inventory_lots == Decimal("2")
    assert accounting.unrealized_pnl is None
    assert accounting.live_net_pnl is None
    assert accounting.accounting_status == "PARTIAL"
