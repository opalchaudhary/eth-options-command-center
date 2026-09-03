from pathlib import Path

from grid_bot.operator_dashboard import (
    health_issue_text,
    health_plain_text,
    human_operator_reason,
    inventory_summary,
    lifecycle_progress_summary,
    live_config,
    orders_are_updating,
    pnl_values,
    preview_edit_summary,
    split_pending_orders,
    time_label,
)


PAGE = Path("pages/DeltaGridBot_V01.py")


def test_operator_health_text_hides_internal_codes() -> None:
    message, details = health_plain_text(
        {
            "overall_status": "CRITICAL",
            "active_issues": [
                {
                    "code": "POSITION_MISMATCH",
                    "message": "POSITION_MISMATCH: raw developer detail",
                }
            ],
        }
    )

    assert message == "The bot needs attention."
    assert details == ["Position / Inventory: Delta position does not match the bot's records."]
    assert "POSITION_MISMATCH" not in details[0]


def test_pending_orders_are_split_sorted_and_operator_safe() -> None:
    live = {
        "known_gridbot_orders": [
            {"side": "buy", "price": "2400", "remaining_quantity": "1", "status": "open", "source_fill_id": "hidden"},
            {"side": "buy", "price": "2410", "remaining_quantity": "2", "status": "partially_filled"},
            {"side": "sell", "price": "2500", "remaining_quantity": "1", "status": "submitted"},
            {"side": "sell", "price": "2490", "remaining_quantity": "1", "status": "open"},
            {"side": "sell", "price": "2510", "remaining_quantity": "1", "status": "cancelled"},
        ]
    }

    buys, sells = split_pending_orders(live)

    assert buys == [
        {"Price": "$2,410.00", "Lots": "2", "Status": "Partially Filled"},
        {"Price": "$2,400.00", "Lots": "1", "Status": "Open"},
    ]
    assert sells == [
        {"Price": "$2,490.00", "Lots": "1", "Status": "Open"},
        {"Price": "$2,500.00", "Lots": "1", "Status": "Pending"},
    ]
    assert all("source_fill_id" not in row for row in buys + sells)


def test_pending_orders_exclude_terminal_rows() -> None:
    live = {
        "known_gridbot_orders": [
            {"side": "buy", "price": "2400", "remaining_quantity": "1", "status": "open"},
            {"side": "buy", "price": "2390", "remaining_quantity": "1", "status": "filled"},
            {"side": "sell", "price": "2500", "remaining_quantity": "1", "status": "deferred"},
            {"side": "sell", "price": "2510", "remaining_quantity": "1", "status": "cancelled"},
            {"side": "sell", "price": "2520", "remaining_quantity": "0", "status": "open"},
        ]
    }

    buys, sells = split_pending_orders(live)

    assert buys == [{"Price": "$2,400.00", "Lots": "1", "Status": "Open"}]
    assert sells == [{"Price": "$2,500.00", "Lots": "1", "Status": "Waiting"}]


def test_transitional_order_board_reports_updating_instead_of_false_zero() -> None:
    live = {
        "lifecycle_state": "STARTING",
        "lifecycle_progress": {"expected_orders": 4, "confirmed_orders": 0},
        "known_gridbot_orders": [],
    }

    assert orders_are_updating(live) is True
    assert orders_are_updating({"lifecycle_state": "RUNNING", "lifecycle_progress": {"expected_orders": 4, "confirmed_orders": 0}}) is False


def test_current_grid_labels_populate_from_worker_config_and_fallback_levels() -> None:
    config = {
        "grid_type": "neutral",
        "lower_price": "2410.05",
        "upper_price": "2590.05",
        "grid_count": 6,
        "spacing_type": "arithmetic",
        "lot_size": "1",
        "max_inventory_lots": "3",
    }
    fallback = {
        "grid_nature": "long_bias",
        "grid_levels": [
            {"price": "2400", "quantity": "2"},
            {"price": "2450", "quantity": "2"},
            {"price": "2500", "quantity": "2"},
        ],
    }

    assert live_config({"config": config}) == config
    derived = live_config(fallback)
    assert derived["grid_type"] == "long_bias"
    assert derived["lower_price"] == "2400"
    assert derived["upper_price"] == "2500"
    assert derived["grid_count"] == 3
    assert derived["lot_size"] == "2"


def test_gridbot_inventory_and_delta_position_are_not_substituted() -> None:
    summary = inventory_summary(
        {
            "config": {"max_inventory_lots": "10"},
            "fill_derived_inventory": "0",
            "delta_position": "0",
            "health": {
                "position_inventory_agreement": {
                    "gridbot_inventory": "-10",
                    "delta_position": "-8",
                    "matches": False,
                    "difference": "2",
                }
            },
        }
    )

    assert summary["label"] == "Short 10 lots"
    assert summary["delta_label"] == "Short 8 lots"
    assert summary["matches"] is False
    assert summary["difference"] == "2"


def test_pnl_values_distinguish_zero_unavailable_and_partial() -> None:
    zero = pnl_values({"accounting": {"accounting_status": "COMPLETE", "live_net_pnl": "0", "net_realized_pnl": "0", "unrealized_pnl": "0", "trading_fees": "0"}})
    unavailable = pnl_values({"accounting": {"accounting_status": "COMPLETE"}})
    explicit_unavailable = pnl_values({"accounting": {"accounting_status": "UNAVAILABLE", "net_realized_pnl": "0", "trading_fees": "0"}})
    partial = pnl_values({"accounting": {"accounting_status": "PARTIAL", "live_net_pnl": "12", "net_realized_pnl": "10", "trading_fees": "1"}})

    assert zero["net"] == "0"
    assert zero["realized"] == "0"
    assert zero["unrealized"] == "0"
    assert zero["fees"] == "0"
    assert unavailable["net"] is None
    assert unavailable["realized"] is None
    assert unavailable["unrealized"] is None
    assert explicit_unavailable["net"] is None
    assert explicit_unavailable["realized"] is None
    assert explicit_unavailable["fees"] is None
    assert partial["net"] is None
    assert partial["realized"] == "10"
    assert partial["fees"] == "1"
    assert partial["incomplete"] is True


def test_recent_activity_time_is_always_ist() -> None:
    assert time_label("2026-08-29T09:00:00+00:00") == "14:30 IST"


def test_health_text_is_plain_language_and_source_labelled() -> None:
    message, details = health_plain_text({"active_issues": [{"code": "SUPABASE_FAILURE", "message": "raw dns error"}]})

    assert message == "The bot needs attention."
    assert details == ["Supabase / Database: Trading records are temporarily unavailable."]
    assert health_issue_text({"code": "DELTA_TIMEOUT"}) == "Delta / Exchange: Delta is taking too long to answer."
    assert health_plain_text({"active_issues": [{"code": "ACCOUNTING_INCOMPLETE"}]}) == ("Profit information is incomplete.", [])
    assert "changed outside the GridBot" in health_issue_text({"code": "EXTERNAL_POSITION_CHANGE"})
    assert "forcibly reduced" in health_issue_text({"code": "FORCED_LIQUIDATION"})


def test_lifecycle_progress_summary_shows_configured_vs_deployed_plainly() -> None:
    lines = lifecycle_progress_summary(
        {
            "lifecycle_state": "STARTING",
            "lifecycle_progress": {
                "operation": "START",
                "stage": "VERIFYING_COMPLETENESS",
                "message": "Waiting for Delta to confirm 2 orders.",
                "expected_orders": 6,
                "confirmed_orders": 4,
                "buy_confirmed_orders": 2,
                "sell_confirmed_orders": 2,
                "missing_orders": 2,
                "retry_attempts": 1,
                "retry_wait_seconds": 1.0,
                "stall_message": "AMBIGUOUS_SUBMISSION",
                "elapsed_seconds": 12.6,
            },
        }
    )

    assert "Configured: 6 levels" in lines
    assert "Deployed: 4 confirmed (2 BUY / 2 SELL), 2 waiting" in lines
    assert "Order submission is being verified with Delta." in lines
    assert all("AMBIGUOUS_SUBMISSION" not in line for line in lines)


def test_operator_reason_messages_hide_internal_codes() -> None:
    assert human_operator_reason("CURRENT_INVENTORY_PRESERVED") == "Current position was preserved."


def test_edit_preview_summary_is_plain_language() -> None:
    lines = preview_edit_summary(
        {
            "order_plan": {
                "remain_count": 2,
                "cancel_count": 1,
                "create_count": 3,
                "defer_count": 0,
            }
        }
    )

    assert lines == [
        "Your current position will not be closed.",
        "2 existing orders will remain",
        "1 orders will be cancelled",
        "3 new orders will be placed",
        "0 orders will wait because of limits or market conditions",
    ]


def test_live_config_uses_worker_config_when_active_run_is_not_embedded() -> None:
    config = {
        "grid_type": "neutral",
        "lower_price": "2410.05",
        "upper_price": "2590.05",
        "grid_count": 6,
        "spacing_type": "arithmetic",
        "lot_size": "1",
        "max_inventory_lots": "3",
    }

    assert live_config({"config": config, "grid_nature": "long_bias"}) == config


def test_dashboard_uses_live_fragment_without_full_page_refresh() -> None:
    text = PAGE.read_text()

    assert "streamlit_autorefresh" not in text
    assert "@fragment(run_every=\"5s\")" in text
    assert "fetch_live_state" in text
    assert "/api/grid/v01/live/state" in text


def test_dashboard_hides_developer_controls_and_raw_internals() -> None:
    text = PAGE.read_text()
    forbidden = [
        "Start Tiny",
        "Preview Tiny",
        "Reconcile",
        "Regrid",
        "Developer Tools",
        "raw JSON",
        "source_fill_id",
        "/api/grid/v01/live/reconcile",
        "/api/grid/v01/live/regrid",
        "/api/grid/v01/live/start-tiny",
    ]

    for needle in forbidden:
        assert needle not in text


def test_history_loads_only_from_manual_action() -> None:
    text = PAGE.read_text()
    fragment_body = text.split("@fragment(run_every=\"5s\")", maxsplit=1)[1].split("def render_idle", maxsplit=1)[0]

    assert "Refresh History" in text
    assert "History loads only when requested." in text
    assert "refresh_history()" not in fragment_body


def test_grid_recommendation_loads_only_from_manual_action() -> None:
    text = PAGE.read_text()
    recommendation_body = text.split("def render_grid_recommendation()", maxsplit=1)[1].split("def render_orders", maxsplit=1)[0]
    fragment_body = text.split("@fragment(run_every=\"5s\")", maxsplit=1)[1].split("def render_idle", maxsplit=1)[0]

    assert "Get Recommendation" in recommendation_body
    assert "Recommendation loads only when requested." in recommendation_body
    assert 'safe_post("/api/grid/v01/recommendation"' in recommendation_body
    assert "/api/grid/v01/recommendation" not in fragment_body


def test_grid_recommendation_persists_in_session_state() -> None:
    text = PAGE.read_text()
    recommendation_body = text.split("def render_grid_recommendation()", maxsplit=1)[1].split("def render_orders", maxsplit=1)[0]

    assert 'st.session_state["gridbot_recommendation"]' in recommendation_body
    assert 'payload = st.session_state.get("gridbot_recommendation")' in recommendation_body
    assert "render_grid_recommendation_payload(payload)" in recommendation_body


def test_grid_recommendation_is_below_current_grid_and_above_health() -> None:
    text = PAGE.read_text()
    dashboard_body = text.split("def render_live_dashboard", maxsplit=1)[1].split("def render_actions", maxsplit=1)[0]

    current_grid_index = dashboard_body.index("Current Grid")
    recommendation_index = dashboard_body.index("render_grid_recommendation()")
    health_index = dashboard_body.index("Health")

    assert current_grid_index < recommendation_index < health_index
