from pathlib import Path

from grid_bot.operator_dashboard import health_plain_text, preview_edit_summary, split_pending_orders


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

    assert message == "The bot needs attention"
    assert details == ["Position problem: Delta position does not match the bot records."]
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
