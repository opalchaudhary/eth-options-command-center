import json
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from backend.routers import mobile
from backend.services import mobile_service
from backend.services.cache import clear_cache


def test_missing_mobile_token_rejected(monkeypatch):
    monkeypatch.setenv("MOBILE_API_TOKEN", "test-token")
    with pytest.raises(HTTPException) as exc:
        mobile.require_mobile_token(None)
    assert exc.value.status_code == 401


def test_invalid_mobile_token_rejected(monkeypatch):
    monkeypatch.setenv("MOBILE_API_TOKEN", "test-token")
    with pytest.raises(HTTPException) as exc:
        mobile.require_mobile_token("Bearer wrong-token")
    assert exc.value.status_code == 401


def test_correct_mobile_token_accepted(monkeypatch):
    monkeypatch.setenv("MOBILE_API_TOKEN", "test-token")
    assert mobile.require_mobile_token("Bearer test-token") is True


def test_mobile_health_response(monkeypatch):
    monkeypatch.setenv("MOBILE_API_TOKEN", "test-token")
    payload = mobile.mobile_health()
    assert payload["service"] == "deltaforge-mobile-api"
    assert payload["version"] == "1"
    assert "timestamp" in payload


def test_subwallet_response_serialization_preserves_partial_failure():
    snapshot = {
        "ok": True,
        "accounts": [
            {
                "id": "main",
                "label": "Main",
                "kind": "main",
                "ok": True,
                "api_key": "must-not-leak",
                "api_secret": "must-not-leak",
                "position_count": 1,
                "greeks": {"delta": 1, "gamma": 0.1, "theta": -2, "vega": 3},
                "balance_summary": {
                    "net_equity": "1000",
                    "by_asset": {
                        "USD": {
                            "balance": "1000",
                            "available_balance": "800",
                            "blocked_margin": "200",
                            "order_margin": "25",
                            "position_margin": "175",
                        }
                    },
                },
                "positions": [
                    {
                        "symbol": "ETH-C",
                        "contract_type": "call_options",
                        "size": "1",
                        "entry_price": "10",
                        "mark_price": "12",
                        "liquidation_price": None,
                        "margin": "50",
                        "realized_pnl": "0",
                        "unrealized_pnl": "2",
                        "computed_delta": "0.5",
                        "computed_gamma": "0.01",
                        "computed_theta": "-0.2",
                        "computed_vega": "0.3",
                        "raw_private_field": "drop-me",
                    }
                ],
            },
            {
                "id": "subwallet_1",
                "label": "Sub 1",
                "kind": "subwallet",
                "ok": False,
                "error": "Delta account unavailable",
                "position_count": 0,
                "balance_summary": {"by_asset": {}},
                "positions": [],
                "greeks": {},
            },
        ],
        "aggregate": {
            "net_equity": 1000,
            "balance": 1000,
            "available_balance": 800,
            "blocked_margin": 200,
            "order_margin": 25,
            "position_margin": 175,
            "greeks": {"delta": 1, "gamma": 0.1, "theta": -2, "vega": 3},
        },
    }
    payload = mobile_service.serialize_subwallets(snapshot)
    assert payload["ok"] is True
    assert len(payload["accounts"]) == 2
    assert payload["accounts"][0]["margin_utilization_pct"] == 20
    assert payload["accounts"][1]["ok"] is False
    assert payload["accounts"][1]["error"] == "Delta account unavailable"
    assert "raw_private_field" not in payload["accounts"][0]["positions"][0]
    encoded = json.dumps(payload)
    assert "must-not-leak" not in encoded
    assert "api_secret" not in encoded
    assert "api_key" not in encoded


def test_iron_fly_response_serialization():
    generated_at = datetime.now(timezone.utc).isoformat()
    result = {
        "ok": True,
        "generated_at": generated_at,
        "symbol": "ETHUSD",
        "recommendation": "WATCHLIST",
        "iron_fly_score": 61.2,
        "confidence": "MEDIUM",
        "selected": {
            "expiry": generated_at,
            "dte": 7,
            "center_strike": 3000,
            "wing_width": 80,
            "score": 61.2,
            "status": "VALID",
            "ranking_reason": "Reason",
            "liquidity_score": 80,
            "expected_move": 120,
            "median_iv": 70,
            "realized_vol_pct": 55,
            "iv_rv_spread": 15,
            "component_scores": {"credit": 70},
            "net_greeks": {"delta": 0.1, "gamma": 0.01, "theta": 1, "vega": -0.2},
            "payoff": {
                "net_credit": 12,
                "max_profit": 12,
                "max_loss": 68,
                "lower_breakeven": 2988,
                "upper_breakeven": 3012,
                "return_on_risk_pct": 17.65,
                "wing_width": 80,
            },
            "legs": [{"action": "sell", "option_type": "put_options", "strike": 3000, "oi": 10}],
        },
        "top_alternatives": [],
        "expiry_comparison": [],
        "risk_factors": ["Risk"],
        "entry_conditions": ["Entry"],
        "adjustment_triggers": ["Adjust"],
        "research_only": True,
    }
    payload = mobile_service.serialize_iron_fly(result)
    assert payload["selected"]["payoff"]["return_on_risk"] == 17.65
    assert payload["selected"]["legs"][0]["open_interest"] == 10
    assert "probability_of_profit" not in json.dumps(payload)
    assert "expected_value" not in json.dumps(payload)
    assert "margin_requirement" not in json.dumps(payload)


def test_mobile_routes_are_read_only(monkeypatch):
    route_methods = {
        route.path: route.methods
        for route in mobile.router.routes
        if getattr(route, "path", "").startswith("/mobile/")
    }
    assert route_methods["/mobile/health"] == {"GET"}
    assert route_methods["/mobile/home"] == {"GET"}
    assert route_methods["/mobile/subwallets"] == {"GET"}
    assert route_methods["/mobile/iron-fly"] == {"GET"}


def test_mobile_endpoint_does_not_return_secrets(monkeypatch):
    monkeypatch.setenv("MOBILE_API_TOKEN", "test-token")
    monkeypatch.setattr(
        mobile_service,
        "get_mobile_subwallets",
        lambda: {
            "ok": True,
            "last_updated": "now",
            "accounts": [],
            "aggregate": {},
        },
    )
    payload = mobile.mobile_subwallets()
    encoded = json.dumps(payload)
    assert "DELTA_API_KEY" not in encoded
    assert "DELTA_API_SECRET" not in encoded
    assert "SUPABASE" not in encoded


def test_mobile_iron_fly_uses_cache(monkeypatch):
    clear_cache()
    calls = {"count": 0}

    def fake_recommendation():
        calls["count"] += 1
        return {
            "ok": True,
            "generated_at": "now",
            "symbol": "ETHUSD",
            "recommendation": "WATCHLIST",
            "iron_fly_score": 55,
            "confidence": "MEDIUM",
            "selected": None,
            "research_only": True,
        }

    monkeypatch.setattr(mobile_service, "build_iron_fly_recommendation", fake_recommendation)
    first = mobile_service.get_mobile_iron_fly()
    second = mobile_service.get_mobile_iron_fly()
    assert first["recommendation"] == "WATCHLIST"
    assert second["recommendation"] == "WATCHLIST"
    assert calls["count"] == 1
    clear_cache()
