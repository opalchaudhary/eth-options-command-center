from datetime import datetime, timedelta, timezone

import pandas as pd

from futures_covered_engine import build_futures_covered_recommendation
from iron_fly_engine import build_iron_fly_recommendation
from strategy_risk_calculator import aggregate_greeks, futures_levels, iron_fly_payoff
from strategy_indicators import trend_snapshot


def _ohlcv(direction="up", rows=80):
    base = 3000
    data = []
    now = datetime.now(timezone.utc) - timedelta(minutes=rows * 5)
    for index in range(rows):
        drift = index * 3 if direction == "up" else -index * 3 if direction == "down" else 0
        close = base + drift
        data.append(
            {
                "timestamp": now + timedelta(minutes=index * 5),
                "open": close - 2,
                "high": close + 8,
                "low": close - 8,
                "close": close,
                "volume": 100 + index,
            }
        )
    return pd.DataFrame(data)


def _options():
    expiry = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
    rows = []
    for strike in range(2800, 3240, 40):
        rows.append(
            {
                "symbol": f"C-{strike}",
                "expiry": expiry,
                "strike": float(strike),
                "type": "call_options",
                "mark_price": max(5, 80 - abs(strike - 3000) * 0.18),
                "oi": 20,
                "volume": 5,
                "iv": 75,
                "delta": max(0.05, min(0.95, 0.5 - (strike - 3000) / 600)),
                "gamma": 0.001,
                "theta": -0.4,
                "vega": 0.2,
            }
        )
        rows.append(
            {
                "symbol": f"P-{strike}",
                "expiry": expiry,
                "strike": float(strike),
                "type": "put_options",
                "mark_price": max(5, 80 - abs(strike - 3000) * 0.18),
                "oi": 20,
                "volume": 5,
                "iv": 75,
                "delta": -max(0.05, min(0.95, 0.5 + (strike - 3000) / 600)),
                "gamma": 0.001,
                "theta": -0.4,
                "vega": 0.2,
            }
        )
    return pd.DataFrame(rows)


def _context(direction="up"):
    ohlcv = _ohlcv(direction=direction)
    options = _options()
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbol": "ETHUSD",
        "spot_price": float(ohlcv["close"].iloc[-1]),
        "mark_price": float(ohlcv["close"].iloc[-1]),
        "orderbook_insights": {
            "status": "ok",
            "bias": "Mild Bullish" if direction == "up" else "Mild Bearish" if direction == "down" else "Neutral",
            "spread_pct": 0.02,
        },
        "option_df": options,
        "expiries": sorted(options["expiry"].unique()),
        "ohlcv_df": ohlcv,
        "trend": trend_snapshot(ohlcv),
        "data_freshness": {},
        "market_events_df": pd.DataFrame(),
        "smc_zones_df": pd.DataFrame(),
        "volume_profile_df": pd.DataFrame(),
        "unavailable_inputs": ["funding_rate"],
    }


def test_futures_levels_long():
    class Config:
        atr_stop_multiplier = 1
        tp1_atr_multiplier = 2
        tp2_atr_multiplier = 3
        tp3_atr_multiplier = 4

    levels = futures_levels("LONG", 3000, 20, Config)
    assert levels["stop_loss"] == 2980
    assert levels["tp1"] == 3040
    assert levels["rr_tp1"] == 2


def test_futures_long_recommendation():
    result = build_futures_covered_recommendation(context=_context("up"))
    assert result["futures"]["recommendation"] == "LONG"
    assert result["covered_call"]["required_underlying_position"] == "Long ETH exposure"


def test_futures_short_recommendation():
    result = build_futures_covered_recommendation(context=_context("down"))
    assert result["futures"]["recommendation"] == "SHORT"
    assert result["covered_put"]["required_underlying_position"] == "Short ETH exposure"


def test_no_uncovered_option_recommendation():
    result = build_futures_covered_recommendation(context=_context("up"))
    assert "uncovered" in result["covered_call"]["uncovered_option_warning"].lower()
    assert "uncovered" in result["covered_put"]["uncovered_option_warning"].lower()


def test_missing_data_no_trade():
    context = _context("up")
    context["ohlcv_df"] = pd.DataFrame()
    context["trend"] = {}
    result = build_futures_covered_recommendation(context=context)
    assert result["futures"]["recommendation"] == "NO_TRADE"
    assert "ohlcv" in result["futures"]["missing_required_inputs"]


def test_covered_call_suitability_present():
    result = build_futures_covered_recommendation(context=_context("flat"))
    assert result["covered_call"]["status"] in ["RECOMMENDED", "WATCHLIST", "NOT_RECOMMENDED"]
    assert result["covered_call"]["required_underlying_position"] == "Long ETH exposure"


def test_iron_fly_payoff_and_breakevens():
    short_call = {"strike": 3000, "mark_price": 80}
    short_put = {"strike": 3000, "mark_price": 82}
    long_call = {"strike": 3080, "mark_price": 30}
    long_put = {"strike": 2920, "mark_price": 28}
    payoff = iron_fly_payoff(short_call, short_put, long_call, long_put)
    assert payoff["net_credit"] == 104
    assert payoff["upper_breakeven"] == 3104
    assert payoff["lower_breakeven"] == 2896


def test_greek_aggregation():
    greeks = aggregate_greeks(
        [
            {"action": "sell", "delta": 0.5, "theta": -1, "gamma": 0.1, "vega": 2},
            {"action": "buy", "delta": 0.2, "theta": -0.2, "gamma": 0.03, "vega": 0.7},
        ]
    )
    assert greeks["delta"] == -0.3
    assert greeks["theta"] == 0.8


def test_iron_fly_optimizer_returns_research_result():
    result = build_iron_fly_recommendation(context=_context("flat"))
    assert result["engine_name"] == "iron_fly_research"
    assert result["recommendation"] in ["RECOMMENDED", "WATCHLIST", "NOT_RECOMMENDED"]
    assert result["research_only"] is True


def test_iron_fly_illiquid_rejection():
    context = _context("flat")
    context["option_df"]["oi"] = 0
    context["option_df"]["volume"] = 0
    result = build_iron_fly_recommendation(context=context)
    assert result["recommendation"] == "NOT_RECOMMENDED"
    assert result["rejection_reasons"]
