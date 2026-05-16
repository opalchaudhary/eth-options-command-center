import pandas as pd

from outcome_tracker import get_recommendation_outcomes, refresh_recent_outcomes
from paper_trading import (
    create_paper_trade,
    get_all_paper_trades,
    get_closed_paper_trades,
    get_open_paper_trades,
    update_open_paper_trades,
)
from recommendation_journal import get_latest_recommendations, save_recommendation_snapshot


def record_validation_cycle(insights):
    recommendation = save_recommendation_snapshot(insights)

    if recommendation and recommendation.get("id"):
        create_paper_trade(recommendation)

    if insights.get("spot_price"):
        update_open_paper_trades(insights.get("spot_price"))

    refresh_recent_outcomes(limit=50)

    return recommendation


def _bucket_confidence(score):
    score = float(score or 0)

    if score < 50:
        return "<50"
    if score < 65:
        return "50-64"
    if score < 80:
        return "65-79"
    return "80+"


def _bucket_conflict(score):
    score = float(score or 0)

    if score < 25:
        return "0-24"
    if score < 50:
        return "25-49"
    if score < 75:
        return "50-74"
    return "75+"


def _empty_performance():
    return {
        "win_rate": 0,
        "average_pnl_usdt": 0,
        "average_pnl_inr": 0,
        "strategy_performance": pd.DataFrame(),
        "confidence_bucket_performance": pd.DataFrame(),
        "conflict_bucket_performance": pd.DataFrame(),
    }


def performance_summary():
    trades = get_all_paper_trades(limit=500)
    recommendations = get_latest_recommendations(limit=500)

    if trades.empty:
        return _empty_performance()

    closed = trades[trades["status"].isin(["CLOSED", "EXPIRED"])] if "status" in trades else pd.DataFrame()

    if closed.empty:
        return {
            **_empty_performance(),
            "open_count": len(trades[trades["status"] == "OPEN"]) if "status" in trades else 0,
            "closed_count": 0,
        }

    closed = closed.copy()
    closed["realized_pnl_usdt"] = pd.to_numeric(closed["realized_pnl_usdt"], errors="coerce").fillna(0)
    closed["realized_pnl_inr"] = pd.to_numeric(closed["realized_pnl_inr"], errors="coerce").fillna(0)
    closed["is_win"] = closed["realized_pnl_usdt"] > 0

    win_rate = round(float(closed["is_win"].mean() * 100), 2)
    avg_pnl_usdt = round(float(closed["realized_pnl_usdt"].mean()), 4)
    avg_pnl_inr = round(float(closed["realized_pnl_inr"].mean()), 2)

    strategy_perf = (
        closed.groupby("strategy")
        .agg(
            trades=("id", "count"),
            win_rate=("is_win", lambda x: round(float(x.mean() * 100), 2)),
            avg_pnl_usdt=("realized_pnl_usdt", "mean"),
            total_pnl_usdt=("realized_pnl_usdt", "sum"),
        )
        .reset_index()
    )

    confidence_perf = pd.DataFrame()
    conflict_perf = pd.DataFrame()

    if not recommendations.empty and "id" in recommendations.columns:
        joined = closed.merge(
            recommendations,
            left_on="recommendation_id",
            right_on="id",
            suffixes=("_trade", "_rec"),
        )

        if not joined.empty:
            joined["confidence_bucket"] = joined["confidence_score"].apply(_bucket_confidence)
            joined["conflict_bucket"] = joined["signal_conflict_score"].apply(_bucket_conflict)

            confidence_perf = (
                joined.groupby("confidence_bucket")
                .agg(
                    trades=("id_trade", "count"),
                    win_rate=("is_win", lambda x: round(float(x.mean() * 100), 2)),
                    avg_pnl_usdt=("realized_pnl_usdt", "mean"),
                )
                .reset_index()
            )

            conflict_perf = (
                joined.groupby("conflict_bucket")
                .agg(
                    trades=("id_trade", "count"),
                    win_rate=("is_win", lambda x: round(float(x.mean() * 100), 2)),
                    avg_pnl_usdt=("realized_pnl_usdt", "mean"),
                )
                .reset_index()
            )

    return {
        "win_rate": win_rate,
        "average_pnl_usdt": avg_pnl_usdt,
        "average_pnl_inr": avg_pnl_inr,
        "open_count": len(trades[trades["status"] == "OPEN"]) if "status" in trades else 0,
        "closed_count": len(closed),
        "strategy_performance": strategy_perf,
        "confidence_bucket_performance": confidence_perf,
        "conflict_bucket_performance": conflict_perf,
    }


def validation_dashboard_data():
    return {
        "latest_recommendations": get_latest_recommendations(limit=25),
        "open_trades": get_open_paper_trades(limit=25),
        "closed_trades": get_closed_paper_trades(limit=25),
        "outcomes": get_recommendation_outcomes(limit=100),
        "performance": performance_summary(),
    }
