from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from statistics import median

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from grid_bot.recommendation_outcome_evaluator import GridRecommendationOutcomeEvaluator, HORIZON_MINUTES


def _rate(count: int, total: int) -> float | None:
    return count / total if total else None


def _aggregate(rows: list[dict]) -> list[dict]:
    by_horizon: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_horizon[row["horizon"]].append(row)
    aggregates = []
    for horizon in HORIZON_MINUTES:
        horizon_rows = by_horizon.get(horizon, [])
        breached = [row for row in horizon_rows if row["upper_breached"] or row["lower_breached"]]
        breach_times = [row["minutes_to_first_breach"] for row in breached if row["minutes_to_first_breach"] is not None]
        width_ratios = [row["actual_recommended_width_ratio"] for row in horizon_rows if row["actual_recommended_width_ratio"] is not None]
        aggregates.append(
            {
                "horizon": horizon,
                "total": len(horizon_rows),
                "containment_rate": _rate(sum(1 for row in horizon_rows if row["stayed_inside_recommended_range"]), len(horizon_rows)),
                "breach_rate": _rate(len(breached), len(horizon_rows)),
                "median_time_to_breach_minutes": median(breach_times) if breach_times else None,
                "median_actual_recommended_width_ratio": median(width_ratios) if width_ratios else None,
            }
        )
    return aggregates


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate stored Grid Parameter recommendation outcomes.")
    parser.add_argument("--symbol", default="ETHUSD")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--persist", action="store_true", help="Write immutable outcomes to Supabase.")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    result = GridRecommendationOutcomeEvaluator().evaluate_pending(symbol=args.symbol, limit=args.limit, persist=args.persist)
    print(f"mode={'persist' if args.persist else 'dry-run'}")
    print(f"recommendations_found={result.recommendations_found}")
    print(f"ohlcv_source={result.ohlcv_source}")
    print(f"ohlcv_resolution={result.ohlcv_resolution}")
    print(f"mature_counts={result.mature_counts}")
    print(f"outcomes_generated={len(result.rows)}")
    print(f"outcomes_persisted={result.persisted}")
    print(f"skipped_existing={result.skipped_existing}")
    print(f"skipped_unmatured={result.skipped_unmatured}")
    for row in _aggregate(result.rows):
        print(row)


if __name__ == "__main__":
    main()
