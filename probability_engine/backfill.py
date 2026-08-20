import argparse
import json
from datetime import timedelta

from probability_engine.services.historical_backtest import (
    DEFAULT_HORIZONS,
    HistoricalBacktestPilot,
    parse_utc,
)


def _parse_horizons(value):
    return tuple(item.strip().upper() for item in value.split(",") if item.strip())


def main():
    parser = argparse.ArgumentParser(description="Controlled Probability Engine historical BACKTEST pilot.")
    parser.add_argument("--symbol", default="ETHUSD")
    parser.add_argument("--start", required=True, help="Pilot start timestamp, UTC ISO format.")
    parser.add_argument("--end", help="Pilot end timestamp, UTC ISO format. Defaults to start + 24h - cadence.")
    parser.add_argument("--sample-minutes", type=int, default=30)
    parser.add_argument("--horizons", default=",".join(DEFAULT_HORIZONS))
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--persist", action="store_true", help="Persist the bounded pilot. Omit for dry-run only.")
    args = parser.parse_args()

    start = parse_utc(args.start)
    end = parse_utc(args.end) if args.end else start + timedelta(hours=24) - timedelta(minutes=args.sample_minutes)
    if end < start:
        raise SystemExit("--end must be greater than or equal to --start")

    result = HistoricalBacktestPilot().run(
        start=start,
        end=end,
        symbol=args.symbol,
        sample_minutes=args.sample_minutes,
        horizons=_parse_horizons(args.horizons),
        dry_run=not args.persist,
        persist=args.persist,
    )
    print(json.dumps(result.to_dict(), indent=2, default=str))


if __name__ == "__main__":
    main()
