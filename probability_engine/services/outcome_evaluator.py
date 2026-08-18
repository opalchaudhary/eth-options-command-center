import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd

from market_data import fetch_ohlcv_window
from probability_engine.config import HORIZON_MINUTES, get_probability_config
from probability_engine.repositories.outcome_repository import OutcomeRepository
from probability_engine.repositories.prediction_repository import PredictionRepository
from probability_engine.services.outcome_service import OutcomeService


logger = logging.getLogger(__name__)

RESOLUTION_SECONDS = 300


def parse_utc(value):
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize(timezone.utc)
    return parsed.tz_convert(timezone.utc).to_pydatetime()


def prediction_window(prediction):
    created_at = parse_utc(prediction["created_at"])
    horizon = str(prediction.get("horizon") or "").upper()
    minutes = HORIZON_MINUTES.get(horizon)
    if minutes is None:
        return None
    return created_at, created_at + timedelta(minutes=minutes)


def is_mature(prediction, now=None):
    now = now or datetime.now(timezone.utc)
    window = prediction_window(prediction)
    return bool(window and now >= window[1])


def _records(rows):
    if rows is None:
        return []
    if hasattr(rows, "empty") and hasattr(rows, "to_dict"):
        if rows.empty:
            return []
        return rows.to_dict("records")
    if isinstance(rows, dict):
        return [rows]
    return list(rows or [])


def _prediction_object(row):
    payload = dict(row)
    metadata = payload.get("metadata_json") or {}
    if isinstance(metadata, str):
        metadata = {}
    payload["metadata_json"] = metadata
    return SimpleNamespace(**payload)


def _window_candles(candles, start_at, end_at):
    if candles is None or candles.empty or "timestamp" not in candles:
        return pd.DataFrame()

    frame = candles.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    start = pd.Timestamp(start_at)
    end = pd.Timestamp(end_at)
    # Delta candle timestamps represent candle open time; exclude candles that
    # begin at/after the horizon end to avoid look-ahead beyond T + H.
    eligible_end = end - pd.Timedelta(seconds=RESOLUTION_SECONDS)
    frame = frame[(frame["timestamp"] >= start) & (frame["timestamp"] <= eligible_end)]
    return frame.sort_values("timestamp").reset_index(drop=True)


def _has_complete_window(candles, start_at, end_at):
    if candles is None or candles.empty:
        return False
    first_ts = pd.Timestamp(candles.iloc[0]["timestamp"])
    last_ts = pd.Timestamp(candles.iloc[-1]["timestamp"])
    start = pd.Timestamp(start_at)
    required_last = pd.Timestamp(end_at) - pd.Timedelta(seconds=RESOLUTION_SECONDS)
    return first_ts <= start + pd.Timedelta(seconds=RESOLUTION_SECONDS) and last_ts >= required_last


class LiveOutcomeEvaluator:
    def __init__(
        self,
        config=None,
        prediction_repository=None,
        outcome_repository=None,
        outcome_service=None,
        candle_fetcher=None,
    ):
        self.config = config or get_probability_config()
        self.prediction_repository = prediction_repository or PredictionRepository()
        self.outcome_repository = outcome_repository or OutcomeRepository()
        self.outcome_service = outcome_service or OutcomeService(self.config)
        self.candle_fetcher = candle_fetcher or fetch_ohlcv_window

    def run(self, now=None):
        now = now or datetime.now(timezone.utc)
        batch_limit = max(1, int(self.config.outcome_batch_limit or 25))
        candidate_limit = batch_limit * 4
        max_candidate_pages = 20
        earliest_maturity = now - timedelta(minutes=min(HORIZON_MINUTES.values()))
        rows = []
        matured = []
        pending = []
        existing_ids = set()

        for page in range(max_candidate_pages):
            page_rows = _records(
                self.prediction_repository.mature_unevaluated(
                    before_iso=earliest_maturity.isoformat(),
                    limit=candidate_limit,
                    offset=page * candidate_limit,
                )
            )
            rows.extend(page_rows)
            page_matured = [row for row in page_rows if is_mature(row, now=now)]
            matured.extend(page_matured)
            page_existing_ids = self.outcome_repository.existing_prediction_ids([row.get("id") for row in page_matured])
            existing_ids.update(page_existing_ids)
            pending.extend(row for row in page_matured if row.get("id") not in page_existing_ids)
            pending = pending[:batch_limit]
            if len(pending) >= batch_limit or len(page_rows) < candidate_limit:
                break

        if not pending:
            return {
                "ok": True,
                "action": "EVALUATED",
                "candidate_count": len(rows),
                "mature_count": len(matured),
                "created_count": 0,
                "skipped_existing_count": len(existing_ids),
                "skipped_incomplete_count": 0,
                "failed_count": 0,
                "batch_limit": batch_limit,
                "candidate_pages_scanned": page + 1 if "page" in locals() else 0,
            }

        candles_by_symbol = self._fetch_candles(pending)
        created = 0
        incomplete = 0
        failed = 0

        for row in pending:
            prediction_id = row.get("id")
            try:
                start_at, end_at = prediction_window(row)
                candles = _window_candles(candles_by_symbol.get(row.get("symbol")), start_at, end_at)
                if not _has_complete_window(candles, start_at, end_at):
                    incomplete += 1
                    continue
                outcome = self.outcome_service.evaluate_prediction(_prediction_object(row), candles)
                if outcome.get("ok") is False:
                    incomplete += 1
                    continue
                outcome["evaluated_at"] = now.isoformat()
                outcome["metadata_json"] = {
                    "horizon": row.get("horizon"),
                    "window_start": start_at.isoformat(),
                    "window_end": end_at.isoformat(),
                    "candle_resolution": "5m",
                    "candle_count": int(len(candles)),
                    "actual_open_semantics": "first 5m candle open at or after prediction created_at",
                    "actual_close_semantics": "last 5m candle close before prediction created_at + horizon",
                    "range_coverage_semantics": "future close contained within predicted range",
                    "range_held_semantics": "entire future high-low path stayed within predicted 70% range",
                    "breakout_boundary_semantics": "predicted 70% range boundaries",
                }
                if self.outcome_repository.safe_insert_outcome(prediction_id, outcome):
                    created += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
                logger.exception("probability.outcome.prediction_failed", extra={"prediction_id": prediction_id})

        return {
            "ok": failed == 0,
            "action": "EVALUATED",
            "candidate_count": len(rows),
            "mature_count": len(matured),
            "attempted_count": len(pending),
            "created_count": created,
            "skipped_existing_count": len(existing_ids),
            "skipped_incomplete_count": incomplete,
            "failed_count": failed,
            "batch_limit": batch_limit,
            "candidate_pages_scanned": page + 1 if "page" in locals() else 0,
        }

    def _fetch_candles(self, predictions):
        windows = [(*prediction_window(row), row.get("symbol") or self.config.symbol) for row in predictions]
        candles_by_symbol = {}
        for symbol in sorted({item[2] for item in windows}):
            starts = [item[0] for item in windows if item[2] == symbol]
            ends = [item[1] for item in windows if item[2] == symbol]
            fetch_start = min(starts)
            fetch_end = max(ends)
            candles_by_symbol[symbol] = self.candle_fetcher(
                symbol=symbol,
                resolution="5m",
                start_at=fetch_start,
                end_at=fetch_end,
            )
        return candles_by_symbol
