import logging
from datetime import datetime, timezone

import pandas as pd

from backend.services.delta_client import eth_market_snapshot, eth_option_chain, ohlcv_snapshot
from probability_engine.config import get_probability_config
from probability_engine.repositories.prediction_repository import PredictionRepository
from probability_engine.repositories.snapshot_repository import SnapshotRepository
from probability_engine.services.feature_engine import FeatureEngine
from probability_engine.services.probability_service import ProbabilityService
from probability_engine.services.regime_engine import RegimeEngine


logger = logging.getLogger(__name__)


def _rows_to_df(rows):
    frame = pd.DataFrame(rows or [])
    if frame.empty:
        return frame
    for column in ["open", "high", "low", "close", "volume"]:
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=[column for column in ["open", "high", "low", "close"] if column in frame])


class ProbabilityMarketDataService:
    def __init__(self, config=None):
        self.config = config or get_probability_config()
        self.feature_engine = FeatureEngine(self.config)
        self.regime_engine = RegimeEngine()
        self.probability_service = ProbabilityService(self.config)
        self.snapshot_repository = SnapshotRepository()
        self.prediction_repository = PredictionRepository()

    def build_current_snapshot(self):
        market = {}
        ohlcv_rows = []
        option_rows = []
        orderbook_insights = {}
        errors = {}

        try:
            market = eth_market_snapshot(include_orderbook=self.config.orderbook_enabled)
            orderbook_insights = market.get("orderbook_insights") or {}
        except Exception as exc:
            logger.exception("probability.delta.market.failed")
            errors["market"] = str(exc)

        try:
            ohlcv_rows = ohlcv_snapshot(symbol=self.config.symbol, resolution="5m", minutes_back=1440).get("rows") or []
        except Exception as exc:
            logger.exception("probability.delta.ohlcv.failed")
            errors["ohlcv"] = str(exc)

        try:
            option_rows = eth_option_chain().get("rows") or []
        except Exception as exc:
            logger.exception("probability.delta.options.failed")
            errors["options"] = str(exc)

        snapshot = self.feature_engine.build_snapshot(
            market=market,
            ohlcv=_rows_to_df(ohlcv_rows),
            option_rows=option_rows,
            orderbook_insights=orderbook_insights,
            cvd_features={},
        )
        snapshot.regime = self.regime_engine.classify(snapshot)
        snapshot.last_delta_update = datetime.now(timezone.utc)
        snapshot.metadata_json = {"errors": errors} if errors else {}
        return snapshot

    def current_intelligence(self):
        snapshot = self.build_current_snapshot()
        predictions = {
            horizon: self.probability_service.predict(snapshot, horizon)
            for horizon in self.config.horizons
        }
        return {
            "ok": True,
            "symbol": snapshot.symbol,
            "spot_price": snapshot.spot_price,
            "regime": snapshot.regime,
            "prediction_status": "STALE_DATA" if snapshot.delta_market_data_status == "STALE_DATA" else "LIVE",
            "horizons": {
                horizon: self._prediction_payload(prediction)
                for horizon, prediction in predictions.items()
            },
            "provider_health": {
                "delta_market_data_status": snapshot.delta_market_data_status,
                "orderflow_provider_status": snapshot.orderflow_provider_status,
            },
        }

    def persist_snapshot(self):
        snapshot = self.build_current_snapshot()
        saved_snapshot = self.snapshot_repository.safe_insert_returning(snapshot)
        ok = bool(saved_snapshot)
        if saved_snapshot and saved_snapshot.get("id"):
            setattr(snapshot, "id", saved_snapshot["id"])
        logger.info("probability.snapshot.created", extra={"ok": ok, "symbol": snapshot.symbol})
        return {"ok": ok, "snapshot": saved_snapshot or snapshot.to_record()}

    def persist_predictions(self):
        snapshot = self.build_current_snapshot()
        saved_snapshot = self.snapshot_repository.safe_insert_returning(snapshot)
        snapshot_id = saved_snapshot.get("id") if saved_snapshot else None
        if not snapshot_id:
            logger.error("probability.prediction.skipped", extra={"reason": "snapshot_id_missing"})
            return {"ok": False, "snapshot_saved": False, "results": [], "reason": "snapshot_id_missing"}

        setattr(snapshot, "id", snapshot_id)
        results = []
        for horizon in self.config.horizons:
            prediction = self.probability_service.predict(snapshot, horizon)
            ok = self.prediction_repository.safe_insert(prediction)
            logger.info("probability.prediction.created", extra={"ok": ok, "horizon": horizon, "snapshot_id": snapshot_id})
            results.append({"horizon": horizon, "ok": ok, "snapshot_id": snapshot_id})
        return {"ok": all(item["ok"] for item in results), "snapshot_saved": True, "snapshot_id": snapshot_id, "results": results}

    def _prediction_payload(self, prediction):
        return {
            "mean_reversion": prediction.mean_reversion_probability,
            "upside_breakout": prediction.upside_breakout_probability,
            "downside_breakdown": prediction.downside_breakdown_probability,
            "range_continuation": prediction.range_continuation_probability,
            "trend_continuation": prediction.trend_continuation_probability,
            "confidence": prediction.confidence,
            "expected_price": prediction.expected_price,
            "median_price": prediction.median_price,
            "expected_equilibrium": prediction.expected_equilibrium,
            "range_50": [prediction.range_50_lower, prediction.range_50_upper],
            "range_70": [prediction.range_70_lower, prediction.range_70_upper],
            "range_90": [prediction.range_90_lower, prediction.range_90_upper],
            "model_version": prediction.model_version,
            "range_model_version": prediction.range_model_version,
            "analogue_sample_size": prediction.analogue_sample_size,
        }
