from __future__ import annotations

import json
import logging
import pickle
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

import database_reader
from probability_engine.config import HORIZON_MINUTES, get_probability_config
from probability_engine.repositories.base_repository import SupabaseRepository
from probability_engine.repositories.prediction_repository import PredictionRepository
from probability_engine.research.step11_indicators import add_indicators
from probability_engine.research.step13_feature_bridge import add_expanding_interaction_features, prepare_point_in_time_features
from probability_engine.research.step14_challenger import apply_platt, predict_model
from probability_engine.research.step15_spec import file_sha256, validate_manifest


logger = logging.getLogger(__name__)

MODEL_DIR = Path(__file__).resolve().parents[1] / "models" / "v2_candidate_v1"
MANIFEST_PATH = MODEL_DIR / "manifest.json"
MODEL_VERSION = "probability_v2_candidate_v1"
FEATURE_VERSION = "probability_v2_features_v1"
LABEL_VERSION = "label_v2"
CALIBRATION_VERSION = "calibration_v2_candidate_v1"
SYMBOL = "ETHUSD"
DIRECT_FEATURES = [
    "atr_slope_96b",
    "rv_slope_96b",
    "atr_pct_12b",
    "bollinger_bandwidth_20b",
    "keltner_width_20b",
    "rv_12b",
    "price_efficiency_96b",
    "rolling_vwap_z_96b",
    "donchian_pos_96b",
    "volume_z_96b",
]
INTERACTION_FEATURES = [
    "high_atr_pct_and_rising_atr_slope",
    "ema_spread_high_and_rising_atr",
    "low_vwap_displacement_and_low_atr",
    "high_vwap_displacement_and_rising_atr",
    "low_atr_and_mid_donchian",
    "rising_atr_and_high_volume",
    "low_atr_pct_and_falling_atr_slope",
    "low_adx_and_falling_atr",
]
CONSTITUENTS = sorted(
    set(
        DIRECT_FEATURES
        + [
            "ema_spread_atr_12_48b",
            "adx_48b",
        ]
    )
)


class V2FeatureSnapshotRepository(SupabaseRepository):
    table_name = "probability_v2_feature_snapshots"


class V2ShadowPredictionRepository(SupabaseRepository):
    table_name = "probability_v2_shadow_predictions"

    def insert_many_returning(self, payloads):
        base_insert_many = getattr(super(), "insert_many_returning", None)
        if base_insert_many is not None:
            return base_insert_many(payloads)
        inserted = []
        for payload in payloads:
            row = self.safe_insert_returning(payload)
            if row:
                inserted.append(row)
        return inserted

    def latest(self, symbol=SYMBOL, limit=200):
        return self.read(
            params={
                "symbol": f"eq.{symbol}",
                "order": "prediction_timestamp.desc,target.asc,horizon.asc",
                "limit": str(limit),
            }
        )


@dataclass
class V2ShadowResult:
    ok: bool
    enabled: bool
    action: str
    direct_predictions: int = 0
    derived_predictions: int = 0
    abstentions: int = 0
    ood_count: int = 0
    feature_snapshot_id: str | None = None
    duration_seconds: float | None = None
    reason: str | None = None
    errors: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def _records(rows):
    if rows is None:
        return []
    if hasattr(rows, "empty") and hasattr(rows, "to_dict"):
        return [] if rows.empty else rows.to_dict("records")
    if isinstance(rows, dict):
        return [rows]
    return list(rows or [])


@lru_cache(maxsize=4)
def _load_manifest_cached(path: str) -> dict:
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    errors = validate_manifest(manifest)
    if errors:
        raise ValueError(f"Invalid V2 manifest: {errors}")
    return manifest


def load_manifest(path: Path = MANIFEST_PATH) -> dict:
    return _load_manifest_cached(str(path))


@lru_cache(maxsize=64)
def _file_sha256_cached(path: str) -> str:
    return file_sha256(Path(path))


def _resolve_model_artifact_path(artifact_path: str) -> Path:
    path = Path(artifact_path)
    if not path.is_absolute():
        candidate = Path(__file__).resolve().parents[2] / path
        if not candidate.exists():
            candidate = MODEL_DIR / Path(artifact_path).name
        path = candidate
    return path


@lru_cache(maxsize=64)
def _load_model_bundle_cached(path: str, expected_hash: str, model_id: str) -> dict:
    artifact_path = Path(path)
    actual_hash = _file_sha256_cached(str(artifact_path))
    if actual_hash != expected_hash:
        raise ValueError(f"Model artifact hash mismatch for {model_id}")
    with artifact_path.open("rb") as handle:
        return pickle.load(handle)


def load_model_bundle(model_record: dict) -> dict:
    path = Path(model_record["artifact_path"])
    path = _resolve_model_artifact_path(str(path))
    return _load_model_bundle_cached(str(path), model_record["artifact_sha256"], model_record["model_id"])


class V2ShadowEngine:
    def __init__(
        self,
        config=None,
        feature_repository=None,
        prediction_repository=None,
        manifest_path: Path = MANIFEST_PATH,
    ):
        self.config = config or get_probability_config()
        self.feature_repository = feature_repository or V2FeatureSnapshotRepository()
        self.prediction_repository = prediction_repository or V2ShadowPredictionRepository()
        self.manifest_path = manifest_path
        self.manifest = load_manifest(manifest_path)
        self.models = [model for model in self.manifest["models"]]
        self.derived_outputs = self.manifest.get("derived_outputs", [])

    def run_shadow_prediction(self, now: datetime | None = None, persist: bool = True, force_disabled: bool = False) -> dict:
        if not self.config.v2_shadow_enabled and not force_disabled:
            return V2ShadowResult(ok=True, enabled=False, action="DISABLED", reason="PROBABILITY_V2_SHADOW_ENABLED=false").to_dict()
        started = time.monotonic()
        now = now or datetime.now(timezone.utc)
        try:
            payload = self.build_shadow_payload(now)
            if not persist:
                payload["duration_seconds"] = round(time.monotonic() - started, 3)
                return payload
            saved_snapshot = self.feature_repository.safe_insert_returning(payload["feature_snapshot"])
            if not saved_snapshot or not saved_snapshot.get("id"):
                return V2ShadowResult(ok=False, enabled=True, action="FAILED", reason="feature_snapshot_insert_failed").to_dict()
            snapshot_id = saved_snapshot["id"]
            predictions = [{**row, "feature_snapshot_id": snapshot_id} for row in payload["predictions"]]
            inserted = self.prediction_repository.insert_many_returning(predictions) if predictions else []
            created = len(inserted or [])
            return V2ShadowResult(
                ok=created == len(predictions),
                enabled=True,
                action="PERSISTED",
                direct_predictions=sum(1 for row in predictions if not row.get("derived")),
                derived_predictions=sum(1 for row in predictions if row.get("derived")),
                abstentions=sum(1 for row in predictions if row.get("abstained")),
                ood_count=sum(1 for row in predictions if row.get("ood_status") == "FLAGGED"),
                feature_snapshot_id=snapshot_id,
                duration_seconds=round(time.monotonic() - started, 3),
            ).to_dict()
        except Exception as exc:
            logger.exception("probability.v2.shadow.failed")
            return V2ShadowResult(ok=False, enabled=True, action="FAILED", reason=str(exc), errors=[str(exc)]).to_dict()

    def build_shadow_payload(self, now: datetime) -> dict:
        prediction_timestamp = pd.Timestamp(now).tz_convert("UTC").floor("5min")
        ohlcv = load_ohlcv_from_supabase(
            symbol=self.config.symbol,
            end_at=prediction_timestamp,
            days=max(30, int(self.config.v2_shadow_history_days)),
        )
        features, feature_source_cutoff = compute_v2_features_asof(ohlcv, prediction_timestamp)
        regime = infer_regime(features)
        ood = evaluate_ood(features)
        snapshot_abstention = None
        if feature_source_cutoff is None or feature_source_cutoff > prediction_timestamp:
            snapshot_abstention = "AS_OF_VIOLATION"
        if features[DIRECT_FEATURES].isna().any(axis=None):
            snapshot_abstention = snapshot_abstention or "MISSING_REQUIRED_FEATURE"
        feature_vector = features.iloc[0].to_dict()
        range_references = load_v1_range_references(self.config.symbol)
        feature_snapshot = {
            "prediction_timestamp": prediction_timestamp.isoformat(),
            "symbol": self.config.symbol,
            "record_type": "LIVE",
            "model_version": MODEL_VERSION,
            "feature_version": FEATURE_VERSION,
            "manifest_hash": self.manifest_hash,
            "feature_source_cutoff": feature_source_cutoff.isoformat() if feature_source_cutoff is not None else None,
            "regime": regime,
            "feature_vector_json": jsonable(feature_vector),
            "threshold_metadata_json": {"threshold_semantics": self.manifest.get("source", {}).get("step13_dataset_hash"), "expanding_history_rows": int(len(ohlcv))},
            "ood_status": ood["status"],
            "ood_reason": ood["reason"],
            "ood_feature_count": ood["feature_count"],
            "abstained": bool(snapshot_abstention),
            "abstention_reason": snapshot_abstention,
            "metadata_json": {
                "manifest_path": str(self.manifest_path),
                "model_count": len(self.models),
                "derived_output_count": len(self.derived_outputs),
                "research_only_shadow": True,
            },
        }
        direct_rows = []
        probabilities_by_semantic = {}
        for model_record in self.models:
            row = self._prediction_row(model_record, features, prediction_timestamp, feature_source_cutoff, regime, ood, snapshot_abstention, range_references.get(model_record["horizon"]))
            direct_rows.append(row)
            probabilities_by_semantic[model_record["semantic_name"]] = row.get("calibrated_probability")
        derived_rows = [
            self._derived_row(record, probabilities_by_semantic, prediction_timestamp, feature_source_cutoff, regime, ood, snapshot_abstention, range_references.get(record["horizon"]))
            for record in self.derived_outputs
        ]
        return {"feature_snapshot": feature_snapshot, "predictions": direct_rows + derived_rows}

    @property
    def manifest_hash(self) -> str:
        return _file_sha256_cached(str(self.manifest_path))

    def _prediction_row(self, model_record, features, prediction_timestamp, feature_source_cutoff, regime, ood, snapshot_abstention, range_reference=None):
        raw_probability = None
        calibrated_probability = None
        abstention_reason = snapshot_abstention
        if requires_range_reference(model_record["target"]) and not range_reference:
            abstention_reason = abstention_reason or "MISSING_RANGE_REFERENCE"
        try:
            bundle = load_model_bundle(model_record)
            missing = [feature for feature in bundle["features"] if pd.isna(features.iloc[0].get(feature))]
            if missing:
                abstention_reason = "MISSING_REQUIRED_FEATURE"
            else:
                x = bundle["preprocessor"].transform(features[bundle["features"]])
                raw_probability = float(predict_model(bundle["model"], x)[0])
                calibrated = apply_platt(np.array([raw_probability]), bundle.get("platt_params"))[0]
                calibrated_probability = float(calibrated)
        except Exception as exc:
            abstention_reason = "MODEL_ARTIFACT_ERROR"
            logger.exception("probability.v2.model_failed", extra={"model_id": model_record.get("model_id")})
        return {
            "prediction_timestamp": prediction_timestamp.isoformat(),
            "symbol": self.config.symbol,
            "record_type": "LIVE",
            "model_version": MODEL_VERSION,
            "model_id": model_record["model_id"],
            "target": model_record["target"],
            "horizon": model_record["horizon"],
            "raw_probability": clamp_probability(raw_probability),
            "calibrated_probability": clamp_probability(calibrated_probability),
            "feature_version": FEATURE_VERSION,
            "label_version": LABEL_VERSION,
            "calibration_version": CALIBRATION_VERSION,
            "regime": regime,
            "historical_quality_grade": model_record.get("quality_grade"),
            "derived": False,
            "derived_from_model_id": None,
            "abstained": bool(abstention_reason),
            "abstention_reason": abstention_reason,
            "ood_status": ood["status"],
            "ood_reason": ood["reason"],
            "ood_feature_count": ood["feature_count"],
            "feature_source_cutoff": feature_source_cutoff.isoformat() if feature_source_cutoff is not None else None,
            "manifest_hash": self.manifest_hash,
            "model_artifact_hash": model_record.get("artifact_sha256"),
            "metadata_json": {
                "semantic_name": model_record.get("semantic_name"),
                "semantic_description": model_record.get("semantic_description"),
                "strategy_relevance": model_record.get("strategy_relevance"),
                "feature_set": model_record.get("feature_set"),
                "model_family": model_record.get("model_family"),
                "calibration": model_record.get("calibration"),
                "probability_quality_is_not_confidence": True,
                **range_reference_metadata(range_reference),
            },
        }

    def _derived_row(self, record, probabilities_by_semantic, prediction_timestamp, feature_source_cutoff, regime, ood, snapshot_abstention, range_reference=None):
        source_name = record["derivation"].replace("1 - ", "")
        source_probability = probabilities_by_semantic.get(source_name)
        abstention_reason = snapshot_abstention
        if requires_range_reference(record["target"]) and not range_reference:
            abstention_reason = abstention_reason or "MISSING_RANGE_REFERENCE"
        if source_probability is None:
            abstention_reason = abstention_reason or "MISSING_DERIVED_SOURCE"
        probability = None if source_probability is None else 1.0 - float(source_probability)
        return {
            "prediction_timestamp": prediction_timestamp.isoformat(),
            "symbol": self.config.symbol,
            "record_type": "LIVE",
            "model_version": MODEL_VERSION,
            "model_id": f"{MODEL_VERSION}__derived__{record['target']}__{record['horizon']}".lower(),
            "target": record["target"],
            "horizon": record["horizon"],
            "raw_probability": clamp_probability(probability),
            "calibrated_probability": clamp_probability(probability),
            "feature_version": FEATURE_VERSION,
            "label_version": LABEL_VERSION,
            "calibration_version": CALIBRATION_VERSION,
            "regime": regime,
            "historical_quality_grade": "DERIVED",
            "derived": True,
            "derived_from_model_id": source_name,
            "abstained": bool(abstention_reason),
            "abstention_reason": abstention_reason,
            "ood_status": ood["status"],
            "ood_reason": ood["reason"],
            "ood_feature_count": ood["feature_count"],
            "feature_source_cutoff": feature_source_cutoff.isoformat() if feature_source_cutoff is not None else None,
            "manifest_hash": self.manifest_hash,
            "model_artifact_hash": None,
            "metadata_json": {
                "semantic_name": record.get("semantic_name"),
                "semantic_description": record.get("semantic_description"),
                "derived": True,
                "derived_from": source_name,
                "complement_tolerance": 1e-12,
                **range_reference_metadata(range_reference),
            },
        }


def load_ohlcv_from_supabase(symbol=SYMBOL, end_at=None, days=120) -> pd.DataFrame:
    end = pd.Timestamp(end_at or datetime.now(timezone.utc)).tz_convert("UTC")
    start = end - pd.Timedelta(days=days)
    rows = []
    page_start = start
    url = f"{database_reader.SUPABASE_URL}/rest/v1/eth_ohlcv"
    for _page in range(max(1, int(days) + 2)):
        response = requests.get(
            url,
            headers=database_reader.HEADERS,
            params={
                "select": "symbol,resolution,candle_time,epoch_time,open,high,low,close,volume",
                "symbol": f"eq.{symbol}",
                "resolution": "eq.5m",
                "candle_time": f"gte.{page_start.isoformat()}",
                "order": "candle_time.asc",
                "limit": "1000",
            },
            timeout=20,
        )
        if response.status_code != 200:
            logger.warning("probability.v2.ohlcv_page_failed", extra={"status_code": response.status_code, "body": response.text[:300]})
            break
        page_rows = response.json()
        if not page_rows:
            break
        rows.extend(page_rows)
        last_time = pd.Timestamp(page_rows[-1]["candle_time"]).tz_convert("UTC")
        if last_time >= end or len(page_rows) < 1000:
            break
        page_start = last_time + pd.Timedelta(microseconds=1)
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["candle_time"] = pd.to_datetime(frame["candle_time"], utc=True)
    frame = frame[frame["candle_time"] <= end].copy()
    return frame.sort_values("candle_time").drop_duplicates("candle_time").reset_index(drop=True)


def load_v1_range_references(symbol=SYMBOL) -> dict[str, dict[str, Any]]:
    repository = PredictionRepository()
    try:
        rows = _records(repository.latest(symbol=symbol, limit=50, record_type="LIVE"))
    except TypeError:
        rows = _records(repository.latest(symbol=symbol, limit=50))
    refs = {}
    for row in rows:
        if row.get("record_type") and row.get("record_type") != "LIVE":
            continue
        if row.get("model_version") != "probability_v1":
            continue
        horizon = str(row.get("horizon") or "").upper()
        if horizon in refs:
            continue
        refs[horizon] = {
            "range_70_lower": row.get("range_70_lower"),
            "range_70_upper": row.get("range_70_upper"),
            "range_model_version": row.get("range_model_version"),
            "range_reference_prediction_id": row.get("id"),
            "range_reference_created_at": row.get("created_at"),
            "range_reference_model_version": row.get("model_version"),
        }
    return refs


def range_reference_metadata(range_reference: dict[str, Any] | None) -> dict[str, Any]:
    if not range_reference:
        return {"range_reference_status": "MISSING"}
    return {"range_reference_status": "OK", **range_reference}


def requires_range_reference(target: str) -> bool:
    return target in {
        "path_inside_70",
        "range_breached",
        "both_side_breach",
        "upper_breach_only",
        "lower_breach_only",
        "realized_over_range_width_ge_1",
        "upside_breakout",
        "downside_breakdown",
    }


def compute_v2_features_asof(ohlcv: pd.DataFrame, prediction_timestamp: pd.Timestamp) -> tuple[pd.DataFrame, pd.Timestamp | None]:
    if ohlcv.empty:
        row = pd.DataFrame([{feature: np.nan for feature in DIRECT_FEATURES + INTERACTION_FEATURES}])
        row["prediction_timestamp"] = pd.Timestamp(prediction_timestamp).tz_convert("UTC")
        return row, None
    timestamps = pd.Series(pd.to_datetime(ohlcv["candle_time"], utc=True))
    prediction_timestamps = timestamps[timestamps <= pd.Timestamp(prediction_timestamp).tz_convert("UTC")]
    # Live semantics use every completed 5m candle as the expanding threshold history.
    feature_frame = prepare_point_in_time_features(ohlcv, prediction_timestamps, CONSTITUENTS)
    feature_frame, _thresholds = add_expanding_interaction_features(feature_frame, INTERACTION_FEATURES, min_periods=300)
    final = feature_frame[feature_frame["prediction_timestamp"] <= pd.Timestamp(prediction_timestamp).tz_convert("UTC")].tail(1)
    if final.empty:
        row = pd.DataFrame([{feature: np.nan for feature in DIRECT_FEATURES + INTERACTION_FEATURES}])
        row["prediction_timestamp"] = pd.Timestamp(prediction_timestamp).tz_convert("UTC")
        return row, None
    feature_source_cutoff = pd.Timestamp(final.iloc[0]["feature_source_timestamp"]).tz_convert("UTC")
    return final[["prediction_timestamp", "feature_source_timestamp"] + DIRECT_FEATURES + INTERACTION_FEATURES].reset_index(drop=True), feature_source_cutoff


def compute_v2_features_for_timestamps(ohlcv: pd.DataFrame, prediction_timestamps: pd.Series | list) -> pd.DataFrame:
    """Historical/as-of mode used for Step 13 parity checks.

    The expanding threshold population is exactly the supplied prediction
    timestamp grid, not every 5m candle. Live mode uses 5m inference timestamps.
    """
    timestamps = pd.Series(pd.to_datetime(prediction_timestamps, utc=True)).sort_values().drop_duplicates()
    feature_frame = prepare_point_in_time_features(ohlcv, timestamps, CONSTITUENTS)
    feature_frame, _thresholds = add_expanding_interaction_features(feature_frame, INTERACTION_FEATURES, min_periods=300)
    return feature_frame[["prediction_timestamp", "feature_source_timestamp"] + DIRECT_FEATURES + INTERACTION_FEATURES].reset_index(drop=True)


def infer_regime(features: pd.DataFrame) -> str:
    atr_pct = features.iloc[0].get("atr_pct_12b")
    atr_slope = features.iloc[0].get("atr_slope_96b")
    if pd.isna(atr_pct) or pd.isna(atr_slope):
        return "UNKNOWN"
    if atr_slope > 0.1:
        return "VOLATILITY_EXPANSION"
    if atr_pct < 0.001:
        return "LOW_VOL_RANGE"
    return "NORMAL_RANGE"


def evaluate_ood(features: pd.DataFrame) -> dict[str, Any]:
    continuous = [feature for feature in DIRECT_FEATURES if feature in features.columns]
    row = features.iloc[0]
    extreme = 0
    missing = 0
    for feature in continuous:
        value = row.get(feature)
        if pd.isna(value):
            missing += 1
            continue
        if abs(float(value)) > 50 and feature not in {"rv_12b"}:
            extreme += 1
    flagged = extreme >= 3 or missing > 0
    return {
        "status": "FLAGGED" if flagged else "OK",
        "reason": "MISSING_OR_EXTREME_FEATURES" if flagged else None,
        "feature_count": int(extreme + missing),
    }


def clamp_probability(value):
    if value is None or pd.isna(value):
        return None
    if not np.isfinite(value):
        return None
    return float(min(1.0, max(0.0, value)))


def jsonable(payload: dict[str, Any]) -> dict[str, Any]:
    result = {}
    for key, value in payload.items():
        if isinstance(value, pd.Timestamp):
            result[key] = value.isoformat()
        elif pd.isna(value) if not isinstance(value, (list, dict, tuple)) else False:
            result[key] = None
        elif isinstance(value, (np.integer, np.floating)):
            result[key] = float(value)
        else:
            result[key] = value
    return result


def shadow_health() -> dict[str, Any]:
    config = get_probability_config()
    try:
        manifest = load_manifest()
        return {
            "ok": True,
            "enabled": config.v2_shadow_enabled,
            "model_version": MODEL_VERSION,
            "feature_version": FEATURE_VERSION,
                "manifest_hash": _file_sha256_cached(str(MANIFEST_PATH)),
            "direct_model_count": len(manifest.get("models", [])),
            "derived_output_count": len(manifest.get("derived_outputs", [])),
            "model_dir": str(MODEL_DIR),
        }
    except Exception as exc:
        return {"ok": False, "enabled": config.v2_shadow_enabled, "error": str(exc)}
