from __future__ import annotations

import hashlib
import json
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

import database_reader
from probability_engine.config import get_probability_config
from probability_engine.repositories.base_repository import SupabaseRepository


logger = logging.getLogger(__name__)

MODEL_VERSION = "probability_v2_1_candidate_v1"
FEATURE_VERSION = "probability_v2_1_features_v1"
CALIBRATION_VERSION = "calibration_v2_1_candidate_v1"
LABEL_VERSION = "label_v2"
V2_MODEL_VERSION = "probability_v2_candidate_v1"
SYMBOL = "ETHUSD"
MODEL_DIR = Path(__file__).resolve().parents[1] / "models" / "v2_1_candidate_v1"
MANIFEST_PATH = MODEL_DIR / "manifest.json"
ORDERFLOW_VERSION = "rich_data_v2_orderflow_ws"
ORDERFLOW_MAX_AGE_SECONDS = 120
REQUIRED_ORDERFLOW_FEATURES = [
    "cvd_increment",
    "cvd_5m",
    "cvd_15m",
    "cvd_1h",
    "taker_buy_ratio",
    "total_volume",
]
HIGH_SCRUTINY_OUTPUTS = {("realized_over_range_width_ge_1", "12H")}


class V21ShadowPredictionRepository(SupabaseRepository):
    table_name = "probability_v2_1_shadow_predictions"

    def insert_many_returning(self, payloads):
        if not payloads:
            return []
        response = requests.post(
            f"{database_reader.SUPABASE_URL}/rest/v1/{self.table_name}",
            headers={
                **database_reader.HEADERS,
                "Content-Type": "application/json",
                "Prefer": "resolution=ignore-duplicates,return=representation",
            },
            params={"on_conflict": "prediction_timestamp,symbol,model_version,target,horizon"},
            json=payloads,
            timeout=20,
        )
        if response.status_code in [200, 201]:
            return response.json() if response.text else []
        if response.status_code == 204:
            return []
        raise RuntimeError(f"V2.1 prediction insert failed: {response.status_code} {response.text[:300]}")

    def latest(self, symbol=SYMBOL, limit=200):
        return self.read(
            params={
                "symbol": f"eq.{symbol}",
                "order": "prediction_timestamp.desc,target.asc,horizon.asc",
                "limit": str(limit),
            }
        )


class V2BaselinePredictionRepository(SupabaseRepository):
    table_name = "probability_v2_shadow_predictions"

    def latest_batch(self, symbol=SYMBOL, limit=80):
        rows = self.read(
            params={
                "select": "id,created_at,feature_snapshot_id,prediction_timestamp,symbol,record_type,model_version,model_id,target,horizon,raw_probability,calibrated_probability,feature_version,label_version,calibration_version,regime,historical_quality_grade,derived,abstained,abstention_reason,ood_status,ood_reason,ood_feature_count,feature_source_cutoff,manifest_hash,model_artifact_hash,metadata_json",
                "symbol": f"eq.{symbol}",
                "record_type": "eq.LIVE",
                "model_version": f"eq.{V2_MODEL_VERSION}",
                "order": "prediction_timestamp.desc,target.asc,horizon.asc",
                "limit": str(limit),
            }
        )
        records = _records(rows)
        if not records:
            return []
        latest_ts = max(pd.Timestamp(row["prediction_timestamp"]).tz_convert("UTC") for row in records)
        return [row for row in records if pd.Timestamp(row["prediction_timestamp"]).tz_convert("UTC") == latest_ts]


def _records(rows):
    if rows is None:
        return []
    if hasattr(rows, "empty") and hasattr(rows, "to_dict"):
        return [] if rows.empty else rows.to_dict("records")
    if isinstance(rows, dict):
        return [rows]
    return list(rows or [])


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _artifact_basename(path: str) -> str:
    return Path(str(path).replace("\\", "/")).name


def _resolve_model_path(path: str) -> Path:
    model_path = Path(__file__).resolve().parents[2] / str(path).replace("\\", "/")
    if not model_path.exists():
        model_path = MODEL_DIR / _artifact_basename(path)
    return model_path


@lru_cache(maxsize=1)
def load_manifest() -> dict[str, Any]:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if manifest.get("model_version") != MODEL_VERSION:
        raise ValueError("V2.1 manifest model_version mismatch")
    if manifest.get("feature_contract") != FEATURE_VERSION:
        raise ValueError("V2.1 manifest feature_contract mismatch")
    if manifest.get("calibration") != CALIBRATION_VERSION:
        raise ValueError("V2.1 manifest calibration mismatch")
    for model in manifest.get("models", []):
        model_path = _resolve_model_path(model["path"])
        actual = file_sha256(model_path)
        if actual != model["sha256"]:
            raise ValueError(f"V2.1 model hash mismatch for {model.get('target')}/{model.get('horizon')}")
    return manifest


@lru_cache(maxsize=4)
def load_model(target: str, horizon: str) -> dict[str, Any] | None:
    manifest = load_manifest()
    for entry in manifest.get("models", []):
        if entry.get("target") == target and str(entry.get("horizon")).upper() == horizon.upper():
            model_path = _resolve_model_path(entry["path"])
            payload = json.loads(model_path.read_text(encoding="utf-8"))
            return {**payload, "artifact_hash": entry["sha256"], "artifact_path": str(model_path)}
    return None


def clamp_probability(value: Any) -> float | None:
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value):
        return None
    return min(1.0, max(0.0, value))


def logit(p: float) -> float:
    p = min(1.0 - 1e-6, max(1e-6, float(p)))
    return math.log(p / (1.0 - p))


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def latest_orderflow_asof(prediction_timestamp: pd.Timestamp) -> tuple[dict[str, Any] | None, str | None, float | None]:
    rows = database_reader.read_supabase_table(
        "orderflow_aggregates",
        params={
            "select": "bucket_timestamp,symbol,version,total_volume,taker_buy_ratio,cvd_increment,cvd_5m,cvd_15m,cvd_1h,source_status,completeness,error_reason",
            "symbol": f"eq.{SYMBOL}",
            "version": f"eq.{ORDERFLOW_VERSION}",
            "bucket_timestamp": f"lte.{prediction_timestamp.isoformat()}",
            "order": "bucket_timestamp.desc",
            "limit": "1",
        },
    )
    records = _records(rows)
    if not records:
        return None, "ORDERFLOW_MISSING", None
    row = records[0]
    source_ts = pd.Timestamp(row.get("bucket_timestamp")).tz_convert("UTC")
    age = (prediction_timestamp - source_ts).total_seconds()
    if source_ts > prediction_timestamp:
        return row, "QUALITY_GATE_FAILED", age
    if age > ORDERFLOW_MAX_AGE_SECONDS:
        return row, "ORDERFLOW_STALE", age
    if row.get("source_status") != "COMPLETE":
        return row, "ORDERFLOW_INCOMPLETE", age
    for feature in REQUIRED_ORDERFLOW_FEATURES:
        value = row.get(feature)
        if value is None:
            return row, f"FEATURE_MISSING:{feature}", age
        try:
            if not math.isfinite(float(value)):
                return row, f"FEATURE_MISSING:{feature}", age
        except (TypeError, ValueError):
            return row, f"FEATURE_MISSING:{feature}", age
    return row, None, age


def predict_model_probability(model_payload: dict[str, Any], baseline_probability: float, orderflow: dict[str, Any]) -> float:
    model = model_payload["model"]
    values = [logit(baseline_probability)] + [float(orderflow[feature]) for feature in model["features"]]
    mu = np.asarray(model["mu"], dtype=float)
    sd = np.asarray(model["sd"], dtype=float)
    beta = np.asarray(model["beta"], dtype=float)
    x = (np.asarray(values, dtype=float) - mu) / np.where(sd == 0, 1.0, sd)
    return float(sigmoid(float(beta[0] + np.dot(x, beta[1:]))))


@dataclass(frozen=True)
class V21Inference:
    probability: float | None
    source: str
    fallback_reason: str | None
    model_artifact_hash: str | None
    feature_source_timestamp: str | None
    feature_freshness_seconds: float | None
    metadata: dict[str, Any]


class V21ShadowEngine:
    def __init__(self, baseline_repository=None, prediction_repository=None):
        self.config = get_probability_config()
        self.baseline_repository = baseline_repository or V2BaselinePredictionRepository()
        self.prediction_repository = prediction_repository or V21ShadowPredictionRepository()

    def run_shadow_prediction(
        self,
        now: datetime | None = None,
        persist: bool = True,
        force_disabled: bool = False,
    ) -> dict[str, Any]:
        started = time.monotonic()
        if not getattr(self.config, "v21_shadow_enabled", False) and not force_disabled:
            return {"ok": True, "enabled": False, "action": "DISABLED", "reason": "PROBABILITY_V2_1_SHADOW_ENABLED=false"}
        try:
            manifest = load_manifest()
            baseline_rows = self.baseline_repository.latest_batch(symbol=self.config.symbol)
            if not baseline_rows:
                return {"ok": False, "enabled": True, "action": "NO_BASELINE", "reason": "No V2.0 baseline rows available"}
            payloads = [self._prediction_payload(row, manifest) for row in baseline_rows]
            payloads = [row for row in payloads if row is not None]
            if not persist:
                return self._result(payloads, created=len(payloads), started=started, action="DRY_RUN")
            inserted = self.prediction_repository.insert_many_returning(payloads)
            return self._result(payloads, created=len(inserted or []), started=started, action="PERSISTED")
        except Exception as exc:
            logger.exception("probability.v2_1.shadow.failed")
            return {"ok": False, "enabled": True, "action": "FAILED", "reason": str(exc), "errors": [str(exc)]}

    def _prediction_payload(self, baseline: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any] | None:
        target = str(baseline.get("target"))
        horizon = str(baseline.get("horizon")).upper()
        baseline_probability = clamp_probability(baseline.get("calibrated_probability"))
        if baseline_probability is None:
            return None
        prediction_timestamp = pd.Timestamp(baseline["prediction_timestamp"]).tz_convert("UTC")
        inference = self._infer(target, horizon, baseline_probability, prediction_timestamp)
        effective_probability = clamp_probability(inference.probability)
        if effective_probability is None:
            inference = V21Inference(
                probability=baseline_probability,
                source="FALLBACK_V2_0",
                fallback_reason="OTHER_SAFE_FALLBACK",
                model_artifact_hash=None,
                feature_source_timestamp=inference.feature_source_timestamp,
                feature_freshness_seconds=inference.feature_freshness_seconds,
                metadata={**inference.metadata, "invalid_probability_fallback": True},
            )
            effective_probability = baseline_probability
        return {
            "prediction_timestamp": prediction_timestamp.isoformat(),
            "symbol": baseline.get("symbol") or self.config.symbol,
            "record_type": "LIVE",
            "model_version": MODEL_VERSION,
            "model_id": f"{MODEL_VERSION}__hybrid__{target}__{horizon.lower()}",
            "target": target,
            "horizon": horizon,
            "v2_baseline_prediction_id": baseline.get("id"),
            "v2_baseline_probability": baseline_probability,
            "effective_probability": effective_probability,
            "inference_source": inference.source,
            "fallback_reason": inference.fallback_reason,
            "feature_version": FEATURE_VERSION,
            "label_version": LABEL_VERSION,
            "calibration_version": CALIBRATION_VERSION,
            "rich_family": "orderflow" if (target, horizon) in rich_champions() else None,
            "feature_source_timestamp": inference.feature_source_timestamp,
            "feature_freshness_seconds": inference.feature_freshness_seconds,
            "model_artifact_hash": inference.model_artifact_hash,
            "manifest_hash": manifest.get("sha256"),
            "high_scrutiny": (target, horizon) in HIGH_SCRUTINY_OUTPUTS,
            "metadata_json": {
                "shadow_only": True,
                "zero_trading_authority": True,
                "v2_model_version": baseline.get("model_version"),
                "v2_model_id": baseline.get("model_id"),
                "v2_manifest_hash": baseline.get("manifest_hash"),
                "v2_feature_snapshot_id": baseline.get("feature_snapshot_id"),
                "orderflow_required_features": REQUIRED_ORDERFLOW_FEATURES if (target, horizon) in rich_champions() else [],
                "inference_metadata": inference.metadata,
            },
        }

    def _infer(self, target: str, horizon: str, baseline_probability: float, prediction_timestamp: pd.Timestamp) -> V21Inference:
        if (target, horizon) not in rich_champions():
            return V21Inference(
                probability=baseline_probability,
                source="FALLBACK_V2_0",
                fallback_reason=None,
                model_artifact_hash=None,
                feature_source_timestamp=None,
                feature_freshness_seconds=None,
                metadata={"champion": "V2_0_FALLBACK_OUTPUT"},
            )
        try:
            model = load_model(target, horizon)
            if model is None:
                return self._fallback(baseline_probability, "MODEL_LOAD_FAILURE")
        except Exception as exc:
            return self._fallback(baseline_probability, "MODEL_LOAD_FAILURE", {"error": str(exc)})
        orderflow, reason, age = latest_orderflow_asof(prediction_timestamp)
        source_ts = orderflow.get("bucket_timestamp") if orderflow else None
        if reason:
            return self._fallback(
                baseline_probability,
                reason,
                {"orderflow": orderflow or {}, "required_features": REQUIRED_ORDERFLOW_FEATURES},
                source_ts,
                age,
            )
        try:
            probability = predict_model_probability(model, baseline_probability, orderflow or {})
        except Exception as exc:
            return self._fallback(baseline_probability, "OTHER_SAFE_FALLBACK", {"error": str(exc)}, source_ts, age)
        return V21Inference(
            probability=probability,
            source="RICH_V2_1",
            fallback_reason=None,
            model_artifact_hash=model.get("artifact_hash"),
            feature_source_timestamp=source_ts,
            feature_freshness_seconds=age,
            metadata={
                "feature_values": {feature: orderflow.get(feature) for feature in REQUIRED_ORDERFLOW_FEATURES},
                "model_path": model.get("artifact_path"),
            },
        )

    def _fallback(self, baseline_probability, reason, metadata=None, source_ts=None, age=None) -> V21Inference:
        return V21Inference(
            probability=baseline_probability,
            source="FALLBACK_V2_0",
            fallback_reason=reason,
            model_artifact_hash=None,
            feature_source_timestamp=source_ts,
            feature_freshness_seconds=age,
            metadata=metadata or {},
        )

    def _result(self, payloads, created, started, action):
        rich_count = sum(1 for row in payloads if row.get("inference_source") == "RICH_V2_1")
        fallback_count = sum(1 for row in payloads if row.get("inference_source") == "FALLBACK_V2_0")
        fallback_reasons: dict[str, int] = {}
        for row in payloads:
            reason = row.get("fallback_reason") or "V2_0_CHAMPION"
            if row.get("inference_source") == "FALLBACK_V2_0":
                fallback_reasons[reason] = fallback_reasons.get(reason, 0) + 1
        probabilities = [row.get("effective_probability") for row in payloads]
        invalid = [p for p in probabilities if p is None or not math.isfinite(float(p)) or not (0 <= float(p) <= 1)]
        return {
            "ok": not invalid,
            "enabled": True,
            "action": action,
            "candidate_count": len(payloads),
            "created_count": int(created),
            "rich_count": rich_count,
            "fallback_count": fallback_count,
            "fallback_reasons": fallback_reasons,
            "invalid_probability_count": len(invalid),
            "prediction_timestamp": payloads[0]["prediction_timestamp"] if payloads else None,
            "duration_seconds": round(time.monotonic() - started, 3),
        }


def rich_champions() -> set[tuple[str, str]]:
    return {
        ("realized_over_range_width_ge_1", "12H"),
        ("up_excursion_ge_1_0_atr", "1H"),
    }


def shadow_health() -> dict[str, Any]:
    config = get_probability_config()
    try:
        manifest = load_manifest()
        return {
            "ok": True,
            "enabled": getattr(config, "v21_shadow_enabled", False),
            "model_version": MODEL_VERSION,
            "feature_version": FEATURE_VERSION,
            "calibration_version": CALIBRATION_VERSION,
            "manifest_sha256": manifest.get("sha256"),
            "model_count": len(manifest.get("models", [])),
            "rich_champions": sorted([f"{target}/{horizon}" for target, horizon in rich_champions()]),
            "shadow_only": True,
            "zero_trading_authority": True,
        }
    except Exception as exc:
        return {"ok": False, "enabled": getattr(config, "v21_shadow_enabled", False), "error": str(exc)}
