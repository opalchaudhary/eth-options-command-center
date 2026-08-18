from fastapi import APIRouter, HTTPException, Query

from backend.services.db_diagnostics import diagnostics_payload
from probability_engine.config import get_probability_config
from probability_engine.repositories.prediction_repository import PredictionRepository
from probability_engine.services.market_data_service import ProbabilityMarketDataService
from probability_engine.services.model_registry import ModelRegistry


router = APIRouter(prefix="/api/probability", tags=["probability"])


def _service():
    return ProbabilityMarketDataService(get_probability_config())


@router.get("/health")
def probability_health():
    config = get_probability_config()
    return {
        "ok": True,
        "service": "market-probability-engine",
        "enabled": config.enabled,
        "retention_enabled": config.retention_enabled,
        "automatic_destructive_retention": False,
        "horizons": config.horizons,
    }


@router.get("/current")
def probability_current():
    try:
        return _service().current_intelligence()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/current/{horizon}")
def probability_current_horizon(horizon: str):
    payload = probability_current()
    key = horizon.upper()
    if key not in payload.get("horizons", {}):
        raise HTTPException(status_code=404, detail=f"Unsupported horizon: {horizon}")
    return {"ok": True, "symbol": payload["symbol"], "spot_price": payload["spot_price"], "regime": payload["regime"], "horizon": key, "prediction": payload["horizons"][key]}


@router.get("/ranges")
def probability_ranges():
    payload = probability_current()
    return {
        "ok": True,
        "symbol": payload["symbol"],
        "ranges": {
            horizon: {
                "range_50": data["range_50"],
                "range_70": data["range_70"],
                "range_90": data["range_90"],
                "expected_price": data["expected_price"],
                "expected_equilibrium": data["expected_equilibrium"],
            }
            for horizon, data in payload.get("horizons", {}).items()
        },
    }


@router.get("/history")
def probability_history(horizon: str | None = Query(default=None), limit: int = Query(default=25, ge=1, le=200)):
    rows = PredictionRepository().latest(horizon=horizon, limit=limit)
    return {"ok": True, "rows": rows}


@router.get("/performance")
def probability_performance():
    return {"ok": True, "rows": [], "message": "Performance rows are available after stored predictions have evaluated outcomes."}


@router.get("/calibration")
def probability_calibration():
    return {"ok": True, "rows": [], "message": "Calibration rows are available after outcome evaluation."}


@router.get("/regimes")
def probability_regimes():
    return {"ok": True, "regimes": ["LOW_VOL_RANGE", "NORMAL_RANGE", "TREND_UP", "TREND_DOWN", "HIGH_VOL", "VOLATILITY_EXPANSION", "EXTREME_FUNDING", "LIQUIDATION_STYLE_MOVE", "UNKNOWN"]}


@router.get("/analogues")
def probability_analogues():
    return {"ok": True, "rows": [], "message": "Historical analogue endpoint is scaffolded; requires backfilled probability snapshots."}


@router.get("/strikes")
def probability_strikes():
    return {"ok": True, "rows": [], "message": "Strike optimizer service is implemented; persistence waits for probability migration deployment."}


@router.get("/strikes/{expiry}")
def probability_strikes_expiry(expiry: str):
    return {"ok": True, "expiry": expiry, "rows": [], "message": "No persisted strike recommendations yet."}


@router.get("/models")
def probability_models():
    config = get_probability_config()
    registry = ModelRegistry(config)
    return {"ok": True, "champion": registry.champion().__dict__, "challengers": registry.challengers(), "auto_promotion": False}


@router.get("/storage-stats")
def probability_storage_stats():
    diagnostics = diagnostics_payload()
    return {
        "ok": True,
        "automatic_destructive_retention": False,
        "retention_enabled": get_probability_config().retention_enabled,
        "tables": [
            "probability_market_snapshots",
            "probability_predictions",
            "probability_outcomes",
            "probability_model_performance",
            "probability_calibration",
            "option_strike_recommendations",
            "option_strike_outcomes",
        ],
        "diagnostics": diagnostics,
    }
