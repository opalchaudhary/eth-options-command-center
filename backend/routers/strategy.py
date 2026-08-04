from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from futures_covered_engine import build_futures_covered_recommendation
from iron_fly_engine import build_iron_fly_recommendation
from strategy_recommendation_repository import latest_strategy_history, maybe_save_recommendation


router = APIRouter(prefix="/strategy", tags=["strategy"])


class RecalculateRequest(BaseModel):
    module: str = "all"
    persist: bool = True


def _with_persistence(result, persist):
    result = dict(result)
    result["persistence"] = maybe_save_recommendation(result, persist=persist)
    return result


@router.get("/futures/latest")
def futures_latest(persist: bool = Query(default=False)):
    try:
        result = build_futures_covered_recommendation()
        return _with_persistence(result, persist)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/covered/latest")
def covered_latest(persist: bool = Query(default=False)):
    try:
        result = build_futures_covered_recommendation()
        return _with_persistence(result, persist)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/iron-fly/latest")
def iron_fly_latest(persist: bool = Query(default=False)):
    try:
        result = build_iron_fly_recommendation()
        return _with_persistence(result, persist)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/history")
def strategy_history(
    engine_name: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
):
    try:
        return {"ok": True, "rows": latest_strategy_history(engine_name=engine_name, limit=limit)}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/recalculate")
def recalculate(request: RecalculateRequest):
    try:
        if request.module == "futures_covered":
            return _with_persistence(build_futures_covered_recommendation(), request.persist)
        if request.module == "iron_fly":
            return _with_persistence(build_iron_fly_recommendation(), request.persist)
        return {
            "ok": True,
            "futures_covered": _with_persistence(build_futures_covered_recommendation(), request.persist),
            "iron_fly": _with_persistence(build_iron_fly_recommendation(), request.persist),
        }
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
