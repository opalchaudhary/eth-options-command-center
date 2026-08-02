from fastapi import APIRouter, HTTPException, Query

from backend.services import market_data_service
from backend.services.cache import ttl_cache
from recommendation_journal import get_latest_recommendations
from backend.services.json_utils import to_jsonable
from backend.services.scheduler_service import BUSY_REFRESH_ERROR, data_refresh_jobs_running


router = APIRouter()


@router.get("/market/eth")
@ttl_cache(30)
def market_eth():
    try:
        return market_data_service.get_eth_market()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/option-chain")
@ttl_cache(60)
def option_chain(
    expiry: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=1000),
    compact: bool = Query(default=True),
    include_raw: bool = Query(default=False),
):
    try:
        return market_data_service.get_option_chain(
            expiry=expiry,
            limit=limit,
            compact=compact,
            include_raw=include_raw,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/insights")
@ttl_cache(30)
def insights(
    expiry: str | None = Query(default=None),
    compact: bool = Query(default=True),
    include_raw: bool = Query(default=False),
):
    try:
        running_jobs = data_refresh_jobs_running()
        if running_jobs:
            return {
                "ok": False,
                "error": BUSY_REFRESH_ERROR,
            }

        response = market_data_service.get_insights(expiry=expiry, compact=compact, include_raw=include_raw)
        return response
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/charts")
@ttl_cache(60)
def charts(
    expiry: str | None = Query(default=None),
    symbol: str = Query(default="ETHUSD"),
    limit: int = Query(default=300, ge=50, le=500),
    compact: bool = Query(default=True),
    include_raw: bool = Query(default=False),
):
    try:
        return market_data_service.get_charts(expiry=expiry, symbol=symbol, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/analytics")
@ttl_cache(60)
def analytics(
    expiry: str | None = Query(default=None),
    symbol: str = Query(default="ETHUSD"),
    limit: int = Query(default=200, ge=50, le=500),
    compact: bool = Query(default=True),
    include_raw: bool = Query(default=False),
):
    try:
        return market_data_service.get_charts(expiry=expiry, symbol=symbol, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/recommendations")
@ttl_cache(60)
def recommendations(limit: int = Query(default=25, ge=1, le=100), compact: bool = Query(default=True)):
    try:
        rows = get_latest_recommendations(limit=limit)
        return {"ok": True, "rows": to_jsonable(rows)}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
