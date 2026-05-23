from fastapi import APIRouter, HTTPException, Query

from backend.services import market_data_service
from backend.services.scheduler_service import BUSY_REFRESH_ERROR, data_refresh_jobs_running


router = APIRouter()


@router.get("/market/eth")
def market_eth():
    try:
        return market_data_service.get_eth_market()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/option-chain")
def option_chain(expiry: str | None = Query(default=None)):
    try:
        return market_data_service.get_option_chain(expiry=expiry)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get("/insights")
def insights(expiry: str | None = Query(default=None)):
    try:
        running_jobs = data_refresh_jobs_running()
        if running_jobs:
            return {
                "ok": False,
                "error": BUSY_REFRESH_ERROR,
            }

        response = market_data_service.get_insights(expiry=expiry)
        return response
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
