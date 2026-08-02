from fastapi import APIRouter, HTTPException

from backend.services import futures_trading_service
from backend.services.cache import ttl_cache


router = APIRouter(prefix="/futures-trading", tags=["futures-trading"])


@router.get("/status")
@ttl_cache(30)
def futures_trading_status(limit: int = 50, compact: bool = True, include_raw: bool = False):
    try:
        return futures_trading_service.status(limit=limit, compact=compact, include_raw=include_raw)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/run-cycle")
def futures_trading_run_cycle():
    try:
        return futures_trading_service.run_cycle()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
