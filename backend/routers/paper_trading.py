from fastapi import APIRouter, HTTPException

from backend.services import paper_trading_service
from backend.services.cache import ttl_cache


router = APIRouter(prefix="/paper-trading", tags=["paper-trading"])


@router.get("/status")
@ttl_cache(30)
def paper_trading_status(limit: int = 50, compact: bool = True, include_raw: bool = False):
    try:
        return paper_trading_service.status(limit=limit, compact=compact, include_raw=include_raw)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/run-cycle")
def paper_trading_run_cycle():
    try:
        return paper_trading_service.run_cycle()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
