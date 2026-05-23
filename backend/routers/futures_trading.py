from fastapi import APIRouter, HTTPException

from backend.services import futures_trading_service


router = APIRouter(prefix="/futures-trading", tags=["futures-trading"])


@router.get("/status")
def futures_trading_status():
    try:
        return futures_trading_service.status()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/run-cycle")
def futures_trading_run_cycle():
    try:
        return futures_trading_service.run_cycle()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

