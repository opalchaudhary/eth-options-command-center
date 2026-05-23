from fastapi import APIRouter, HTTPException

from backend.services import paper_trading_service


router = APIRouter(prefix="/paper-trading", tags=["paper-trading"])


@router.get("/status")
def paper_trading_status():
    try:
        return paper_trading_service.status()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/run-cycle")
def paper_trading_run_cycle():
    try:
        return paper_trading_service.run_cycle()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

