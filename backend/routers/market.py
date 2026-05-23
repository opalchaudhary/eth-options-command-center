from fastapi import APIRouter, HTTPException, Query

from backend.services import market_data_service


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
def insights(expiry: str | None = Query(default=None), refresh: bool = Query(default=False)):
    try:
        refresh_result = None
        if refresh:
            refresh_result = {
                "options": market_data_service.refresh_options(expiry=expiry),
                "market_sources": market_data_service.refresh_market_sources(),
            }
        response = market_data_service.get_insights(expiry=expiry)
        if refresh_result is not None:
            response["refresh"] = refresh_result
            options_result = refresh_result.get("options") or {}
            response["expiry_count"] = options_result.get("expiry_count")
            response["row_count"] = options_result.get("row_count")
        return response
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
