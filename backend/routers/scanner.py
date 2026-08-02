from fastapi import APIRouter, HTTPException, Query

import alt_futures_engine
from backend.services.cache import ttl_cache
from backend.services.json_utils import to_jsonable


router = APIRouter(prefix="/scanner", tags=["scanner"])


@router.get("/alt-futures")
@ttl_cache(60)
def alt_futures_status(
    limit: int = Query(default=50, ge=10, le=250),
    compact: bool = Query(default=True),
    include_raw: bool = Query(default=False),
):
    try:
        dashboard = alt_futures_engine.alt_futures_dashboard_data(run_cycle=False)
        if compact and not include_raw:
            candidates = dashboard.get("candidates") or []
            dashboard["candidates"] = [
                {
                    "symbol": item.get("symbol"),
                    "classification": item.get("classification"),
                    "direction": item.get("direction"),
                    "score": item.get("score"),
                    "price": item.get("price"),
                    "spread_pct": item.get("spread_pct"),
                    "scores": item.get("scores") or {},
                    "reason": item.get("reason"),
                }
                for item in candidates[:limit]
            ]
        return {"ok": True, "dashboard": to_jsonable(dashboard)}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
