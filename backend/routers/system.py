from fastapi import APIRouter

from backend.services.cache import ttl_cache
from backend.services.db_diagnostics import diagnostics_payload
from backend.services.scheduler_service import scheduler_status


router = APIRouter(prefix="/system", tags=["system"])


@router.get("/status")
@ttl_cache(15)
def system_status():
    return scheduler_status()


@router.get("/diagnostics")
@ttl_cache(60)
def system_diagnostics():
    return diagnostics_payload()
