from fastapi import APIRouter

from backend.services.scheduler_service import scheduler_status


router = APIRouter(prefix="/system", tags=["system"])


@router.get("/status")
def system_status():
    return scheduler_status()
