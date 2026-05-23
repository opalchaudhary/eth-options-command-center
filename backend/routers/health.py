from fastapi import APIRouter

from backend.services.delta_client import delta_credentials_status
from backend.services.supabase_client import supabase_status


router = APIRouter()


@router.get("/health")
def health():
    return {
        "ok": True,
        "service": "eth-options-command-center-api",
        "delta": delta_credentials_status(),
        "supabase": supabase_status(),
    }

