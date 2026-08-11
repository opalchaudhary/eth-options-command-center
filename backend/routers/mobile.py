import hmac

from fastapi import APIRouter, Depends, Header, HTTPException, status

from backend import config
from backend.services import mobile_service


def _unauthorized(message="Missing or invalid mobile API token."):
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=mobile_service.error_payload("UNAUTHORIZED", message),
        headers={"WWW-Authenticate": "Bearer"},
    )


def require_mobile_token(authorization: str | None = Header(default=None)):
    expected = config.get_config_value("MOBILE_API_TOKEN")
    if not expected:
        _unauthorized("Mobile API token is not configured.")

    if not authorization or not authorization.startswith("Bearer "):
        _unauthorized()

    supplied = authorization.removeprefix("Bearer ").strip()
    if not supplied or not hmac.compare_digest(supplied, str(expected)):
        _unauthorized()

    return True


router = APIRouter(prefix="/mobile", tags=["mobile"], dependencies=[Depends(require_mobile_token)])


@router.get("/health")
def mobile_health():
    return {
        "ok": True,
        "service": "deltaforge-mobile-api",
        "timestamp": mobile_service.utc_timestamp(),
        "version": "1",
    }


@router.get("/home")
def mobile_home():
    try:
        return mobile_service.get_mobile_home()
    except Exception:
        return mobile_service.error_payload("HOME_UNAVAILABLE", "Unable to retrieve mobile dashboard.")


@router.get("/subwallets")
def mobile_subwallets():
    try:
        return mobile_service.get_mobile_subwallets()
    except Exception:
        return mobile_service.error_payload("SUBWALLETS_UNAVAILABLE", "Unable to retrieve account snapshot.")


@router.get("/iron-fly")
def mobile_iron_fly():
    try:
        return mobile_service.get_mobile_iron_fly()
    except Exception:
        return mobile_service.error_payload("IRON_FLY_UNAVAILABLE", "Unable to retrieve Iron Fly recommendation.")
