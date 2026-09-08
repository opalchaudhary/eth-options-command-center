from __future__ import annotations

from fastapi import APIRouter, Request, Response, status
from pydantic import BaseModel

from backend.auth import SESSION_COOKIE_NAME, SESSION_EXPIRY_HOURS, auth_store, token_from_request


router = APIRouter(prefix="/api/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/login")
def login(payload: LoginRequest, request: Request, response: Response):
    try:
        session = auth_store.authenticate(payload.username, payload.password, request.client.host if request.client else None)
    except Exception:
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return {"ok": False, "detail": "Invalid username or password."}
    response.set_cookie(
        SESSION_COOKIE_NAME,
        session.token,
        max_age=SESSION_EXPIRY_HOURS * 60 * 60,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
    )
    return {
        "ok": True,
        "user": session.user,
        "session_token": session.token,
        "expires_at": session.expires_at,
        "expires_in_seconds": SESSION_EXPIRY_HOURS * 60 * 60,
    }


@router.post("/logout")
def logout(request: Request, response: Response):
    token = token_from_request(request)
    if token:
        try:
            auth_store.revoke_session(token)
        except Exception:
            pass
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")
    return {"ok": True}


@router.get("/me")
def me(request: Request):
    return {"ok": True, "user": getattr(request.state, "deltaforge_user", None)}

