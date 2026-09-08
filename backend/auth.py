from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from backend.config import SUPABASE_KEY, SUPABASE_URL


SESSION_COOKIE_NAME = "deltaforge_session"
SESSION_HEADER_NAME = "Authorization"
SESSION_EXPIRY_HOURS = int(os.getenv("DELTAFORGE_SESSION_EXPIRY_HOURS", "12"))
PASSWORD_HASH_ITERATIONS = int(os.getenv("DELTAFORGE_PASSWORD_HASH_ITERATIONS", "390000"))
FAILED_LOGIN_LIMIT = int(os.getenv("DELTAFORGE_FAILED_LOGIN_LIMIT", "5"))
FAILED_LOGIN_WINDOW_SECONDS = int(os.getenv("DELTAFORGE_FAILED_LOGIN_WINDOW_SECONDS", "900"))
FAILED_LOGIN_COOLDOWN_SECONDS = int(os.getenv("DELTAFORGE_FAILED_LOGIN_COOLDOWN_SECONDS", "300"))
PUBLIC_AUTH_PATHS = {
    ("GET", "/health"),
    ("POST", "/api/auth/login"),
    ("POST", "/api/auth/logout"),
}
PUBLIC_PATH_PREFIXES = ("/mobile",)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_timestamp(value: datetime | None = None) -> str:
    return (value or utc_now()).isoformat()


def hash_password(password: str, *, iterations: int = PASSWORD_HASH_ITERATIONS) -> str:
    if not password:
        raise ValueError("Password must not be empty.")
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return "pbkdf2_sha256${iterations}${salt}${digest}".format(
        iterations=iterations,
        salt=base64.urlsafe_b64encode(salt).decode("ascii").rstrip("="),
        digest=base64.urlsafe_b64encode(digest).decode("ascii").rstrip("="),
    )


def _b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def verify_password(password: str, password_hash: str) -> bool:
    try:
        scheme, iterations, salt, expected = str(password_hash or "").split("$", 3)
        if scheme != "pbkdf2_sha256":
            return False
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), _b64decode(salt), int(iterations))
        return hmac.compare_digest(base64.urlsafe_b64encode(digest).decode("ascii").rstrip("="), expected)
    except Exception:
        return False


def session_token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def token_from_request(request: Request) -> str | None:
    authorization = request.headers.get(SESSION_HEADER_NAME) or ""
    if authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
        if token:
            return token
    cookie_token = request.cookies.get(SESSION_COOKIE_NAME)
    return cookie_token or None


@dataclass(frozen=True)
class AuthSession:
    token: str
    expires_at: str
    user: dict[str, Any]


class SupabaseAuthStore:
    def __init__(self, supabase_url: str | None = None, supabase_key: str | None = None):
        self.supabase_url = (supabase_url or SUPABASE_URL or "").rstrip("/")
        self.supabase_key = supabase_key or SUPABASE_KEY or ""

    @property
    def enabled(self) -> bool:
        return bool(self.supabase_url and self.supabase_key)

    def _headers(self, prefer: str | None = None) -> dict[str, str]:
        headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
        }
        if prefer:
            headers["Prefer"] = prefer
        return headers

    def _url(self, table: str) -> str:
        if not self.enabled:
            raise RuntimeError("Supabase auth storage is not configured.")
        return f"{self.supabase_url}/rest/v1/{table}"

    def _select(self, table: str, params: dict[str, str]) -> list[dict[str, Any]]:
        response = requests.get(self._url(table), headers=self._headers(), params=params, timeout=15)
        if response.status_code != 200:
            raise RuntimeError(f"Supabase auth select failed for {table}: {response.status_code}")
        return response.json()

    def _insert(self, table: str, payload: dict[str, Any], *, returning: bool = False) -> list[dict[str, Any]]:
        response = requests.post(
            self._url(table),
            headers=self._headers("return=representation" if returning else "return=minimal"),
            json=payload,
            timeout=15,
        )
        if response.status_code not in {200, 201, 204}:
            raise RuntimeError(f"Supabase auth insert failed for {table}: {response.status_code}")
        return response.json() if returning and response.text else []

    def _patch(self, table: str, params: dict[str, str], payload: dict[str, Any]) -> None:
        response = requests.patch(self._url(table), headers=self._headers("return=minimal"), params=params, json=payload, timeout=15)
        if response.status_code not in {200, 204}:
            raise RuntimeError(f"Supabase auth update failed for {table}: {response.status_code}")

    def user_by_username(self, username: str) -> dict[str, Any] | None:
        rows = self._select("deltaforge_users", {"select": "*", "username": f"eq.{username}", "limit": "1"})
        return rows[0] if rows else None

    def user_by_id(self, user_id: str) -> dict[str, Any] | None:
        rows = self._select("deltaforge_users", {"select": "id,username,is_active,created_at,updated_at,last_login_at", "id": f"eq.{user_id}", "limit": "1"})
        return rows[0] if rows else None

    def record_login_attempt(self, username: str, ip_address: str | None, success: bool, reason: str | None = None) -> None:
        self._insert(
            "deltaforge_login_attempts",
            {
                "username": username[:200],
                "ip_address": ip_address,
                "success": success,
                "failure_reason": None if success else (reason or "invalid_login")[:100],
                "created_at": iso_timestamp(),
            },
        )

    def login_locked(self, username: str) -> bool:
        since = iso_timestamp(utc_now() - timedelta(seconds=FAILED_LOGIN_WINDOW_SECONDS))
        rows = self._select(
            "deltaforge_login_attempts",
            {
                "select": "created_at",
                "username": f"eq.{username}",
                "success": "eq.false",
                "created_at": f"gte.{since}",
                "order": "created_at.desc",
                "limit": str(FAILED_LOGIN_LIMIT),
            },
        )
        if len(rows) < FAILED_LOGIN_LIMIT:
            return False
        latest = datetime.fromisoformat(str(rows[0]["created_at"]).replace("Z", "+00:00"))
        return (utc_now() - latest).total_seconds() < FAILED_LOGIN_COOLDOWN_SECONDS

    def create_or_update_user(self, username: str, password_hash: str, is_active: bool = True) -> dict[str, Any]:
        existing = self.user_by_username(username)
        payload = {"password_hash": password_hash, "is_active": is_active, "updated_at": iso_timestamp()}
        if existing:
            self._patch("deltaforge_users", {"id": f"eq.{existing['id']}"}, payload)
            return {**existing, **payload}
        rows = self._insert(
            "deltaforge_users",
            {
                "username": username,
                "password_hash": password_hash,
                "is_active": is_active,
                "created_at": iso_timestamp(),
                "updated_at": iso_timestamp(),
            },
            returning=True,
        )
        return rows[0] if rows else {"username": username, "is_active": is_active}

    def authenticate(self, username: str, password: str, ip_address: str | None = None) -> AuthSession:
        normalized = username.strip()
        if not normalized or self.login_locked(normalized):
            self.record_login_attempt(normalized or "<empty>", ip_address, False, "locked_or_invalid")
            raise ValueError("Invalid username or password.")
        user = self.user_by_username(normalized)
        valid = bool(user and user.get("is_active") is True and verify_password(password, str(user.get("password_hash") or "")))
        self.record_login_attempt(normalized, ip_address, valid, None if valid else "invalid_login")
        if not valid:
            raise ValueError("Invalid username or password.")
        token = secrets.token_urlsafe(32)
        expires_at = iso_timestamp(utc_now() + timedelta(hours=SESSION_EXPIRY_HOURS))
        self._insert(
            "deltaforge_sessions",
            {
                "user_id": user["id"],
                "session_token_hash": session_token_hash(token),
                "expires_at": expires_at,
                "created_at": iso_timestamp(),
            },
        )
        self._patch("deltaforge_users", {"id": f"eq.{user['id']}"}, {"last_login_at": iso_timestamp(), "updated_at": iso_timestamp()})
        safe_user = {key: user.get(key) for key in ["id", "username", "is_active", "created_at", "updated_at", "last_login_at"]}
        return AuthSession(token=token, expires_at=expires_at, user=safe_user)

    def session_user(self, token: str) -> dict[str, Any] | None:
        rows = self._select(
            "deltaforge_sessions",
            {
                "select": "id,user_id,expires_at,revoked_at",
                "session_token_hash": f"eq.{session_token_hash(token)}",
                "limit": "1",
            },
        )
        if not rows:
            return None
        session = rows[0]
        if session.get("revoked_at"):
            return None
        expires_at = datetime.fromisoformat(str(session.get("expires_at")).replace("Z", "+00:00"))
        if expires_at <= utc_now():
            return None
        user = self.user_by_id(str(session.get("user_id")))
        if not user or user.get("is_active") is not True:
            return None
        return user

    def revoke_session(self, token: str) -> None:
        self._patch("deltaforge_sessions", {"session_token_hash": f"eq.{session_token_hash(token)}"}, {"revoked_at": iso_timestamp()})


auth_store = SupabaseAuthStore()


def public_path(method: str, path: str) -> bool:
    if method.upper() == "OPTIONS":
        return True
    if (method.upper(), path.rstrip("/") or "/") in PUBLIC_AUTH_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in PUBLIC_PATH_PREFIXES)


class DeltaForgeAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if public_path(request.method, request.url.path):
            return await call_next(request)
        token = token_from_request(request)
        user = auth_store.session_user(token) if token else None
        if not user:
            return JSONResponse({"detail": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)
        request.state.deltaforge_user = user
        return await call_next(request)
