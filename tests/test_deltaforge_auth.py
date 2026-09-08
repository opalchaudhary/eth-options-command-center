from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from fastapi import Response
from fastapi.responses import JSONResponse

import backend.auth as auth_module
import backend.routers.auth as auth_router
from backend.auth import DeltaForgeAuthMiddleware, SupabaseAuthStore, hash_password, iso_timestamp, public_path, session_token_hash, utc_now, verify_password
from backend.routers.auth import LoginRequest


class InMemoryAuthStore(SupabaseAuthStore):
    def __init__(self):
        self.users: list[dict[str, Any]] = []
        self.sessions: list[dict[str, Any]] = []
        self.attempts: list[dict[str, Any]] = []
        self._next_id = 1
        super().__init__("https://example.supabase.co", "service-key")

    def _id(self) -> str:
        value = f"id-{self._next_id}"
        self._next_id += 1
        return value

    def _select(self, table: str, params: dict[str, str]) -> list[dict[str, Any]]:
        if table == "deltaforge_users":
            rows = list(self.users)
            if "username" in params:
                rows = [row for row in rows if row["username"] == params["username"].removeprefix("eq.")]
            if "id" in params:
                rows = [row for row in rows if row["id"] == params["id"].removeprefix("eq.")]
        elif table == "deltaforge_sessions":
            rows = list(self.sessions)
            if "session_token_hash" in params:
                rows = [row for row in rows if row["session_token_hash"] == params["session_token_hash"].removeprefix("eq.")]
        elif table == "deltaforge_login_attempts":
            rows = list(self.attempts)
            if "username" in params:
                rows = [row for row in rows if row["username"] == params["username"].removeprefix("eq.")]
            if "success" in params:
                expected = params["success"].removeprefix("eq.").lower() == "true"
                rows = [row for row in rows if row["success"] is expected]
            if "created_at" in params:
                since = params["created_at"].removeprefix("gte.")
                rows = [row for row in rows if row["created_at"] >= since]
            rows.sort(key=lambda row: row["created_at"], reverse=True)
        else:
            rows = []
        limit = int(params.get("limit", len(rows)) or len(rows))
        return [dict(row) for row in rows[:limit]]

    def _insert(self, table: str, payload: dict[str, Any], *, returning: bool = False) -> list[dict[str, Any]]:
        row = {"id": self._id(), **payload}
        if table == "deltaforge_users":
            self.users.append(row)
        elif table == "deltaforge_sessions":
            self.sessions.append(row)
        elif table == "deltaforge_login_attempts":
            self.attempts.append(row)
        return [dict(row)] if returning else []

    def _patch(self, table: str, params: dict[str, str], payload: dict[str, Any]) -> None:
        rows = self.users if table == "deltaforge_users" else self.sessions
        for row in rows:
            if "id" in params and row.get("id") != params["id"].removeprefix("eq."):
                continue
            if "session_token_hash" in params and row.get("session_token_hash") != params["session_token_hash"].removeprefix("eq."):
                continue
            row.update(payload)


def patch_store(store: InMemoryAuthStore, monkeypatch) -> None:
    monkeypatch.setattr(auth_module, "auth_store", store)
    monkeypatch.setattr(auth_router, "auth_store", store)


def login_payload(username: str, password: str) -> tuple[int, dict[str, Any]]:
    request = SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"))
    response = Response()
    payload = auth_router.login(LoginRequest(username=username, password=password), request, response)
    return response.status_code or 200, payload


def middleware_status(path: str, *, token: str | None = None) -> int:
    request = SimpleNamespace(
        method="GET",
        url=SimpleNamespace(path=path),
        headers={"Authorization": f"Bearer {token}"} if token else {},
        cookies={},
        state=SimpleNamespace(),
    )

    async def call_next(_request):
        return JSONResponse({"ok": True})

    middleware = DeltaForgeAuthMiddleware(app=lambda scope, receive, send: None)
    response = asyncio.run(middleware.dispatch(request, call_next))
    return response.status_code


def create_user(store: InMemoryAuthStore, username: str = "operator", password: str = "CorrectHorseBattery1!", *, active: bool = True) -> dict[str, Any]:
    return store.create_or_update_user(username, hash_password(password), is_active=active)


def test_password_hashes_are_salted_and_verified():
    password_hash = hash_password("CorrectHorseBattery1!")

    assert password_hash.startswith("pbkdf2_sha256$")
    assert "CorrectHorseBattery1!" not in password_hash
    assert verify_password("CorrectHorseBattery1!", password_hash)
    assert not verify_password("wrong-password", password_hash)


def test_login_success_returns_session_without_password_hash(monkeypatch):
    store = InMemoryAuthStore()
    create_user(store)
    patch_store(store, monkeypatch)

    status_code, payload = login_payload("operator", "CorrectHorseBattery1!")

    assert status_code == 200
    assert payload["ok"] is True
    assert payload["session_token"]
    assert payload["user"]["username"] == "operator"
    assert "password_hash" not in payload["user"]
    assert store.sessions[0]["session_token_hash"] == session_token_hash(payload["session_token"])


def test_wrong_unknown_and_inactive_logins_are_rejected_with_generic_message(monkeypatch):
    store = InMemoryAuthStore()
    create_user(store, active=False)
    patch_store(store, monkeypatch)

    cases = [
        {"username": "operator", "password": "wrong-password"},
        {"username": "missing", "password": "CorrectHorseBattery1!"},
        {"username": "operator", "password": "CorrectHorseBattery1!"},
    ]

    for payload in cases:
        status_code, body = login_payload(payload["username"], payload["password"])
        assert status_code == 401
        assert body == {"ok": False, "detail": "Invalid username or password."}


def test_private_api_requires_valid_session(monkeypatch):
    store = InMemoryAuthStore()
    create_user(store)
    patch_store(store, monkeypatch)

    assert public_path("GET", "/health")
    assert middleware_status("/api/private/probe") == 401

    _, login = login_payload("operator", "CorrectHorseBattery1!")

    assert middleware_status("/api/private/probe", token=login["session_token"]) == 200


def test_expired_session_is_rejected(monkeypatch):
    store = InMemoryAuthStore()
    create_user(store)
    patch_store(store, monkeypatch)
    _, login = login_payload("operator", "CorrectHorseBattery1!")
    token = login["session_token"]
    store.sessions[0]["expires_at"] = iso_timestamp(utc_now() - timedelta(minutes=1))

    assert middleware_status("/api/private/probe", token=token) == 401


def test_logout_revokes_session(monkeypatch):
    store = InMemoryAuthStore()
    create_user(store)
    patch_store(store, monkeypatch)
    _, login = login_payload("operator", "CorrectHorseBattery1!")
    token = login["session_token"]
    request = SimpleNamespace(headers={"Authorization": f"Bearer {token}"}, cookies={})
    response = Response()

    assert auth_router.logout(request, response) == {"ok": True}
    assert middleware_status("/api/private/probe", token=token) == 401
    assert store.sessions[0]["revoked_at"]


def test_brute_force_lockout_blocks_valid_password_temporarily(monkeypatch):
    store = InMemoryAuthStore()
    create_user(store)
    patch_store(store, monkeypatch)

    for _ in range(auth_module.FAILED_LOGIN_LIMIT):
        status_code, _ = login_payload("operator", "wrong-password")
        assert status_code == 401

    status_code, body = login_payload("operator", "CorrectHorseBattery1!")

    assert status_code == 401
    assert body == {"ok": False, "detail": "Invalid username or password."}


def test_streamlit_pages_call_auth_gate_before_operational_api_usage():
    page_paths = [Path("app.py"), *Path("pages").glob("*.py")]
    assert page_paths

    for path in page_paths:
        source = path.read_text(encoding="utf-8")
        assert "from streamlit_auth import require_authentication" in source, path
        guard_index = source.index("require_authentication()")
        api_indexes = [source.find(token) for token in ["api_get(", "api_post(", "api_patch("] if source.find(token) != -1]
        assert not api_indexes or guard_index < min(api_indexes), path


def test_gridbot_background_worker_does_not_require_human_authentication():
    main_source = Path("backend/main.py").read_text(encoding="utf-8")
    worker_source = Path("grid_bot/continuous_worker.py").read_text(encoding="utf-8")

    assert "start_continuous_gridbot_worker()" in main_source
    assert "require_authentication" not in worker_source
    assert "DeltaForgeAuthMiddleware" not in worker_source
