import os

import requests

try:
    import streamlit as st
except Exception:  # pragma: no cover - backend/test import fallback
    st = None


DEFAULT_BACKEND_URL = "http://localhost:8000"
AUTH_SESSION_STATE_KEY = "deltaforge_session_token"


def backend_url():
    return os.getenv("FASTAPI_BACKEND_URL", DEFAULT_BACKEND_URL).rstrip("/")


DEFAULT_STREAMLIT_REQUEST_TIMEOUT_SECONDS = 15
MAX_STREAMLIT_REQUEST_TIMEOUT_SECONDS = 120


def _bounded_timeout(timeout):
    requested = timeout or DEFAULT_STREAMLIT_REQUEST_TIMEOUT_SECONDS
    return max(1, min(requested, MAX_STREAMLIT_REQUEST_TIMEOUT_SECONDS))


def _session_token():
    if st is None:
        return None
    try:
        return st.session_state.get(AUTH_SESSION_STATE_KEY)
    except Exception:
        return None


def _auth_headers(auth_token=None, include_auth=True):
    if not include_auth:
        return {}
    token = auth_token if auth_token is not None else _session_token()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _handle_auth_failure(response):
    if response.status_code == 401 and st is not None:
        try:
            st.session_state.pop(AUTH_SESSION_STATE_KEY, None)
            st.session_state.pop("deltaforge_user", None)
            st.session_state["deltaforge_auth_required"] = True
        except Exception:
            pass


def api_get(path, params=None, timeout=DEFAULT_STREAMLIT_REQUEST_TIMEOUT_SECONDS, auth_token=None, include_auth=True):
    timeout = _bounded_timeout(timeout)
    response = requests.get(f"{backend_url()}{path}", params=params, headers=_auth_headers(auth_token, include_auth), timeout=timeout)
    _handle_auth_failure(response)
    response.raise_for_status()
    return response.json()


def api_post(path, payload=None, timeout=DEFAULT_STREAMLIT_REQUEST_TIMEOUT_SECONDS, auth_token=None, include_auth=True):
    timeout = _bounded_timeout(timeout)
    response = requests.post(f"{backend_url()}{path}", json=payload or {}, headers=_auth_headers(auth_token, include_auth), timeout=timeout)
    _handle_auth_failure(response)
    response.raise_for_status()
    return response.json()


def api_patch(path, payload=None, timeout=DEFAULT_STREAMLIT_REQUEST_TIMEOUT_SECONDS, auth_token=None, include_auth=True):
    timeout = _bounded_timeout(timeout)
    response = requests.patch(f"{backend_url()}{path}", json=payload or {}, headers=_auth_headers(auth_token, include_auth), timeout=timeout)
    _handle_auth_failure(response)
    response.raise_for_status()
    return response.json()


def backend_health(timeout=5):
    try:
        return api_get("/health", timeout=timeout, include_auth=False)
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "backend_url": backend_url(),
        }
