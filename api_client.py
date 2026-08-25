import os

import requests


DEFAULT_BACKEND_URL = "http://localhost:8000"


def backend_url():
    return os.getenv("FASTAPI_BACKEND_URL", DEFAULT_BACKEND_URL).rstrip("/")


MAX_STREAMLIT_REQUEST_TIMEOUT_SECONDS = 15


def _bounded_timeout(timeout):
    return min(timeout or MAX_STREAMLIT_REQUEST_TIMEOUT_SECONDS, MAX_STREAMLIT_REQUEST_TIMEOUT_SECONDS)


def api_get(path, params=None, timeout=MAX_STREAMLIT_REQUEST_TIMEOUT_SECONDS):
    timeout = _bounded_timeout(timeout)
    response = requests.get(f"{backend_url()}{path}", params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def api_post(path, payload=None, timeout=MAX_STREAMLIT_REQUEST_TIMEOUT_SECONDS):
    timeout = _bounded_timeout(timeout)
    response = requests.post(f"{backend_url()}{path}", json=payload or {}, timeout=timeout)
    response.raise_for_status()
    return response.json()


def api_patch(path, payload=None, timeout=MAX_STREAMLIT_REQUEST_TIMEOUT_SECONDS):
    timeout = _bounded_timeout(timeout)
    response = requests.patch(f"{backend_url()}{path}", json=payload or {}, timeout=timeout)
    response.raise_for_status()
    return response.json()


def backend_health(timeout=5):
    try:
        return api_get("/health", timeout=timeout)
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "backend_url": backend_url(),
        }
