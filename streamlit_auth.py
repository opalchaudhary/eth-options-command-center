from __future__ import annotations

import html

import requests
import streamlit as st
import streamlit.components.v1 as components

from api_client import AUTH_SESSION_STATE_KEY, api_get, api_post


COOKIE_NAME = "deltaforge_session"
USER_STATE_KEY = "deltaforge_user"


def _cookie_token() -> str | None:
    try:
        return st.context.cookies.get(COOKIE_NAME)
    except Exception:
        return None


def _set_cookie_script(token: str, max_age_seconds: int) -> None:
    escaped_name = html.escape(COOKIE_NAME, quote=True)
    escaped_token = html.escape(token, quote=True)
    components.html(
        f"""
        <script>
        const secure = window.location.protocol === "https:" ? "; Secure" : "";
        document.cookie = "{escaped_name}={escaped_token}; Path=/; Max-Age={int(max_age_seconds)}; SameSite=Lax" + secure;
        </script>
        """,
        height=0,
    )


def _clear_cookie_script() -> None:
    escaped_name = html.escape(COOKIE_NAME, quote=True)
    components.html(
        f"""
        <script>
        document.cookie = "{escaped_name}=; Path=/; Max-Age=0; SameSite=Lax";
        </script>
        """,
        height=0,
    )


def _clear_auth_state() -> None:
    st.session_state.pop(AUTH_SESSION_STATE_KEY, None)
    st.session_state.pop(USER_STATE_KEY, None)


def _token() -> str | None:
    return st.session_state.get(AUTH_SESSION_STATE_KEY) or _cookie_token()


def _validate_existing_session() -> bool:
    token = _token()
    if not token:
        return False
    try:
        payload = api_get("/api/auth/me", timeout=5, auth_token=token)
    except Exception:
        _clear_auth_state()
        return False
    if not payload.get("ok"):
        _clear_auth_state()
        return False
    st.session_state[AUTH_SESSION_STATE_KEY] = token
    st.session_state[USER_STATE_KEY] = payload.get("user") or {}
    _set_cookie_script(token, 12 * 60 * 60)
    return True


def _render_login() -> None:
    _clear_cookie_script()
    st.title("DeltaForge")
    with st.form("deltaforge_login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login", type="primary")
    if not submitted:
        st.stop()
    try:
        payload = api_post(
            "/api/auth/login",
            {"username": username, "password": password},
            timeout=10,
            include_auth=False,
        )
    except requests.HTTPError:
        st.error("Invalid username or password.")
        st.stop()
    except Exception:
        st.error("Login is temporarily unavailable.")
        st.stop()
    token = payload.get("session_token")
    if not token:
        st.error("Login is temporarily unavailable.")
        st.stop()
    st.session_state[AUTH_SESSION_STATE_KEY] = token
    st.session_state[USER_STATE_KEY] = payload.get("user") or {}
    _set_cookie_script(token, int(payload.get("expires_in_seconds") or 12 * 60 * 60))
    st.rerun()


def require_authentication() -> dict:
    if not _validate_existing_session():
        _render_login()
    user = st.session_state.get(USER_STATE_KEY) or {}
    if st.sidebar.button("Logout", use_container_width=True):
        token = st.session_state.get(AUTH_SESSION_STATE_KEY)
        try:
            api_post("/api/auth/logout", {}, timeout=5, auth_token=token)
        except Exception:
            pass
        _clear_auth_state()
        _clear_cookie_script()
        st.rerun()
    return user

