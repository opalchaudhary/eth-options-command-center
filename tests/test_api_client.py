import api_client


class _SessionState(dict):
    pass


class _Context:
    def __init__(self, cookies):
        self.cookies = cookies


class _Streamlit:
    def __init__(self, session_state=None, cookies=None):
        self.session_state = _SessionState(session_state or {})
        self.context = _Context(cookies or {})


class _Response:
    status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return {"ok": True}


def test_lifecycle_timeouts_are_not_capped_at_15_seconds():
    assert api_client._bounded_timeout(10) == 10
    assert api_client._bounded_timeout(60) == 60
    assert api_client._bounded_timeout(90) == 90
    assert api_client._bounded_timeout(240) == api_client.MAX_STREAMLIT_REQUEST_TIMEOUT_SECONDS


def test_streamlit_auth_token_prefers_session_state(monkeypatch):
    monkeypatch.setattr(
        api_client,
        "st",
        _Streamlit(
            session_state={api_client.AUTH_SESSION_STATE_KEY: "session-token"},
            cookies={api_client.DELTAFORGE_SESSION_COOKIE_NAME: "cookie-token"},
        ),
    )

    assert api_client.current_streamlit_auth_token() == "session-token"


def test_streamlit_auth_token_recovers_from_cookie(monkeypatch):
    monkeypatch.setattr(
        api_client,
        "st",
        _Streamlit(cookies={api_client.DELTAFORGE_SESSION_COOKIE_NAME: "cookie-token"}),
    )

    assert api_client.current_streamlit_auth_token() == "cookie-token"


def test_api_get_uses_cookie_recovered_token(monkeypatch):
    seen = {}

    def fake_get(url, params=None, headers=None, timeout=None):
        seen.update({"url": url, "headers": headers, "timeout": timeout})
        return _Response()

    monkeypatch.setattr(
        api_client,
        "st",
        _Streamlit(cookies={api_client.DELTAFORGE_SESSION_COOKIE_NAME: "cookie-token"}),
    )
    monkeypatch.setattr(api_client.requests, "get", fake_get)

    assert api_client.api_get("/api/grid/v01/live/compact") == {"ok": True}
    assert seen["headers"] == {"Authorization": "Bearer cookie-token"}


def test_auth_headers_absent_when_no_canonical_token(monkeypatch):
    monkeypatch.setattr(api_client, "st", _Streamlit())

    assert api_client._auth_headers() == {}
