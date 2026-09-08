import api_client


def test_lifecycle_timeouts_are_not_capped_at_15_seconds():
    assert api_client._bounded_timeout(10) == 10
    assert api_client._bounded_timeout(60) == 60
    assert api_client._bounded_timeout(90) == 90
    assert api_client._bounded_timeout(240) == api_client.MAX_STREAMLIT_REQUEST_TIMEOUT_SECONDS
