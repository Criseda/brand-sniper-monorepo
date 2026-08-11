import pytest
from shared_utils.backend_auth import (
    BACKEND_API_KEY_ENV,
    BACKEND_API_KEY_HEADER,
    MIN_BACKEND_API_KEY_LENGTH,
    BackendApiKeyConfigError,
    backend_api_headers,
    get_backend_api_key,
)


def test_get_backend_api_key_returns_configured_secret(monkeypatch):
    api_key = "a" * MIN_BACKEND_API_KEY_LENGTH
    monkeypatch.setenv(BACKEND_API_KEY_ENV, api_key)

    assert get_backend_api_key() == api_key
    assert backend_api_headers() == {BACKEND_API_KEY_HEADER: api_key}


@pytest.mark.parametrize(
    ("value", "message"),
    [
        pytest.param(None, "is not set", id="missing"),
        pytest.param("   ", "is not set", id="blank"),
        pytest.param("too-short", "at least", id="too-short"),
    ],
)
def test_get_backend_api_key_rejects_missing_or_weak_values(monkeypatch, value, message):
    if value is None:
        monkeypatch.delenv(BACKEND_API_KEY_ENV, raising=False)
    else:
        monkeypatch.setenv(BACKEND_API_KEY_ENV, value)

    with pytest.raises(BackendApiKeyConfigError, match=message):
        get_backend_api_key()
