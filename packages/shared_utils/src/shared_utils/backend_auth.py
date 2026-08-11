"""Shared authentication helpers for calls to the backend API."""

from __future__ import annotations

import os

BACKEND_API_KEY_ENV = "BACKEND_API_KEY"
BACKEND_API_KEY_HEADER = "X-API-Key"
MIN_BACKEND_API_KEY_LENGTH = 32


class BackendApiKeyConfigError(RuntimeError):
    """Raised when backend API authentication is not configured safely."""


def get_backend_api_key() -> str:
    """Return the configured backend API key, rejecting missing or weak values."""
    api_key = os.getenv(BACKEND_API_KEY_ENV, "").strip()
    if not api_key:
        raise BackendApiKeyConfigError(f"{BACKEND_API_KEY_ENV} environment variable is not set")
    if len(api_key) < MIN_BACKEND_API_KEY_LENGTH:
        raise BackendApiKeyConfigError(f"{BACKEND_API_KEY_ENV} must contain at least {MIN_BACKEND_API_KEY_LENGTH} characters")
    return api_key


def backend_api_headers() -> dict[str, str]:
    """Build the authentication headers required by backend API clients."""
    return {BACKEND_API_KEY_HEADER: get_backend_api_key()}
