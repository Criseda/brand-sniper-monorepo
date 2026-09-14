"""Provider-neutral LLM configuration boundary for the Adversarial CFO.

Reads the ``LLM_*`` environment contract (see ``.env.example``) and validates
it at startup so any OpenAI-compatible Chat Completions endpoint can be used
without changing source code. No hosted provider URL, credential variable, or
model identifier is hard-coded here.
"""

import os
from dataclasses import dataclass, field

DEFAULT_REQUEST_TIMEOUT_SECONDS = 60.0
LEGACY_GROQ_ENV_VAR = "GROQ_API_KEY"

_MIGRATION_MESSAGE = (
    "Legacy Groq configuration detected (GROQ_API_KEY is set) but the provider-neutral "
    "LLM contract is incomplete. Migrate: set LLM_BASE_URL (e.g. https://api.groq.com/openai/v1), "
    "LLM_MODEL (e.g. openai/gpt-oss-120b), LLM_API_KEY (your previous Groq key), and optionally "
    "LLM_FALLBACK_MODELS (e.g. openai/gpt-oss-20b,qwen/qwen3.6-27b). "
    "See .env.example. API key values are never logged."
)


@dataclass(frozen=True)
class LLMSettings:
    """Validated, provider-neutral CFO LLM configuration."""

    base_url: str
    model: str
    api_key: str | None = field(default=None, repr=False)
    fallback_models: tuple[str, ...] = field(default_factory=tuple)
    request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS

    @property
    def all_models(self) -> list[str]:
        """Primary model followed by ordered fallbacks."""
        return [self.model, *self.fallback_models]


def _parse_fallback_models(raw: str | None, primary: str) -> tuple[str, ...]:
    if raw is None or not raw.strip():
        return ()
    seen: list[str] = []
    for entry in raw.split(","):
        name = entry.strip()
        if not name or name == primary or name in seen:
            continue
        seen.append(name)
    if not seen:
        raise SystemExit(
            "Invalid LLM_FALLBACK_MODELS: set a comma-separated list of model identifiers "
            "or unset the variable. See .env.example."
        )
    return tuple(seen)


def _parse_timeout(raw: str | None) -> float:
    if raw is None or not raw.strip():
        return DEFAULT_REQUEST_TIMEOUT_SECONDS
    try:
        timeout = float(raw.strip())
    except ValueError:
        raise SystemExit(
            "Invalid LLM_REQUEST_TIMEOUT_SECONDS: must be a positive number of seconds. See .env.example."
        ) from None
    if timeout <= 0:
        raise SystemExit("Invalid LLM_REQUEST_TIMEOUT_SECONDS: must be a positive number of seconds. See .env.example.")
    return timeout


def load_llm_settings(env: dict[str, str] | os._Environ[str] | None = None) -> LLMSettings:
    """Load and validate the LLM contract from the environment.

    Raises SystemExit with an actionable message (never including secret
    values) when the configuration is missing or malformed.
    """
    source = os.environ if env is None else env
    get = source.get

    base_url = (get("LLM_BASE_URL") or "").strip().rstrip("/")
    model = (get("LLM_MODEL") or "").strip()
    api_key = (get("LLM_API_KEY") or "").strip() or None
    allow_no_auth = (get("LLM_ALLOW_NO_AUTH") or "").strip().lower() == "true"

    missing = [name for name, value in (("LLM_BASE_URL", base_url), ("LLM_MODEL", model)) if not value]
    if missing:
        if (get(LEGACY_GROQ_ENV_VAR) or "").strip():
            raise SystemExit(_MIGRATION_MESSAGE)
        raise SystemExit(
            f"Missing required environment variable(s): {', '.join(missing)}. Add them to the root .env (see .env.example)."
        )

    if not base_url.startswith(("http://", "https://")):
        raise SystemExit("Invalid LLM_BASE_URL: must start with http:// or https://. See .env.example.")

    if api_key is None and not allow_no_auth:
        raise SystemExit(
            "Missing required environment variable(s): LLM_API_KEY. "
            "Set it, or set LLM_ALLOW_NO_AUTH=true only for a local endpoint "
            "that explicitly permits unauthenticated access. See .env.example."
        )

    fallbacks = _parse_fallback_models(get("LLM_FALLBACK_MODELS"), model)
    timeout = _parse_timeout(get("LLM_REQUEST_TIMEOUT_SECONDS"))
    return LLMSettings(
        base_url=base_url,
        model=model,
        api_key=api_key,
        fallback_models=fallbacks,
        request_timeout_seconds=timeout,
    )
