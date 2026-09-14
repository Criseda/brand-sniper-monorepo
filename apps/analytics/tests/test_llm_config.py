import pytest
from llm_config import DEFAULT_REQUEST_TIMEOUT_SECONDS, load_llm_settings


def _env(**overrides):
    base = {
        "LLM_BASE_URL": "https://api.example.com/v1",
        "LLM_MODEL": "openai/gpt-oss-120b",
        "LLM_API_KEY": "secret",
    }
    base.update(overrides)
    return base


def test_loads_primary_with_defaults():
    settings = load_llm_settings(_env())

    assert settings.base_url == "https://api.example.com/v1"
    assert settings.model == "openai/gpt-oss-120b"
    assert settings.api_key == "secret"
    assert settings.fallback_models == ()
    assert settings.request_timeout_seconds == DEFAULT_REQUEST_TIMEOUT_SECONDS
    assert settings.all_models == ["openai/gpt-oss-120b"]


def test_parses_ordered_fallbacks_deduped():
    settings = load_llm_settings(_env(LLM_FALLBACK_MODELS="openai/gpt-oss-20b, qwen/qwen3.6-27b,openai/gpt-oss-20b"))

    assert settings.fallback_models == ("openai/gpt-oss-20b", "qwen/qwen3.6-27b")
    assert settings.all_models == ["openai/gpt-oss-120b", "openai/gpt-oss-20b", "qwen/qwen3.6-27b"]


def test_primary_excluded_from_fallbacks():
    settings = load_llm_settings(_env(LLM_FALLBACK_MODELS="openai/gpt-oss-120b,openai/gpt-oss-20b"))

    assert settings.fallback_models == ("openai/gpt-oss-20b",)


def test_missing_base_url_fails():
    env = _env()
    del env["LLM_BASE_URL"]

    with pytest.raises(SystemExit, match="LLM_BASE_URL"):
        load_llm_settings(env)


def test_missing_model_fails():
    env = _env()
    del env["LLM_MODEL"]

    with pytest.raises(SystemExit, match="LLM_MODEL"):
        load_llm_settings(env)


def test_malformed_fallback_list_fails():
    with pytest.raises(SystemExit, match="LLM_FALLBACK_MODELS"):
        load_llm_settings(_env(LLM_FALLBACK_MODELS=" , , "))


def test_invalid_timeout_fails():
    with pytest.raises(SystemExit, match="LLM_REQUEST_TIMEOUT_SECONDS"):
        load_llm_settings(_env(LLM_REQUEST_TIMEOUT_SECONDS="not-a-number"))

    with pytest.raises(SystemExit, match="LLM_REQUEST_TIMEOUT_SECONDS"):
        load_llm_settings(_env(LLM_REQUEST_TIMEOUT_SECONDS="-5"))


def test_custom_timeout_parsed():
    assert load_llm_settings(_env(LLM_REQUEST_TIMEOUT_SECONDS="15")).request_timeout_seconds == 15.0


def test_non_http_base_url_fails():
    with pytest.raises(SystemExit, match="LLM_BASE_URL"):
        load_llm_settings(_env(LLM_BASE_URL="api.example.com/v1"))


def test_missing_api_key_fails_for_authenticated_endpoint():
    env = _env()
    del env["LLM_API_KEY"]

    with pytest.raises(SystemExit, match="LLM_API_KEY"):
        load_llm_settings(env)


def test_local_no_auth_endpoint_runs_without_api_key():
    settings = load_llm_settings(_env(LLM_BASE_URL="http://localhost:11434/v1", LLM_ALLOW_NO_AUTH="true", LLM_API_KEY=""))

    assert settings.api_key is None
    assert settings.base_url == "http://localhost:11434/v1"


def test_legacy_groq_env_reports_migration_without_secrets():
    env = {"GROQ_API_KEY": "super-secret-value"}

    with pytest.raises(SystemExit) as exc_info:
        load_llm_settings(env)

    message = str(exc_info.value)
    assert "LLM_BASE_URL" in message
    assert "LLM_MODEL" in message
    assert "super-secret-value" not in message


def test_missing_config_never_exposes_secrets():
    env = {"LLM_API_KEY": "super-secret-value"}

    with pytest.raises(SystemExit) as exc_info:
        load_llm_settings(env)

    assert "super-secret-value" not in str(exc_info.value)


def test_settings_repr_never_exposes_api_key():
    settings = load_llm_settings(_env())

    assert "secret" not in repr(settings)
