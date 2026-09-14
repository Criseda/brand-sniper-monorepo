import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True)
def _mock_llm_config(monkeypatch):
    monkeypatch.setenv("LLM_BASE_URL", "http://fake-llm:8080/v1")
    monkeypatch.setenv("LLM_MODEL", "openai/gpt-oss-120b")
    monkeypatch.setenv("LLM_FALLBACK_MODELS", "openai/gpt-oss-20b,qwen/qwen3.6-27b")
    monkeypatch.setenv("LLM_API_KEY", "MOCK_API_KEY")
    monkeypatch.setenv("BACKEND_API_KEY", "analytics-test-key-that-is-at-least-32-characters")
    import evaluate_performance

    evaluate_performance._reset_llm_state()


@pytest.fixture(autouse=True, scope="session")
def prefect_test():
    # Run prefect flows against a single managed ephemeral server for the whole
    # analytics session. Function-scoping this fixture spun up a new server per
    # test (76+ lifecycles), which dominated suite runtime and deadlocked on
    # Linux CI at server teardown/setup boundaries.
    with prefect_test_harness():
        yield
