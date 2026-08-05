import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True)
def _mock_groq_key(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "MOCK_API_KEY")


@pytest.fixture(autouse=True, scope="session")
def prefect_test():
    # Run prefect flows against a single managed ephemeral server for the whole
    # analytics session. Function-scoping this fixture spun up a new server per
    # test (76+ lifecycles), which dominated suite runtime and deadlocked on
    # Linux CI at server teardown/setup boundaries.
    with prefect_test_harness():
        yield
