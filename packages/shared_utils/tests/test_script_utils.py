import os
import sys

import pytest
from shared_utils import setup_script_environment, validate_required_env


@pytest.fixture
def script_env(tmp_path, monkeypatch):
    """Creates a fake repo layout (root + app .env files) and isolates sys.path."""
    root = tmp_path
    app_dir = root / "apps" / "analytics"
    app_dir.mkdir(parents=True)
    (root / ".env").write_text("ROOT_ONLY=1\nSHARED_VAR=root\n", encoding="utf-8")
    (app_dir / ".env").write_text("APP_ONLY=1\nSHARED_VAR=app\n", encoding="utf-8")
    monkeypatch.setattr(sys, "path", [p for p in sys.path])
    return app_dir / "script.py", root


class _FakeStream:
    def __init__(self):
        self.reconfigured = False

    def reconfigure(self, encoding=None):
        self.reconfigured = True


def test_setup_script_environment_reconfigures_streams(script_env, monkeypatch):
    script_path, _ = script_env
    fake_out = _FakeStream()
    fake_err = _FakeStream()
    monkeypatch.setattr(sys, "stdout", fake_out)
    monkeypatch.setattr(sys, "stderr", fake_err)

    setup_script_environment(script_path)

    assert fake_out.reconfigured
    assert fake_err.reconfigured


def test_setup_script_environment_skips_streams_without_reconfigure(script_env, monkeypatch):
    script_path, _ = script_env
    monkeypatch.setattr(sys, "stdout", object())
    monkeypatch.setattr(sys, "stderr", object())

    setup_script_environment(script_path)


def test_setup_script_environment_loads_env_with_app_override(script_env, monkeypatch):
    script_path, root = script_env
    monkeypatch.delenv("SHARED_VAR", raising=False)
    monkeypatch.delenv("ROOT_ONLY", raising=False)
    monkeypatch.delenv("APP_ONLY", raising=False)

    assert setup_script_environment(script_path) == root

    assert os.getenv("ROOT_ONLY") == "1"
    assert os.getenv("APP_ONLY") == "1"
    assert os.getenv("SHARED_VAR") == "app"
    assert str(root) in sys.path


def test_validate_required_env_passes_when_all_present(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.setenv("GROQ_API_KEY", "key")

    validate_required_env(["DATABASE_URL", "GROQ_API_KEY"])


def test_validate_required_env_exits_when_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("GROQ_API_KEY", "key")

    with pytest.raises(SystemExit) as excinfo:
        validate_required_env(["DATABASE_URL", "GROQ_API_KEY"])

    message = str(excinfo.value)
    assert "DATABASE_URL" in message
    assert "GROQ_API_KEY" not in message
