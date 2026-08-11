import importlib.util
from pathlib import Path

import pytest


def load_compose_checker():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "check_compose_hardening.py"
    spec = importlib.util.spec_from_file_location("compose_hardening_checker_test", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load Compose hardening checker")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


checker = load_compose_checker()
EXPECTED_LOGGING = checker.EXPECTED_LOGGING
validate_stack = checker.validate_stack


def service_config(*, healthcheck=None, depends_on=None):
    return {
        "logging": EXPECTED_LOGGING,
        "deploy": {"resources": {"limits": {"memory": 1024}}},
        "healthcheck": healthcheck or {"test": ["CMD", "true"]},
        "networks": {"application": {}},
        "depends_on": depends_on or {},
    }


def validate_service(service):
    return validate_stack(
        "synthetic",
        {"services": {"app": service}},
        {"app": {"application"}},
        {},
        {},
    )


@pytest.mark.parametrize(
    "healthcheck",
    [
        pytest.param({"disable": True}, id="disabled"),
        pytest.param({"test": ["NONE"]}, id="none-command"),
    ],
)
def test_disabled_healthcheck_fails_policy(healthcheck):
    failures = validate_service(service_config(healthcheck=healthcheck))

    assert failures == ["synthetic:app has no enabled healthcheck"]


def test_service_started_dependency_fails_policy():
    failures = validate_service(service_config(depends_on={"redis": {"condition": "service_started"}}))

    assert failures == ["synthetic:app waits for redis with 'service_started', expected 'service_healthy'"]


def test_enabled_healthcheck_and_healthy_dependency_pass_policy():
    failures = validate_service(service_config(depends_on={"redis": {"condition": "service_healthy"}}))

    assert failures == []
