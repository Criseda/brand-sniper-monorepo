import logging

import pytest
from shared_utils.logging_utils import get_logger


@pytest.mark.parametrize(
    ("log_level", "expected"),
    [
        pytest.param("DEBUG", logging.DEBUG, id="debug"),
        pytest.param("debug", logging.DEBUG, id="debug_lowercase"),
        pytest.param("INFO", logging.INFO, id="info"),
        pytest.param("WARNING", logging.WARNING, id="warning"),
        pytest.param("ERROR", logging.ERROR, id="error"),
        pytest.param("CRITICAL", logging.CRITICAL, id="critical"),
    ],
)
def test_get_logger_respects_log_level(monkeypatch, log_level, expected):
    monkeypatch.setenv("LOG_LEVEL", log_level)
    logger = get_logger(f"test.level.{log_level.lower()}")

    assert logger.level == expected


def test_get_logger_defaults_to_info_when_unset(monkeypatch):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    logger = get_logger("test.level.unset")

    assert logger.level == logging.INFO


def test_get_logger_falls_back_to_info_on_invalid_level(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "BANANAS")
    logger = get_logger("test.level.invalid")

    assert logger.level == logging.INFO


def test_get_logger_does_not_duplicate_handlers(monkeypatch):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    logger = get_logger("test.handlers")

    assert len(logger.handlers) == 1
    get_logger("test.handlers")
    assert len(logger.handlers) == 1


def test_get_logger_rejects_non_string_name():
    with pytest.raises(TypeError):
        get_logger(None)
    with pytest.raises(TypeError):
        get_logger(123)
