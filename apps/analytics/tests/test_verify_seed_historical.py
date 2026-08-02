import logging
from contextlib import asynccontextmanager

import pytest
import verify_seed_historical


class _ExecuteResult:
    def __init__(self, value):
        self.value = value

    def fetchone(self):
        return self.value

    def __iter__(self):
        return iter(self.value)


class _FakeConn:
    def __init__(self, results):
        self.results = list(results)

    async def execute(self, stmt):
        return _ExecuteResult(self.results.pop(0))

    async def scalar(self, stmt):
        return self.results.pop(0)


class _FakeEngine:
    def __init__(self, results):
        self.results = results

    def connect(self):
        @asynccontextmanager
        async def _cm():
            yield _FakeConn(self.results)

        return _cm()


@pytest.fixture
def fake_engine(monkeypatch):
    def install(results):
        engine = _FakeEngine(results)
        monkeypatch.setattr(verify_seed_historical, "async_engine", engine)
        return engine

    return install


VALID_STATS = (1000, "2024-01-01", "2025-01-01", 0, 0)
SAMPLES = [("AK-47 | Redline (Field-Tested)", 500), ("AWP | Asiimov (Field-Tested)", 300)]


@pytest.mark.asyncio
async def test_verification_happy_path(fake_engine, caplog):
    fake_engine([("ix_historical_prices_item_date", True), VALID_STATS, 900, 1000, 0, SAMPLES])

    with caplog.at_level(logging.INFO, logger="analytics.verify"):
        await verify_seed_historical.run_verification()

    assert "Index 'ix_historical_prices_item_date' is VALID" in caplog.text
    assert "Total rows found    : 1,000" in caplog.text
    assert "No duplicates found" in caplog.text
    assert "AK-47 | Redline (Field-Tested)" in caplog.text


@pytest.mark.asyncio
async def test_verification_invalid_index_warns(fake_engine, caplog):
    fake_engine([("ix_historical_prices_item_date", False), VALID_STATS, 900, 1000, 0, []])

    with caplog.at_level(logging.WARNING, logger="analytics.verify"):
        await verify_seed_historical.run_verification()

    assert "INVALID/CORRUPT" in caplog.text
    assert "Rebuilding might be required" in caplog.text


@pytest.mark.asyncio
async def test_verification_missing_index_logs_error(fake_engine, caplog):
    fake_engine([None, VALID_STATS, 900, 1000, 0, []])

    with caplog.at_level(logging.ERROR, logger="analytics.verify"):
        await verify_seed_historical.run_verification()

    assert "was NOT found in system catalog" in caplog.text


@pytest.mark.asyncio
async def test_verification_empty_table_reports_failure(fake_engine, caplog):
    fake_engine([("ix_historical_prices_item_date", True), (0, None, None, 0, 0)])

    with caplog.at_level(logging.ERROR, logger="analytics.verify"):
        await verify_seed_historical.run_verification()

    assert "No records found in 'historical_prices'. Seeding failed." in caplog.text


@pytest.mark.asyncio
async def test_verification_duplicates_warn(fake_engine, caplog):
    fake_engine([("ix_historical_prices_item_date", True), VALID_STATS, 900, 1000, 5, []])

    with caplog.at_level(logging.WARNING, logger="analytics.verify"):
        await verify_seed_historical.run_verification()

    assert "Found 5 keys with duplicate dates" in caplog.text
