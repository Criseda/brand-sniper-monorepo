import logging
from contextlib import asynccontextmanager

import pytest
import seed_historical


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class FakeAsyncpgConn:
    def __init__(self):
        self.transaction_depth = 0
        self.committed_transactions = 0
        self.fetchval_results = []
        self.copy_calls = []
        self.copy_error = None

    def transaction(self):
        @asynccontextmanager
        async def _tx():
            self.transaction_depth += 1
            yield
            self.transaction_depth -= 1
            self.committed_transactions += 1

        return _tx()

    async def fetchval(self, query, *args, **kwargs):
        if self.fetchval_results:
            return self.fetchval_results.pop(0)
        raise AssertionError("fetchval called without queued result")

    async def copy_to_table(self, table, source, columns, format):
        if self.copy_error is not None:
            raise self.copy_error
        self.copy_calls.append((table, source.read().decode(), columns))


class FakeConn:
    def __init__(self, engine):
        self.engine = engine

    async def execute(self, stmt):
        if self.engine.connect_calls == 1:
            return _Rows(self.engine.cached_items)
        self.engine.executed_sql.append(str(stmt))
        return _Result([])

    async def get_raw_connection(self):
        return _RawConnection(self.engine.asyncpg_conn)


class _RawConnection:
    def __init__(self, driver_connection):
        self.driver_connection = driver_connection


class FakeEngine:
    def __init__(self, cached_items, asyncpg_conn):
        self.cached_items = cached_items
        self.asyncpg_conn = asyncpg_conn
        self.connect_calls = 0
        self.executed_sql = []

    def begin(self):
        @asynccontextmanager
        async def _begin():
            yield FakeConn(self)

        return _begin()

    def connect(self):
        @asynccontextmanager
        async def _connect():
            self.connect_calls += 1
            yield FakeConn(self)

        return _connect()


@pytest.fixture
def install_fake_engine(monkeypatch):
    def install(cached_items, asyncpg_conn=None):
        engine = FakeEngine(cached_items, asyncpg_conn or FakeAsyncpgConn())
        monkeypatch.setattr(seed_historical, "async_engine", engine)
        return engine

    return install


def _write_csv(path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


CSV_HEADER = "name,unix timestamp,price,quantity"


@pytest.mark.asyncio
async def test_missing_data_dir_returns_early(monkeypatch, tmp_path, install_fake_engine, caplog):
    engine = install_fake_engine([])
    monkeypatch.setattr(seed_historical, "DATA_DIR", tmp_path / "nope")

    with caplog.at_level(logging.ERROR, logger="analytics.seed"):
        await seed_historical.seed_historical_data()

    assert "Data directory not found" in caplog.text
    assert engine.connect_calls == 0
    assert engine.executed_sql == []


@pytest.mark.asyncio
async def test_seeds_files_and_restores_index(monkeypatch, tmp_path, install_fake_engine):
    csv_dir = tmp_path / "items"
    _write_csv(
        csv_dir / "AK-47%20%7C%20Redline%20(Field-Tested).csv",
        [CSV_HEADER, "AK-47 | Redline,1700000000,10.5,3", "AK-47 | Redline,1700000001,11.0,2"],
    )
    asyncpg_conn = FakeAsyncpgConn()
    engine = install_fake_engine([("AK-47 | Redline (Field-Tested)", 1)], asyncpg_conn)
    monkeypatch.setattr(seed_historical, "DATA_DIR", csv_dir)

    await seed_historical.seed_historical_data()

    assert any("DROP INDEX IF EXISTS ix_historical_prices_item_date" in sql for sql in engine.executed_sql)
    assert any("CREATE INDEX IF NOT EXISTS ix_historical_prices_item_date" in sql for sql in engine.executed_sql)
    assert len(asyncpg_conn.copy_calls) == 1
    table, payload, columns = asyncpg_conn.copy_calls[0]
    assert table == "historical_prices"
    assert columns == ["item_id", "sale_date", "median_price_cents", "volume_sold"]
    assert "1,2023-11-14 22:13:20+00:00,1050,3" in payload


@pytest.mark.asyncio
async def test_new_items_are_inserted_and_cached(monkeypatch, tmp_path, install_fake_engine):
    csv_dir = tmp_path / "items"
    _write_csv(
        csv_dir / "AK-47%20%7C%20Redline%20(Field-Tested).csv",
        [CSV_HEADER, "AK-47 | Redline,1700000000,10.5,3"],
    )
    asyncpg_conn = FakeAsyncpgConn()
    asyncpg_conn.fetchval_results.append(42)
    install_fake_engine([], asyncpg_conn)
    monkeypatch.setattr(seed_historical, "DATA_DIR", csv_dir)

    await seed_historical.seed_historical_data()

    assert len(asyncpg_conn.copy_calls) == 1
    assert "42,2023-11-14 22:13:20+00:00,1050,3" in asyncpg_conn.copy_calls[0][1]


@pytest.mark.asyncio
async def test_empty_csv_is_skipped(monkeypatch, tmp_path, install_fake_engine):
    csv_dir = tmp_path / "items"
    _write_csv(csv_dir / "AK-47%20%7C%20Redline%20(Field-Tested).csv", [CSV_HEADER])
    asyncpg_conn = FakeAsyncpgConn()
    install_fake_engine([("AK-47 | Redline (Field-Tested)", 1)], asyncpg_conn)
    monkeypatch.setattr(seed_historical, "DATA_DIR", csv_dir)

    await seed_historical.seed_historical_data()

    assert asyncpg_conn.copy_calls == []


@pytest.mark.asyncio
async def test_fully_corrupt_timestamps_are_skipped(monkeypatch, tmp_path, install_fake_engine):
    csv_dir = tmp_path / "items"
    _write_csv(
        csv_dir / "AK-47%20%7C%20Redline%20(Field-Tested).csv",
        [CSV_HEADER, "AK-47 | Redline,garbage,10.5,3", "AK-47 | Redline,also-bad,11.0,1"],
    )
    asyncpg_conn = FakeAsyncpgConn()
    install_fake_engine([("AK-47 | Redline (Field-Tested)", 1)], asyncpg_conn)
    monkeypatch.setattr(seed_historical, "DATA_DIR", csv_dir)

    await seed_historical.seed_historical_data()

    assert asyncpg_conn.copy_calls == []


@pytest.mark.asyncio
async def test_file_insertion_failure_is_isolated(monkeypatch, tmp_path, install_fake_engine, caplog):
    csv_dir = tmp_path / "items"
    _write_csv(
        csv_dir / "AK-47%20%7C%20Redline%20(Field-Tested).csv",
        [CSV_HEADER, "AK-47 | Redline,1700000000,10.5,3"],
    )
    asyncpg_conn = FakeAsyncpgConn()
    asyncpg_conn.copy_error = RuntimeError("disk full")
    engine = install_fake_engine([("AK-47 | Redline (Field-Tested)", 1)], asyncpg_conn)
    monkeypatch.setattr(seed_historical, "DATA_DIR", csv_dir)

    with caplog.at_level(logging.WARNING, logger="analytics.seed"):
        await seed_historical.seed_historical_data()

    assert "Skipping data for" in caplog.text
    assert any("CREATE INDEX IF NOT EXISTS" in sql for sql in engine.executed_sql)


@pytest.mark.asyncio
async def test_truncate_executed_when_requested(monkeypatch, tmp_path, install_fake_engine):
    csv_dir = tmp_path / "items"
    _write_csv(
        csv_dir / "AK-47%20%7C%20Redline%20(Field-Tested).csv",
        [CSV_HEADER, "AK-47 | Redline,1700000000,10.5,3"],
    )
    engine = install_fake_engine([("AK-47 | Redline (Field-Tested)", 1)])
    monkeypatch.setattr(seed_historical, "DATA_DIR", csv_dir)

    await seed_historical.seed_historical_data(truncate=True)

    assert any("TRUNCATE TABLE historical_prices" in sql for sql in engine.executed_sql)


@pytest.mark.asyncio
async def test_missing_raw_connection_propagates_but_restores_index(monkeypatch, tmp_path, install_fake_engine):
    csv_dir = tmp_path / "items"
    _write_csv(
        csv_dir / "AK-47%20%7C%20Redline%20(Field-Tested).csv",
        [CSV_HEADER, "AK-47 | Redline,1700000000,10.5,3"],
    )
    engine = install_fake_engine([("AK-47 | Redline (Field-Tested)", 1)])
    engine.asyncpg_conn = None
    monkeypatch.setattr(seed_historical, "DATA_DIR", csv_dir)

    with pytest.raises(RuntimeError, match="Failed to acquire raw asyncpg connection"):
        await seed_historical.seed_historical_data()

    assert any("CREATE INDEX IF NOT EXISTS" in sql for sql in engine.executed_sql)
