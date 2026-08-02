import json
from contextlib import asynccontextmanager

import pytest
import update_baselines


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, rows, sticker_rows):
        self.rows = rows
        self.sticker_rows = sticker_rows
        self.execute_count = 0

    async def execute(self, stmt):
        self.execute_count += 1
        if self.execute_count == 1:
            return _Result(self.rows)
        return _Result(self.sticker_rows)


class FakeEngine:
    def __init__(self, rows, sticker_rows):
        self.rows = rows
        self.sticker_rows = sticker_rows

    def connect(self):
        @asynccontextmanager
        async def _connect():
            yield FakeConn(self.rows, self.sticker_rows)

        return _connect()


class FakePipe:
    def __init__(self):
        self.sets = []
        self.execute_called = False

    def set(self, key, value):
        self.sets.append((key, value))
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def execute(self):
        self.execute_called = True


class FakeRedis:
    instances: list["FakeRedis"] = []

    def __init__(self):
        self.pipe = FakePipe()
        self.hset_calls = []
        self.closed = False
        FakeRedis.instances.append(self)

    @classmethod
    def from_url(cls, url, **kwargs):
        instance = cls()
        instance.from_url_args = (url, kwargs)
        return instance

    def pipeline(self, transaction=False):
        return self.pipe

    async def hset(self, name, mapping=None):
        self.hset_calls.append((name, mapping))

    async def aclose(self):
        self.closed = True


@pytest.fixture
def fake_redis(monkeypatch):
    FakeRedis.instances = []
    monkeypatch.setattr(update_baselines, "Redis", FakeRedis)
    return FakeRedis


def _install_fake_engine(monkeypatch, rows, sticker_rows):
    engine = FakeEngine(rows, sticker_rows)
    monkeypatch.setattr(update_baselines, "async_engine", engine)
    return engine


BASELINE_ROW = (
    "AK-47 | Redline (Field-Tested)",
    1500,
    1600,
    2000,
    100,
    2.5,
)
STICKER_ROW = ("Titan | Katowice 2014", 500000)


@pytest.mark.asyncio
async def test_sync_pushes_baselines_and_stickers(monkeypatch, fake_redis):
    _install_fake_engine(monkeypatch, [BASELINE_ROW], [STICKER_ROW])

    await update_baselines.sync_baselines_to_edge()

    redis = fake_redis.instances[0]
    assert redis.from_url_args == ("redis://localhost:6380", {"username": "default", "password": None})
    assert redis.pipe.execute_called is True
    assert redis.pipe.sets[0][0] == "baseline:AK-47 | Redline (Field-Tested)"
    data = json.loads(redis.pipe.sets[0][1])
    assert data == {
        "support_floor_cents": 1500,
        "latest_price_cents": 1600,
        "rolling_30d_avg_cents": 2000,
        "volatility_cents": 100,
        "drift_percent": 2.5,
        "coefficient_of_variation": 0.05,
    }
    assert redis.hset_calls == [("sticker_prices", {"Titan | Katowice 2014": "500000"})]
    assert redis.closed is True


@pytest.mark.asyncio
async def test_sync_uses_configured_edge_redis_url(monkeypatch, fake_redis):
    _install_fake_engine(monkeypatch, [], [])
    monkeypatch.setenv("EDGE_REDIS_URL", "redis://edge-host:7000")
    monkeypatch.setenv("REDIS_PASSWORD", "secret")

    await update_baselines.sync_baselines_to_edge()

    redis = fake_redis.instances[0]
    assert redis.from_url_args == ("redis://edge-host:7000", {"username": "default", "password": "secret"})


@pytest.mark.asyncio
async def test_sync_no_rows_skips_pipeline_and_hset(monkeypatch, fake_redis):
    _install_fake_engine(monkeypatch, [], [])

    await update_baselines.sync_baselines_to_edge()

    redis = fake_redis.instances[0]
    assert redis.pipe.execute_called is False
    assert redis.pipe.sets == []
    assert redis.hset_calls == []


@pytest.mark.asyncio
async def test_sync_zero_coefficient_of_variation_when_avg_missing(monkeypatch, fake_redis):
    row = ("Item", 1500, 1600, None, 100, 2.5)
    _install_fake_engine(monkeypatch, [row], [])

    await update_baselines.sync_baselines_to_edge()

    redis = fake_redis.instances[0]
    data = json.loads(redis.pipe.sets[0][1])
    assert data["coefficient_of_variation"] == 0.0
