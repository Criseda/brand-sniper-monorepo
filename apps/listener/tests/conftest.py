import json

import pytest


class MockRedis:
    def __init__(self):
        self.data = {}

    async def set(self, key, value):
        self.data[key] = value

    async def get(self, key):
        return self.data.get(key)

    async def hget(self, name, key):
        hash_data = self.data.get(name, {})
        return hash_data.get(key)

    async def hset(self, name, key, value):
        if name not in self.data:
            self.data[name] = {}
        self.data[name][key] = value

    async def pipeline(self, transaction=False):
        return MockPipeline(self)

    async def close(self):
        pass

    async def aclose(self):
        pass


class MockPipeline:
    def __init__(self, redis: MockRedis):
        self.redis = redis
        self.operations: list[tuple] = []

    def set(self, key, value):
        self.operations.append(("set", key, value))
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def execute(self):
        for op in self.operations:
            if op[0] == "set":
                self.redis.data[op[1]] = op[2]
        self.operations.clear()


@pytest.fixture
def mock_redis():
    r = MockRedis()
    baseline = {"support_floor_cents": 1500, "latest_price_cents": 1600}
    r.data["baseline:AK-47 | Redline (Field-Tested)"] = json.dumps(baseline)
    r.data["sticker_prices"] = {"Titan | Katowice 2014": "500000", "iBUYPOWER | Cologne 2014": "15000", "Cheap Sticker": "50"}
    return r
