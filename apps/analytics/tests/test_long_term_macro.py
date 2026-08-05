from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import long_term_macro
import pytest
from long_term_macro import analyze_long_term_macro, calculate_macro_trends, fetch_historical_prices_chunk, fetch_tracked_items


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, rows_by_call):
        self.rows_by_call = list(rows_by_call)
        self.executed = []

    async def execute(self, stmt):
        self.executed.append(str(stmt))
        return _Result(self.rows_by_call.pop(0))

    def begin(self):
        @asynccontextmanager
        async def _begin():
            yield self

        return _begin()


class FakeEngine:
    def __init__(self, rows_by_call):
        self.rows_by_call = rows_by_call
        self.conns = []

    def connect(self):
        @asynccontextmanager
        async def _connect():
            conn = FakeConn(self.rows_by_call)
            self.conns.append(conn)
            yield conn

        return _connect()


def _install_fake_engine(monkeypatch, rows_by_call):
    engine = FakeEngine(rows_by_call)
    monkeypatch.setattr(long_term_macro, "async_engine", engine)
    return engine


def _price_points(count=35, start_price=10000):
    start = datetime(2025, 1, 1)
    return [
        {
            "sale_date": start + timedelta(days=day),
            "median_price_cents": start_price,
            "volume_sold": 10,
        }
        for day in range(count)
    ]


# ---------------------------------------------------------------------------
# calculate_macro_trends (pure pandas logic)
# ---------------------------------------------------------------------------


class TestCalculateMacroTrends:
    @pytest.mark.asyncio
    async def test_empty_price_data_returns_empty_dict(self):
        assert await calculate_macro_trends(1, "Item", []) == {}

    @pytest.mark.asyncio
    async def test_flat_prices_produce_stable_metrics(self):
        analysis = await calculate_macro_trends(1, "AK-47 | Redline (Field-Tested)", _price_points(35))

        assert analysis["latest_price_cents"] == 10000
        assert analysis["rolling_30d_avg_cents"] == 10000
        assert analysis["rolling_90d_avg_cents"] == 10000
        assert analysis["drift_percent"] == 0.0
        assert analysis["volatility_cents"] == 0
        assert analysis["coefficient_of_variation"] == 0.0
        assert analysis["avg_volume_30d"] == 10.0
        assert analysis["support_floor_cents"] == 10000
        assert analysis["total_points_analyzed"] == 35
        assert analysis["monthly_seasonality"] == {1: 10000.0, 2: 10000.0}
        assert analysis["item_id"] == 1
        assert analysis["market_hash_name"] == "AK-47 | Redline (Field-Tested)"

    @pytest.mark.asyncio
    async def test_single_price_point_handles_nan_metrics(self):
        analysis = await calculate_macro_trends(2, "Lonely Item", _price_points(1))

        assert analysis["latest_price_cents"] == 10000
        assert analysis["volatility_cents"] == 0
        assert analysis["support_floor_cents"] == 10000
        assert analysis["drift_percent"] == 0.0

    @pytest.mark.asyncio
    async def test_downtrend_detected_in_drift(self):
        prices = [10000] * 15 + [6000 - i * 10 for i in range(30)]
        start = datetime(2025, 1, 1)
        points = [
            {
                "sale_date": start + timedelta(days=i),
                "median_price_cents": prices[i],
                "volume_sold": 5,
            }
            for i in range(len(prices))
        ]
        analysis = await calculate_macro_trends(3, "Falling Item", points)

        assert analysis["drift_percent"] < 0
        assert analysis["coefficient_of_variation"] > 0


# ---------------------------------------------------------------------------
# DB-backed tasks (mocked async_engine)
# ---------------------------------------------------------------------------


class TestFetchTasks:
    @pytest.mark.asyncio
    async def test_fetch_tracked_items_returns_dicts(self, monkeypatch):
        _install_fake_engine(monkeypatch, [[(1, "AK-47 | Redline (Field-Tested)", "Rifle"), (2, "Gloves", "Glove")]])

        items = await fetch_tracked_items()

        assert items == [
            {"id": 1, "market_hash_name": "AK-47 | Redline (Field-Tested)", "item_type": "Rifle"},
            {"id": 2, "market_hash_name": "Gloves", "item_type": "Glove"},
        ]

    @pytest.mark.asyncio
    async def test_fetch_historical_prices_groups_by_item(self, monkeypatch):
        rows = [
            (1, datetime(2025, 1, 1), 100, 2),
            (1, datetime(2025, 1, 2), 200, 3),
            (2, datetime(2025, 1, 1), 50, 1),
        ]
        _install_fake_engine(monkeypatch, [rows])

        by_item = await fetch_historical_prices_chunk([1, 2])

        assert list(by_item.keys()) == [1, 2]
        assert by_item[1][0] == {"sale_date": datetime(2025, 1, 1), "median_price_cents": 100, "volume_sold": 2}
        assert by_item[1][1]["median_price_cents"] == 200
        assert by_item[2][0]["volume_sold"] == 1

    @pytest.mark.asyncio
    async def test_save_baselines_writes_and_logs_top_drift(self, monkeypatch, caplog):
        fake_engine = _install_fake_engine(monkeypatch, [[], []])
        results = [
            {
                "item_id": 1,
                "market_hash_name": "Slow Item",
                "latest_price_cents": 10000,
                "rolling_30d_avg_cents": 10000,
                "rolling_90d_avg_cents": 10000,
                "drift_percent": 1.0,
                "volatility_cents": 100,
                "avg_volume_30d": 5.0,
                "support_floor_cents": 9000,
            },
            {
                "item_id": 2,
                "market_hash_name": "Hot Item",
                "latest_price_cents": 20000,
                "rolling_30d_avg_cents": 20000,
                "rolling_90d_avg_cents": 20000,
                "drift_percent": 9.0,
                "volatility_cents": 200,
                "avg_volume_30d": 5.0,
                "support_floor_cents": 18000,
            },
        ]

        await long_term_macro.save_macro_baselines_to_db(results)

        assert len(fake_engine.conns) == 1
        assert len(fake_engine.conns[0].executed) == 2
        assert "Highest upward trend: 'Hot Item' with 9.00% drift." in caplog.text

    @pytest.mark.asyncio
    async def test_save_baselines_without_results_is_noop(self, monkeypatch):
        fake_engine = _install_fake_engine(monkeypatch, [[]])

        await long_term_macro.save_macro_baselines_to_db([])

        assert fake_engine.rows_by_call == [[]]


# ---------------------------------------------------------------------------
# Orchestration flow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flow_processes_limit_and_syncs_edge(monkeypatch):
    items = [
        {"id": 1, "market_hash_name": "AK-47 | Redline (Field-Tested)", "item_type": "Rifle"},
        {"id": 2, "market_hash_name": "★ Butterfly Knife | Doppler (Factory New)", "item_type": "Knife"},
        {"id": 3, "market_hash_name": "Empty Item", "item_type": "Rifle"},
    ]
    price_data = {1: _price_points(35), 2: _price_points(35), 3: []}

    mock_fetch_items = AsyncMock(return_value=items)
    mock_fetch_chunk = AsyncMock(side_effect=lambda ids: {i: price_data[i] for i in ids})
    mock_save = AsyncMock()
    mock_sync = AsyncMock()
    monkeypatch.setattr(long_term_macro, "fetch_tracked_items", mock_fetch_items)
    monkeypatch.setattr(long_term_macro, "fetch_historical_prices_chunk", mock_fetch_chunk)
    monkeypatch.setattr(long_term_macro, "save_macro_baselines_to_db", mock_save)
    monkeypatch.setattr(long_term_macro, "run_sync_baselines_to_edge", mock_sync)

    await analyze_long_term_macro(limit_items=2)

    mock_fetch_items.assert_awaited_once()
    mock_fetch_chunk.assert_awaited_once_with([1, 2])
    mock_save.assert_awaited_once()
    saved = mock_save.await_args.args[0]
    assert len(saved) == 2
    assert mock_sync.await_count == 1


@pytest.mark.asyncio
async def test_flow_without_limit_processes_all(monkeypatch):
    items = [{"id": 1, "market_hash_name": "AK-47 | Redline (Field-Tested)", "item_type": "Rifle"}]
    price_data = {1: _price_points(35)}

    mock_fetch_items = AsyncMock(return_value=items)
    mock_fetch_chunk = AsyncMock(side_effect=lambda ids: {i: price_data[i] for i in ids})
    mock_save = AsyncMock()
    mock_sync = AsyncMock()
    monkeypatch.setattr(long_term_macro, "fetch_tracked_items", mock_fetch_items)
    monkeypatch.setattr(long_term_macro, "fetch_historical_prices_chunk", mock_fetch_chunk)
    monkeypatch.setattr(long_term_macro, "save_macro_baselines_to_db", mock_save)
    monkeypatch.setattr(long_term_macro, "run_sync_baselines_to_edge", mock_sync)

    await analyze_long_term_macro(limit_items=None)

    mock_fetch_chunk.assert_awaited_once_with([1])
    mock_save.assert_awaited_once()
