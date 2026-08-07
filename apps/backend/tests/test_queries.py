import sys
from datetime import datetime
from pathlib import Path

import aiohttp
import pytest
from shared_utils.models import HistoricalPrice, ItemMacroBaseline, LiveMarketTick, MarketItem
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlmodel import SQLModel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import queries
from queries import (
    _analyze_real_time_history,
    _apply_liquidity_guardrails,
    _compute_discount_corridor,
    _compute_snipe_threshold,
    _detect_regime_shift,
    get_item_market_context,
)

VERSIONED_NAME = "★ Butterfly Knife | Doppler (Phase 3) (Factory New)"
BASE_NAME = "★ Butterfly Knife | Doppler (Factory New)"

KNIFE_HISTORY = {
    "item_page": "https://skinport.com/item/butterfly",
    "market_page": "https://skinport.com/market",
    "last_24_hours": {"median": 700.0, "volume": 4},
    "last_7_days": {"median": 700.0, "volume": 30},
    "last_30_days": {"median": 690.0, "volume": 100},
    "last_90_days": {"median": 690.0, "volume": 400},
}

DOWNTREND_HISTORY = {
    "last_24_hours": {"median": 600.0, "volume": 4},
    "last_7_days": {"median": 700.0, "volume": 30},
    "last_30_days": {"median": 700.0, "volume": 100},
    "last_90_days": {"median": 700.0, "volume": 400},
}


def _macro_baseline(item_id: int, latest_price_cents: int, avg_volume_30d: float) -> ItemMacroBaseline:
    return ItemMacroBaseline(
        item_id=item_id,
        latest_price_cents=latest_price_cents,
        rolling_30d_avg_cents=latest_price_cents,
        rolling_90d_avg_cents=latest_price_cents,
        drift_percent=0.02,
        volatility_cents=3000,
        avg_volume_30d=avg_volume_30d,
        support_floor_cents=55000,
        updated_at=datetime(2025, 1, 1),
    )


async def _create_tables(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


async def _seed(maker, rows) -> None:
    async with maker() as session:
        session.add_all(rows)
        await session.commit()


async def _seed_knife(maker) -> None:
    base_item = MarketItem(id=1, market_hash_name=BASE_NAME, item_type="Knife")
    versioned_item = MarketItem(id=2, market_hash_name=VERSIONED_NAME, item_type="Knife")
    await _seed(
        maker,
        [
            base_item,
            versioned_item,
            HistoricalPrice(
                item_id=1,
                sale_date=datetime(2025, 1, 1),
                median_price_cents=100000,
                volume_sold=10,
            ),
            HistoricalPrice(
                item_id=1,
                sale_date=datetime(2025, 1, 2),
                median_price_cents=100000,
                volume_sold=12,
            ),
            LiveMarketTick(item_id=2, price_cents=70000, marketplace_source="skinport", inserted_at=datetime(2025, 1, 1)),
            LiveMarketTick(item_id=2, price_cents=70000, marketplace_source="skinport", inserted_at=datetime(2025, 1, 1)),
            LiveMarketTick(item_id=2, price_cents=50000, marketplace_source="steam", inserted_at=datetime(2025, 1, 1)),
            _macro_baseline(item_id=2, latest_price_cents=70000, avg_volume_30d=2.5),
        ],
    )


class TestDiscountCorridor:
    @pytest.mark.parametrize(
        ("item_type", "expected"),
        [
            ("Knife", 0.25),
            ("Glove", 0.25),
            ("Sticker", 0.35),
            ("Patch", 0.35),
            ("Rifle", 0.30),
            ("Unknown", 0.30),
        ],
    )
    def test_discount_factor_by_class(self, item_type, expected):
        assert _compute_discount_corridor(item_type) == expected


class TestLiquidityGuardrails:
    def test_no_baseline_is_liquid(self):
        assert _apply_liquidity_guardrails(None, "Rifle") == (True, None)

    def test_high_tier_floor_applies(self):
        baseline = _macro_baseline(item_id=1, latest_price_cents=20000, avg_volume_30d=0.04)
        assert _apply_liquidity_guardrails(baseline, "Rifle") == (False, 0.04)

    def test_high_tier_floor_boundary_liquid(self):
        baseline = _macro_baseline(item_id=1, latest_price_cents=20000, avg_volume_30d=0.05)
        assert _apply_liquidity_guardrails(baseline, "Rifle")[0] is True

    def test_knife_type_gets_low_floor_even_when_cheap(self):
        baseline = _macro_baseline(item_id=1, latest_price_cents=10000, avg_volume_30d=0.03)
        assert _apply_liquidity_guardrails(baseline, "Glove") == (False, 0.03)

    def test_low_tier_floor_applies(self):
        baseline = _macro_baseline(item_id=1, latest_price_cents=10000, avg_volume_30d=0.4)
        assert _apply_liquidity_guardrails(baseline, "Rifle") == (False, 0.4)

    def test_low_tier_actively_traded_liquid(self):
        baseline = _macro_baseline(item_id=1, latest_price_cents=10000, avg_volume_30d=0.6)
        assert _apply_liquidity_guardrails(baseline, "Rifle") == (True, 0.6)


class TestRegimeShift:
    def test_no_steam_baseline_no_shift(self):
        assert _detect_regime_shift(None, 70000, 0.25) == (False, True)

    def test_no_baseline_comparison_no_shift(self):
        assert _detect_regime_shift(100000.0, None, 0.25) == (False, True)

    def test_major_deviation_flags_regime_shift(self):
        assert _detect_regime_shift(100000.0, 40000, 0.25) == (True, False)

    def test_expected_cash_band_keeps_steam_baseline(self):
        assert _detect_regime_shift(100000.0, 70000, 0.25) == (False, True)

    def test_deviation_at_threshold_is_not_a_shift(self):
        assert _detect_regime_shift(100000.0, 56000, 0.25) == (False, True)


class TestSnipeThreshold:
    def test_no_downtrend_uses_base_discount(self):
        assert _compute_snipe_threshold(10000, False, 0.0) == 8500

    def test_downtrend_severity_capped_at_max_penalty(self):
        assert _compute_snipe_threshold(10000, True, 0.3) == 7000

    def test_downtrend_severity_scales_penalty(self):
        assert _compute_snipe_threshold(10000, True, 0.1) == 7500


class TestAnalyzeRealTimeHistory:
    def test_empty_history(self):
        assert _analyze_real_time_history({}) == (None, False, 0.0)

    def test_median_extracted_without_downtrend(self):
        median, downtrend, severity = _analyze_real_time_history(KNIFE_HISTORY)
        assert median == 70000
        assert downtrend is False
        assert severity == 0.0

    def test_downtrend_signals_accumulate(self):
        median, downtrend, severity = _analyze_real_time_history(DOWNTREND_HISTORY)
        assert median == 60000
        assert downtrend is True
        assert severity == pytest.approx(100 / 700, rel=1e-6)


@pytest.mark.asyncio
class TestGetItemMarketContext:
    async def test_happy_path_versioned_item(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed_knife(maker)
        mocker.patch.object(queries, "fetch_skinport_sales_history", return_value=KNIFE_HISTORY)

        context = await get_item_market_context(VERSIONED_NAME)

        assert context == {
            "historical_steam_avg_cents": 100000,
            "historical_skinport_avg_cents": 70000,
            "real_time_skinport_median_cents": 70000,
            "cash_equivalent_avg_cents": 70000,
            "snipe_threshold_cents": 59500,
            "item_type": "Knife",
            "is_liquid": True,
            "avg_volume_30d": 2.5,
            "drift_percent": 0.02,
            "volatility_cents": 3000,
            "support_floor_cents": 55000,
            "regime_shift_detected": False,
            "downtrend_detected": False,
            "downtrend_severity": 0.0,
            "item_page": "https://skinport.com/item/butterfly",
            "market_page": "https://skinport.com/market",
        }

    async def test_unknown_item_returns_none(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        mocker.patch.object(queries, "fetch_skinport_sales_history", return_value={})

        assert await get_item_market_context("Not A Real Item") is None

    async def test_missing_macro_baseline_uses_defaults(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed(
            maker,
            [
                MarketItem(id=3, market_hash_name="AK-47 | Redline (Field-Tested)", item_type="Rifle"),
                HistoricalPrice(
                    item_id=3,
                    sale_date=datetime(2025, 1, 1),
                    median_price_cents=10000,
                    volume_sold=50,
                ),
            ],
        )
        mocker.patch.object(queries, "fetch_skinport_sales_history", return_value={})

        context = await get_item_market_context("AK-47 | Redline (Field-Tested)")

        assert context["drift_percent"] == 0.0
        assert context["volatility_cents"] == 0
        assert context["support_floor_cents"] is None
        assert context["avg_volume_30d"] is None
        assert context["is_liquid"] is True
        assert context["cash_equivalent_avg_cents"] == 7000
        assert context["snipe_threshold_cents"] == 5950

    async def test_downtrend_steepens_snipe_threshold(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed_knife(maker)
        mocker.patch.object(queries, "fetch_skinport_sales_history", return_value=DOWNTREND_HISTORY)

        context = await get_item_market_context(VERSIONED_NAME)

        assert context["downtrend_detected"] is True
        assert context["downtrend_severity"] == pytest.approx(100 / 700, rel=1e-6)
        assert context["cash_equivalent_avg_cents"] == 60000
        assert context["snipe_threshold_cents"] == 42429

    async def test_regime_shift_detected_when_steam_drift_corrupted(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed_knife(maker)
        shift_history = {
            "last_24_hours": {"median": 400.0, "volume": 4},
            "last_7_days": {"median": 400.0, "volume": 30},
            "last_30_days": {"median": 400.0, "volume": 100},
            "last_90_days": {"median": 400.0, "volume": 400},
        }
        mocker.patch.object(queries, "fetch_skinport_sales_history", return_value=shift_history)

        context = await get_item_market_context(VERSIONED_NAME)

        assert context["regime_shift_detected"] is True
        assert context["cash_equivalent_avg_cents"] == 40000

    async def test_illiquid_item_returns_no_threshold(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed_knife(maker)
        await _seed(
            maker,
            [
                MarketItem(id=4, market_hash_name="★ Karambit | Fade (Factory New)", item_type="Knife"),
                _macro_baseline(item_id=4, latest_price_cents=70000, avg_volume_30d=0.01),
            ],
        )
        mocker.patch.object(queries, "fetch_skinport_sales_history", return_value={})

        context = await get_item_market_context("★ Karambit | Fade (Factory New)")

        assert context["is_liquid"] is False
        assert context["avg_volume_30d"] == 0.01
        assert context["cash_equivalent_avg_cents"] is None
        assert context["snipe_threshold_cents"] is None

    async def test_api_empty_response_falls_back_to_skinport_ticks(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed(
            maker,
            [
                MarketItem(id=3, market_hash_name="AK-47 | Redline (Field-Tested)", item_type="Rifle"),
                LiveMarketTick(item_id=3, price_cents=6500, marketplace_source="skinport", inserted_at=datetime(2025, 1, 1)),
                LiveMarketTick(item_id=3, price_cents=6500, marketplace_source="skinport", inserted_at=datetime(2025, 1, 1)),
            ],
        )
        mocker.patch.object(queries, "fetch_skinport_sales_history", return_value={})

        context = await get_item_market_context("AK-47 | Redline (Field-Tested)")

        assert context["real_time_skinport_median_cents"] is None
        assert context["downtrend_detected"] is False
        assert context["historical_steam_avg_cents"] is None
        assert context["cash_equivalent_avg_cents"] == 6500
        assert context["snipe_threshold_cents"] == 5525
        assert context["item_page"] is None

    async def test_db_failure_propagates(self, mocker):
        from contextlib import asynccontextmanager

        class ExplodingSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                pass

            async def exec(self, *args, **kwargs):
                raise SQLAlchemyError("database down")

        @asynccontextmanager
        async def exploding_scope():
            async with ExplodingSession() as session:
                yield session

        mocker.patch.object(queries, "session_scope", exploding_scope)

        with pytest.raises(SQLAlchemyError, match="database down"):
            await get_item_market_context(VERSIONED_NAME)


@pytest.mark.asyncio
class TestFetchSkinportSalesHistory:
    @pytest.fixture(autouse=True)
    def clear_cache(self):
        queries.sales_history_cache.clear()

    class _Client:
        def __init__(self, response):
            self.response = response

        def get(self, *args, **kwargs):
            return self.response

    class _OkResponse:
        status = 200

        def __init__(self, payload):
            self.payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def json(self):
            return self.payload

    class _ErrorResponse:
        status = 500

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

    class _FailingResponse:
        async def __aenter__(self):
            raise aiohttp.ClientError("connection refused")

        async def __aexit__(self, exc_type, exc, tb):
            return None

    async def test_client_error_returns_empty_dict(self, mocker):
        mocker.patch.object(queries, "_get_session", return_value=self._Client(self._FailingResponse()))

        assert await queries.fetch_skinport_sales_history("Boundary Item") == {}

    async def test_non_200_returns_empty_dict(self, mocker):
        mocker.patch.object(queries, "_get_session", return_value=self._Client(self._ErrorResponse()))

        assert await queries.fetch_skinport_sales_history("Boundary Item Two") == {}

    async def test_success_returns_first_entry(self, mocker):
        payload = [{"last_24_hours": {"median": 2.0, "volume": 5}}]
        mocker.patch.object(queries, "_get_session", return_value=self._Client(self._OkResponse(payload)))

        assert await queries.fetch_skinport_sales_history("Boundary Item Three") == payload[0]

    async def test_version_filter_selects_matching_entry(self, mocker):
        payload = [
            {"version": "Factory New", "last_24_hours": {"median": 1.0, "volume": 1}},
            {"version": "Minimal Wear", "last_24_hours": {"median": 2.0, "volume": 1}},
        ]
        mocker.patch.object(queries, "_get_session", return_value=self._Client(self._OkResponse(payload)))

        result = await queries.fetch_skinport_sales_history("Boundary Item Four", version="Minimal Wear")

        assert result["version"] == "Minimal Wear"

    async def test_version_no_match_falls_back_to_first_entry(self, mocker):
        payload = [
            {"version": "Factory New", "last_24_hours": {"median": 1.0, "volume": 1}},
            {"version": "Minimal Wear", "last_24_hours": {"median": 2.0, "volume": 1}},
        ]
        mocker.patch.object(queries, "_get_session", return_value=self._Client(self._OkResponse(payload)))

        result = await queries.fetch_skinport_sales_history("Boundary Item Five", version="Souvenir")

        assert result["version"] == "Factory New"

    async def test_empty_list_payload_returns_empty_dict(self, mocker):
        mocker.patch.object(queries, "_get_session", return_value=self._Client(self._OkResponse([])))

        assert await queries.fetch_skinport_sales_history("Boundary Item Six") == {}

    async def test_cached_entry_skips_api_call(self, mocker):
        class CountingClient:
            def __init__(self):
                self.get_calls = 0

            def get(self, *args, **kwargs):
                self.get_calls += 1
                return self._OkResponse([{"last_24_hours": {"median": 5.0, "volume": 2}}])

            class _OkResponse:
                status = 200

                def __init__(self, payload):
                    self.payload = payload

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return None

                async def json(self):
                    return self.payload

        client = CountingClient()
        mocker.patch.object(queries, "_get_session", return_value=client)

        first = await queries.fetch_skinport_sales_history("Boundary Item Seven")
        second = await queries.fetch_skinport_sales_history("Boundary Item Seven")

        assert first == second
        assert client.get_calls == 1


@pytest.mark.asyncio
class TestGetStickerPriceCents:
    async def test_api_median_resolves_to_cents(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        mocker.patch.object(queries, "fetch_skinport_sales_history", return_value=KNIFE_HISTORY)

        assert await queries.get_sticker_price_cents("Titan | Katowice 2014") == 70000

    async def test_api_empty_falls_back_to_db_history_avg(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed(
            maker,
            [
                MarketItem(id=10, market_hash_name="Titan | Katowice 2014", item_type="Sticker"),
                HistoricalPrice(item_id=10, sale_date=datetime(2025, 1, 1), median_price_cents=12345, volume_sold=5),
            ],
        )
        mocker.patch.object(queries, "fetch_skinport_sales_history", return_value={})

        assert await queries.get_sticker_price_cents("Titan | Katowice 2014") == 12345

    async def test_db_history_missing_falls_back_to_live_ticks(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed(
            maker,
            [
                MarketItem(id=11, market_hash_name="Titan | Katowice 2014", item_type="Sticker"),
                LiveMarketTick(item_id=11, price_cents=9999, marketplace_source="skinport", inserted_at=datetime(2025, 1, 1)),
                LiveMarketTick(item_id=11, price_cents=9999, marketplace_source="skinport", inserted_at=datetime(2025, 1, 1)),
            ],
        )
        mocker.patch.object(queries, "fetch_skinport_sales_history", return_value={})

        assert await queries.get_sticker_price_cents("Titan | Katowice 2014") == 9999

    async def test_unknown_sticker_returns_none(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        mocker.patch.object(queries, "fetch_skinport_sales_history", return_value={})

        assert await queries.get_sticker_price_cents("Not A Real Sticker") is None

    async def test_api_median_missing_falls_back_to_db(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed(
            maker,
            [
                MarketItem(id=12, market_hash_name="Titan | Katowice 2014", item_type="Sticker"),
                HistoricalPrice(item_id=12, sale_date=datetime(2025, 1, 1), median_price_cents=777, volume_sold=1),
            ],
        )
        mocker.patch.object(
            queries,
            "fetch_skinport_sales_history",
            return_value={"last_24_hours": {"median": None, "volume": 0}},
        )

        assert await queries.get_sticker_price_cents("Titan | Katowice 2014") == 777

    async def test_db_failure_returns_none_without_raising(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed(
            maker,
            [MarketItem(id=13, market_hash_name="Titan | Katowice 2014", item_type="Sticker")],
        )
        mocker.patch.object(queries, "fetch_skinport_sales_history", return_value={})
        mocker.patch.object(
            queries,
            "session_scope",
            side_effect=SQLAlchemyError("database down"),
        )

        assert await queries.get_sticker_price_cents("Titan | Katowice 2014") is None


@pytest.mark.asyncio
class TestSearchMacroTrends:
    async def test_empty_query_returns_empty_list(self, db_maker):
        maker, engine = db_maker
        await _create_tables(engine)

        assert await queries.search_macro_trends("") == []

    async def test_no_matching_items_returns_empty_list(self, db_maker):
        maker, engine = db_maker
        await _create_tables(engine)

        assert await queries.search_macro_trends("Butterfly") == []

    async def test_skips_items_without_baseline(self, db_maker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed(
            maker,
            [
                MarketItem(id=20, market_hash_name="AK-47 | Redline (Field-Tested)", item_type="Rifle"),
                MarketItem(id=21, market_hash_name="AWP | Asiimov (Field-Tested)", item_type="Rifle"),
                _macro_baseline(item_id=21, latest_price_cents=50000, avg_volume_30d=1.0),
            ],
        )

        results = await queries.search_macro_trends("AK-47")

        assert results == []

    async def test_happy_path_returns_baseline_fields(self, db_maker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed(
            maker,
            [
                MarketItem(id=22, market_hash_name="AK-47 | Redline (Field-Tested)", item_type="Rifle"),
                _macro_baseline(item_id=22, latest_price_cents=15000, avg_volume_30d=3.0),
            ],
        )

        results = await queries.search_macro_trends("redline")

        assert len(results) == 1
        assert results[0]["market_hash_name"] == "AK-47 | Redline (Field-Tested)"
        assert results[0]["drift_percent"] == 0.02
        assert results[0]["latest_price_cents"] == 15000


class TestPrivateFetchers:
    @pytest.mark.asyncio
    async def test_resolve_item_falls_back_to_base_when_version_missing(self, db_maker):
        maker, engine = db_maker
        await _create_tables(engine)
        await _seed(
            maker,
            [MarketItem(id=30, market_hash_name=BASE_NAME, item_type="Knife")],
        )

        async with maker() as session:
            resolved = await queries._resolve_item(session, VERSIONED_NAME, BASE_NAME, "Phase 3")

        assert resolved == (30, "Knife", 30)

    @pytest.mark.asyncio
    async def test_resolve_item_unknown_returns_none(self, db_maker):
        maker, engine = db_maker
        await _create_tables(engine)

        async with maker() as session:
            assert await queries._resolve_item(session, "Ghost Item", "Ghost Item", None) is None

    @pytest.mark.asyncio
    async def test_fetchers_log_and_return_none_on_sqlalchemy_error(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        mocker.patch("sqlmodel.ext.asyncio.session.AsyncSession.exec", side_effect=SQLAlchemyError("boom"))

        async with maker() as session:
            assert await queries._fetch_steam_baseline(session, "Item", 1) is None
            assert await queries._fetch_skinport_baseline(session, "Item", 1) is None
            assert await queries._fetch_macro_baseline(session, "Item", 1) is None

    @pytest.mark.asyncio
    async def test_resolve_item_propagates_sqlalchemy_error(self, db_maker, mocker):
        maker, engine = db_maker
        await _create_tables(engine)
        mocker.patch("sqlmodel.ext.asyncio.session.AsyncSession.exec", side_effect=SQLAlchemyError("boom"))

        async with maker() as session:
            with pytest.raises(SQLAlchemyError, match="boom"):
                await queries._resolve_item(session, VERSIONED_NAME, BASE_NAME, "Phase 3")


@pytest.mark.asyncio
class TestHttpSession:
    async def test_get_session_reuses_singleton(self):
        first = await queries._get_session()
        second = await queries._get_session()
        try:
            assert first is second
            assert not first.closed
        finally:
            await queries.close_http_session()

    async def test_close_http_session_closes_and_resets(self):
        session = await queries._get_session()
        assert session is not None

        await queries.close_http_session()

        assert queries._backend_session is None
        assert session.closed


class TestAnalyzeRealTimeHistoryEdge:
    def test_history_without_usable_median_returns_defaults(self):
        history = {"last_24_hours": {"median": None, "volume": 0}}

        assert _analyze_real_time_history(history) == (None, False, 0.0)
