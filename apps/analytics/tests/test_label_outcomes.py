from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import label_outcomes
import pytest
from label_outcomes import (
    FeedSale,
    LabelConfig,
    default_start,
    fetch_feed_sales,
    iter_windows,
    label_listing_outcomes,
    label_listings,
    parse_sale_row,
    save_outcomes,
)
from sqlalchemy.dialects import postgresql

T0 = datetime(2026, 9, 1, 12, 0, 0)
DAY = timedelta(days=1)
# 14 days, plus the settle time: the earliest moment a T0 listing can be labeled.
AS_OF = T0 + 15 * DAY
NAME = "AK-47 | Slate (Field-Tested)"


def listed(listing_id, price_cents, at=T0, name=NAME):
    return FeedSale(listing_id=listing_id, market_hash_name=name, feed_name=name, price_cents=price_cents, seen_at=at)


def sold(listing_id, price_cents, at, name=NAME):
    return FeedSale(listing_id=listing_id, market_hash_name=name, feed_name=name, price_cents=price_cents, seen_at=at)


def comparable_sales(prices, start=T0 + 8 * DAY):
    return [sold(f"other-{i}", price, start + timedelta(hours=i)) for i, price in enumerate(prices)]


def only_label(listed_sales, sold_sales, config=None, as_of=AS_OF):
    rows = label_listings(listed_sales, sold_sales, config or LabelConfig(), as_of)
    assert len(rows) == 1
    return rows[0]


def test_clear_winner():
    # Median comparable resale 1500 - 120 fee - 1000 buy = 380
    row = only_label([listed("L1", 1000)], comparable_sales([1400, 1500, 1600]))

    assert row["comparable_sales"] == 3
    assert row["resale_price_cents"] == 1500
    assert row["resale_net_margin_cents"] == 380
    assert row["is_profitable"] is True
    assert row["label_available_at"] == T0 + 14 * DAY
    assert row["label_version"] == "v1"


def test_clear_loser():
    # Resale at 1000 after the 8% fee loses money on a 1000 buy.
    row = only_label([listed("L1", 1000)], comparable_sales([990, 1000, 1010]))

    assert row["resale_net_margin_cents"] == -80
    assert row["is_profitable"] is False


def test_illiquid_item_is_neutral():
    row = only_label([listed("L1", 1000)], comparable_sales([5000, 5000]))

    assert row["comparable_sales"] == 2
    assert row["is_profitable"] is None
    assert row["resale_price_cents"] is None
    assert row["resale_net_margin_cents"] is None


def test_listing_never_seen_to_sell_is_censored_not_a_loss():
    row = only_label([listed("L1", 1000)], comparable_sales([1400, 1500, 1600]))

    assert row["sale_censored"] is True
    assert row["listing_sold_within_s"] is None
    # Censoring says nothing about profitability: the market-level label is still a win.
    assert row["is_profitable"] is True


def test_listing_seen_to_sell_records_time_to_sale():
    row = only_label([listed("L1", 1000)], [sold("L1", 1000, T0 + timedelta(minutes=5))])

    assert row["sale_censored"] is False
    assert row["listing_sold_within_s"] == 300


def test_look_ahead_guard_ignores_sales_after_the_horizon():
    horizon_end = T0 + 14 * DAY
    late = [
        sold("late-1", 9000, horizon_end + timedelta(seconds=1)),
        sold("late-2", 9000, horizon_end + DAY),
        sold("late-3", 9000, horizon_end + 2 * DAY),
        # The listing's own sale after the horizon is not known at label time either.
        sold("L1", 1000, horizon_end + timedelta(minutes=1)),
    ]
    row = only_label([listed("L1", 1000)], late, as_of=horizon_end + 10 * DAY)

    assert row["comparable_sales"] == 0
    assert row["is_profitable"] is None
    assert row["sale_censored"] is True


def test_sales_inside_the_trade_hold_are_not_comparable():
    # Resale is impossible before the 7-day hold ends, so earlier sales cannot set the resale price.
    during_hold = [sold(f"h{i}", 9000, T0 + DAY + timedelta(hours=i)) for i in range(5)]
    row = only_label([listed("L1", 1000)], during_hold)

    assert row["comparable_sales"] == 0
    assert row["is_profitable"] is None


def test_window_bounds_are_inclusive():
    hold_end = T0 + 7 * DAY
    horizon_end = T0 + 14 * DAY
    row = only_label([listed("L1", 1000)], [sold("a", 1500, hold_end), sold("b", 1500, horizon_end), sold("c", 1500, hold_end)])

    assert row["comparable_sales"] == 3


def test_other_items_and_the_listing_itself_are_not_comparable():
    sales = [
        *comparable_sales([1500, 1500, 1500], start=T0 + 8 * DAY),
        sold("x1", 1, T0 + 9 * DAY, name="AK-47 | Slate (Minimal Wear)"),
        sold("L1", 1, T0 + 9 * DAY),  # the listing's own (late) sale is excluded from comparables
    ]
    row = only_label([listed("L1", 1000)], sales)

    assert row["comparable_sales"] == 3
    assert row["resale_price_cents"] == 1500


def test_listing_not_labeled_before_horizon_and_settle_time():
    config = LabelConfig()
    ready_at = T0 + timedelta(seconds=config.horizon_seconds + config.settle_seconds)

    assert label_listings([listed("L1", 1000)], [], config, ready_at - timedelta(seconds=1)) == []
    assert len(label_listings([listed("L1", 1000)], [], config, ready_at)) == 1


def test_duplicate_sightings_keep_the_first_listing_and_sale():
    sightings = [listed("L1", 1200, at=T0 + timedelta(minutes=3)), listed("L1", 1000, at=T0)]
    sales = [sold("L1", 1000, T0 + timedelta(minutes=10)), sold("L1", 1000, T0 + timedelta(minutes=20))]
    row = only_label(sightings, sales)

    assert row["listed_at"] == T0
    assert row["listed_price_cents"] == 1000
    assert row["listing_sold_within_s"] == 600


def test_duplicate_sold_sightings_count_once_as_comparable():
    sales = [
        sold("S1", 1500, T0 + 8 * DAY),
        sold("S1", 1500, T0 + 8 * DAY + timedelta(seconds=5)),
        sold("S2", 1500, T0 + 9 * DAY),
    ]
    row = only_label([listed("L1", 1000)], sales)

    assert row["comparable_sales"] == 2


def test_even_number_of_comparables_uses_lower_median():
    row = only_label([listed("L1", 1000)], comparable_sales([1200, 1400, 1600, 5000]))

    assert row["resale_price_cents"] == 1400


def test_min_margin_from_config_decides_profitability():
    from shared_utils import SKINPORT_FEES
    from shared_utils.pnl import VenueFees

    strict_fees = VenueFees(
        venue="skinport", fee_tiers=SKINPORT_FEES.fee_tiers, hold_seconds=SKINPORT_FEES.hold_seconds, min_margin_cents=500
    )
    row = only_label([listed("L1", 1000)], comparable_sales([1500, 1500, 1500]), LabelConfig(fees=strict_fees))

    assert row["resale_net_margin_cents"] == 380
    assert row["is_profitable"] is False


def test_labels_are_deterministic_and_ordered():
    listings = [listed("L2", 900, at=T0 + timedelta(hours=1)), listed("L1", 1000)]
    sales = comparable_sales([1500, 1400, 1600])

    first = label_listings(listings, sales, LabelConfig(), AS_OF + DAY)
    second = label_listings(list(reversed(listings)), list(reversed(sales)), LabelConfig(), AS_OF + DAY)

    assert first == second
    assert [row["listing_id"] for row in first] == ["L1", "L2"]


def test_default_config_uses_the_registered_skinport_fees():
    from shared_utils import SKINPORT_FEES

    assert LabelConfig().fees is SKINPORT_FEES


@pytest.mark.parametrize(
    "kwargs",
    [{"horizon_seconds": 7 * 86_400}, {"min_comparable_sales": 0}, {"settle_seconds": -1}],
    ids=["horizon_within_hold", "no_min_sales", "negative_settle"],
)
def test_invalid_label_config_is_rejected(kwargs):
    with pytest.raises(ValueError):
        LabelConfig(**kwargs)


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ((T0, "58903454", NAME, "default", "397"), FeedSale("58903454", NAME, NAME, 397, T0)),
        (
            (T0, 1, "★ Karambit | Doppler (Factory New)", "Phase 2", 90000),
            FeedSale("1", "★ Karambit | Doppler (Phase 2) (Factory New)", "★ Karambit | Doppler (Factory New)", 90000, T0),
        ),
        ((T0, None, NAME, "default", "397"), None),
        ((T0, "1", None, "default", "397"), None),
        ((T0, "1", NAME, "default", "abc"), None),
        ((T0, "1", NAME, "default", None), None),
        ((T0, "1", NAME, "default", "0"), None),
    ],
    ids=["plain", "phase", "no_product_id", "no_name", "bad_price", "no_price", "zero_price"],
)
def test_parse_sale_row(row, expected):
    assert parse_sale_row(row) == expected


def test_iter_windows_covers_range_without_gaps():
    windows = iter_windows(T0, T0 + timedelta(hours=30), timedelta(hours=12))

    assert windows == [
        (T0, T0 + timedelta(hours=12)),
        (T0 + timedelta(hours=12), T0 + timedelta(hours=24)),
        (T0 + timedelta(hours=24), T0 + timedelta(hours=30)),
    ]
    assert iter_windows(T0, T0, timedelta(hours=1)) == []


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    async def execute(self, stmt, params=None):
        self.executed.append((stmt, params))
        return _Result(self.rows)


class FakeEngine:
    def __init__(self, rows=()):
        self.conn = FakeConn(list(rows))

    def _context(self):
        @asynccontextmanager
        async def _ctx():
            yield self.conn

        return _ctx()

    def connect(self):
        return self._context()

    def begin(self):
        return self._context()


@pytest.mark.asyncio
async def test_fetch_feed_sales_filters_by_names_and_parses(monkeypatch):
    engine = FakeEngine([(T0, "1", NAME, "default", "500"), (T0, None, NAME, "default", "500")])
    monkeypatch.setattr(label_outcomes, "async_engine", engine)

    sales = await fetch_feed_sales("sold", T0, T0 + DAY, names=[NAME])

    assert sales == [FeedSale("1", NAME, NAME, 500, T0)]
    stmt, params = engine.conn.executed[0]
    assert "ANY(:names)" in str(stmt)
    assert params == {"source": "skinport", "event_type": "sold", "start": T0, "end": T0 + DAY, "names": [NAME]}


@pytest.mark.asyncio
async def test_fetch_feed_sales_without_names_has_no_name_filter(monkeypatch):
    engine = FakeEngine()
    monkeypatch.setattr(label_outcomes, "async_engine", engine)

    assert await fetch_feed_sales("listed", T0, T0 + DAY) == []
    stmt, params = engine.conn.executed[0]
    assert "ANY(:names)" not in str(stmt)
    assert "names" not in params


@pytest.mark.asyncio
async def test_save_outcomes_upserts_keeping_the_earliest_sighting(monkeypatch):
    engine = FakeEngine()
    monkeypatch.setattr(label_outcomes, "async_engine", engine)
    rows = label_listings([listed("L1", 1000)], comparable_sales([1500, 1500, 1500]), LabelConfig(), AS_OF)

    await save_outcomes(rows)

    stmt, _ = engine.conn.executed[0]
    sql = str(stmt.compile(dialect=postgresql.dialect()))
    assert "ON CONFLICT (source, listing_id, label_version) DO UPDATE" in sql
    assert "WHERE listing_outcomes.listed_at >= excluded.listed_at" in sql
    assert "resale_net_margin_cents = excluded.resale_net_margin_cents" in sql


@pytest.mark.asyncio
async def test_save_outcomes_with_no_rows_writes_nothing(monkeypatch):
    engine = FakeEngine()
    monkeypatch.setattr(label_outcomes, "async_engine", engine)

    await save_outcomes([])

    assert engine.conn.executed == []


@pytest.mark.asyncio
async def test_flow_labels_only_matured_listings_and_is_rerunnable(monkeypatch):
    feed = {
        "listed": [listed("L1", 1000), listed("L2", 1000, at=T0 + 5 * DAY)],
        "sold": comparable_sales([1500, 1500, 1500]),
    }
    fetch_calls = []

    async def fake_fetch(event_type, start, end, names=None):
        fetch_calls.append((event_type, start, end, names))
        return [sale for sale in feed[event_type] if start <= sale.seen_at < end]

    store = {}

    async def fake_save(rows):
        for row in rows:
            store[(row["source"], row["listing_id"], row["label_version"])] = row

    monkeypatch.setattr(label_outcomes, "fetch_feed_sales", fake_fetch)
    monkeypatch.setattr(label_outcomes, "save_outcomes", fake_save)

    # As of T0 + 16 days only L1 has matured; L2 (listed on day 5) must wait until day 19.
    as_of = T0 + 16 * DAY
    first_total = await label_listing_outcomes(start=T0, end=T0 + 10 * DAY, as_of=as_of, window_hours=24)
    snapshot = dict(store)
    second_total = await label_listing_outcomes(start=T0, end=T0 + 10 * DAY, as_of=as_of, window_hours=24)

    assert first_total == second_total == 1
    assert list(store) == [("skinport", "L1", "v1")]
    assert store == snapshot
    # The sold query never reaches past the last scanned listing time plus the horizon.
    last_listing_end = as_of - timedelta(seconds=LabelConfig().horizon_seconds + LabelConfig().settle_seconds)
    sold_ends = [end for event_type, _, end, _ in fetch_calls if event_type == "sold"]
    assert max(sold_ends) <= last_listing_end + timedelta(seconds=LabelConfig().horizon_seconds)
    assert all(names == [NAME] for event_type, _, _, names in fetch_calls if event_type == "sold")


@pytest.mark.asyncio
async def test_flow_does_nothing_before_any_horizon_has_passed(monkeypatch):
    async def fail_fetch(*args, **kwargs):
        raise AssertionError("must not query when nothing can be labeled")

    monkeypatch.setattr(label_outcomes, "fetch_feed_sales", fail_fetch)

    assert await label_listing_outcomes(start=T0, end=T0 + DAY, as_of=T0 + 2 * DAY) == 0


@pytest.mark.asyncio
async def test_flow_rejects_non_positive_window():
    with pytest.raises(ValueError):
        await label_listing_outcomes(start=T0, end=T0 + DAY, as_of=AS_OF, window_hours=0)


def test_default_start_covers_the_last_matured_days():
    config = LabelConfig()
    now = T0 + 30 * DAY

    start = default_start(now, 3, config)

    assert start == now - timedelta(seconds=config.horizon_seconds + config.settle_seconds) - 3 * DAY
