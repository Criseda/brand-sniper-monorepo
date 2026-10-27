"""DMarket labels (#339): sales from venue_sales, the item's own seller fee, and waiting for the sales sync."""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import label_outcomes
import outcome_data
import pytest
from label_outcomes import label_listing_outcomes
from outcome_data import fetch_item_fees, fetch_venue_sales, latest_sales_sync
from outcome_labels import (
    OWN_SALE_CLOCK_SKEW_SECONDS,
    SALES_FROM_VENUE_SALES,
    FeedSale,
    ItemFees,
    LabelConfig,
    SoldIndex,
    default_start,
    label_listing,
    label_listings,
    latest_labelable,
)

T0 = datetime(2026, 10, 26, 12, 0, 0)
DAY = timedelta(days=1)
NAME = "AK-47 | Redline (Field-Tested)"
CONFIG = LabelConfig.for_venue("dmarket", "v1-dmarket")
# A sync that started a day after the horizon of a T0 listing: the earliest that lets it be labeled.
SYNCED_AT = T0 + 15 * DAY
AS_OF = SYNCED_AT + DAY
BUILT_BEFORE = T0 - timedelta(hours=3)


def listed(listing_id="L1", price_cents=1000, at=T0, float_value=0.25, pattern=12, name=NAME):
    return FeedSale(listing_id, name, price_cents, at, float_value=float_value, pattern=pattern)


def sale(sale_id, price_cents, at, float_value=None, pattern=None, name=NAME):
    return FeedSale(f"sale-{sale_id}", name, price_cents, at, float_value=float_value, pattern=pattern)


def comparable(prices, start=T0 + 8 * DAY):
    """Sales of other items of the same name after DMarket's 7 day hold."""
    return [
        sale(index, price, start + timedelta(hours=index), float_value=0.5 + index / 100) for index, price in enumerate(prices)
    ]


def fees(fee_bps=200, built_at=BUILT_BEFORE, name=NAME):
    return ItemFees.build([(built_at, {name: fee_bps})])


def only_label(listings, sales, item_fees=None, config=CONFIG):
    rows = label_listings(listings, sales, config, AS_OF, item_fees if item_fees is not None else fees(), SYNCED_AT)
    assert len(rows) == 1
    return rows[0]


# --- The label version ---


def test_dmarket_labels_read_venue_sales_with_item_fees_and_wait_a_day_for_the_sync():
    assert CONFIG.sales_source == SALES_FROM_VENUE_SALES
    assert CONFIG.item_seller_fees and CONFIG.sales_settle_seconds == 86_400 and CONFIG.own_sale_by_item
    assert CONFIG.fees.venue == "dmarket" and CONFIG.fees.hold_seconds == 7 * 86_400

    contract = CONFIG.contract()
    assert contract["sales"]["source"] == SALES_FROM_VENUE_SALES and contract["sales"]["sale_type"] == "offer"
    assert contract["sales"]["settle_seconds"] == 86_400
    assert "build in force at listed_at" in contract["item_seller_fees"]


def test_the_feed_versions_keep_the_contract_they_were_registered_with():
    assert "sales" not in LabelConfig().contract() and "item_seller_fees" not in LabelConfig().contract()


def test_parameters_passed_to_for_venue_win_over_the_venue_defaults():
    assert LabelConfig.for_venue("dmarket", "v2-dmarket", sales_settle_seconds=3_600).sales_settle_seconds == 3_600


@pytest.mark.parametrize(
    "kwargs",
    [{"sales_source": "csv"}, {"sales_settle_seconds": 60}, {"own_sale_by_item": True}, {"sales_settle_seconds": -1}],
    ids=["unknown_source", "settle_with_feed_sales", "own_sale_with_feed_sales", "negative_settle"],
)
def test_invalid_sales_parameters_are_rejected(kwargs):
    with pytest.raises(ValueError):
        LabelConfig(**kwargs)


# --- The item's own seller fee ---


def test_the_resale_pays_the_item_fee_of_the_build_in_force_at_listing_time():
    # 1500 resale less 2% (30), less the 1000 paid; DMarket's buyer pays no fee.
    row = only_label([listed()], comparable([1400, 1500, 1600]))
    assert (row["resale_price_cents"], row["resale_net_margin_cents"], row["is_profitable"]) == (1500, 470, True)


def test_an_item_without_a_fee_in_that_build_pays_the_ten_percent_default():
    row = only_label([listed()], comparable([1500, 1500, 1500]), fees(name="Other item"))
    assert row["resale_net_margin_cents"] == 1500 - 150 - 1000


def test_a_build_stored_after_the_listing_does_not_set_its_fee():
    later = ItemFees.build([(BUILT_BEFORE, {NAME: 1000}), (T0 + timedelta(hours=1), {NAME: 200})])
    assert later.fee_bps(NAME, T0) == 1000
    assert ItemFees().fee_bps(NAME, T0) is None
    row = only_label([listed()], comparable([1500, 1500, 1500]), later)
    assert row["resale_net_margin_cents"] == 1500 - 150 - 1000


def test_a_version_with_item_fees_refuses_to_label_without_them():
    with pytest.raises(ValueError, match="item fees"):
        label_listing(listed(), SoldIndex.build([]), CONFIG)


# --- Waiting for the sales sync ---


def test_nothing_is_labelable_before_the_first_sales_sync():
    assert latest_labelable(AS_OF, CONFIG, None) == datetime.min
    assert label_listings([listed()], comparable([1500] * 3), CONFIG, AS_OF, fees(), None) == []


def test_a_listing_waits_for_a_sync_a_day_after_its_horizon():
    early_sync = SYNCED_AT - timedelta(seconds=1)
    assert label_listings([listed()], comparable([1500] * 3), CONFIG, AS_OF, fees(), early_sync) == []
    assert len(label_listings([listed()], comparable([1500] * 3), CONFIG, AS_OF, fees(), SYNCED_AT)) == 1
    # The labeler's own settle time still applies when the sync is recent.
    assert latest_labelable(T0 + 14 * DAY, CONFIG, SYNCED_AT + 30 * DAY) == T0 - timedelta(hours=1)


def test_the_daily_run_starts_days_back_from_where_the_sync_lets_labels_reach():
    assert default_start(AS_OF + 10 * DAY, 3, CONFIG, SYNCED_AT) == T0 - 3 * DAY
    with pytest.raises(ValueError, match="sales sync"):
        default_start(AS_OF, 3, CONFIG)


# --- The listing's own sale ---

OWN_SALE_CONFIG = CONFIG


def test_the_first_sale_of_the_same_item_at_the_listing_price_is_its_sale():
    own = sale("own", 1000, T0 + timedelta(hours=2), float_value=0.25000000000004, pattern=12)
    row = only_label([listed()], [own, *comparable([1500, 1500, 1500])], config=OWN_SALE_CONFIG)

    assert row["listing_sold_within_s"] == 7_200 and row["sale_censored"] is False
    # The listing's own sale is not one of the prices it is compared with.
    assert row["comparable_sales"] == 3
    assert "before the item's next listing, at the listing price" in OWN_SALE_CONFIG.contract()["sales"]["own_sale"]


def test_a_sale_after_the_item_was_listed_again_is_the_later_listings():
    # Relisted at the same price under a new listing ID an hour later, then sold: the sale is the relist's.
    relist_at = T0 + timedelta(hours=1)
    first = FeedSale("L1", NAME, 1000, T0, float_value=0.25, pattern=12, next_listed_at=relist_at)
    relist = listed("L2", at=relist_at)
    own = sale("own", 1000, T0 + timedelta(hours=2), float_value=0.25, pattern=12)

    rows = label_listings([first, relist], [own], CONFIG, AS_OF + DAY, fees(), SYNCED_AT + DAY)

    assert [(row["listing_id"], row["listing_sold_within_s"]) for row in rows] == [("L1", None), ("L2", 3_600)]


def test_a_first_sale_at_another_price_means_the_item_sold_under_another_listing():
    # Relisted above the DMarket price (not recorded) and sold there, then bought and listed again at 1000.
    other = sale("other", 1300, T0 + timedelta(hours=1), float_value=0.25, pattern=12)
    later = sale("later", 1000, T0 + 3 * DAY, float_value=0.25, pattern=12)
    assert only_label([listed()], [other, later])["sale_censored"] is True


@pytest.mark.parametrize(
    "own",
    [
        sale("relisted", 1100, T0 + DAY, float_value=0.25, pattern=12),
        sale("other-seed", 1000, T0 + DAY, float_value=0.25, pattern=13),
        sale("before", 1000, T0 - timedelta(seconds=OWN_SALE_CLOCK_SKEW_SECONDS + 1), float_value=0.25, pattern=12),
        sale("after-horizon", 1000, T0 + 14 * DAY + timedelta(seconds=1), float_value=0.25, pattern=12),
    ],
    ids=["other_price", "other_seed", "before_listing", "after_horizon"],
)
def test_other_sales_of_the_item_are_not_its_sale(own):
    row = only_label([listed()], [own, *comparable([1500, 1500, 1500])], config=OWN_SALE_CONFIG)
    assert row["listing_sold_within_s"] is None and row["sale_censored"] is True


def test_a_sale_just_before_the_listing_time_sold_at_once():
    own = sale("own", 1000, T0 - timedelta(seconds=OWN_SALE_CLOCK_SKEW_SECONDS - 1), float_value=0.25, pattern=12)
    assert only_label([listed()], [own], config=OWN_SALE_CONFIG)["listing_sold_within_s"] == 0


def test_an_item_without_a_float_cannot_be_matched_to_its_sale():
    case = listed(float_value=None, pattern=None, name="Revolution Case")
    own = sale("own", 1000, T0 + DAY, name="Revolution Case")
    assert only_label([case], [own], fees(name="Revolution Case"), OWN_SALE_CONFIG)["sale_censored"] is True


def test_without_own_sale_matching_every_label_of_the_version_is_censored():
    config = LabelConfig.for_venue("dmarket", "test-censored", own_sale_by_item=False)
    own = sale("own", 1000, T0 + timedelta(hours=2), float_value=0.25, pattern=12)
    assert only_label([listed()], [own], config=config)["sale_censored"] is True
    assert config.contract()["sales"]["own_sale"] == "none (censored)"


# --- Database access ---


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows

    def scalar(self):
        return self.rows[0][0] if self.rows else None


class FakeEngine:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.executed = []

    @asynccontextmanager
    async def _connection(self):
        yield self

    def connect(self):
        return self._connection()

    async def execute(self, stmt, params=None):
        self.executed.append((str(stmt), params))
        return _Result(self.rows)


@pytest.mark.asyncio
async def test_venue_sales_are_read_as_listing_purchases_with_their_item(monkeypatch):
    engine = FakeEngine([(T0, 7, NAME, 1500, 0.25, 12)])
    monkeypatch.setattr(outcome_data, "async_engine", engine)

    sales = await fetch_venue_sales("dmarket", T0, T0 + DAY)

    assert sales == [FeedSale("sale-7", NAME, 1500, T0, float_value=0.25, pattern=12)]
    sql, params = engine.executed[0]
    assert "FROM venue_sales" in sql and "sale_type = 'offer'" in sql
    assert params == {"venue": "dmarket", "start": T0, "end": T0 + DAY}


@pytest.mark.asyncio
async def test_listings_can_carry_when_their_item_was_next_listed(monkeypatch):
    engine = FakeEngine([(T0, "L1", NAME, 1000, 0.25, 12, T0 + DAY)])
    monkeypatch.setattr(outcome_data, "async_engine", engine)

    (listing,) = await outcome_data.fetch_listing_events("dmarket", "listed", T0, T0 + DAY, with_next_listing=True)

    assert listing.next_listed_at == T0 + DAY and listing.float_value == 0.25
    sql, _params = engine.executed[0]
    assert "AS next_listed_at" in sql and "later.float_value = tick.float_value" in sql


@pytest.mark.asyncio
async def test_item_fees_are_grouped_by_build(monkeypatch):
    later = T0 + DAY
    engine = FakeEngine([(BUILT_BEFORE, NAME, 200), (BUILT_BEFORE, "Other", 1000), (later, NAME, 1000)])
    monkeypatch.setattr(outcome_data, "async_engine", engine)

    item_fees = await fetch_item_fees("dmarket", T0, T0 + 2 * DAY)

    assert item_fees.built_at == [BUILT_BEFORE, later]
    assert (item_fees.fee_bps(NAME, T0), item_fees.fee_bps(NAME, later), item_fees.fee_bps("Other", later)) == (200, 1000, None)
    sql, params = engine.executed[0]
    assert "seller_fee_bps IS NOT NULL" in sql and params == {"venue": "dmarket", "start": T0, "end": T0 + 2 * DAY}


@pytest.mark.asyncio
async def test_the_sales_sync_time_is_the_newest_build(monkeypatch):
    monkeypatch.setattr(outcome_data, "async_engine", FakeEngine([(SYNCED_AT,)]))
    assert await latest_sales_sync("dmarket") == SYNCED_AT
    monkeypatch.setattr(outcome_data, "async_engine", FakeEngine())
    assert await latest_sales_sync("dmarket") is None


# --- The flow ---


def _patch_flow(monkeypatch, synced_at, listings, sales):
    store: list[dict] = []
    calls: dict[str, list] = {"venue_sales": [], "item_fees": [], "registered": []}

    async def fake_sync(venue):
        return synced_at

    async def fake_listings(venue, event_type, start, end, names=None, with_next_listing=False):
        assert (venue, event_type, with_next_listing) == ("dmarket", "listed", True)
        return [listing for listing in listings if start <= listing.seen_at < end]

    async def fake_sales(venue, start, end):
        calls["venue_sales"].append((venue, start, end))
        return [item for item in sales if start <= item.seen_at < end]

    async def fake_fees(venue, start, end):
        calls["item_fees"].append((venue, start, end))
        return fees()

    async def fake_save(rows, final_cutoff):
        store.extend(rows)
        return len(rows)

    async def fake_register(config):
        calls["registered"].append(config.version)

    monkeypatch.setattr(label_outcomes, "latest_sales_sync", fake_sync)
    monkeypatch.setattr(label_outcomes, "fetch_listing_events", fake_listings)
    monkeypatch.setattr(label_outcomes, "fetch_venue_sales", fake_sales)
    monkeypatch.setattr(label_outcomes, "fetch_item_fees", fake_fees)
    monkeypatch.setattr(label_outcomes, "save_outcomes", fake_save)
    monkeypatch.setattr(label_outcomes, "register_label_version", fake_register)
    return store, calls


@pytest.mark.asyncio
async def test_the_flow_labels_dmarket_listings_from_venue_sales_with_item_fees(monkeypatch):
    # A sync an hour later than the earliest one, so the T0 listing lies inside [start, cutoff).
    synced_at = SYNCED_AT + timedelta(hours=1)
    store, calls = _patch_flow(monkeypatch, synced_at, [listed()], comparable([1400, 1500, 1600]))
    start = T0 - DAY

    assert await label_listing_outcomes(start=start, end=T0 + DAY, as_of=AS_OF, config=CONFIG) == 1

    (row,) = store
    assert (row["venue"], row["label_version"], row["resale_net_margin_cents"]) == ("dmarket", "v1-dmarket", 470)
    # The run reads no further than the sync lets labels reach: listings to the cutoff, sales to its horizon.
    cutoff = latest_labelable(AS_OF, CONFIG, synced_at)
    assert cutoff == T0 + timedelta(hours=1)
    assert calls["venue_sales"] == [("dmarket", start, cutoff + 14 * DAY)]
    assert calls["item_fees"] == [("dmarket", start, cutoff)]
    assert calls["registered"] == ["v1-dmarket"]


@pytest.mark.asyncio
async def test_the_flow_labels_nothing_before_the_first_sales_sync(monkeypatch):
    store, calls = _patch_flow(monkeypatch, None, [listed()], comparable([1500] * 3))

    assert await label_listing_outcomes(start=T0, end=T0 + DAY, as_of=AS_OF, config=CONFIG) == 0

    assert store == [] and calls["registered"] == [] and calls["venue_sales"] == []
