import pytest
from shared_utils.pnl import (
    RESALE_VENUES,
    SKINPORT_FEES,
    VENUE_FEES,
    WAXPEER_FEES,
    FeeTier,
    UnknownVenueError,
    VenueFees,
    buyer_fee_cents,
    fees_for,
    holding_seconds,
    is_profitable_margin,
    net_resale_margin_cents,
    resale_venue_for,
    seller_fee_cents,
)


@pytest.mark.parametrize(
    ("resale_price_cents", "expected_fee_cents"),
    [
        (1000, 80),  # 8% standard tier
        (1001, 81),  # 80.08 rounds up to the next cent
        (99_999, 8_000),  # just below the reduced-fee threshold: 7999.92 rounds up
        (100_000, 6_000),  # threshold reached: 6%
        (250_000, 15_000),
        (0, 0),
        (1, 1),  # any non-zero fee rounds up to at least one cent
    ],
    ids=["standard", "round_up", "below_threshold", "at_threshold", "high_tier", "zero", "one_cent"],
)
def test_skinport_seller_fee_tiers(resale_price_cents, expected_fee_cents):
    assert seller_fee_cents(resale_price_cents, SKINPORT_FEES) == expected_fee_cents


@pytest.mark.parametrize(
    ("buy_price_cents", "resale_price_cents", "expected_margin_cents"),
    [
        (1000, 1500, 380),  # 1500 - 120 fee - 1000
        (1000, 1087, 0),  # 1087 - 87 fee - 1000: break-even
        (1000, 1000, -80),  # reselling at the buy price loses the fee
        (1000, 800, -264),  # 800 - 64 fee - 1000
        (90_000, 100_000, 4_000),  # 6% tier: 100000 - 6000 - 90000
    ],
    ids=["winner", "break_even", "same_price_loses_fee", "loser", "high_tier"],
)
def test_net_resale_margin(buy_price_cents, resale_price_cents, expected_margin_cents):
    assert net_resale_margin_cents(buy_price_cents, resale_price_cents, SKINPORT_FEES) == expected_margin_cents


@pytest.mark.parametrize(
    ("margin_cents", "min_margin_cents", "expected"),
    [(0, 0, True), (-1, 0, False), (49, 50, False), (50, 50, True)],
    ids=["zero_meets_zero", "negative", "below_min", "at_min"],
)
def test_is_profitable_margin_uses_min_margin(margin_cents, min_margin_cents, expected):
    fees = VenueFees(
        venue="test", fee_tiers=(FeeTier(0, 500),), buyer_fee_bps=0, hold_seconds=0, min_margin_cents=min_margin_cents
    )
    assert is_profitable_margin(margin_cents, fees) is expected


def test_custom_fee_schedule_is_used():
    fees = VenueFees(
        venue="test", fee_tiers=(FeeTier(0, 1000), FeeTier(500, 200)), buyer_fee_bps=0, hold_seconds=0, min_margin_cents=0
    )
    assert net_resale_margin_cents(100, 400, fees) == 400 - 40 - 100
    assert net_resale_margin_cents(100, 500, fees) == 500 - 10 - 100


def test_skinport_hold_is_seven_days():
    assert SKINPORT_FEES.hold_seconds == 7 * 86_400


@pytest.mark.parametrize(
    "kwargs",
    [
        {"fee_tiers": ()},
        {"fee_tiers": (FeeTier(100, 800),)},
        {"fee_tiers": (FeeTier(0, 10_000),)},
        {"fee_tiers": (FeeTier(0, -1),)},
        {"hold_seconds": -1},
        {"min_margin_cents": -1},
        {"buyer_fee_bps": -1},
        {"buyer_fee_bps": 10_000},
    ],
    ids=[
        "no_tiers",
        "no_zero_tier",
        "fee_100_percent",
        "negative_fee",
        "negative_hold",
        "negative_min_margin",
        "negative_buyer_fee",
        "buyer_fee_100_percent",
    ],
)
def test_invalid_venue_fees_are_rejected(kwargs):
    params = {
        "venue": "test",
        "fee_tiers": (FeeTier(0, 800),),
        "buyer_fee_bps": 0,
        "hold_seconds": 0,
        "min_margin_cents": 0,
    } | kwargs
    with pytest.raises(ValueError):
        VenueFees(**params)


def test_negative_prices_are_rejected():
    with pytest.raises(ValueError):
        seller_fee_cents(-1, SKINPORT_FEES)
    with pytest.raises(ValueError):
        net_resale_margin_cents(-1, 100, SKINPORT_FEES)
    with pytest.raises(ValueError):
        buyer_fee_cents(-1, SKINPORT_FEES)


@pytest.mark.parametrize("venue", ["skinport", "Skinport", "SKINPORT"])
def test_fees_for_finds_registered_venue_case_insensitively(venue):
    assert fees_for(venue) is SKINPORT_FEES


def test_fees_for_unknown_venue_fails_instead_of_falling_back():
    with pytest.raises(UnknownVenueError, match="csfloat"):
        fees_for("csfloat")


def test_registry_is_keyed_by_each_schedules_own_venue():
    assert all(name == fees.venue for name, fees in VENUE_FEES.items())


# Two made-up venues for the cross-venue cases: the numbers are chosen for easy arithmetic, they are
# not any real venue's schedule.
BUY_VENUE = VenueFees(venue="buy_venue", fee_tiers=(FeeTier(0, 200),), buyer_fee_bps=250, hold_seconds=0, min_margin_cents=0)
SELL_VENUE = VenueFees(
    venue="sell_venue",
    fee_tiers=(FeeTier(0, 500), FeeTier(50_000, 300)),
    buyer_fee_bps=0,
    hold_seconds=3 * 86_400,
    min_margin_cents=100,
)


def test_skinport_has_no_buyer_fee():
    assert SKINPORT_FEES.buyer_fee_bps == 0
    assert buyer_fee_cents(123_456, SKINPORT_FEES) == 0


@pytest.mark.parametrize(
    ("buy_price_cents", "resale_price_cents"),
    [(1000, 1500), (1000, 1087), (1000, 800), (90_000, 100_000), (0, 0), (1, 1)],
)
def test_same_venue_is_the_default_and_matches_passing_both_sides(buy_price_cents, resale_price_cents):
    same_venue = net_resale_margin_cents(buy_price_cents, resale_price_cents, SKINPORT_FEES)
    explicit = net_resale_margin_cents(buy_price_cents, resale_price_cents, buy_fees=SKINPORT_FEES, sell_fees=SKINPORT_FEES)
    assert same_venue == explicit
    # Without a buyer fee the margin is the seller-fee-only formula from before cross-venue support.
    assert same_venue == resale_price_cents - seller_fee_cents(resale_price_cents, SKINPORT_FEES) - buy_price_cents


def test_cross_venue_charges_buyer_fee_on_buy_venue_and_seller_fee_on_sell_venue():
    # Buy for 1000 + 25 buyer fee (2.5%), resell for 1500 - 75 seller fee (5%).
    assert net_resale_margin_cents(1000, 1500, buy_fees=BUY_VENUE, sell_fees=SELL_VENUE) == 1500 - 75 - 1000 - 25


def test_cross_venue_direction_matters():
    # Reversed: no buyer fee on SELL_VENUE, 2% seller fee on BUY_VENUE.
    assert net_resale_margin_cents(1000, 1500, buy_fees=SELL_VENUE, sell_fees=BUY_VENUE) == 1500 - 30 - 1000


def test_cross_venue_uses_the_sell_venue_fee_tiers():
    # 50000 reaches SELL_VENUE's 3% tier; BUY_VENUE has a single 2% tier and must not be used.
    assert net_resale_margin_cents(40_000, 50_000, buy_fees=BUY_VENUE, sell_fees=SELL_VENUE) == 50_000 - 1_500 - 40_000 - 1_000
    assert net_resale_margin_cents(40_000, 49_999, buy_fees=BUY_VENUE, sell_fees=SELL_VENUE) == 49_999 - 2_500 - 40_000 - 1_000


@pytest.mark.parametrize(
    ("buy_price_cents", "expected_fee_cents"),
    [(1000, 25), (1001, 26), (1, 1), (0, 0)],
    ids=["exact", "round_up", "one_cent", "zero"],
)
def test_buyer_fee_rounds_up_so_the_margin_is_never_overstated(buy_price_cents, expected_fee_cents):
    assert buyer_fee_cents(buy_price_cents, BUY_VENUE) == expected_fee_cents


def test_both_fees_round_against_the_trade():
    # 1001 * 2.5% = 25.025 -> 26 buyer fee; 1001 * 5% = 50.05 -> 51 seller fee.
    assert net_resale_margin_cents(1001, 1001, buy_fees=BUY_VENUE, sell_fees=SELL_VENUE) == 1001 - 51 - 1001 - 26


def test_cross_venue_hold_is_the_sell_venue_hold():
    assert holding_seconds(buy_fees=BUY_VENUE, sell_fees=SELL_VENUE) == 3 * 86_400
    assert holding_seconds(buy_fees=SELL_VENUE, sell_fees=BUY_VENUE) == 0
    assert holding_seconds(SKINPORT_FEES, SKINPORT_FEES) == SKINPORT_FEES.hold_seconds


def test_cross_venue_minimum_margin_is_the_sell_venue_one():
    margin = net_resale_margin_cents(1000, 1200, buy_fees=BUY_VENUE, sell_fees=SELL_VENUE)
    assert margin == 1200 - 60 - 1000 - 25
    assert is_profitable_margin(margin, SELL_VENUE) is True
    assert is_profitable_margin(99, SELL_VENUE) is False


def test_waxpeer_fee_schedule():
    # 6% seller fee at every price, no buyer fee, 7 day hold (checked 2026-09-30, docs/waxpeer_feed.md).
    assert fees_for("waxpeer") is WAXPEER_FEES
    assert seller_fee_cents(10_000, WAXPEER_FEES) == 600
    assert seller_fee_cents(1_000_000, WAXPEER_FEES) == 60_000
    assert buyer_fee_cents(10_000, WAXPEER_FEES) == 0
    assert WAXPEER_FEES.hold_seconds == 7 * 86_400


@pytest.mark.parametrize(
    ("venue", "expected"),
    [("waxpeer", "skinport"), ("Waxpeer", "skinport"), ("skinport", "skinport"), ("SKINPORT", "skinport")],
)
def test_resale_venue(venue, expected):
    assert resale_venue_for(venue) == expected


def test_every_resale_venue_pair_has_fees():
    for buy_venue, sell_venue in RESALE_VENUES.items():
        assert buy_venue in VENUE_FEES
        assert sell_venue in VENUE_FEES


def test_waxpeer_listing_resold_on_skinport():
    # Buy on Waxpeer for $10.00 (no buyer fee), resell on Skinport for $12.00 minus its 8% seller fee.
    margin = net_resale_margin_cents(1000, 1200, buy_fees=WAXPEER_FEES, sell_fees=fees_for(resale_venue_for("waxpeer")))
    assert margin == 1200 - 96 - 1000
