import pytest
from shared_utils.pnl import (
    SKINPORT_FEES,
    VENUE_FEES,
    FeeTier,
    UnknownVenueError,
    VenueFees,
    fees_for,
    is_profitable_margin,
    net_resale_margin_cents,
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
    fees = VenueFees(venue="test", fee_tiers=(FeeTier(0, 500),), hold_seconds=0, min_margin_cents=min_margin_cents)
    assert is_profitable_margin(margin_cents, fees) is expected


def test_custom_fee_schedule_is_used():
    fees = VenueFees(venue="test", fee_tiers=(FeeTier(0, 1000), FeeTier(500, 200)), hold_seconds=0, min_margin_cents=0)
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
    ],
    ids=["no_tiers", "no_zero_tier", "fee_100_percent", "negative_fee", "negative_hold", "negative_min_margin"],
)
def test_invalid_venue_fees_are_rejected(kwargs):
    params = {"venue": "test", "fee_tiers": (FeeTier(0, 800),), "hold_seconds": 0, "min_margin_cents": 0} | kwargs
    with pytest.raises(ValueError):
        VenueFees(**params)


def test_negative_prices_are_rejected():
    with pytest.raises(ValueError):
        seller_fee_cents(-1, SKINPORT_FEES)
    with pytest.raises(ValueError):
        net_resale_margin_cents(-1, 100, SKINPORT_FEES)


@pytest.mark.parametrize("venue", ["skinport", "Skinport", "SKINPORT"])
def test_fees_for_finds_registered_venue_case_insensitively(venue):
    assert fees_for(venue) is SKINPORT_FEES


def test_fees_for_unknown_venue_fails_instead_of_falling_back():
    with pytest.raises(UnknownVenueError, match="csfloat"):
        fees_for("csfloat")


def test_registry_is_keyed_by_each_schedules_own_venue():
    assert all(name == fees.venue for name, fees in VENUE_FEES.items())
