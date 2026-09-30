"""
Fee-aware P&L for a buy-then-resell trade, on one venue or across two.

This is the single profit definition for the listener's estimate, the outcome labeler, backtests,
and alerts. Do not compute a profit number anywhere else.

    net_margin = resale_price - seller_fee_on_sell_venue(resale_price)
                 - buy_price - buyer_fee_on_buy_venue(buy_price)

The buy venue charges the buyer fee and the sell venue charges the seller fee, so a trade bought and
resold on the same venue passes the same `VenueFees` for both sides. A cross-venue trade is held for
the sell venue's trade hold (`holding_seconds`), and its minimum margin is the sell venue's.

Money is integer cents throughout and fees are integer basis points, so results are exact and
reproducible. Both fees are rounded up to the next cent, which never overstates the margin.

Fees are always passed explicitly: look them up per venue with `fees_for(venue)`, which fails on a
venue that has no schedule instead of silently pricing it with another venue's fees. To add a venue,
define its `VenueFees` (with sources) below and register it in `VENUE_FEES`.

Current Skinport parameters (checked 2026-09-25):
- Seller fee: 8% on public sales, 6% when the sale price is at least 1000 in the listing currency
  (https://skinport.com/blog/reduced-fee-high-tier-items). Private sales (2%) are not modeled.
- Buyer fee: none on the listing price.
- Trade hold: CS2 items carry Steam's 7-day trade protection after a trade, so a bought item cannot
  be resold for 7 days (up to 8 in the worst case).

Current Waxpeer parameters (checked 2026-09-30):
- Seller fee: 6% (https://waxpeer.com/faq/fees-sell). `GET /v1/user` returned `sell_fees` 0.94 for
  our account, the same rate.
- Buyer fee: none on the listing price. Deposit and withdrawal fees depend on the payment method and
  are not modeled.
- Trade hold: 7 days. Waxpeer lets a trade locked item be listed at once, but the buyer receives it only
  when Steam's trade protection ends, and sale proceeds stay on the hold balance for 7 days
  (https://waxpeer.com/faq/trade-lock, https://waxpeer.com/faq/hold-balance).

A venue with no sales data of its own is judged against another venue: `resale_venue_for` names the
venue whose baselines score its listings, whose sales label them, and whose seller fee applies on
resale. Waxpeer publishes no sales, so its listings are judged against Skinport (docs/waxpeer_feed.md).
"""

from dataclasses import dataclass

BASIS_POINTS = 10_000
SECONDS_PER_DAY = 86_400

# How a paper trade's `estimated_profit_cents` was computed. Rows written before the shared P&L
# function existed are tagged "gross" (baseline price minus buy price, no fees).
PROFIT_ESTIMATE_BASIS_GROSS = "gross"
PROFIT_ESTIMATE_BASIS_NET = "net_of_seller_fee"


class UnknownVenueError(ValueError):
    """Raised when no fee schedule is registered for a venue."""


@dataclass(frozen=True)
class FeeTier:
    """Seller fee that applies when the resale price is at least `min_price_cents`."""

    min_price_cents: int
    fee_bps: int


@dataclass(frozen=True)
class VenueFees:
    """Venue economics: buyer fee, seller fee tiers, trade hold, and the smallest margin worth taking.

    `buyer_fee_bps` has no default, so every venue states its buy-side cost instead of implying zero.
    """

    venue: str
    fee_tiers: tuple[FeeTier, ...]
    buyer_fee_bps: int
    hold_seconds: int
    min_margin_cents: int

    def __post_init__(self) -> None:
        if not self.fee_tiers:
            raise ValueError("At least one fee tier is required")
        if min(tier.min_price_cents for tier in self.fee_tiers) != 0:
            raise ValueError("One fee tier must start at 0 cents so every price has a fee")
        for tier in self.fee_tiers:
            if not 0 <= tier.fee_bps < BASIS_POINTS:
                raise ValueError(f"Fee must be in [0, {BASIS_POINTS}) basis points, got {tier.fee_bps}")
        if not 0 <= self.buyer_fee_bps < BASIS_POINTS:
            raise ValueError(f"Buyer fee must be in [0, {BASIS_POINTS}) basis points, got {self.buyer_fee_bps}")
        if self.hold_seconds < 0:
            raise ValueError("hold_seconds must not be negative")
        if self.min_margin_cents < 0:
            raise ValueError("min_margin_cents must not be negative")

    def seller_fee_bps(self, resale_price_cents: int) -> int:
        """Fee rate of the highest tier whose threshold the resale price reaches."""
        applicable = [tier for tier in self.fee_tiers if resale_price_cents >= tier.min_price_cents]
        return max(applicable, key=lambda tier: tier.min_price_cents).fee_bps


SKINPORT_FEES = VenueFees(
    venue="skinport",
    fee_tiers=(
        FeeTier(min_price_cents=0, fee_bps=800),
        FeeTier(min_price_cents=100_000, fee_bps=600),
    ),
    buyer_fee_bps=0,
    hold_seconds=7 * SECONDS_PER_DAY,
    min_margin_cents=0,
)

WAXPEER_FEES = VenueFees(
    venue="waxpeer",
    fee_tiers=(FeeTier(min_price_cents=0, fee_bps=600),),
    buyer_fee_bps=0,
    hold_seconds=7 * SECONDS_PER_DAY,
    min_margin_cents=0,
)

# Every venue the system can price. A venue missing here cannot be traded or labeled.
VENUE_FEES: dict[str, VenueFees] = {
    SKINPORT_FEES.venue: SKINPORT_FEES,
    WAXPEER_FEES.venue: WAXPEER_FEES,
}

# Venues whose listings are judged against, and resold on, another venue. Any other venue resells
# where it buys.
RESALE_VENUES: dict[str, str] = {
    WAXPEER_FEES.venue: SKINPORT_FEES.venue,
}


def fees_for(venue: str) -> VenueFees:
    """Fee schedule of a venue (case-insensitive). Raises UnknownVenueError when none is registered."""
    fees = VENUE_FEES.get(venue.lower())
    if fees is None:
        registered = ", ".join(sorted(VENUE_FEES))
        raise UnknownVenueError(f"No fee schedule registered for venue '{venue}' (registered: {registered})")
    return fees


def resale_venue_for(venue: str) -> str:
    """The venue a listing bought on `venue` is scored against and resold on (lowercase)."""
    buy_venue = venue.lower()
    return RESALE_VENUES.get(buy_venue, buy_venue)


def seller_fee_cents(resale_price_cents: int, fees: VenueFees) -> int:
    """Seller fee on a resale, rounded up to the next cent."""
    if resale_price_cents < 0:
        raise ValueError("resale_price_cents must not be negative")
    fee_bps = fees.seller_fee_bps(resale_price_cents)
    return _fee_rounded_up_cents(resale_price_cents, fee_bps)


def buyer_fee_cents(buy_price_cents: int, fees: VenueFees) -> int:
    """Buyer fee on a purchase, rounded up to the next cent."""
    if buy_price_cents < 0:
        raise ValueError("buy_price_cents must not be negative")
    return _fee_rounded_up_cents(buy_price_cents, fees.buyer_fee_bps)


def _fee_rounded_up_cents(price_cents: int, fee_bps: int) -> int:
    return -(-price_cents * fee_bps // BASIS_POINTS)


def net_resale_margin_cents(
    buy_price_cents: int,
    resale_price_cents: int,
    buy_fees: VenueFees,
    sell_fees: VenueFees | None = None,
) -> int:
    """Profit in cents from buying at `buy_price_cents` and reselling at `resale_price_cents`, after fees.

    `buy_fees` belongs to the venue the item is bought on and `sell_fees` to the venue it is resold on.
    Without `sell_fees` the item is resold on the venue it was bought on.
    """
    if sell_fees is None:
        sell_fees = buy_fees
    proceeds_cents = resale_price_cents - seller_fee_cents(resale_price_cents, sell_fees)
    cost_cents = buy_price_cents + buyer_fee_cents(buy_price_cents, buy_fees)
    return proceeds_cents - cost_cents


def holding_seconds(buy_fees: VenueFees, sell_fees: VenueFees) -> int:
    """How long a bought item is held before it can be resold: the sell venue's trade hold."""
    return sell_fees.hold_seconds


def is_profitable_margin(net_margin_cents: int, fees: VenueFees) -> bool:
    """True when a net margin reaches the minimum margin of `fees` (the sell venue's)."""
    return net_margin_cents >= fees.min_margin_cents
