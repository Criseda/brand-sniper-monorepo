"""
Fee-aware P&L for a buy-then-resell trade.

This is the single profit definition for the listener's estimate, the outcome labeler, backtests,
and alerts. Do not compute a profit number anywhere else.

    net_margin = resale_price - seller_fee(resale_price) - buy_price

Money is integer cents throughout and fees are integer basis points, so results are exact and
reproducible. The seller fee is rounded up to the next cent, which never overstates the margin.

Current Skinport parameters (checked 2026-09-25):
- Seller fee: 8% on public sales, 6% when the sale price is at least 1000 in the listing currency
  (https://skinport.com/blog/reduced-fee-high-tier-items). Private sales (2%) are not modeled.
- Buyer fee: none on the listing price.
- Trade hold: CS2 items carry Steam's 7-day trade protection after a trade, so a bought item cannot
  be resold for 7 days (up to 8 in the worst case).
"""

from dataclasses import dataclass

BASIS_POINTS = 10_000
SECONDS_PER_DAY = 86_400


@dataclass(frozen=True)
class FeeTier:
    """Seller fee that applies when the resale price is at least `min_price_cents`."""

    min_price_cents: int
    fee_bps: int


@dataclass(frozen=True)
class VenueFees:
    """Venue economics for resale: seller fee tiers, trade hold, and the smallest margin worth taking."""

    venue: str
    fee_tiers: tuple[FeeTier, ...]
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
    hold_seconds=7 * SECONDS_PER_DAY,
    min_margin_cents=0,
)


def seller_fee_cents(resale_price_cents: int, fees: VenueFees = SKINPORT_FEES) -> int:
    """Seller fee on a resale, rounded up to the next cent."""
    if resale_price_cents < 0:
        raise ValueError("resale_price_cents must not be negative")
    fee_bps = fees.seller_fee_bps(resale_price_cents)
    return -(-resale_price_cents * fee_bps // BASIS_POINTS)


def net_resale_margin_cents(buy_price_cents: int, resale_price_cents: int, fees: VenueFees = SKINPORT_FEES) -> int:
    """Profit in cents from buying at `buy_price_cents` and reselling at `resale_price_cents`, after fees."""
    if buy_price_cents < 0:
        raise ValueError("buy_price_cents must not be negative")
    return resale_price_cents - seller_fee_cents(resale_price_cents, fees) - buy_price_cents


def is_profitable_margin(net_margin_cents: int, fees: VenueFees = SKINPORT_FEES) -> bool:
    """True when a net margin reaches the venue's minimum margin."""
    return net_margin_cents >= fees.min_margin_cents
