"""
Fills `live_market_ticks.observed_at` on Skinport listing rows written before the column existed (#256).

Every Skinport listing tick was parsed from one entry of a recorded `saleFeed` event, and the backend wrote the
event's receive time, cut to whole seconds, to the tick's `inserted_at`. So a tick and its raw event match on
(listing ID, event type, receive second), and the event's `received_at` is the tick's millisecond receive time.
On 2026-09-28, 2026-10-10 and 2026-10-24 every tick matched exactly one event and every price agreed.

Only rows with a NULL observed_at are touched, so the script can run again after the listeners are upgraded,
for batches an older listener delivered in between. Waxpeer rows stay NULL: no label version reads them yet.

Usage (from apps/analytics):
    uv run python backfill_observed_at.py
    uv run python backfill_observed_at.py --start 2026-10-01 --end 2026-10-08
"""

import asyncio
from datetime import datetime, timedelta

from shared_utils import setup_script_environment

setup_script_environment(__file__)

from outcome_labels import iter_windows
from shared_utils import get_logger, utc_now_naive, validate_required_env
from shared_utils.db_connection import async_engine
from sqlalchemy import text

logger = get_logger(__name__)

# The first listing-level ticks (#232).
FIRST_LISTING_TICK = datetime(2026, 9, 24)

# One statement per window keeps each transaction short on the hot ingest table. The productId field name is
# Skinport's (docs/skinport_feed.md); it is read here once, for history, and nowhere in the labeler.
_BACKFILL_QUERY = """
UPDATE live_market_ticks AS tick
SET observed_at = event.received_at
FROM (
    SELECT fe.event_type,
           sale->>'productId' AS listing_id,
           date_trunc('second', fe.received_at) AS received_second,
           MIN(fe.received_at) AS received_at
    FROM feed_events AS fe
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(fe.payload->'sales') = 'array' THEN fe.payload->'sales' ELSE '[]'::jsonb END
    ) AS sale
    WHERE fe.venue = 'skinport'
      AND fe.received_at >= :start
      AND fe.received_at < :end
      AND sale->>'productId' IS NOT NULL
    GROUP BY 1, 2, 3
) AS event
WHERE tick.venue = 'skinport'
  AND tick.listing_id = event.listing_id
  AND tick.event_type = event.event_type
  AND tick.inserted_at = event.received_second
  AND tick.inserted_at >= :start
  AND tick.inserted_at < :end
  AND tick.observed_at IS NULL
"""

_REMAINING_QUERY = """
SELECT COUNT(*)
FROM live_market_ticks
WHERE venue = 'skinport' AND listing_id IS NOT NULL AND observed_at IS NULL
  AND inserted_at >= :start AND inserted_at < :end
"""


async def backfill_observed_at(start: datetime, end: datetime, window: timedelta = timedelta(days=1)) -> int:
    """Fills observed_at on Skinport listing rows received in [start, end). Returns how many rows were filled."""
    filled = 0
    for window_start, window_end in iter_windows(start, end, window):
        async with async_engine.begin() as conn:
            result = await conn.execute(text(_BACKFILL_QUERY), {"start": window_start, "end": window_end})
        window_filled = max(int(result.rowcount or 0), 0)
        filled += window_filled
        logger.info("[BACKFILL] %s .. %s: %d Skinport listing rows filled.", window_start, window_end, window_filled)

    async with async_engine.connect() as conn:
        remaining = (await conn.execute(text(_REMAINING_QUERY), {"start": start, "end": end})).scalar_one()
    logger.info("[BACKFILL] %d rows filled; %d Skinport listing rows in the range have no observed_at.", filled, remaining)
    return filled


if __name__ == "__main__":  # pragma: no cover - entrypoint glue, covered via unit tests
    import argparse

    parser = argparse.ArgumentParser(description="Fill observed_at on Skinport listing ticks from their feed events.")
    parser.add_argument("--start", type=datetime.fromisoformat, default=FIRST_LISTING_TICK, help="First receive time (UTC)")
    parser.add_argument("--end", type=datetime.fromisoformat, help="End of the range, exclusive (default: now)")
    args = parser.parse_args()

    validate_required_env(["DATABASE_URL"])
    asyncio.run(backfill_observed_at(args.start, args.end or utc_now_naive()))
