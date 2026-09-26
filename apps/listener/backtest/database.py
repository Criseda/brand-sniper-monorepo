"""
Replay input from PostgreSQL: recorded feed events, REST snapshots, and the current macro baselines.

Rows are streamed with server-side cursors, so a range of days does not have to fit in memory.
"""

from collections.abc import AsyncIterator
from datetime import datetime, timedelta

from backtest.sources import BaselineSnapshot, PollClock, RecordedEvent, RecordedFeedEvent, RecordedSnapshot, merge_ordered
from shared_utils import edge_baseline_payload
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

STREAM_CHUNK_ROWS = 5_000

# Exact duplicate rows (same type, receive time, and payload) can only come from storing one received
# event twice, so they are dropped. Re-sent events with a new receive time are kept: live saw them too.
_FEED_EVENTS_QUERY = text(
    """
    SELECT received_at, payload
    FROM (
        SELECT DISTINCT ON (event_type, received_at, md5(payload::text)) id, received_at, payload
        FROM feed_events
        WHERE source = :source AND received_at >= :start AND received_at < :end
        ORDER BY event_type, received_at, md5(payload::text), id
    ) AS unique_events
    ORDER BY received_at, id
    """
)

# REST snapshots are the ticks without a feed event type.
_SNAPSHOTS_QUERY = text(
    """
    SELECT tick.inserted_at, item.market_hash_name, tick.price_cents
    FROM live_market_ticks AS tick
    JOIN market_items AS item ON item.id = tick.item_id
    WHERE tick.marketplace_source = :source
      AND tick.event_type IS NULL
      AND tick.inserted_at >= :start
      AND tick.inserted_at < :end
    ORDER BY tick.inserted_at, tick.id
    """
)

_BASELINES_QUERY = text(
    """
    SELECT item.market_hash_name, item.item_type, baseline.support_floor_cents, baseline.latest_price_cents,
           baseline.rolling_30d_avg_cents, baseline.volatility_cents, baseline.drift_percent, baseline.updated_at
    FROM item_macro_baselines AS baseline
    JOIN market_items AS item ON item.id = baseline.item_id
    ORDER BY item.market_hash_name
    """
)


_EPOCH = datetime(1970, 1, 1)
_ONE_MILLISECOND = timedelta(milliseconds=1)


def _epoch_ms(naive_utc: datetime) -> int:
    """Epoch milliseconds of a stored timestamp (naive UTC), without float rounding."""
    return (naive_utc - _EPOCH) // _ONE_MILLISECOND


def _default_engine() -> AsyncEngine:
    # Imported lazily: the module builds its engine from DATABASE_URL at import time.
    from shared_utils.db_connection import async_engine

    return async_engine


async def stream_feed_events(start: datetime, end: datetime, source: str, engine: AsyncEngine) -> AsyncIterator[RecordedEvent]:
    async with engine.connect() as connection:
        result = await connection.stream(
            _FEED_EVENTS_QUERY.execution_options(yield_per=STREAM_CHUNK_ROWS),
            {"source": source, "start": start, "end": end},
        )
        async for received_at, payload in result:
            yield RecordedFeedEvent(received_at_ms=_epoch_ms(received_at), payload=payload)


async def stream_snapshots(start: datetime, end: datetime, source: str, engine: AsyncEngine) -> AsyncIterator[RecordedEvent]:
    async with engine.connect() as connection:
        result = await connection.stream(
            _SNAPSHOTS_QUERY.execution_options(yield_per=STREAM_CHUNK_ROWS),
            {"source": source, "start": start, "end": end},
        )
        clock = PollClock()
        async for inserted_at, market_hash_name, price_cents in result:
            yield RecordedSnapshot(
                observed_at_ms=clock.stamp(_epoch_ms(inserted_at)),
                market_hash_name=market_hash_name,
                price_cents=price_cents,
            )


async def stream_recorded_events(
    start: datetime,
    end: datetime,
    *,
    source: str = "skinport",
    engine: AsyncEngine | None = None,
) -> AsyncIterator[RecordedEvent]:
    """Feed events and REST snapshots in [start, end) (naive UTC), merged into replay order."""
    engine = engine or _default_engine()
    async for event in merge_ordered(
        stream_feed_events(start, end, source, engine),
        stream_snapshots(start, end, source, engine),
    ):
        yield event


async def load_current_baselines(engine: AsyncEngine | None = None) -> BaselineSnapshot:
    """
    The macro baselines as they are now, built exactly as `update_baselines.py` pushes them to the edge.

    `item_macro_baselines` keeps one row per item and overwrites it, so a replay of a past range uses
    today's baselines (a look-ahead). The snapshot records `as_of` so the decision log shows it.
    """
    engine = engine or _default_engine()
    baselines: dict[str, dict] = {}
    sticker_prices: dict[str, int] = {}
    newest: datetime | None = None
    async with engine.connect() as connection:
        result = await connection.execute(_BASELINES_QUERY)
        for name, item_type, support_floor, latest_price, rolling_30d_avg, volatility, drift, updated_at in result:
            baselines[name] = edge_baseline_payload(
                support_floor_cents=support_floor,
                latest_price_cents=latest_price,
                rolling_30d_avg_cents=rolling_30d_avg,
                volatility_cents=volatility,
                drift_percent=drift,
            )
            if item_type == "Sticker":
                sticker_prices[name] = latest_price
            if newest is None or updated_at > newest:
                newest = updated_at
    return BaselineSnapshot(
        baselines=baselines,
        sticker_prices=sticker_prices,
        as_of=newest.isoformat() if newest is not None else None,
    )
