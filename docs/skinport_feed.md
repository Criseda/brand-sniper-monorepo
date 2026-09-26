# Skinport Sale Feed: Schema, Capture, and Retention

This note documents what Skinport's live `saleFeed` actually sends. It is based on raw traffic captured on
2026-09-24 and describes how Brand Sniper records that traffic (#232). The captured samples are checked
in, sanitized, as test fixtures:

- `apps/listener/tests/fixtures/skinport_listed_event.json`
- `apps/listener/tests/fixtures/skinport_sold_event.json`

Sanitization replaced the seller `steamid`, the inspect `link`, and the image hashes and URLs. It also
trimmed the `tags` and `screenshots` arrays. Every other field is exactly as received.

## Official Skinport API reference

Start with these pages when working on anything that talks to Skinport. Where the live feed disagrees with
the docs, this note records what the feed actually sends.

| Page | What it covers | Used by |
|---|---|---|
| [WebSocket: Sale Feed](https://docs.skinport.com/websocket/sale-feed) | Socket.IO + msgpack `saleFeed`: `saleFeedJoin` parameters, event types, sale fields | Listener sidecar, `parse_sale_feed_message` (#232) |
| [Items](https://docs.skinport.com/items) | `GET /v1/items`: per-item `min_price`, `max_price`, `mean_price`, `median_price`, `suggested_price`, `quantity`, `item_page`, `market_page`, plus an undocumented `version` (phase). No auth, 8 requests / 5 min, cached 5 min, `Accept-Encoding: br` required. `tradable=0` returns **only** trade locked listings; see [REST lowest asks](#rest-lowest-asks-v1items) | Listener REST snapshots (`poll_market_stream`) |
| [Sales](https://docs.skinport.com/category/sales) | Index of the sales endpoints (history, out of stock) | - |
| [Sales History](https://docs.skinport.com/sales/history) | `GET /v1/sales/history`: min/max/avg/median price and sale **volume** over 24 h, 7 d, 30 d, 90 d per item. No auth, 8 requests / 5 min, cached 5 min | Candidate liquidity source for outcome labels (#233) |
| [Account Transactions](https://docs.skinport.com/account/transactions) | `GET /v1/account/transactions`: our own credits, purchases, withdrawals, with `amount` and `fee` per transaction. Basic auth, paginated | Realized fees and P&L once trading is live (#233, #235) |

Where the live feed disagrees with the Sale Feed docs (verified against 477 captured sales on 2026-09-24):

- **`saleId`**: the docs say it is null on `listed` and populated on `sold`. In practice it was null on
  every sale of both types. The parser still builds a per-listing link when a `saleId` arrives.
- **`lock`**: documented as an ISO 8601 trade-lock timestamp. It was null on every sale.
- **Unsupported event types**: the docs list `price_changed` and `canceled` as not emitted. A price edit or
  a cancelled listing is therefore invisible to us. A listing with no later `sold` event means "not seen to
  sell", **not** "did not sell at that price". Outcome labels (#233) must treat such listings as censored.

## REST lowest asks (`/v1/items`)

I checked this on 2026-09-26 (#267), after two REST approvals were priced below every sale of the item in
the last 30 days: a Flip Knife Doppler Phase 4 (Factory New) at $201.12 and a Survival Knife Forest DDPAT
(Field-Tested) at $28.85. All times below are UTC.

**The phase is right.** `/v1/items` returns one entry per phase, each with its own `version` and item page
(`flip-knife-doppler-factory-new+phase-4`). The Phase 4 entry really had a $201.12 lowest ask. A cheaper
phase is not being filed under the wrong name.

**`tradable=0` returns only listings that are not tradable yet.** The docs say `tradable` is a boolean that
defaults to `true` and shows only tradable items. I read `tradable=0` as "include the trade locked ones
too". It is the opposite set. Two requests a minute apart:

| | `tradable=1` | `tradable=0` |
|:---|---:|---:|
| Entries | 25,349 | 21,361 |
| Entries with a price | 25,349 | 10,175 |
| Listings (`quantity` summed) | 3,688,384 | 81,050 |
| Flip Knife Doppler Phase 4 (Factory New) | $339.98 lowest, 21 listings | $201.12 lowest, 7 listings |
| Survival Knife Forest DDPAT (Field-Tested) | $42.68 lowest, 9 listings | $40.91 lowest, 3 listings |

The poller has sent `tradable: 0` since the first Skinport commit (2026-06-24), so every REST snapshot in
`live_market_ticks` (the rows with a null `event_type`) is the lowest ask among trade locked listings. For
items priced in both sets, that ask is a median 11.1% below the tradable lowest ask (p25 5.1%, p75 21.1%,
p90 36.0%). The discount is what a buyer gets for waiting out the lock, so a REST approval was mostly
measuring the lock. The fix is #275.

**The live feed seems to announce a trade locked listing only when its lock ends.** From 2026-09-25 the
REST lowest ask dropped 356 times. In none of them did the feed announce a listing at that price in the 15
minutes before; 6 were announced later (a median 76 minutes after the drop) and 350 never. The Survival
Knife shows the pattern: REST showed $28.85 from 12:16, the feed announced listing 60897807 at $28.85 at
13:02:43, and it sold 36 seconds later. The feed's `lock` field was null on every captured sale, so I cannot
confirm this from the feed itself.

**A `sold` event is not always a completed trade.** The same listing 60897807 was announced as `listed`
again at 13:52:59, at $44.42, with the same `assetId` and Steam `assetid`. A completed trade would move the
item to a new Steam asset, so that sale most likely fell through. Outcome labels (#233) treat `sold` as a
sale; I have not measured how often this happens.

**Rate limits are tighter than documented once they trip.** Two requests one second apart got HTTP 429, and
retrying every 95 seconds kept it that way. From 14:32 the listener's authenticated requests then got 429
for over an hour, at intervals of 20 minutes, while unauthenticated requests from the same machine got 200.
`/v1/items` does not need auth. After a 429, back off for a long time and never retry in a loop.

## Transport

- Socket.IO over WebSocket to `https://skinport.com`, using the msgpack parser (`socket.io-msgpack-parser`).
- Subscribe by emitting `saleFeedJoin` with `{ currency: "USD", locale: "en", appid: 730 }`.
- Server events seen after joining: `saleFeed` (the market feed), plus one `maintenanceUpdated` (`false`)
  and one `steamStatusUpdated` (`"operational"`) on connect. Only `saleFeed` is recorded.

## `saleFeed` event

```json
{ "eventType": "listed" | "sold", "sales": [ <sale>, ... ] }
```

| `eventType` | Meaning | Seen in capture |
|---|---|---|
| `listed` | New offers placed on the market | Yes. Bursty: long quiet stretches, then dozens of events in a few minutes |
| `sold` | Offers that just sold. This is the market's own ground truth for outcome labels | Yes. Steady |
| `price_changed`, `canceled` | Documented as unsupported (never emitted) | No |

No other `eventType` values were seen. The sidecar still forwards any unknown type. The listener stores it
raw and never lets it into the Z-score/DRE path (see [Recording pipeline](#recording-pipeline)).

One event carries 1 to 37 sales (the median is 1). A `sold` event can batch several sales of the same item,
such as a run of cases.

## Sale object

`listed` and `sold` sales use the same schema: the same 58 keys, and `saleStatus` matches the event type. Fields
that matter for trading and labeling:

| Field | Type | Notes |
|---|---|---|
| `productId` | int | **The only identifier populated on both event types.** Stored as `listing_id`, so it is the key that joins a listing to its sale. Confirmed: within 15 minutes of capture, 12 `productId`s appeared as both `listed` and `sold` |
| `id`, `saleId`, `shortId` | int / null | Always `0` / `null` / `null` in the public feed, for both event types (the docs claim `saleId` is set on sold) |
| `assetId` | int | Skinport-internal asset number |
| `assetid`, `classid` | string | Steam economy identifiers |
| `itemId` | int | Skinport catalog item (shared by all copies of one skin) |
| `url` | string | Item slug. `https://skinport.com/item/<url>` opens the item page with its open offers. A per-listing link needs `saleId`, which the feed does not send |
| `marketHashName`, `version` | string | Name, plus a phase such as `Phase 2` (or `default`). Combined with `build_versioned_name`, the same as the REST path |
| `salePrice` | int | Price in cents of the requested currency (USD) |
| `suggestedPrice`, `referencePrice` | int | Skinport's own reference prices, in cents |
| `saleStatus` | string | `listed` or `sold`, mirrors `eventType` |
| `saleType` | string | `public` in every sample |
| `wear` | float / null | Float value. Null for items without wear (cases, agents, stickers) |
| `pattern` | int / null | Paint seed. Stored as `pattern` |
| `finish` | int / null | Paint index (skin finish). Stored as `paint_index` |
| `stickers` | array | `name`, `slot`, `wear`, `value`, `rotation`, `offset_x`, `offset_y`, `scale`, `img`, ... `value` was null in every sample |
| `charms` | array | `name`, `pattern`, `img`, `value`, ... |
| `lock` | null | Documented as an ISO 8601 trade-lock timestamp. Null in every sample |
| `steamid` | string | Seller SteamID64. Kept verbatim in `feed_events`; redact it in any shared sample |

Trimmed, sanitized `sold` sale:

```json
{
  "id": 0,
  "saleId": null,
  "shortId": null,
  "productId": 58903454,
  "assetId": 686938719,
  "assetid": "52525270001",
  "itemId": 66529,
  "url": "ak-47-slate-field-tested",
  "marketHashName": "AK-47 | Slate (Field-Tested)",
  "version": "default",
  "salePrice": 397,
  "suggestedPrice": 397,
  "referencePrice": 473,
  "currency": "USD",
  "saleStatus": "sold",
  "saleType": "public",
  "wear": 0.3607417345046997,
  "pattern": 415,
  "finish": 1035,
  "lock": null,
  "steamid": "76561190000000000",
  "stickers": [
    { "name": "9z Team (Glitter) | Antwerp 2022", "slot": 2, "wear": null, "value": null, "rotation": -3 }
  ],
  "charms": []
}
```

## Recording pipeline

1. **Sidecar** (`scrapers/skinport_websocket/sidecar.js`) forwards every `saleFeed` event without changing
   it. It wraps each event as `{"receivedAt": <epoch ms>, "payload": <event>}` and publishes it to the edge
   Redis channel `skinport:sale_feed`.
2. **Listener** (`parse_sale_feed_message` in `scrapers/skinport.py`) turns each message into:
   - one `FeedEvent` holding the raw payload, and
   - one listing-level `MarketTick` per well-formed sale. A malformed sale is skipped, but the raw event still
     keeps it.
3. **Consumer** (`tick_consumer` in `main.py`):
   - `sold` (and any unknown type) ticks are buffered for persistence only. They never touch the dedup
     cache, the sliding price window, or the DRE.
   - `listed` ticks and REST snapshots follow the unchanged Z-score/DRE path.
   - A `listed` tick that the dedup filter drops (same item, same price, within 300 s) is still recorded
     when it has a `listing_id`, because it is a distinct listing. It does not re-enter the price window.
4. **Durable batch path**: raw events travel in the same batch as the ticks (`StoredBatch.feed_events`),
   through the Redis Stream pending queue and dead-letter queue to `POST /api/v1/ingest/bulk`.
   Idempotency is still per `batch_id`, and the payload digest covers the feed events. Batches written before
   #232 keep their digest and wire format, so replaying them is still acknowledged as a duplicate.
5. **PostgreSQL**:
   - `feed_events` is append-only: `source`, `event_type`, `received_at`, `payload` JSONB.
   - `live_market_ticks` gains `listing_id`, `event_type`, `pattern`, `stickers` (JSONB), and `listing_url`,
     and now fills the existing `float_value` and `paint_index`. On REST snapshot rows all listing columns
     stay NULL. On listing rows, `stickers` is `[]` when the item has none.

Raw events get the same durability as ticks already had:

- **In listener memory** until a flush. A flush happens when either buffer reaches `CHUNK_LIMIT` (each REST
  poll triggers one, roughly every 5 minutes) and on graceful shutdown. A listener crash loses whatever is
  still buffered. Both compose stacks give the listener `stop_grace_period: 120s`, so `docker stop` and
  redeploys wait for the drain (up to about 3x `LISTENER_SHUTDOWN_GRACE_SECONDS`) instead of killing it
  after Docker's 10 s default.
- **In the edge Redis stream** after a flush, until the backend acknowledges the batch. This survives a
  listener crash or restart, but **not a Redis restart**: the edge Redis runs with `--save '' --appendonly no`
  (RAM only), so pending and dead-letter batches are lost with it.
- **In PostgreSQL** once the backend commits the batch.

Tightening the in-memory window is tracked in #252.

## Volume

| Stream | Measured | Daily estimate (continuous uptime) |
|---|---|---|
| REST snapshots to `live_market_ticks` | about 10.1k ticks per 305 s poll (`listener_ticks_processed_total`, 2026-09-24) | about 2.9M rows/day |
| `saleFeed` events to `feed_events` | 37 events in a sold-only 14.6 min window; 85 events in a 5 min window with a `listed` burst | about 3.6k to 25k events/day |
| `saleFeed` sales to `live_market_ticks` | 159 sales / 14.6 min; 343 sales / 5 min | about 16k to 100k rows/day |

Storage:

- `live_market_ticks`: about 120 bytes/row including indexes (1,182 MB for 9.8M rows). That is about
  350 MB/day from REST snapshots. Listing rows are larger because of `stickers`, but there are far fewer
  of them.
- `feed_events`: about 5 KB per event on disk, including TOAST and indexes (416 kB for the first 85 events,
  about 4 sales each). Raw JSON is about 2.1 KB per sale. Expect roughly **20 to 125 MB/day**, or
  0.6 to 3.8 GB per month.

The feed rate swings widely between quiet and busy periods. Re-measure it after a week of uptime, using
`increase(listener_feed_events_received_total[1d])` (labeled by `event_type`) together with
`pg_total_relation_size('feed_events')`.

## Retention policy

- **`feed_events` (raw)**: keep 90 days in PostgreSQL. That covers the Skinport trade-hold and outcome-label
  horizon (#233) and the replay windows the backtest harness (#248) needs. Rows older than 90 days are exported
  to compressed JSONL or Parquet in object storage, month by month, and only then deleted. Delete in batches
  using the `received_at` index, and never delete without a verified export: raw payloads are the only way to
  re-parse fields that are not extracted today.
- **`live_market_ticks` (listing-level)**: no expiry during Milestone 5. These rows are the training and label
  corpus.
- The export/delete job is not automated yet. The operational reminder, with the first due date, lives in
  [`deployment.md`](deployment.md#data-retention).
