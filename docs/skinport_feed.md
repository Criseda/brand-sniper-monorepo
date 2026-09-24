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
| [Items](https://docs.skinport.com/items) | `GET /v1/items`: per-item `min_price`, `max_price`, `mean_price`, `median_price`, `suggested_price`, `quantity`, `item_page`, `market_page`. No auth, 8 requests / 5 min, cached 5 min, `Accept-Encoding: br` required | Listener REST snapshots (`poll_market_stream`) |
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

The durability guarantee is the same as for ticks before #232. A batch is durable once it is written to the
edge Redis stream. It is flushed when either buffer reaches `CHUNK_LIMIT`, which each REST poll triggers
roughly every 5 minutes, and on graceful shutdown. Events still buffered in memory are lost if the process
crashes before a flush.

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
