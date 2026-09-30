# Waxpeer Live Feed: Schema, Filter, and Storage

This note documents what Waxpeer's live listing feed actually sends and how Brand Sniper records it (#261).
I read all 65 pages of the Waxpeer API docs (version 2.2.9, their `sitemap.xml`, `llms-full.txt` and
`openapi.json`) and measured the feed on 2026-09-30. All times are UTC.

A sanitized sample of the feed is checked in as a test fixture:
`apps/listener/tests/fixtures/waxpeer_feed_frames.txt`. It holds the raw text frames as they arrived, in
order. Sanitization replaced the seller `steam_id` and the session IDs. Every other field is as received.

## Official Waxpeer API reference

| Page | What it covers | Used by |
|---|---|---|
| [Socket.IO](https://docs.waxpeer.com/socketio) | Public feed at `wss://waxpeer.com/socket.io/?EIO=4&transport=websocket`. Subscribe to `csgo` to get `new`, `update` and `removed` per listing. Auth by the API key in the `authorization` header | `scrapers/waxpeer.py` |
| [Authentication](https://docs.waxpeer.com/authentication) | REST calls take the key as the query parameter `api`, never as a Bearer token. Keys come from https://waxpeer.com/profile/user | Probes only |
| [GET /v1/user](https://docs.waxpeer.com/api/user/user) | Account data, including `sell_fees` (the share a seller keeps) | Fee check |
| [GET /v1/prices/snapshot](https://docs.waxpeer.com/api/buy-items/prices-snapshot) | Every listing, one CSV row each (`format=csv`, gzip), rebuilt about once a minute. `include_hold=1` adds trade locked listings | Not used yet (see [Not done yet](#not-done-yet)) |
| [GET /v1/check-availability](https://docs.waxpeer.com/api/buy-items/check-availability) | Whether up to 100 listing IDs are still on sale | Used once to test what `removed` means |
| [Changelog](https://docs.waxpeer.com/changelog) | Dated API changes. The CSV snapshot gained columns on 2026-09-28 and 2026-09-29 | Check it before changing the scraper |
| [WebSocket (trade socket)](https://docs.waxpeer.com/websocket) | `wss://wssex.waxpeer.com`, for sellers who deliver trades | Not used: it carries no market listings |

Waxpeer's FAQ has the fees the API docs leave out: [selling fees](https://waxpeer.com/faq/fees-sell),
[trade lock](https://waxpeer.com/faq/trade-lock), [hold balance](https://waxpeer.com/faq/hold-balance) and
[delivery time](https://waxpeer.com/faq/delivery-time).

## What the live feed does that the docs do not say

**A removal is not a sale.** The docs never say what `removed` means, and Waxpeer publishes no sales at all:
the `history` block of `POST /v1/mass-info` is marked "currently disabled", and `GET /v1/history` lists only
my own purchases. So I tested it. Of 100 listings removed during a 3 minute recording, 20 were back on sale
a minute later under the same item ID. 264 of the 2,088 removals were followed by a `new` event for the same
listing within seconds, usually at a slightly different price, which is a seller relisting. One seller made
242 removals in bursts two seconds apart, which looks like a whole inventory going offline (Waxpeer hides the
listings of sellers outside their "Prime Time" hours). A removal can be a sale, a delisting, a relisting or
a seller going offline, and nothing tells them apart.

**The name has the Doppler phase written in.** The feed sends `★ Karambit | Doppler Phase 3 (Factory New)`
with `phase: "Phase 3"`, where Steam's market hash name is `★ Karambit | Doppler (Factory New)`. The scraper
removes the phase from the name before adding it back the way Skinport ticks carry it
(`★ Karambit | Doppler (Phase 3) (Factory New)`). Without that, no Doppler listing matched a Skinport
baseline. Gems (`Ruby`, `Sapphire`, `Emerald`, `Black Pearl`) are written the same way.

**The event type is the Socket.IO event name.** The docs show an `event` field inside the payload. In
practice the payload has no such field and the type is the event name: `42["new", {...}]`.

**There is no paint seed.** Listings carry `float`, `paint_index` (the finish), `phase` and `sticker_names`,
but no pattern. `classid` and `instance_id` are null on some `new` events.

**The feed works without a key.** It answered the same without the `authorization` header. The listener
sends the key anyway, because that is what the docs describe.

**Prices are thousandths of a dollar.** `1000` is $1. The scraper converts to cents rounding up, so a buy
price is never understated (1,592 is 160 cents).

**Updates are almost all price changes.** Of the updates to listings seen earlier in the recording, every
one changed the price: 62% were cuts and 38% were rises. Sellers run repricing bots.

## Transport

The listener speaks Engine.IO v4 over an aiohttp websocket itself, so it needs no Node.js sidecar and no
browser cookies. The exchange is:

1. The server sends the open packet `0{"sid":...,"pingInterval":25000,"pingTimeout":20000,...}`.
2. The listener connects to the default namespace with `40`; the server answers `40{"sid":...}`.
3. The listener subscribes with `42["sub",{"name":"csgo"}]`; the server answers `42["subscribed",{"name":"csgo"}]`
   and then `handshake`, followed by the listing events.
4. The server pings with `2` every 25 seconds and the listener answers `3`. When nothing arrives for the
   ping interval plus the ping timeout (45 seconds), the connection is treated as dead.

When the connection ends, the scraper hands on the raw payloads still pending and raises
`FeedConnectionLost`. The listener's producer loop reconnects after 10 seconds.

## Events

| Field | Type | On | Notes |
|---|---|---|---|
| `item_id` | string | all | Steam asset ID of the listing. The listing key (`listing_id`) |
| `name` | string | all | Market hash name, with a Doppler phase written in (see above) |
| `price` | int | all | Thousandths of a dollar |
| `game` | string | all | `csgo` |
| `auto` | bool | new, update | True when the listing delivers without the seller |
| `float` | number or null | new, update | Wear |
| `paint_index` | int or null | new, update | Finish |
| `phase` | string | new, update | Doppler phase or gem; empty string otherwise |
| `sticker_names` | list or null | new, update | Applied stickers and charms by item name (`Sticker | Crown (Foil)`, `Charm | Lil' Zen`) |
| `classid`, `instance_id` | int or null | new, update | Steam class and instance IDs |
| `steam_id` | string | most new and update | SteamID64 of the account holding the item |
| `steam_price` | int | new, update | Waxpeer's suggested price |
| `unlock_at`, `send_until` | RFC 3339 | trade locked listings | When the lock ends, and the delivery deadline |

A `removed` event usually carries only `game`, `item_id`, `name` and `price` (2,129 of 2,147 in the
recording).

## Why Waxpeer is judged against Skinport

Waxpeer has no sales data, so it cannot have baselines built from sales the way Skinport does, and I cannot
label a Waxpeer listing by what Waxpeer sold. A baseline built from Waxpeer's lowest asks would be the kind of
asking price `docs/data_sources.md` warns against. So a Waxpeer listing is treated as something I could buy
on Waxpeer and resell on Skinport:

- `shared_utils.resale_venue_for("waxpeer")` is `skinport`.
- The Waxpeer listener loads the Skinport baselines and scores each listing against them. Its Z-score price
  window holds Waxpeer prices only (`price_window:waxpeer:<item>`).
- The DRE uses the Skinport baseline and Skinport sticker prices.
- The profit estimate buys at the Waxpeer price with Waxpeer's fees and resells at the Skinport baseline
  price less Skinport's seller fee (#260).
- Paper trades record `venue = waxpeer`.

## What the listener records

The feed is far too busy to record: about 430 events a second on average, with bursts to 2,800, which is
about 11 GB of raw JSON a day. My database has 32 GiB in total. So the scraper records and scores only
listings that could be worth buying:

- A `new` listing, or an `update` of a listing it does not follow yet, is recorded when its price is below
  the item's latest Skinport price. It is also recorded above that price when its stickers are worth more
  than $100 at Skinport prices and the premium is at most 3% of that value, because the DRE's sticker rules
  could approve it there.
- A recorded listing is followed. A later `update` is recorded when it cuts the price and the new price
  still qualifies. A rise is dropped, because it cannot create a new opportunity.
- The `removed` event of a followed listing is recorded, because it ends the listing's time on the market.
  Other removals are dropped.
- Items without a Skinport baseline are dropped. They can be neither scored nor labeled.

Measured against Skinport build 4 (7,947 baselines) on the 3 minute recording:

| Events | Per second | Below the Skinport price | Below 92% of it | No Skinport baseline |
|---|---:|---:|---:|---:|
| `new` | 12.5 | 4.0 | 1.5 | 3.0 |
| Price cuts of listings already seen | 93 | 7.5 | 3.4 | 31 |
| Updates of listings not seen yet | 230 | 23 | 15 | 85 |

The last row is the start of a connection, when every listing already on the market shows up for the first
time through an update. It fades as the scraper sees each listing once. In steady state that is about 11.5
recorded listings a second plus the removals of followed ones.

Every event is counted in `listener_feed_events_filtered_total{event, outcome}`, where the outcome is
`recorded`, `no_baseline`, `above_resale_price`, `not_a_price_cut`, `untracked_removal` or `malformed`.

## Recording pipeline

- Recorded listings are `MarketTick`s with `venue = waxpeer`, kind `listed` and event type `listed`. They go
  through the same dedup, price window, Z-score and DRE path as Skinport listings, and into
  `live_market_ticks` with `venue = waxpeer`.
- Recorded removals are ticks with event type `removed`. They are stored but never touch the price window
  or the DRE.
- The raw payloads of recorded events go into `feed_events` with `venue = waxpeer` and
  `event_type = batch`, one row per 500 events or 10 seconds, whichever comes first. The payload is
  `{"venue": "waxpeer", "channel": "csgo", "events": [{"event", "received_at_ms", "data"}, ...]}`, where
  `data` is the payload exactly as received. Grouping them lets PostgreSQL compress the row. A listener that
  stops loses at most the last 10 seconds of raw payloads; the ticks are not affected.
- The Waxpeer listener runs as its own container (`listener-waxpeer`, `LISTENER_VENUE=waxpeer`). It keeps
  its batch streams under `listener:ingest:waxpeer:*` in the shared Redis, so neither listener recovers the
  other's pending batches. Replay its dead letters with `python replay_batches.py --venue waxpeer`.
- `listener_feed_connected` is 1 while the feed is subscribed. `listener_feed_resale_prices` is the number of
  Skinport prices the filter holds; at 0 it records nothing. `listener_feed_tracked_listings` counts the
  listings it follows (at most `WAXPEER_TRACKED_LISTINGS_MAX`, 200,000). Prometheus labels every series from
  this process `listener="waxpeer"`.

## Fees

| Parameter | Value | Source |
|:---|:---|:---|
| Seller fee | 6% | [Waxpeer FAQ](https://waxpeer.com/faq/fees-sell); `GET /v1/user` returned `sell_fees` 0.94 for my account |
| Buyer fee | none on the listing price | Deposit and withdrawal fees depend on the payment method and are not modeled |
| Trade hold | 7 days | A trade locked item can be listed at once, but the buyer gets it only when Steam's trade protection ends, and sale proceeds stay on the hold balance for 7 days |

These are registered as `WAXPEER_FEES` in `shared_utils/pnl.py`. The seller fee matters only if an item is
ever resold on Waxpeer; today every Waxpeer listing is resold on Skinport.

## Rate limits

The feed has no documented limit. The REST endpoints I looked at:

- Search by name (`/v1/search-items-by-name`, `/v2/search-items-by-name`, `/v1/mass-info`): 20 requests a
  minute, shared.
- CSV snapshot: HTTP 429 with `Retry-After: 1` when too many are being rendered, 503 while it is not ready.
  One snapshot was 1,315,982 listings for 25,050 names, 65 MB gzipped and 270 MB unpacked.
- No response carried rate limit headers.

## Volume

| Stream | Estimate (continuous uptime) |
|---|---|
| Recorded listings to `live_market_ticks` | about 11.5 a second plus removals of followed listings, roughly 1.3M rows and 200 MB a day |
| Raw payloads to `feed_events` | one row per 500 events or 10 seconds, roughly 50 to 120 MB a day after compression |

These are estimates from 3 minutes of traffic. Re-measure after a day of uptime with
`sum(increase(listener_feed_events_filtered_total{listener="waxpeer",outcome="recorded"}[1d]))` and
`SELECT pg_size_pretty(pg_total_relation_size('feed_events'))`.

## Retention

Waxpeer rows in `feed_events` follow the same policy as Skinport's (`docs/deployment.md`, Data Retention):
90 days in PostgreSQL, then export and delete.

## Not done yet

- Outcome labels for Waxpeer listings. They need the labeler to price a Waxpeer listing against Skinport
  comparable sales, and to treat a removal as censored rather than sold.
- Replay and backtests of Waxpeer events. The replay harness reads Skinport events only.
- Links to the listing. I have not confirmed Waxpeer's URL format, so `listing_url` is empty.
- Catching up from the CSV snapshot after the PC was off. The feed only reports changes, so listings that
  appeared while it was off are seen when they next change.
