# DMarket Trading API: Listings, Sales, Fees, and How Brand Sniper Uses Them

This note documents what DMarket's Trading API actually returns and how Brand Sniper uses it (#339). I read
the official reference and checked it against the live API with my own key on 2026-10-25, between 13:26 and
14:40 UTC. All times are UTC.

Sanitized samples of the live responses are checked in as test fixtures in `apps/listener/tests/fixtures/`:

| Fixture | What it holds |
|---|---|
| `dmarket_offers_page.json` | 25 of the newest bot held listings (`orderBy=createdAt&orderDir=desc`) and three Karambit Doppler listings |
| `dmarket_last_sales.json` | The newest 20 sales of `AWP | Asiimov (Field-Tested)` and of `★ Karambit | Doppler (Factory New)` |
| `dmarket_customized_fees.json` | The default fee and the first 10 reduced fees |
| `dmarket_rate_limited.json` | A 429 response, with its rate limit headers |

Sanitization replaced the account IDs of owners, depositors and bots, the Steam asset IDs, and the inspect
links. Every other field is as received.

## Official DMarket API reference

The reference is a single page, [docs.dmarket.com/v1/swagger.html](https://docs.dmarket.com/v1/swagger.html),
"DMarket trading API v2.0.0" (OpenAPI 3.0.4), served from `https://api.dmarket.com`. DMarket keeps reference
clients that sign requests in four languages at [dmarket/dm-trading-tools](https://github.com/dmarket/dm-trading-tools).

| Endpoint | What it gives | Used by |
|---|---|---|
| `GET /marketplace-api/v2/offers` | Listings for sale. `gameId=a8db` is CS2 | `scrapers/dmarket.py` |
| `GET /trade-aggregator/v1/last-sales` | Sales history of one title, up to 12 months | Baseline builder and labeler |
| `GET /exchange/v1/customized-fees` | The default seller fee and the titles with a reduced fee | Baseline builder |
| `POST /marketplace-api/v1/aggregated-prices` | Best ask and best buy order per title, with counts | Probes only |
| `GET /marketplace-api/v1/targets-by-title/{game_id}/{title}` | Buy orders for one title | Probes only |
| `PATCH /exchange/v1/offers-buy` | Buys listings | Not used: buying is not part of #339 |

DMarket's [FAQ](https://dmarket.com/faq) has the fees and rate limits the reference leaves out. The
[terms of use](https://dmarket.com/terms-of-use) (last updated 2023-12-18) have no clause against automation,
and the FAQ describes the Trading API as a way to trade in bulk with preset algorithms.

## Authentication

Every request, including the read only ones, carries three headers. Without them every endpoint answers 401.

| Header | Value |
|---|---|
| `X-Api-Key` | The public key, lowercase hex |
| `X-Sign-Date` | Unix time in seconds. DMarket refuses it when it is more than 2 minutes old |
| `X-Request-Sign` | `dmar ed25519 ` followed by the hex Ed25519 signature |

The signed text is the HTTP method, the route path with its query string, the body, and the timestamp, joined
with nothing between them. The path is signed decoded and sent percent encoded; the query string is signed
exactly as it is sent. The secret key DMarket hands out is 64 bytes in hex: the 32 byte Ed25519 seed followed
by the public key. The listener signs with the seed through the `cryptography` package.

Keys are generated under Trading API in the DMarket account settings. They go in `apps/listener/.env` as
`DMARKET_PUBLIC_KEY` and `DMARKET_SECRET_KEY`. A key only needs to read; nothing here buys or sells.

## What the live API does that the docs do not say

**The rate limit is 30 requests a second, shared.** The FAQ gives 10 requests a second for market items and 6
for last sales. With my key every response said `x-ratelimit-limit-second: 30`, and a listings request and a
sales request made right after each other drew on the same remaining count. Without a key the limit was 12. A
burst of 45 parallel requests got 30 answers and 15 refusals. The refusal is HTTP 429 with
`{"message": "API rate limit exceeded"}` and `Retry-After: 1`, and the next second was clear again.

**The default seller fee is 10%.** The FAQ says the CS2 fee is usually 2%. `customized-fees` returned a default
of `fraction: "0.1"` and 11,780 titles reduced to 2%, each up to a `maxPrice`. The FAQ's 2% is the reduced fee
of the popular titles, and every other title pays 10%. All 11,780 reduced fees expired at the same time,
about 2.3 days after I fetched them, so the list is renewed every few days.

**Prices come in three formats.** A listing's `priceCents` is cents as a string (`"21345"`). A sale's `price` is
dollars as a string, and the decimals are left out when they are zero (`"24.56"`, but also `"1550"`).
`aggregated-prices` sends `{"Currency": "USD", "Amount": "2467"}`, where the amount is cents. The listener
parses each with `Decimal` and never through a float.

**Listings arrive fast.** Bot held listings appeared at 5.6 to 8.8 new ones a second, about 16,000 in 30
minutes. Over 5 minutes, 1,681 new listings were 1,680 different items, so this is not one item being relisted
again and again. Person to person listings arrived at 0.15 a second. No listing changed its price under the same
`offerId` in 30 minutes, so a new price is a new listing.

**DMarket's bots trade too.** Most bot held listings belong to a few accounts: 116 owners listed the 1,681 items,
and the largest listed 184. One AWP listed by a bot at $53.99 had the float and paint seed of a sale into a buy
order at $53.55 eighteen hours earlier. The bots buy through buy orders and list again.

**About half the sales are buy order fills.** A sale's `txOperationType` is `Offer` when someone bought a
listing and `Target` when a seller sold into a buy order. Across 120 titles with enough of both, 47% of the sales
were `Target`, and they sold for a median 6.8% less than the `Offer` sales of the same title (10th percentile
20% less).

**The sales history goes back a full year.** Paging with `offset` reached 12 months for every title I tried. An
AWP Asiimov in Field Tested had 3,417 sales in 365 days and a Karambit Doppler in Factory New 2,610. An AK-47
Redline in Field Tested and the Revolution Case had more than 8,000, where I stopped. Sales are sparse even so:
several hours passed between the newest sales of the AK-47 Redline.

**The `title` filter on listings is fuzzy.** `title=★ Karambit | Doppler` also returned Gamma Doppler and StatTrak
listings. A listing's `attributes.title` is the exact market hash name.

**`total` stops at 10,000.** Every listing request reported `total: "10000"` without a title filter.

**The cursor is a time and an ID.** `cursor` is base64 JSON such as `[1792935756000,"f3a42338-..."]`, the
`createdAt` in milliseconds and the `offerId` of the last listing on the page. Listings come newest first and,
within one second, by `offerId` ascending. `createdAt` has whole seconds for bot held listings and nanoseconds
for person to person listings.

## Listings

`GET /marketplace-api/v2/offers` returns `{"items": [...], "total": "10000", "cursor": "..."}`. The fields the
listener reads:

| Field | Type | Notes |
|---|---|---|
| `offerId` | string | UUID of the listing. The listing key (`listing_id`) |
| `priceCents` | string | Price in cents |
| `createdAt` | RFC 3339 | When the listing was created |
| `offerType` | string | `dmarket` for a bot held listing, `p2p` for a person to person one |
| `discountPercent` | number | Discount against DMarket's own appraised price. Not used |
| `attributes.title` | string | Market hash name, without the Doppler phase |
| `attributes.cs2.float` | string | Wear, as a decimal string. `"0"` on items without wear |
| `attributes.cs2.paintSeed` | int | Pattern. 0 on items without one |
| `attributes.cs2.paintIndex` | int | Finish |
| `attributes.cs2.phase` | string | Doppler phase or gem as a slug, `phase-2`; empty otherwise |
| `attributes.cs2.stickers` | list | Applied stickers, each with `name` (`FaZe Clan | Atlanta 2017`) and `wear` |
| `attributes.tradable` | bool | False while the item is trade locked |
| `attributes.tradeLockDays` | int | Days left on the trade lock |
| `attributes.unlockDate` | RFC 3339 | When the lock ends |

The Doppler phase is put into the name the way Skinport ticks carry it, with `build_versioned_name`:
`phase-2` on `★ Karambit | Doppler (Factory New)` becomes `★ Karambit | Doppler (Phase 2) (Factory New)`.

`settlementTime`, `revertPendingUntil` and `tradeProtectionRemoved` sit next to the trade lock fields. I have
not bought anything, so I do not know what they mean for a buyer yet (see [Not verified yet](#not-verified-yet)).

## Sales history

`GET /trade-aggregator/v1/last-sales?gameId=a8db&title=...&limit=20&offset=0` returns
`{"sales": [...]}`, newest first. `limit` is at most 20.

| Field | Type | Notes |
|---|---|---|
| `price` | string | Dollars, decimals optional |
| `date` | string | Unix seconds |
| `txOperationType` | string | `Offer` (a listing was bought) or `Target` (a buy order was filled) |
| `offerAttributes.floatValue` | number | Wear |
| `offerAttributes.paintSeed` | int | Pattern |
| `offerAttributes.phaseTitle` | string | Doppler phase slug, `phase-3` |
| `offerAttributes.stickers` | list | Applied stickers |

A sale names no listing, so a listing cannot be joined to its sale directly. Several sales of a cheap item often
share one timestamp, which looks like one order buying several at once.

## Fees

| Parameter | Value | Source |
|:---|:---|:---|
| Seller fee | 2% on the 11,780 titles of the reduced list, up to each title's `maxPrice`; 10% on everything else | `GET /exchange/v1/customized-fees`, 2026-10-25 |
| Seller fee, person to person | 4% | Same endpoint with `offerType=p2p`. Not used |
| Buyer fee | None on the listing price | FAQ, "the buyer pays no fees for this operation" |
| Trade hold | 7 days until I can check it | See [Not verified yet](#not-verified-yet) |

The fee depends on the title, and the gap between 2% and 10% is larger than most margins the DRE approves, so
neither the worst case nor a price tier is good enough. `VenueFees` gets an optional fee per item, and
`DMARKET_FEES` in `shared_utils/pnl.py` has the 10% default. The baseline builder fetches the reduced list on
every build and stores each item's fee with its baseline (`venue_baselines.seller_fee_bps`), so the listener
loads the fee together with the baseline and a replay or a label uses the fee that applied at the time. A price
above the title's `maxPrice` pays the default.

## Rate limits

The budget is 30 requests a second for the whole account, so the listener and the baseline builder split it.
The listener stays under 10 a second and the builder under 10, which leaves room for probes. Both read
`x-ratelimit-remaining-second` on every response, pause until the next second when it reaches zero, and on a 429
wait for `Retry-After`, doubling the wait on repeated refusals up to 30 seconds.
`listener_dmarket_rate_limited_total` counts the refusals.

## Which listings are recorded

Only bot held listings (`offerType=dmarket`). The reference says person to person listings "are bought on
dmarket.com, not through this API", and the issue leaves them out unless the API can buy them.

Recording every bot held listing would be 500,000 to 750,000 rows a day, where all of Skinport's listings since
September fill 162,000. So, as for Waxpeer, the listener records only the listings that could be worth buying:

- A listing is recorded when its price is below the item's latest DMarket baseline price.
- Items without a DMarket baseline are dropped, because they can be neither scored nor labeled.
- A listing leaving the market is not recorded. Polling by `createdAt` only sees new listings, and checking
  every recorded listing again would take more requests than the budget has. Labels compare a listing with
  later sales of similar items instead, as `v1` does.

To estimate the volume I priced the 1,681 listings of the 5 minute recording against each title's median
DMarket sale over the 30 days before. 1,486 had at least 5 sales in that time, 413 were below the median and
64 below 92% of it.

| Listings | Per second |
|---|---:|
| All bot held | 5.6 |
| With at least 5 sales in 30 days | 5.0 |
| Below the 30 day median | 1.4 |
| Below 92% of it | 0.2 |

At 1.4 a second that is about 120,000 rows a day with the PC on all day. Every listing is counted in
`listener_feed_events_filtered_total{event, outcome}` as for Waxpeer, with the outcome `recorded`,
`no_baseline`, `above_resale_price` or `malformed`.

## Why DMarket is judged against its own sales

DMarket publishes its sales, so unlike Waxpeer it can have baselines and labels of its own. A DMarket listing
is treated as something I could buy on DMarket and resell on DMarket:

- `resale_venue_for("dmarket")` is `dmarket`; DMarket has no entry in `RESALE_VENUES`.
- The DMarket listener loads the DMarket baselines and scores each listing against them. Its price window
  holds DMarket prices only (`price_window:dmarket:<item>`).
- The profit estimate buys at the listing price with no buyer fee and resells at the DMarket baseline price
  less the item's DMarket seller fee.
- Paper trades record `venue = dmarket`.

Reselling on Skinport would ignore the sales DMarket does publish, and would carry Skinport's 7 day hold on
every trade. Comparing the two venues is the cross venue spread feature, which is separate work.

## Polling and catching up

The listener asks for the newest page every 1.5 seconds (`DMARKET_POLL_INTERVAL_SECONDS`) and pages back
through `cursor` until it reaches a listing it has seen. In the 30 minute recording a poll never needed more
than 3 pages, so polling takes about 2 requests a second.

The edge Redis lives only in RAM, so after my PC was off it is empty, and anything kept only there is lost
exactly when catching up needs it. The listener therefore keeps its resume point in two places. While it runs,
the newest `createdAt` it has polled and the IDs of the listings in that second are in the edge Redis
(`dmarket:resume`). When that key is missing, the listener asks the backend for the edge receive time of the
newest DMarket listing in `live_market_ticks`, and the IDs recorded in the two minutes before it.

From the resume point the listener pages back, oldest listings last, until it reaches a listing created more
than two minutes before the resume point, and skips the IDs it already recorded. The overlap covers clock drift
between my PC and DMarket. A day off at 6 to 9 listings a second is up to 8,000 pages, about 13 minutes at the
listener's 10 requests a second. Catching up runs next to the live poll, so new listings are still read on
time. Listings that were sold or delisted while the PC was off are gone and cannot be recovered.

## Baselines

The baselines come from DMarket's own sales through the venue aware pipeline of #259, as a separate build with
`venue = dmarket`. The titles are the ones in the newest Skinport build, about 8,000.

The builder stores every sale it reads in `venue_sales`, unique per venue, title, time, price and pattern, and
reads each title only back to the newest sale it already has. A partial build can stop at any point and the
next run carries on, and a rerun reads nothing twice. The first run reads 90 days of history. Over the 708
titles of the 5 minute recording, 30 days took 4.3 requests a title on average and 11 at the 90th percentile,
so 90 days for 8,000 titles is roughly 100,000 requests, close to three hours at 10 a second. Later runs read
only the sales since the last run.

The baseline uses `Offer` sales only. The DRE compares a listing with what listings sell for, and a buy order
fill sells about 7% lower, by an amount that moves with the share of buy order fills. The fields follow
`shared_utils/baselines.py` as for Skinport, computed from the stored sales instead of Skinport's own summary.

## Labels

DMarket listings are labeled with `label_outcomes.py --venue dmarket` under a new label version. A label
compares a listing with the `Offer` sales of the same item in the 14 days after it, read from `venue_sales`, and
prices the trade with DMarket's fees for that item. `v1` is not reused.

## Volume

| Stream | Estimate (continuous uptime) |
|---|---|
| Recorded listings to `live_market_ticks` | about 1.4 a second, roughly 120,000 rows a day |
| Raw payloads to `feed_events` | one row per 500 recorded listings or 10 seconds |
| Sales to `venue_sales` | 90 days of history once, then the sales of each day |

These come from 5 minutes of traffic on a Sunday afternoon. Measure again after a day of uptime with
`sum(increase(listener_feed_events_filtered_total{listener="dmarket",outcome="recorded"}[1d]))`.

## Not verified yet

- The trade hold after a purchase. A bot held item stays in the buyer's DMarket inventory, so it may be
  listed again before Steam's trade protection ends. Bot held listings carry `tradeLockDays` up to 7, and
  `settlementTime` and `revertPendingUntil` point at Steam's trade protection. Only a real purchase can show it.
  Until then `hold_seconds` is 7 days, which never overstates a profit.
- Whether person to person listings can be bought through the API. The reference says no, and
  `offers-buy` accepts `type: p2p`. Testing it needs a purchase, and the listener does not record them.
- How long a sale takes to show up in `last-sales`. The newest sales were hours old, which may be how often
  they happen or a delay. The labeler waits 14 days, so a delay of hours does not change a label.
- Whether a listing that disappears was sold or delisted. Of the 4 listings I saw disappear in 30 minutes, none matched a
  sale by float and paint seed in the 20 newest sales of its title.
