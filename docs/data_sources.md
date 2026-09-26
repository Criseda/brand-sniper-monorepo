# Data Sources

This is the market data I have for Brand Sniper, where each part came from, and what I trust it for.
Read it before working on baselines, backtests, training data or a new venue. The numbers below come
from the production database on 2026-09-26.

## Summary

| Source | Table | Venue | Covers | Status |
|:---|:---|:---|:---|:---|
| Kaggle Steam price dataset | `historical_prices` | Steam Community Market | 2013-04-26 to 2024-06-15 | Static and older than the crash. Long term context only. |
| Skinport sales history (`/v1/sales/history`) | `baseline_builds`, `venue_baselines` | Skinport | One build per day from 2026-09-26 | The live baselines (#259). |
| Skinport REST snapshots (`/v1/items`) | `live_market_ticks` with no `event_type` | Skinport | 22 days between June and September 2026 | Lowest ask per item, not sales. Trade locked listings only until #275 (see below). Gaps whenever my PC was off. |
| Skinport sale feed (WebSocket) | `feed_events`, `live_market_ticks` with an `event_type` | Skinport | From 2026-09-24 21:58 UTC | Every `listed` and `sold` event with listing details (#232). Labels are built from this. |
| Outcome labels | `listing_outcomes` | Skinport | Empty until about 2026-10-08 | A listing gets its label 14 days and 1 hour after it was seen (#233). |

## The Kaggle dataset

I seeded the project with the [Steam Market Price Dataset for CS:GO](https://www.kaggle.com/datasets/leawind/steam-market-price-dataset-csgo)
on Kaggle. It has one CSV per item, named after the URL encoded `market_hash_name`, with the columns
`price, quantity, date, unix timestamp`. Each row is the median Steam Community Market sale price and the
volume for one time bucket. In total it is about 22,500 files, 105 million rows and 22,446 items.

The files go in `data/items/`, which is not in git. `apps/analytics/validate_historical.py` checks them
and `apps/analytics/seed_historical.py` loads them once into `historical_prices`. Nothing writes to that
table afterwards.

Until #259 every live baseline came from it: `apps/analytics/long_term_macro.py` computed the
`item_macro_baselines` rows and a sync script copied them into the edge Redis. That sync is gone. The
`item_macro_baselines` rows are still computed and the backend still reads them, along with
`historical_prices`, for the long term Steam context it gives the CFO (`apps/backend/queries.py`), but the
listener no longer sees them.

### Why these baselines are wrong today

The dataset stops on 2024-06-15, which is before the CS2 market crash. On 2025-10-23 Valve let players
trade up five Covert skins into a knife or gloves
([HLTV](https://www.hltv.org/news/43004/knife-trade-ups-and-return-of-retakes-headline-cs2-update)).
Knife and glove prices dropped hard after that, and Covert skins went up because everyone needed them
for the contracts. So the baselines describe a market that no longer exists.

It is also the wrong venue. Steam prices include Steam's fee of about 15% and are paid in wallet money
you cannot withdraw, so they sit above Skinport prices. Compared against a Steam average, a normal
Skinport listing looks cheap.

And it never changes. Because nothing updates `historical_prices`, the daily baseline job gets the same
numbers every time, and the "30 day average" is really the last 30 days of the dataset. The one good side
effect is that replaying an old date does not leak future baselines, as long as this stays true.

This is how far off they were on 2026-09-26. I divided each item's baseline latest price by its median
Skinport price over the two days before. That median mixes asks and sales, so it is a rough measure.

| Item type | Items | Baseline / current | Current median at or below the baseline support floor |
|:---|---:|---:|---:|
| Knife | 1,102 | 1.73 | 44% |
| Glove | 228 | 1.71 | 80% |
| Weapon skin | 5,588 | 0.67 | 13% |
| Sticker | 865 | 0.86 | 19% |
| Agent | 160 | 0.63 | 1% |

With these numbers the DRE treated ordinary knife and glove listings as bargains and overstated the
profit, and it missed real bargains on items that went up. Do not use the Kaggle baselines as the live
reference price. The dataset is still useful for how an item behaved over the years.

## Current baselines

Since #259 the baselines come from recent sales on the venue being traded. For Skinport,
`apps/analytics/build_baselines.py` makes one request to the sales history endpoint, which returns the min,
max, average, median and volume of every item's sales over the last 24 hours, 7, 30 and 90 days, in USD.
Skinport computes these itself, so they have no holes when my PC was off. Each run is stored as a new
build in `baseline_builds` with one `venue_baselines` row per item, and builds are never overwritten.

How each field is filled (the code is `shared_utils/baselines.py`):

| Field | Source |
|:---|:---|
| Latest price | 24 hour median when the item sold at least 5 times that day, else the 7 day median (at least 3 sales), else the 30 day median |
| 30 and 90 day average | 30 and 90 day medians. The mean is not used because rare patterns and stickers pull it up. |
| Volume | 30 day sales divided by 30 |
| Volatility | Once an item has 14 daily medians from earlier builds in the last 30 days, their standard deviation. Before that, an estimate: (30 day median minus 30 day minimum) divided by the square root of 2 ln n, where n is the number of sales. It never goes below 1% of the median. |
| Support floor | With enough daily medians, their 10th percentile. Before that, the 30 day median minus 1.28 times the volatility, which is the 10th percentile of a normal distribution. |
| Sticker prices | The latest price of each `Sticker | ...` item, keyed by the name without that prefix, because that is how a listing names an applied sticker |

An item with fewer than 5 sales in 30 days gets no baseline, and the DRE skips it. On 2026-09-26 that
left about 7,900 of 37,000 Skinport items with a baseline.

The volatility estimate only looks at the low side of the sales. The highest sale is useless here: an
AK-47 Redline in Field Tested had a 30 day median of $29.29 and a maximum of $456.06, because of rare
patterns and stickers. The lowest of n sales from a roughly normal spread sits about the square root of
2 ln n standard deviations under the median. A few underpriced sales make the estimate a bit larger, so
it errs towards approving less. This estimate is approximate. It gets replaced item by item as daily
builds pile up, and `volatility_method` on each row says which one was used.

The listener gets the newest build from the backend (`GET /api/v1/baselines/{venue}/latest`) as soon as it
starts and then every 15 minutes, and loads it into one Redis hash per venue (`baselines:<venue>`,
`sticker_prices:<venue>`, `baseline_meta:<venue>`). Replays use the build that was current at each moment,
so old decisions are replayed with the baselines they actually had (see [`backtesting.md`](backtesting.md)).

## Skinport data

[`docs/skinport_feed.md`](skinport_feed.md) covers the feed fields and quirks.

The REST poller calls `https://api.skinport.com/v1/items` and stores each item's `min_price`, the lowest
ask at that moment. These are not sale prices. There are about 10.4 million rows over 22 days: 3 in June,
7 in July, 8 in August and 4 so far in September 2026. One stray row is dated 2024-06-15. Only the server
insert time is stored, not the edge time (#256).

**Until #275 every REST snapshot is a trade locked price.** From the first Skinport commit on 2026-06-24
the poller sent `tradable=0`, which returns only listings that are not tradable yet (#267, details in
[`docs/skinport_feed.md`](skinport_feed.md#rest-lowest-asks-v1items)). Those asks sit a median 11% below the
lowest tradable ask, because the buyer waits out the lock. Since #275 was deployed the poller asks for
tradable listings only. The cutover shows in the data as the jump from about 10,000 priced items per poll to
about 25,000. Snapshots before it cannot stand in for prices I could buy at and resell, so replays, the
scorecard (#249) and any training set must leave them out or report them apart.

The sale feed runs through the Node.js sidecar in `apps/listener/scrapers/skinport_websocket/`. Since
2026-09-24 21:58 UTC it records every `listed` and `sold` event, raw in `feed_events` and normalized in
`live_market_ticks` with the listing ID, float, pattern, stickers and link. The receive times are exact.

The sales history endpoint (`/v1/sales/history`) feeds the current baselines, described above.

## My PC is not always on

Both Docker stacks run on my PC, which I turn off. That covers the listener, the edge Redis, Prefect,
MLflow, Prometheus and Grafana. Only PostgreSQL on Azure stays up all the time. That has three effects.

1. The recorded data has gaps. The feed does not replay what happened while the PC was off, so those
   listings and sales are missing. Around a gap, labels see fewer comparable sales and mark more listings
   `sale_censored`. A backtest should not read a gap as a quiet market.
2. Jobs pinned to a fixed time skip days without anyone noticing. Jobs should instead process everything
   missed since their last successful run and be safe to run again. The outcome labeler already works like
   that.
3. The edge Redis only lives in RAM, so it is empty after every restart. Whatever the listener needs,
   baselines included, has to be loaded when the stack starts and not only by a daily job.

The baseline builder follows these rules. It runs as the `baseline-builder` service, checks when it starts
and then every hour, and only builds when the newest build is more than 20 hours old. The listener loads
the newest build when it starts and reloads it by itself after a Redis restart.

### No baselines on the edge from July to September 2026 (fixed in #259)

From around 2026-07-11, the last time I ran the old sync script by hand, until #259, the edge Redis had no
baselines. Without a baseline the DRE rejects every anomaly, so the listener approved nothing in that time.
`simulated_trades` only has 22 paper trades, all from 10 and 11 July 2026, and 12 of them are knives.
Nothing warned about it. Recording (#232) was not affected because it does not depend on decisions.

Since #259 the listener loads baselines itself, exports `listener_baselines_loaded` and
`listener_baseline_build_age_seconds`, logs an error while they are missing or more than 48 hours old,
and its `/health` answers 503 in that state, so Docker shows the container as unhealthy.

While fixing this I also found that the sticker premium rule never worked: sticker prices were keyed as
`Sticker | Crown (Foil)` but listings name the sticker `Crown (Foil)`, so the lookup always missed. The new
sticker prices use the listing's naming.

## Venues

Fees for each venue live in `packages/shared_utils/src/shared_utils/pnl.py` (`VenueFees`, `fees_for`),
described in section 4.2 of [`docs/roadmap_proven_edge.md`](roadmap_proven_edge.md). Skinport is the only
venue connected so far. CSFloat (#33) and Waxpeer (#261) come next. When picking a venue I prefer an
official API with a key over one that needs browser cookies or a JavaScript sidecar, which is what Skinport's
feed needs.
