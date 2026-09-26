# Data Sources

This is the market data I have for Brand Sniper, where each part came from, and what I trust it for.
Read it before working on baselines, backtests, training data or a new venue. The numbers below come
from the production database on 2026-09-26.

## Summary

| Source | Table | Venue | Covers | Status |
|:---|:---|:---|:---|:---|
| Kaggle Steam price dataset | `historical_prices` | Steam Community Market | 2013-04-26 to 2024-06-15 | Static and older than the crash. Long term context only. |
| Skinport REST snapshots (`/v1/items`) | `live_market_ticks` with no `event_type` | Skinport | 22 days between June and September 2026 | Lowest ask per item, not sales. Gaps whenever my PC was off. |
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

Every baseline comes from it. `apps/analytics/long_term_macro.py` computes the `item_macro_baselines` rows
(latest price, 30 and 90 day averages, volatility, support floor) and `update_baselines.py` copies them
into the edge Redis as `baseline:<market_hash_name>`. The Z score and the DRE read them from there. The
backend also reads `historical_prices` for its long term Steam price (`apps/backend/queries.py`).

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

With these numbers the DRE treats ordinary knife and glove listings as bargains and overstates the profit,
and it misses real bargains on items that went up. Do not use the Kaggle baselines as the live reference
price. The dataset is still useful for how an item behaved over the years. Live baselines should come from
recent prices on the venue being traded (#259).

## Skinport data

[`docs/skinport_feed.md`](skinport_feed.md) covers the feed fields and quirks.

The REST poller calls `https://api.skinport.com/v1/items` and stores each item's `min_price`, the lowest
ask at that moment. These are not sale prices. There are about 10.4 million rows over 22 days: 3 in June,
7 in July, 8 in August and 4 so far in September 2026. One stray row is dated 2024-06-15. Only the server
insert time is stored, not the edge time (#256).

The sale feed runs through the Node.js sidecar in `apps/listener/scrapers/skinport_websocket/`. Since
2026-09-24 21:58 UTC it records every `listed` and `sold` event, raw in `feed_events` and normalized in
`live_market_ticks` with the listing ID, float, pattern, stickers and link. The receive times are exact.

Skinport also has a sales history endpoint (`/v1/sales/history`) with 7, 30 and 90 day aggregates per
item. I do not use it yet, but it is the obvious source for current Skinport baselines.

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

### No baselines on the edge since July 2026

The edge Redis has had no `baseline:*` keys since around 2026-07-11, the last time I ran
`update_baselines.py` by hand. Without a baseline the DRE rejects every anomaly, so the listener has not
approved anything since then. `simulated_trades` only has 22 paper trades, all from 10 and 11 July 2026, and
12 of them are knives. Nothing warned about it. Recording (#232) was not affected because it does not depend
on decisions. Reloading the Kaggle baselines would not fix this, for the reasons above. The fix is current
baselines per venue that load when the listener starts, plus a health check that fails when they are
missing (#259).

## Venues

Fees for each venue live in `packages/shared_utils/src/shared_utils/pnl.py` (`VenueFees`, `fees_for`),
described in section 4.2 of [`docs/roadmap_proven_edge.md`](roadmap_proven_edge.md). Skinport is the only
venue connected so far. CSFloat (#33) and Waxpeer (#261) come next. When picking a venue I prefer an
official API with a key over one that needs browser cookies or a JavaScript sidecar, which is what Skinport's
feed needs.
