# Data Sources & Data State

What market data Brand Sniper has, where it came from, what each source is fit for, and the known
problems with it. Read this before touching baselines, backtests, training data, or a new venue.
Figures are a snapshot taken on **2026-09-26** from the production database.

## Summary

| Source | Table | Venue | Covers | Status |
|:---|:---|:---|:---|:---|
| Kaggle Steam price dataset | `historical_prices` | Steam Community Market | 2013-04-26 to 2024-06-15 | Static, **pre-crash**. Long-term context only; not a live reference price. |
| Skinport REST snapshots (`/v1/items`) | `live_market_ticks` (`event_type` NULL) | Skinport | 22 days between 2026-06 and 2026-09 | Lowest ask per item, not sales. Gaps whenever the PC was off. |
| Skinport sale feed (WebSocket) | `feed_events`, `live_market_ticks` (`event_type` set) | Skinport | From 2026-09-24 21:58 UTC | Every `listed` and `sold` event, listing-level (#232). The basis for labels. |
| Outcome labels | `listing_outcomes` | Skinport | Empty until about 2026-10-08 | A label exists only 14 days + 1 h after a listing (#233). |

## Kaggle: Steam Community Market price history

- **Source:** [Steam Market Price Dataset - CS:GO](https://www.kaggle.com/datasets/leawind/steam-market-price-dataset-csgo) (Kaggle, `leawind`).
- **What it is:** one CSV per item (URL-encoded `market_hash_name` as the file name) with columns
  `price, quantity, date, unix timestamp`: the Steam Community Market's median sale price and volume per
  time bucket. About 22,500 files, 105M rows, 22,446 items.
- **Where it lives:** downloaded into `data/items/` (not in git), checked with
  `apps/analytics/validate_historical.py`, and loaded once into `historical_prices` with
  `apps/analytics/seed_historical.py`. Nothing writes to `historical_prices` after that.
- **How it is used:** `apps/analytics/long_term_macro.py` computes every `item_macro_baselines` row
  (latest price, 30/90-day averages, volatility, support floor) from it; `update_baselines.py` pushes those
  rows into the edge Redis as `baseline:<market_hash_name>`, where the Z-score and the DRE read them. The
  backend's long-term Steam baseline query (`apps/backend/queries.py`) also reads it.

### Problems

1. **It ends before the CS2 market crash.** The data stops on 2024-06-15. On 2025-10-23 Valve added
   knife and glove trade-up contracts (five Covert skins trade up into a knife or gloves). Knife and glove
   prices fell sharply, and Covert skins rose because they became trade-up inputs
   ([HLTV](https://www.hltv.org/news/43004/knife-trade-ups-and-return-of-retakes-headline-cs2-update)).
   The baselines therefore describe a market that no longer exists.
2. **It is the wrong venue.** Steam prices include Steam's roughly 15% fee and are paid in
   non-withdrawable Steam wallet funds, so they sit above third-party markets such as Skinport. Measuring a
   Skinport listing against a Steam average makes it look cheaper than it is.
3. **It is static.** Because `historical_prices` never changes, the daily baseline job recomputes the same
   numbers every run. The "30-day average" is the last 30 days of the dataset, not the last 30 days of the
   market. (Upside: a replay does not suffer baseline look-ahead as long as this stays true.)

Measured on 2026-09-26: the baseline's latest price divided by the item's median Skinport price over the
previous two days (asks and sales mixed, so rough):

| Item type | Items | Baseline / current | Current median at or below the baseline support floor |
|:---|---:|---:|---:|
| Knife | 1,102 | 1.73 | 44% |
| Glove | 228 | 1.71 | 80% |
| Weapon skin | 5,588 | 0.67 | 13% |
| Sticker | 865 | 0.86 | 19% |
| Agent | 160 | 0.63 | 1% |

With these baselines the DRE approves ordinary knife and glove listings as bargains and overstates
their profit, and it misses real bargains on items that have risen. **Do not use the Kaggle baselines as the
live reference price.** Keep the dataset for long-term context (for example, how an item behaved over
years), and build live baselines from current data from the venue being traded.

## Skinport data

See [`docs/skinport_feed.md`](skinport_feed.md) for the feed's fields and quirks.

- **REST snapshots** poll `https://api.skinport.com/v1/items` and store each item's `min_price` (the
  lowest current ask). They are not sale prices. About 10.4M rows over 22 days (2026-06: 3 days,
  2026-07: 7, 2026-08: 8, 2026-09: 4 so far); one stray row is dated 2024-06-15. Only the server insert time
  is stored (#256).
- **Sale feed** (Node.js sidecar, `apps/listener/scrapers/skinport_websocket/`) records every `listed` and
  `sold` event since 2026-09-24 21:58 UTC, raw in `feed_events` and normalized in `live_market_ticks`
  with listing ID, float, pattern, stickers, and link. Exact edge receive times.
- **Sales history** (`/v1/sales/history`, 7/30/90-day aggregates per item) is documented but not yet used.
  It is the natural source for current Skinport baselines.

## Operating model: the edge is not always on

Both Docker stacks (edge and server: listener, edge Redis, Prefect, MLflow, Prometheus, Grafana) run on
the owner's PC, which is **not on 24/7**. Only PostgreSQL (Azure) is always on. Consequences:

- **Recorded data has gaps.** The feed is not replayed after downtime, so listings and sales that happened
  while the PC was off are missing. Labels count fewer comparable sales and mark more listings
  `sale_censored` across a gap; backtests must not treat a gap as a quiet market.
- **Scheduled jobs must catch up.** A job pinned to a fixed time silently skips days. Prefer flows that
  process everything missed since their last successful run and are safe to re-run (the outcome labeler
  already upserts over a date range).
- **Edge state must rebuild on startup.** The edge Redis is RAM-only. Anything the listener needs, such
  as baselines, has to be reloaded when the stack starts, not only by a daily job.

### Known incident: no baselines on the edge since July 2026

The edge Redis has held no `baseline:*` keys since roughly 2026-07-11, when `update_baselines.py` last ran
by hand. Without a baseline the DRE rejects every anomaly, so the listener has approved nothing since
then: `simulated_trades` holds only 22 paper trades, all from 2026-07-10/11 (12 of them knives). Nothing
warned about it. Recording (#232) is unaffected, because it does not depend on decisions. Reloading the
Kaggle baselines is not the fix, because of the problems above; the fix is current, venue-specific
baselines that load at listener startup, with a health check that fails when they are missing.

## Venues

Fees per venue live in `packages/shared_utils/src/shared_utils/pnl.py` (`VenueFees`, `fees_for`); see
section 4.2 of [`docs/roadmap_proven_edge.md`](roadmap_proven_edge.md). Skinport is the only venue connected.
More venues are planned under #33. Prefer venues with an official, key-based API over ones that need
browser cookies or a JavaScript sidecar, as Skinport's feed does.
