# Replay & Backtest Harness

The harness (`apps/listener/backtest/`) replays recorded market events through the listener's **real**
decision code and writes a decision log per run. Backtests (#249), model parity checks (#234, #236), and
the latency benchmark (#175) all use it, so their results come from one engine and can be compared.

- **Real decision path.** Replay calls the same functions as the live listener: the dedup rule, the
  sliding price window, the Z-score (`zscore.py`), and the Edge DRE (`rules_engine.py`), all in
  `detection.py`. Only the edge Redis is swapped for an in-memory store that follows Redis sorted-set
  semantics exactly.
- **Deterministic.** Events are replayed in a total order, and the log holds no wall-clock times or
  durations. The same input and configuration always produce a byte-identical log. Nothing is random,
  so there is no seed.
- **Offline.** A fixture replay needs no network and no database. A database replay only reads PostgreSQL.

## Running it

From `apps/listener`:

```bash
# Replay a fixture (the CI fixture lives in tests/fixtures/replay/)
uv run python -m backtest run --fixture tests/fixtures/replay/events.jsonl \
  --baselines tests/fixtures/replay/baselines.json --out decisions.jsonl

# Replay a date range from PostgreSQL, with two hours of warm-up before the start
uv run python -m backtest run --start 2026-10-01T00:00 --end 2026-10-08T00:00 --warmup-hours 2 \
  --out decisions.jsonl

# Export a small sanitized fixture from PostgreSQL
uv run python -m backtest export --start 2026-09-24T21:58 --end 2026-09-24T23:40 \
  --out-dir tests/fixtures/replay --max-items 25
```

Times are ISO 8601. A value without an offset is taken as UTC.

| Option | Meaning |
|:---|:---|
| `--strategy` | Decision strategy: `zscore_dre` (default, the live rules) or `zscore_dre_sweep` (the same decisions plus the inputs the scorecard's threshold sweep needs). |
| `--warmup-hours` | Database replay: also replay this many hours before `--start`. It fills the price windows and the dedup cache; those decisions are not logged. |
| `--log-from` | Fixture replay: the same idea. Events before this time only warm up. |
| `--speed` | Replay at recorded pace times this factor (`1` = real time). Omit it to run flat out. |
| `--timings` | Also write each decision's latency (nanoseconds, JSON Lines). This file is not deterministic and is kept apart from the decision log. |
| `--baselines` | Baseline file. Required with `--fixture`; a database replay defaults to the current `item_macro_baselines`. |

Detection thresholds come from the environment, as in the live listener: the CLI loads the root `.env`
and then `apps/listener/.env` before it imports the decision code. The values used are written to the
log header.

**Always warm up.** The live edge Redis keeps each item's window across listener restarts, but a replay
starts with empty windows. Until an item has `MIN_HISTORY_POINTS` prices, the Z-score falls back to the
macro baseline alone. On the committed fixture that changes the result from 14 approvals (cold) to none
after 30 minutes of warm-up, which is what the live listener did over that period (no paper trades).
Before #265 the cold run showed 25, because 11 of them were the same REST snapshot approved again on a
later poll.

## Input

**Feed events** come from `feed_events` and go through the live sidecar-message parser
(`scrapers.skinport.parse_sale_feed_message`), so replay normalizes exactly as the listener does.
Stored rows that are exact duplicates (same type, receive time, and payload) are dropped; a re-sent event
with its own receive time is kept, because the live listener saw it too.

**REST snapshots** come from `live_market_ticks` rows with no `event_type`. They carry only the server's
insert time, not the edge time. One poll is inserted as several batches over a few seconds, so rows less
than 60 seconds apart are treated as one poll and stamped with the poll's first insert time. That keeps
the 300-second dedup rule from dropping ticks that live kept. Snapshot timing is therefore approximate to
a few seconds until the edge time is persisted (#256). Feed-event timing is exact.

Only ticks the live listener stored exist in the database. Since #265 it stores every REST snapshot,
including the ones the dedup rule drops. Before that it did not store a REST tick that repeated the
previous price within 300 seconds, but live dropped those as duplicates anyway, so the decisions are
unaffected.

REST snapshots stored before #275 are the lowest ask among trade locked listings, not tradable ones (see
[`docs/data_sources.md`](data_sources.md#skinport-data)). A replay across the cutover mixes two kinds of
price in the same windows, so compare runs on one side of it.

**Baselines** come from the dated builds in `baseline_builds` (see [data_sources.md](data_sources.md)),
shaped exactly as the backend serves them to the listener. A database replay loads every build that was in
effect during the range, and switches to each one when replay time reaches its build time, so decisions use
the baselines that were current then. Before the first build exists there is nothing honest to use, so the
first build is used and that switch is marked `look_ahead` in the log. The CLI also warns when that
happens. `--baselines FILE` replaces the schedule with one fixed snapshot.

Live, the listener picks up a new build within 15 minutes, while a replay switches at the build time
exactly. That difference only matters for the few minutes after each daily build.

**Fixture format** (JSON Lines, one event per line):

```json
{"kind": "feed", "received_at_ms": 1790287133318, "payload": {"eventType": "listed", "sales": [...]}}
{"kind": "snapshot", "observed_at_ms": 1790287150000, "market_hash_name": "AK-47 | Slate (Field-Tested)", "price_cents": 412}
```

The baseline file is `{"baselines": {name: document}, "sticker_prices": {name: cents}, "as_of": ...}`.
`export` writes the build that was in effect at `--start`. The committed fixture's baselines predate #259
and come from the old Kaggle pipeline; they are only test input.
`export` keeps only the sale fields the parser and labeler read (`SANITIZED_SALE_FIELDS` in
`backtest/sources.py`). That drops personal data the live feed carries, such as the seller's `steamid`. The `feed_events` table
itself keeps the payload verbatim, `steamid` included (see [skinport_feed.md](skinport_feed.md)), so never
share raw rows or a fixture written by hand from them.

## Replay order and routing

Events are ordered by time, then snapshots before feed events recorded in the same millisecond, then
recording order. Each event expands into the stream items the live producers put on the listener queue,
and each item is routed as `tick_consumer` routes it:

1. Raw feed events and `sold` ticks are outcomes: counted, never scored.
2. A tick at the same price as the item's previous tick on the same venue within 300 seconds is a
   duplicate: logged with reason `duplicate`, never scored.
3. Any other tick enters the price window, and the strategy decides on it.

## Decision log

JSON Lines with sorted keys, LF line endings, and floats rounded to 6 digits. Format 2 (since #259):

```json
{"type": "run", "format": 2, "strategy": "zscore_dre", "config": {...}, "source": "...", "baseline_mode": "schedule", "baseline_builds": [3, 4], "log_from_ms": 1790290800000}
{"type": "baseline", "build_id": 3, "built_at": "2026-10-01T06:12:40", "from_ms": 1790283600000, "look_ahead": false, "sha256": "..."}
{"type": "decision", "seq": 1, "timestamp": 1790287133, "venue": "skinport", "listing_id": "60823173", "event_type": "listed", "market_hash_name": "...", "price_cents": 412, "approve": false, "reason": "below_threshold", "score": -0.84, "features": {"mean_cents": 430.5, "window_size": 20, "z_source": "local"}}
{"type": "summary", "events": 355, "feed_events": 30, "ticks": 558, "outcome_ticks": 92, "duplicates": 137, "unchanged_snapshots": 268, "decisions": 61, "logged": 149, "approved": 14}
```

A replay with a fixed baseline file writes `"baseline_mode": "fixed"` with `baseline_sha256` and
`baseline_as_of` in the header instead, and no `baseline` rows.

Every listing-level tick is logged, so the log joins to `listing_outcomes` on (`venue` = `source`,
`listing_id`). REST snapshots are logged only when approved; live can trade on them too.

`zscore_dre` reasons: `insufficient_history`, `below_threshold`, `dre_rejected`, and on approval the DRE
rule that approved (`support_floor`, `macro_sigma`, `stickers_below_base`, `sticker_premium`). An
approval's `features.estimated_net_profit_cents` is the fee-aware estimate the live paper trade records.

## Reading the results

**Check which baselines were used.** The `baseline` rows show every build that took effect and when.
A `look_ahead` switch means the range starts before the first build, so the decisions before that build
used later information. Keep that part out of any result that matters. Ranges before 2026-09-26 have no
build at all.

**Most listings are never scored.** A `listed` tick at the same price as the item's previous tick within
300 seconds is dropped by the live dedup rule before the Z-score runs. On the committed fixture that is 112
of the 141 listing ticks, almost all of them (107) repeating an earlier listing of the same item at
the same price, the rest repeating a REST snapshot. Such rows carry reason `duplicate` and no score: the
rules never looked at them, so count them as "not evaluated", not as rejections, when you measure
precision or coverage against `listing_outcomes`. The summary's `duplicates` count (which also includes
REST snapshots) shows the scale per run.

**An unchanged REST snapshot is not scored again.** A snapshot at the same price as the item's previous
REST snapshot is not scored, however old that one is (#265). Polls are 305 seconds apart plus the time a
poll takes, so the 300 second rule never dropped one, and until #265 every poll scored the same lowest ask
again and could paper trade it again. The price still enters the window as before, so every other decision
is the same as it was. The summary counts these as `unchanged_snapshots` and they are not logged. The
header's `dedup_rule` names the rule a run used.

**Dedup state and price windows are kept per venue.** Since #270 the header's `state_key` is
`venue_and_item`: each venue has its own dedup state and its own price window per item, and the dedup
cache holds up to `dedup_cache_max_size_per_venue` items per venue. With Skinport as the only venue this
makes the same decisions as before; it matters once a second venue feeds the listener.

## Strategies

A strategy implements `decide(tick, context) -> Decision(approve, reason, score, features)` and
`config()` (see `backtest/strategy.py`). The harness owns routing, deduplication, and the price window,
so several strategies see identical inputs, as shadow mode (#236) needs. `context.store` exposes the
window, baselines, and sticker prices as they were at the tick. Register a new strategy in `STRATEGIES`.

`zscore_dre_sweep` makes exactly the decisions `zscore_dre` makes. It also writes each scored tick's
sticker count, and for a tick below the Z threshold it writes the verdict the DRE would have given
(`dre_reason`, null for a rejection). The DRE never reads the Z threshold or the savings floor, so with
those two fields any other threshold can be scored from the same log (see the threshold sweep below).

## Baseline scorecard

The scorecard (#249) is the number every later change has to beat: how the current rules would have done,
scored against what the market actually did. One command replays a range with `zscore_dre_sweep` and scores it:

```bash
make scorecard START=2026-09-26T17:30 END=2026-10-10T00:00
```

That is the same as these two steps, which I can also run apart (for example to score one log against a
new label version):

```bash
# From apps/listener
uv run python -m backtest run --start 2026-09-26T17:30 --end 2026-10-10T00:00 --warmup-hours 2 \
  --strategy zscore_dre_sweep --out ../../data/scorecard/decisions.jsonl

# From apps/analytics (run label_outcomes.py over the range first)
uv run python baseline_scorecard.py --decisions ../../data/scorecard/decisions.jsonl
```

It writes `docs/benchmarks/baseline_scorecard.md` and `docs/benchmarks/baseline_scorecard.json` and logs
both to the MLflow experiment `baseline-scorecard`, with the headline numbers as metrics and the thresholds,
label version and fees as parameters, so model runs (#234) can be compared with it. `--no-mlflow` skips
MLflow, `--as-of` fixes the time labels are read at (the default is now), `--label-version` picks another
label version, and `--slice-days` sets the length of the time slices (7 by default).

**What it counts as a trade.** Every approval in the range. A feed listing counts once however often it
was approved, because it can only be bought once. Every approved REST snapshot counts, as the live
listener paper trades each one. I leave out two kinds of approval and report how many:

- Anything before 2026-09-26 17:30 UTC. REST snapshots were trade locked asks until #275 went live at 15:47
  that day, and the price windows held them for another 20 polls.
- Anything decided while a `look_ahead` baseline was in effect, since that used a later build.

**Labels.** A feed listing gets its `listing_outcomes` label. A REST snapshot has no listing ID, so the
scorecard labels it with the same v1 rule, treating the lowest ask as a listing seen at decision time.
Only its own sale cannot be left out of the comparable sales. A trade is pending until its label horizon
(14 days and the hour of settle time) has passed, and unlabeled when the horizon has passed but no label
exists yet; the report then says to run `label_outcomes.py` over the range. A neutral label (fewer than 3
comparable sales) counts as a trade but not towards precision or P&L.

**Metrics.** Precision is the share of labeled trades that were profitable. Net P&L is each trade's
`resale_net_margin_cents`: the fee aware margin from reselling at the median comparable sale after the
trade hold. Max drawdown is the largest fall of cumulative P&L from its peak, in decision order. Recall is
the share of listings labeled profitable in the range that the rules approved, so it covers feed listings
only; listings the dedup rule never scored count as missed. The report gives these for the whole range,
for each time slice, and broken down by tick kind, DRE rule, Z-score source, buy price, item type and
liquidity (30 day sales per day from the baseline build the decision used).

**Small samples.** Below 30 labeled trades (or 30 profitable listings, for recall) a cell says
"insufficient data" instead of a number. Precision and recall carry Wilson 95% intervals. The mean P&L per
trade carries a percentile bootstrap interval (2000 resamples, seed 249) up to 500 trades, and a normal
interval above that, so a rerun with the same log and `--as-of` gives the same report.

**Threshold sweep.** The report also scores a grid of Z thresholds (-1.5 to -3.0) and savings floors ($0.25
to $2.00), with the sticker threshold kept at its live value. It covers feed listings only, because a
replay logs a REST snapshot only when the live rules approve it. It is in sample: the grid is read from the
same data it is scored on, so its best cell flatters itself. The cell with the live thresholds must
approve exactly what the replay approved; if not, the report warns not to trust the table. The scorecard
never changes a threshold. A change it suggests becomes its own issue.

**When it means something.** Labels arrive 14 days and an hour after a listing, so the first listings after
the #275 cutover are labeled from 2026-10-10 18:30 UTC. Until a range has 30 labeled trades, every
metric says insufficient data.

## Parity and tests

`tests/test_backtest.py` drives the live `main.tick_consumer` and the harness over the same streams,
the recorded fixture and a synthetic stream that reaches every DRE rule. It asserts that the same ticks
reach the DRE and that the same trades execute, with the same Z-scores and profit estimates. Two runs
must also produce byte-identical logs.

The latency hook (`on_timing`, or `--timings`) measures the window update plus the decision for each
tick, for #175.
