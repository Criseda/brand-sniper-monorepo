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
| `--strategy` | Decision strategy: `zscore_dre` (default, the live rules), `zscore_dre_sweep` (the same decisions plus the inputs the scorecard's threshold sweep needs) or `zscore_dre_features` (the sweep log plus the student model's features of every feed listing, #234). |
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
macro baseline alone, and since #308 a macro Z score never approves. On the committed fixture a cold run
used to approve 14 ticks, all of them on the macro Z score; it now approves none, as does a run after 30
minutes of warm-up, which is what the live listener did over that period (no paper trades).

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
2. A feed listing already seen at the same price, or a REST snapshot at the same price as the item's previous
   tick on the same venue within 300 seconds, is a duplicate: logged with reason `duplicate`, never scored.
3. A REST snapshot at the same price as the item's previous REST snapshot is not scored. It enters the price
   window only while the window holds fewer than `MIN_HISTORY_POINTS` prices or once an hour (see below).
4. Any other tick enters the price window, and the strategy decides on it.

## Decision log

JSON Lines with sorted keys, LF line endings, and floats rounded to 6 digits. Format 2 (since #259):

```json
{"type": "run", "format": 2, "strategy": "zscore_dre", "config": {...}, "source": "...", "baseline_mode": "schedule", "baseline_builds": [3, 4], "log_from_ms": 1790290800000}
{"type": "baseline", "build_id": 3, "built_at": "2026-10-01T06:12:40", "from_ms": 1790283600000, "look_ahead": false, "sha256": "..."}
{"type": "decision", "seq": 1, "timestamp": 1790287133, "venue": "skinport", "listing_id": "60823173", "event_type": "listed", "market_hash_name": "...", "price_cents": 412, "approve": false, "reason": "below_threshold", "score": -0.84, "features": {"mean_cents": 430.5, "window_size": 20, "z_source": "local"}}
{"type": "summary", "events": 355, "feed_events": 30, "ticks": 558, "outcome_ticks": 92, "duplicates": 37, "unchanged_snapshots": 263, "unchanged_snapshots_windowed": 44, "decisions": 166, "logged": 141, "approved": 0}
```

A replay with a fixed baseline file writes `"baseline_mode": "fixed"` with `baseline_sha256` and
`baseline_as_of` in the header instead, and no `baseline` rows.

Every listing-level tick is logged, so the log joins to `listing_outcomes` on (`venue`,
`listing_id`). REST snapshots are logged only when approved; live can trade on them too.

`zscore_dre` reasons: `insufficient_history`, `macro_only` (scored with the macro Z score, which never
triggers, see below), `below_threshold`, `dre_rejected`, and on approval the DRE
rule that approved (`support_floor`, `macro_sigma`, `stickers_below_base`, `sticker_premium`). An
approval's `features.estimated_net_profit_cents` is the fee-aware estimate the live paper trade records.

## Reading the results

**Check which baselines were used.** The `baseline` rows show every build that took effect and when.
A `look_ahead` switch means the range starts before the first build, so the decisions before that build
used later information. Keep that part out of any result that matters. Ranges before 2026-09-26 have no
build at all.

**Every listing is scored once.** Since #312 a feed listing is a duplicate only when the same listing was
already seen at the same price. Another listing of the item at the same price is a separate offer, so it is
scored. Before that the 300 second rule dropped it too, and about half of all listings were never scored: on the
committed fixture 112 of the 141 listing ticks, now 7, all of them a listing seen again. A row with reason
`duplicate` has no score: the rules never looked at it, so count it as "not evaluated", not as a rejection, when
you measure precision or coverage against `listing_outcomes`. The summary's `duplicates` count (which also
includes REST snapshots) shows the scale per run, and the header's `dedup_rule` names the rule a run used
(`time_window_and_unchanged_snapshot` before #312). What the old rule dropped, and why I chose this one, is in
[benchmarks/dedup_listings.md](benchmarks/dedup_listings.md).

**An unchanged REST snapshot is not scored again.** A snapshot at the same price as the item's previous
REST snapshot is not scored, however old that one is (#265). Polls are 305 seconds apart plus the time a
poll takes, so the 300 second rule never dropped one, and until #265 every poll scored the same lowest ask
again and could paper trade it again. The summary counts these as `unchanged_snapshots` and they are not
logged.

**What the price window holds.** Since #269 an unchanged REST snapshot enters the price window only while
the window holds fewer than `MIN_HISTORY_POINTS` prices, or when no REST snapshot of the item has entered it
for an hour. Before, every poll entered, so a quiet item's window held 20 copies of one price and its local
variance went to about zero. The rule is `shared_utils.price_window`, which the listener, the replay and the
feature module (#234) all use. The summary counts the unchanged snapshots that entered as
`unchanged_snapshots_windowed`, and the header names the rule in `window_rule` (a log without it is from
before #269, when every poll entered). How I chose it, and what it did to the scorecard, is in
[benchmarks/price_window.md](benchmarks/price_window.md).

A replay keeps every window across gaps in the data, while live the edge Redis is empty after my PC has been
off. Right after a restart the live listener scores more items with the macro Z score than a replay does.

**A macro Z score never approves.** Since #308 a tick scored with the macro Z score (its window holds fewer
than `MIN_HISTORY_POINTS` prices) is logged with reason `macro_only` and its score, and never reaches the
DRE. Those were mostly REST lowest asks scored on the first poll after a restart, and they lost money. The
header's `macro_zscore_triggers` says whether a run let them trigger (`MACRO_ZSCORE_TRIGGERS`, false by
default); a log without it is from before #308, when they did. How I chose it is in
[benchmarks/cold_start.md](benchmarks/cold_start.md).

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

`zscore_dre_features` writes everything `zscore_dre_sweep` writes, and adds `features.student` to every
scored feed listing: the student model's features (`shared_utils.features`), gathered by
`apps/listener/student_features.py` from the price window, baseline and sticker prices the listener had at
that moment. The scorecard reads this log as well, so one replay serves both.

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

**What it counts as a trade.** Every purchase live trading would make from the approvals in the range,
with the once only rule it has used since #271 (`shared_utils.purchase_keys`). A feed listing counts once
however often it was approved, because it can only be bought once. A REST snapshot has no listing ID, so
its item and price count once per venue and UTC day, and not at all on a day a listing of that item was
bought at that price. The report says how many approvals repeated a purchase. I leave out two kinds of
approval and report how many:

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

## Student model

The student model (#234) is trained on the same replay. One command replays a range with
`zscore_dre_features`, then trains and evaluates the model:

```bash
make student START=2026-09-26T17:30 END=2026-10-27T00:00
```

The second step on its own, from apps/analytics:

```bash
uv run python train_student.py --decisions ../../data/student/decisions.jsonl
```

`train_student.py` joins every scored feed listing to its final `listing_outcomes` label
(`fetch_final_labels`, so the same `--as-of` reads the same labels), leaves out neutral labels, and trains
LightGBM with walk forward folds (`--fold-hours`, 24 by default). A fold with cutoff t trains on listings
decided before t whose label was available by t, and validates on the listings of the next window. With a
14 day horizon that puts at least 14 days between the newest training listing and the validation window,
so the first folds have nothing to train on. Inside a fold the model is fitted on every training listing.
The training listings are also cut into five blocks in time, and each block after the first is scored by a
model fitted on the blocks before it. Those out of sample scores, about 80% of the training listings, fit a
Platt scaling of the raw score and pick the operating threshold: the one with the most net P&L on them that
still approves at least 30 of them. `docs/benchmarks/student_threshold.md` explains why I pick it this way.

On the validation listings the live rules, the model at its threshold, and the model at the rules' volume
(as many listings as the rules approved, most probable first) are scored with the scorecard's own
`score_trades`, so precision, net P&L and the 30 trade minimum mean exactly what they mean in the
scorecard. It also reports ROC-AUC, PR-AUC, the Brier score and a calibration table.

A fold, or the final model, needs 1000 labeled listings and 100 of the rarer outcome. The final model is
trained on every listing, exported to ONNX (float32 `features` [n, 11] in, `probability` [n] out, the
feature names and versions as metadata), and checked against the native model. It writes
`docs/benchmarks/student_model.{md,json}` and `student_model.onnx`, logs them to the MLflow experiment
`student-model` with the label config and feature version as parameters, and registers the ONNX model as
`brand-sniper-student` when the run is not exploratory and at least one fold was evaluated. The version's
`candidate` tag is true only when the model beat the rules out of sample (see section 4.6 of the roadmap).

`--exploratory` also reads labels that are not final and lets training labels overlap the validation
window. It is for checking the pipeline on the labels that exist. Its numbers are not an evaluation, and it
never registers a model.

**When it means something.** On 2026-10-13 the labels cover listings from 2026-09-26 17:30 to 09-28 23:26,
one weekly slice with a gap where my PC was off. Of their 6,654 profitable or unprofitable labels only 2,266
belong to a scored first sighting: the dedup rule drops a listing at the same price as the item's previous
tick within 5 minutes, so it gets no features, live or in replay. Those 2,266 are enough to train a fold
(221 profitable), but a strict fold can only validate on listings decided 14 days after them, so from about
2026-10-13. Their labels exist from about 2026-10-27 and are final from about 2026-10-30. The rules approve
100 to 280 feed listings on a day the PC was on, and about a third of those get a decisive label, so one such
day of validation listings gives the 30 labeled trades a comparison needs. The first strict run is therefore
due around 2026-10-31, and it will still rest on one training week and a day or two of validation, which is
not enough to judge the model. Until then the committed report (`docs/benchmarks/student_model.md`) is an
exploratory run.

## Parity and tests

`tests/test_backtest.py` drives the live `main.tick_consumer` and the harness over the same streams,
the recorded fixture and a synthetic stream that reaches every DRE rule. It asserts that the same ticks
reach the DRE and that the same trades execute, with the same Z-scores and profit estimates. Two runs
must also produce byte-identical logs.

The latency hook (`on_timing`, or `--timings`) measures the window update plus the decision for each
tick, for #175.
