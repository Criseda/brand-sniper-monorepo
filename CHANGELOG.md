# Changelog

All notable changes to the Brand Sniper monorepo will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Agent & Contributor Convention**:
> When submitting a pull request that implements or resolves an issue, add a concise one-line summary under the `[Unreleased]` section below, categorized appropriately (`Added`, `Changed`, `Performance`, `Fixed`, etc.), referencing the issue number (e.g., `- Added listing-level tick persistence (#232)`).

---

## [Unreleased]

### Active Milestone: [Proven Edge: Outcome-Labeled Learning Loop (Milestone 5)](https://github.com/Criseda/brand-sniper-monorepo/milestone/5)

#### Added
- DMarket groundwork (#339, part 1): `docs/dmarket_api.md` from a live probe, a signed and rate limited DMarket client (`shared_utils.dmarket`), `DMARKET_FEES` with a per item seller fee in `shared_utils.pnl`, the `venue_sales` table and `venue_baselines.seller_fee_bps` (migration `c3e9a1d7b5f2`), and DMarket baselines built from DMarket's own listing sales (`build_baselines.py --venue dmarket`, resumable, with each item's fee), run by the `baseline-builder-dmarket` service.
- Student feature `price_to_latest`, the price over the baseline's latest price (feature version 3), and a student report section that scores the model on the DRE's deals before the fee floor: ROC-AUC, and net P&L when it keeps as many as the fee floor does, next to the fee floor and the latest price ratio (#333).
- CFO calibration experiment: point in time CFO tools, adversarial and neutral prompts with a probability of profit, scored against v1 labels with Brier score, reliability and an item bootstrap against the DRE alone, and a `claude-code` backend that asks Claude through Claude Code on a Claude plan with no API key (`apps/analytics/cfo_calibration.py`, `make cfo-calibration CFO_BACKEND=claude-code`, `docs/benchmarks/cfo_calibration.md`) (#250).
- Active execution roadmap and architectural blueprint for Milestone 5 (`docs/roadmap_proven_edge.md`).
- Project Changelog tracking versioned releases and unreleased PR deliveries (`CHANGELOG.md`).
- Raw Skinport feed capture (all event types, append-only `feed_events` JSONB) and listing-level tick fields (listing ID, event type, float, pattern, stickers, link) end to end, with feed-schema and retention notes in `docs/skinport_feed.md` (#232).
- Deterministic replay & backtest harness (`apps/listener/backtest`, `python -m backtest run|export`): replays recorded feed events and REST snapshots through the live decision code, writes byte-identical decision logs, ships a sanitized CI fixture and a live-parity test; see `docs/backtesting.md` (#248).
- `docs/data_sources.md` describes the data we have, why the Kaggle Steam baselines cannot be used as live prices, the Skinport data, the edge PC not running all the time, and the edge running without baselines since July 2026.
- `docs/skinport_feed.md` records why REST lowest asks sat below every recent sale: `/v1/items` with `tradable=0` returns only trade locked listings, so every stored REST snapshot is a trade locked price (fix in #275). It also records that a feed `sold` can be relisted with the same Steam asset, and how the rate limit behaves after a 429 (#267).
- Baseline scorecard (`apps/analytics/baseline_scorecard.py`, `make scorecard START=... END=...`): scores replayed approvals against outcome labels (precision and recall with Wilson intervals, fee aware net P&L with a seeded bootstrap, max drawdown, time slices, breakdowns by tick kind, DRE rule, Z-score source, price, item type and liquidity, an in sample threshold sweep), says "insufficient data" below 30 labeled trades, leaves out decisions before tradable REST prices, writes `docs/benchmarks/baseline_scorecard.{md,json}` and logs an MLflow run; the `zscore_dre_sweep` replay strategy records what the sweep needs (#249).
- First baseline scorecard (`docs/benchmarks/baseline_scorecard.{md,json}`): Skinport decisions from 2026-09-26 17:30 to 2026-10-13 01:27 UTC, 202 labeled trades, precision 48.5% and a net loss of $1.44 per trade (#249).
- Waxpeer venue (#261): a second listener (`listener-waxpeer`) reads Waxpeer's public Socket.IO feed directly, records and scores only listings priced below the item's Skinport price (plus their later price cuts and removal), judges them against the Skinport baselines with buy on Waxpeer, resell on Skinport P&L, stores raw payloads grouped per 500 events or 10 seconds, and registers `WAXPEER_FEES` (6% seller fee); paper trades record their `venue`; see `docs/waxpeer_feed.md`.
- Fee-aware P&L function (`shared_utils.pnl`) and a versioned market-outcome labeler (`listing_outcomes` table, `label_outcomes.py` Prefect flow) with censoring and a look-ahead guard (#233).

- Current baselines per venue (#259): `build_baselines.py` builds Skinport baselines from its sales history into dated `baseline_builds` / `venue_baselines` (always on `baseline-builder` service that catches up after downtime), the backend serves the newest build (`GET /api/v1/baselines/{venue}/latest`), the listener loads it at startup into per venue Redis hashes with metrics and a 503 health state while baselines are missing or stale, and replays switch builds as replay time passes them (decision log format 2).
- Label versions are a contract (#234): `label_versions` stores each version's config, the labeler refuses to write under a version stored with other parameters, labels are final 3 days after they first become labelable and never rewritten after that, `fetch_final_labels` reads them reproducibly for training with the config for MLflow, and migration `7c4e2a9f1b38` registers `v1` after checking every row against it and adds the (`label_version`, `label_available_at`) index.
- Student model (#234): shared feature module (`shared_utils.features`, feature version 1, version 2 since #312) gathered by the listener's `student_features.py` and logged by the new `zscore_dre_features` replay strategy, with a live, replay and training parity test; `train_student.py` (CLI, Prefect flow, `make student`) trains LightGBM with walk forward folds on final labels, calibrates it, exports and checks an ONNX model, compares it with the live rules using the scorecard metrics, and registers it in MLflow only when the size rule holds; the edge baseline document carries `avg_volume_30d`.
- Discord and Telegram trade alerts for every DRE approval, with the item details, fee aware net margin, decision source and a link to the listing (Waxpeer listings now carry `listing_url`); sent from bounded per channel queues with deduplication, rate limits and 429 handling, latency measured, and every attempt recorded in a new `trade_alerts` table (#18).
- Ingress to decision latency benchmark, part 1 (#175): `python -m latency_bench` (`make latency-bench`, `deployments/latency-bench`) plays seeded scenarios from a de-identified fixture of recorded sales through a local Socket.IO stand-in for Skinport into the real sidecar, Redis Pub/Sub, `tick_consumer`, price window, Z score and DRE, times every stage on one monotonic clock, and reports latency, throughput, drops, CPU, memory and loop lag; the sidecar takes `SKINPORT_WS_URL` and an opt-in timing file; a single listing is decided in 1.3 ms at the median and 2.7 ms at p99, about 1,500 a second at most, which meets the proposed budget; results in `docs/benchmarks/ingress_latency.md`, and the unverified `<5ms` claim in `docs/architecture.md` is replaced.
- README speed chart (#175): decision latency of the Python ingress from the benchmark next to a blink, a human reaction and how fast Skinport listings sell, on a log scale, with its sources; drawn by `scripts/ingress_speed_chart.py` in light and dark.

#### Changed
- Retired the Kaggle Steam history: migration `a7d4e2c9f1b3` drops `historical_prices` (about 10 GiB), the market context no longer falls back on its Steam average (`historical_steam_avg_cents` and `regime_shift_detected` are gone), and the seed, validation and `long_term_macro.py` scripts and the unused `get_sticker_price_cents` are removed. `item_macro_baselines` stays as frozen CFO context; `docs/data_sources.md` explains how to restore the history.
- The CSFloat venue (#33) is not planned because CSFloat's Terms of Service forbid automated collection of listings; `AGENTS.md` and the roadmap list the DMarket venue (#339) in its place.
- CI skips the `quality` and `test` jobs on pull requests that only change docs (Markdown files, `docs/`, `LICENSE`); the skipped jobs still satisfy the required status checks.
- The outcome labeler reads normalized listing rows of `live_market_ticks` through a new `observed_at` column (edge receive time to the millisecond, sent by the listeners, backfilled for Skinport) instead of parsing raw `feed_events` JSON, takes the venue as a parameter, reads sold events once per run, and is split into `outcome_labels.py` (pure), `outcome_data.py` and the flow; `v1` labels are unchanged (#256).
- The compiled Rust edge engine (#237, #238, #239) is not planned after the #175 benchmark; `AGENTS.md` and the roadmap no longer list it as parked, and #238 comes back only if #236 shows in-process ONNX scoring breaks the latency budget.
- The Skinport listener reads the sale feed itself (#175): `scrapers/skinport_socket.py` speaks Engine.IO v4 with MessagePack Socket.IO packets over an aiohttp websocket, so the Node.js sidecar, Redis Pub/Sub relay and Node.js in the listener image are gone; the feed reconnects after 1 s, doubling to 30 s while it keeps failing (`LISTENER_FEED_RECONNECT_FIRST_DELAY_SECONDS`, `LISTENER_FEED_RECONNECT_MAX_DELAY_SECONDS`), `listener_feed_connected` and `listener_feed_reconnects_total` now cover Skinport too, and the latency benchmark measures this path; results and the decision in `docs/benchmarks/ingress_latency.md`.
- CFO response token cap raised from 2048 to 8192 and configurable via `LLM_MAX_TOKENS`, with an optional `LLM_REASONING_EFFORT` passed to reasoning models.
- A REST lowest ask that has not changed since the last poll enters the price window only while the window holds fewer than `MIN_HISTORY_POINTS` prices or once an hour (`shared_utils.price_window`, replay header `window_rule`, counter `listener_snapshots_unchanged_windowed_total`), instead of on every poll; in sample it loses $1.00 per trade instead of $1.44, see `docs/benchmarks/price_window.md` (#269).
- A macro Z score (a price window shorter than `MIN_HISTORY_POINTS`, as on the first poll after a restart) no longer triggers the DRE: the replay logs it as `macro_only`, and `MACRO_ZSCORE_TRIGGERS=true` restores the old rule. Replayed with restarts, in sample, it loses $0.40 per trade instead of $1.33, see `docs/benchmarks/cold_start.md` (#308).
- The DRE no longer approves a deal whose fee aware estimate is zero or less at the baseline's latest price (support floor and macro sigma rules; the sticker rules are left alone). The replay logs it as `below_fee_floor`, `DRE_FEE_FLOOR=false` turns it off. Replayed over the labeled range it removes 100 labeled trades, 17% profitable, and saves $624 ($335 to $975), so the DRE loses $0.38 per trade instead of $1.74, see `docs/benchmarks/fee_floor.md` (#331).
- Baseline scorecard counts each purchase once, as live trading buys it since #271: a listing once, a REST item and price once per venue and UTC day (none on a day a listing of it was bought at that price), from the shared `shared_utils.purchase_keys` rule the live executor now uses; the report counts the repeat approvals (#249).
- Only a listener that reads a feed directly exports the `listener_feed_*` metrics, so the Skinport listener no longer reports a disconnected feed (#261).
- The Skinport listener's batch streams are `listener:ingest:skinport:*` like every venue's; it moves entries left under the pre-#261 names when it starts, and so does `replay_batches.py --venue skinport` (#261).
- Every venue column is named `venue` (`live_market_ticks.marketplace_source` and the `source` columns of `feed_events`, `listing_outcomes` and `ingestion_batches`, migration `b1e6f0a8c4d3`); the bulk ingest payload and stored batches say `venue` (the backend still accepts `source`, and the idempotency digest is unchanged); the backtest takes `--venue` (#261).
- One listener service per venue, named `listener-<venue>` (`listener-skinport`, container `sniper_listener_skinport`, was `listener`); `LISTENER_VENUE` replaces `LISTENER_PLATFORM`; Prometheus labels each listener's series `listener=<venue>` and the dashboard and alerts split by it; the direct feed metrics are `listener_feed_*` (#261).
- The listener keeps dedup state and price windows per venue and item (`price_window:<venue>:<item>`; the old `market:ticks:<item>` windows are renamed to Skinport keys when it starts), caps the dedup cache per venue and counts evictions (`listener_dedup_cache_evictions_total`), every tick states its kind (`TickKind`) instead of a missing event type meaning a REST snapshot, and the replay log header records `state_key` (#270).
- `net_resale_margin_cents` takes the buy venue's fees and the sell venue's fees separately (same venue by default, identical results for Skinport), `VenueFees` requires a buyer fee (`buyer_fee_bps`, 0 for Skinport, rounded up like the seller fee), a cross venue trade is held for the sell venue's trade hold (`holding_seconds`), and the scorecard records the buyer fee (#260).
- The Grafana "DRE Confirmation Rate" stat is replaced by approvals per hour by DRE rule, Z-score source and tick kind (REST snapshot or listing); the anomaly counters carry those labels, the `[ANOMALY] Confirmed` log names the rule, and `LowConfirmationRate` becomes `DRERejectsAlmostEverything`, a baseline health check over an hour with at least 20 flags (#266).
- The live DRE and profit estimate no longer read the Kaggle based baselines; `update_baselines.py` and the edge sync in `long_term_macro.py` are removed (#259).
- The listener's decision path (dedup, price window, Z-score scoring, DRE hand-off) moved from `main.py` to `detection.py` unchanged, with direct tests; the DRE reports which rule approved (`dre_approval_reason`), and edge baseline documents are built by one shared function (`edge_baseline_payload`) (#248).
- The listener's paper-trade profit estimate deducts the venue's seller fee through the shared P&L function; trades record the estimate's basis (`profit_estimate_basis`, existing rows tagged `gross`) and store no estimate when there is no baseline price (#233).
- Fee schedules are looked up per venue (`fees_for`) and `MarketTick` carries its `venue`; an unregistered venue fails instead of being priced with Skinport fees (#233).
- Paper trades record the bought listing (`listing_id`, `float_value`), and the CFO audits that listing's float instead of the latest tick for the item (#232).
- Fixed the net-margin formula in `docs/roadmap_proven_edge.md` failing to render on GitHub (`_` inside `\text{}`).
- Re-planned Milestone 5 around outcome-labeled learning (Proven Edge); replaced `docs/roadmap_edge_distillation.md` and parked the Rust edge engine (#237-#239) pending a benchmark-driven decision.
- Bump pandas 3.0.5 to 3.0.6, prefect 3.8.5 to 3.8.6, mlflow 3.16.0 to 3.16.1, ruff 0.16.7 to 0.16.8, coverage 7.16.0 to 7.16.1, prefect docker image to 3.8.7.dev4-python3.12 (restores #241-#246).

#### Fixed
- A hard kill of the listener loses at most 10 seconds of the feed (#252): a batch is also written to the edge Redis stream once its oldest record has waited `LISTENER_FLUSH_INTERVAL_SECONDS` (default 10), instead of only at `CHUNK_LIMIT` (about once per REST poll) or on shutdown; a test checks that every listener's compose `stop_grace_period` covers the shutdown drain.
- A Skinport listener that fell far behind lost sale feed events without notice when Redis closed its Pub/Sub subscription (#326): the feed no longer goes through Pub/Sub, and a lost connection is logged and counted.
- The student model picked a low operating threshold from a few hundred calibration rows; it now fits on every training row and calibrates and picks the threshold on inner walk forward scores of about 80% of them, comparison in `docs/benchmarks/student_threshold.md` (#234).
- Two REST snapshot and listing buys of one item at one price on one venue can no longer both be recorded when they arrive at the same moment: the backend serializes them with a transaction lock (#271).
- The dedup rule no longer drops a feed listing because another listing of the item had the same price; a listing is a duplicate only when the same listing is seen again at the same price, so live and replay score every listing once. Student features move to version 2, since `listings_per_hour_item` now counts those listings (#312).
- sqlmodel 0.0.47 maps a plain `datetime` to `TIMESTAMPTZ` and rejects naive values; the model timestamp fields are now `NaiveDatetime`, so they keep the existing `TIMESTAMP` columns holding naive UTC.
- `uv audit` failed the CI `quality` job on three vulnerable locked dependencies; mako is now 1.4.3, multidict 6.9.1 and werkzeug 3.1.9.
- `uv audit` failed the CI `quality` job on new advisories for certifi, pillow and virtualenv; certifi is now 2026.10.19, pillow 13.0.0 and virtualenv 21.14.12.
- Prometheus stayed down after a Docker engine restart while every other server stack container came back; its config is now mounted as a folder (`config/prometheus/`) instead of two single Windows files, which Docker Desktop can fail to remount on restart.
- Skinport locked the REST poll out for about an hour after listener restarts, because a restarted listener polled at once, however recently the previous one had; the listener now keeps the time of its next `/v1/items` request in the edge Redis and waits for it after a restart, obeys `Retry-After` on a 429 instead of guessing, and skips a malformed item instead of dropping the rest of the poll (#282).
- The dedup cache held 25,000 items, fewer than the about 25,300 a tradable Skinport poll returns, so items were evicted every poll and their unchanged lowest asks were scored again; the default is now 100,000 per venue, and the Grafana gauge leaves out the cache size series from before #270 and measures against the 100,000 cap, instead of its own peak, which kept it red (#270).
- The REST poller asked `/v1/items` for `tradable=0`, which returns only trade locked listings, so every REST snapshot since 2026-06-24 was a trade locked lowest ask. It now polls `tradable=1` without credentials (the authenticated requests were rate limited for over an hour while anonymous ones went through), and the docs mark the cutover (#275).
- The DRE approvals panel from #266 drew one hard to read line per label combination, including an unnamed one from the unlabelled counter; it is now a bar gauge of approvals in the selected time range, labelled by rule, Z-score source and tick kind (#266).
- A REST snapshot whose lowest ask had not changed was scored and paper traded again on every poll, because polls are further apart than the 300 s dedup window. It still enters the price window but is no longer scored, every snapshot is recorded, and the paper executor buys each listing (or REST item and price) once (#265).
- The #259 migration branched from an older revision and left Alembic with two heads; it now revises the current head, and a test fails whenever migrations have more than one head.
- The DRE sticker premium rule never matched a sticker price: prices were keyed `Sticker | <name>` while listings name the applied sticker `<name>`. Sticker prices now use the listing's naming (#259).
- The listener container gets a 120 s `stop_grace_period` in both compose stacks, so Docker no longer kills its shutdown drain after 10 s and drops buffered feed events (#252).
- A listing is paper traded once across listener restarts and for any executor: unique indexes on `simulated_trades` (one buy per venue and listing, one REST snapshot buy per venue, item and price per UTC day), the trade endpoint answers a repeat `200 ALREADY_RECORDED`, the in memory check moved into `ExecutionService`, and migration `d064786e47a8` removes repeated buys, keeping the first (#271).

#### In Progress / Planned
- `[PE-05]` Ingress-to-decision latency benchmark and Rust decision gate (#175).
- `[PE-06]` Shared feature module & walk-forward student model with ONNX export (#234).
- `[PE-07]` Shadow-mode in-process ONNX scoring with promotion gate (#236).
- `[PE-08]` CFO calibration experiment against realized outcomes (#250).
- `[PE-09]` Realized-performance & feature drift monitoring (#235).
- `[PE-10]` Discord/Telegram alerts with direct listing links (#18).
- `[PE-11]` CSFloat venue (#33); Steam dropped as a venue (reference price only).
- `[PE-13]` P&L for buying and selling on different venues (#260).
- `[PE-14]` Waxpeer venue (#261).

---

## [1.0.0] - 2026-08-01

### Added
- Hybrid Cloud-Edge architecture separating Edge Node (Listener + DRE hot path) from Server Node (FastAPI compute + PostgreSQL + Analytics).
- Skinport live listing telemetry via Node.js WebSocket sidecar (`sidecar.js`) relaying over Redis Pub/Sub (`skinport:live_listings`).
- Edge Redis (port 6380) sliding-window sorted set for rolling price calculations.
- Bayesian shrinkage 1D Z-score anomaly detector regularized against 30-day macro baseline averages.
- Deterministic Rules Engine (DRE) evaluating hard floors, 2-sigma macro floors, and sticker premium percentages.
- Asynchronous Adversarial CFO agent in `apps/analytics` querying live floor, float wear, and macro news via Groq / OpenAI tool-calling.
- Full MLflow experiment tracking for CFO audit verdicts and reasoning traces.
- Long-term macro baseline calculations computed via Prefect and synced to Edge Redis.
- Prometheus metrics endpoints on listener (`:9100`) and backend (`:8080/metrics`), scraped by Grafana dashboards.
- Idempotent bulk ingestion pipeline with SHA256 batch ledger.
- Full test suite with SQLite in-memory FastAPI testing, mocked MLflow/LLM tests, and >=70% test coverage enforcement.
