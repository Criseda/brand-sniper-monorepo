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
- Active execution roadmap and architectural blueprint for Milestone 5 (`docs/roadmap_proven_edge.md`).
- Project Changelog tracking versioned releases and unreleased PR deliveries (`CHANGELOG.md`).
- Raw Skinport feed capture (all event types, append-only `feed_events` JSONB) and listing-level tick fields (listing ID, event type, float, pattern, stickers, link) end to end, with feed-schema and retention notes in `docs/skinport_feed.md` (#232).
- Deterministic replay & backtest harness (`apps/listener/backtest`, `python -m backtest run|export`): replays recorded feed events and REST snapshots through the live decision code, writes byte-identical decision logs, ships a sanitized CI fixture and a live-parity test; see `docs/backtesting.md` (#248).
- `docs/data_sources.md` describes the data we have, why the Kaggle Steam baselines cannot be used as live prices, the Skinport data, the edge PC not running all the time, and the edge running without baselines since July 2026.
- `docs/skinport_feed.md` records why REST lowest asks sat below every recent sale: `/v1/items` with `tradable=0` returns only trade locked listings, so every stored REST snapshot is a trade locked price (fix in #275). It also records that a feed `sold` can be relisted with the same Steam asset, and how the rate limit behaves after a 429 (#267).
- Baseline scorecard (`apps/analytics/baseline_scorecard.py`, `make scorecard START=... END=...`): scores replayed approvals against outcome labels (precision and recall with Wilson intervals, fee aware net P&L with a seeded bootstrap, max drawdown, time slices, breakdowns by tick kind, DRE rule, Z-score source, price, item type and liquidity, an in sample threshold sweep), says "insufficient data" below 30 labeled trades, leaves out decisions before tradable REST prices, writes `docs/benchmarks/baseline_scorecard.{md,json}` and logs an MLflow run; the `zscore_dre_sweep` replay strategy records what the sweep needs (#249).
- Fee-aware P&L function (`shared_utils.pnl`) and a versioned market-outcome labeler (`listing_outcomes` table, `label_outcomes.py` Prefect flow) with censoring and a look-ahead guard (#233).

- Current baselines per venue (#259): `build_baselines.py` builds Skinport baselines from its sales history into dated `baseline_builds` / `venue_baselines` (always on `baseline-builder` service that catches up after downtime), the backend serves the newest build (`GET /api/v1/baselines/{venue}/latest`), the listener loads it at startup into per venue Redis hashes with metrics and a 503 health state while baselines are missing or stale, and replays switch builds as replay time passes them (decision log format 2).

#### Changed
- The listener keeps dedup state and price windows per venue and item (`price_window:<venue>:<item>`; the old `market:ticks:<item>` windows are renamed to Skinport keys when it starts), caps the dedup cache per venue and counts evictions (`listener_dedup_cache_evictions_total`), every tick states its kind (`TickKind`) instead of a missing event type meaning a REST snapshot, and the replay log header records `state_key` (#270).
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
- The REST poller asked `/v1/items` for `tradable=0`, which returns only trade locked listings, so every REST snapshot since 2026-06-24 was a trade locked lowest ask. It now polls `tradable=1` without credentials (the authenticated requests were rate limited for over an hour while anonymous ones went through), and the docs mark the cutover (#275).
- The DRE approvals panel from #266 drew one hard to read line per label combination, including an unnamed one from the unlabelled counter; it is now a bar gauge of approvals in the selected time range, labelled by rule, Z-score source and tick kind (#266).
- A REST snapshot whose lowest ask had not changed was scored and paper traded again on every poll, because polls are further apart than the 300 s dedup window. It still enters the price window but is no longer scored, every snapshot is recorded, and the paper executor buys each listing (or REST item and price) once (#265).
- The #259 migration branched from an older revision and left Alembic with two heads; it now revises the current head, and a test fails whenever migrations have more than one head.
- The DRE sticker premium rule never matched a sticker price: prices were keyed `Sticker | <name>` while listings name the applied sticker `<name>`. Sticker prices now use the listing's naming (#259).
- The listener container gets a 120 s `stop_grace_period` in both compose stacks, so Docker no longer kills its shutdown drain after 10 s and drops buffered feed events (#252).

#### In Progress / Planned
- `[PE-04]` Baseline scorecard for the current Z-score DRE (#249).
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
