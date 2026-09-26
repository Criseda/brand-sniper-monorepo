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
- Data-source reference (`docs/data_sources.md`): the Kaggle Steam dataset and why its pre-crash baselines are not a live reference price, the Skinport data on hand, the not-always-on edge, and the July 2026 missing-baselines incident.
- Fee-aware P&L function (`shared_utils.pnl`) and a versioned market-outcome labeler (`listing_outcomes` table, `label_outcomes.py` Prefect flow) with censoring and a look-ahead guard (#233).

#### Changed
- The listener's decision path (dedup, price window, Z-score scoring, DRE hand-off) moved from `main.py` to `detection.py` unchanged, with direct tests; the DRE reports which rule approved (`dre_approval_reason`), and edge baseline documents are built by one shared function (`edge_baseline_payload`) (#248).
- The listener's paper-trade profit estimate deducts the venue's seller fee through the shared P&L function; trades record the estimate's basis (`profit_estimate_basis`, existing rows tagged `gross`) and store no estimate when there is no baseline price (#233).
- Fee schedules are looked up per venue (`fees_for`) and `MarketTick` carries its `venue`; an unregistered venue fails instead of being priced with Skinport fees (#233).
- Paper trades record the bought listing (`listing_id`, `float_value`), and the CFO audits that listing's float instead of the latest tick for the item (#232).
- Fixed the net-margin formula in `docs/roadmap_proven_edge.md` failing to render on GitHub (`_` inside `\text{}`).
- Re-planned Milestone 5 around outcome-labeled learning (Proven Edge); replaced `docs/roadmap_edge_distillation.md` and parked the Rust edge engine (#237-#239) pending a benchmark-driven decision.
- Bump pandas 3.0.5 to 3.0.6, prefect 3.8.5 to 3.8.6, mlflow 3.16.0 to 3.16.1, ruff 0.16.7 to 0.16.8, coverage 7.16.0 to 7.16.1, prefect docker image to 3.8.7.dev4-python3.12 (restores #241-#246).

#### Fixed
- The listener container gets a 120 s `stop_grace_period` in both compose stacks, so Docker no longer kills its shutdown drain after 10 s and drops buffered feed events (#252).

#### In Progress / Planned
- `[PE-04]` Baseline scorecard for the current Z-score DRE (#249).
- `[PE-05]` Ingress-to-decision latency benchmark and Rust decision gate (#175).
- `[PE-06]` Shared feature module & walk-forward student model with ONNX export (#234).
- `[PE-07]` Shadow-mode in-process ONNX scoring with promotion gate (#236).
- `[PE-08]` CFO calibration experiment against realized outcomes (#250).
- `[PE-09]` Realized-performance & feature drift monitoring (#235).
- `[PE-10]` Discord/Telegram alerts with direct listing links (#18).
- `[PE-11]` CSFloat & Steam multi-venue scrapers (#33).

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
