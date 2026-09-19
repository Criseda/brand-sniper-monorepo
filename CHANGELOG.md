# Changelog

All notable changes to the Brand Sniper monorepo will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Agent & Contributor Convention**:
> When submitting a pull request that implements or resolves an issue, add a concise one-line summary under the `[Unreleased]` section below, categorized appropriately (`Added`, `Changed`, `Performance`, `Fixed`, etc.), referencing the issue number (e.g., `- Added reservoir sampling for borderline ticks (#232)`).

---

## [Unreleased]

### Active Milestone: [Edge-Inference & Distillation Engine (Milestone 5)](https://github.com/Criseda/brand-sniper-monorepo/milestone/5)

#### Added
- Active execution roadmap and architectural blueprint for Milestone 5 (`docs/roadmap_edge_distillation.md`).
- Project Changelog tracking versioned releases and unreleased PR deliveries (`CHANGELOG.md`).

#### In Progress / Planned
- `[EID-01]` Reservoir sampling in listener for borderline ticks & near-misses (#232).
- `[EID-02]` CFO Oracle automated ground-truth labeling pipeline (#233).
- `[EID-03]` Walk-forward student model training & ONNX export flow (#234).
- `[EID-04]` Wasserstein distance & PSI distribution drift monitor (#235).
- Ingress-to-decision deterministic latency benchmark suite (#175).
- `[EID-05]` In-memory ring buffers & in-process ONNX inference in Python (#236).
- `[EID-06]` Unified compiled Rust edge daemon with SIMD-JSON parsing (#237).
- `[EID-07]` Lock-free in-memory sliding windows & in-process ORT inference (#238).
- `[EID-08]` Zero-downtime model hot-reloading & latency collapse report (#239).
- CSFloat & Steam multi-venue scrapers (#33).
- Discord and Telegram real-time trade notification integration (#18).

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
