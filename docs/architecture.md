# Architecture

## Overview

Brand Sniper is a distributed, hybrid cloud-edge system for algorithmic CS2 market sniping.
It separates real-time anomaly detection (hot path) from AI-powered audit (cold path) across two
topological layers: the **Edge Node** and the **Server Node**.

## Topology

```mermaid
flowchart TD
    classDef edgeNode fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef computeNode fill:#1E293B,stroke:#A78BFA,stroke-width:2px,color:#F8FAFC
    classDef dataStore fill:#334155,stroke:#94A3B8,stroke-width:1px,color:#E2F0F8
    classDef process fill:#0F172A,stroke:#64748B,stroke-width:1px,color:#E2F0F8,shape:rect
    classDef alert fill:#7F1D1D,stroke:#FCA5A5,stroke-width:2px,color:#FEE2E2

    Market[LIVE MARKET DATA STREAM]

    subgraph Edge[EDGE NODE / Hot Path]
        direction TB
        Listener[\"/apps/listener Async Polling\"]
        DRE[\"Deterministic Rules Engine & Executor\"]
        EdgeRedis[(Edge Redis Cache 6380)]

        Listener -->|High-Frequency Ticks| DRE
        DRE <-->|O(1) Baseline Lookup| EdgeRedis
    end
    Edge:::edgeNode

    Market -->|WebSockets / REST| Listener

    subgraph Compute[SERVER NODE / Cold Path]
        direction TB
        Backend[\"/apps/backend FastAPI Core\"]
        Analytics[\"/apps/analytics Prefect & CFO\"]
        Postgres[(PostgreSQL)]
        Prometheus[(Prometheus / Grafana)]
        MLflow[(MLflow Model Registry)]
        Gemini[Google Gemini API]

        Backend -->|Logs simulated trades| Postgres
        Backend -->|Scrapes Metrics| Prometheus
        Analytics -->|Fetches trades to Audit| Postgres
        Analytics <-->|Agentic Reasoning Loop| Gemini
        Analytics -->|Logs Audits| MLflow
        Analytics -->|Syncs baselines to Edge| EdgeRedis
    end
    Compute:::computeNode

    DRE -->|Async POST Trade Logs| Backend
    Listener -->|Async POST Batched Ticks| Backend
```

## Hot Path: Real-Time Anomaly Detection

The **Listener** (`apps/listener`) runs 24/7 inside Docker on the Edge Node.
It ingests real-time market data ticks via:

- **REST polling** — Skinport asset directory stream
- **WebSocket sidecar** — Node.js process (`scrapers/skinport_websocket/`) for push-based Socket.IO feeds

Incoming prices are written concurrently to an edge-local Redis hot-cache (sliding window, port **6380**).
The **Deterministic Rules Engine (DRE)** evaluates every tick using Z-score anomaly detection against
the cached price history. On a confirmed anomaly:

1. The DRE queries Edge Redis in O(1) time for long-term ML baselines
2. The local `PaperExecutor` executes a simulated trade synchronously
3. The trade log is POSTed asynchronously to the backend

**Average hot-path latency: <5ms.** No network call to the server node is required for the trade decision.

## Cold Path: Adversarial CFO Audit

The **Analytics** app (`apps/analytics`) runs offline as a batch job. Orchestrated by **Prefect**,
it evaluates every simulated trade logged to PostgreSQL.

The **Adversarial CFO** is a Google Gemini agent armed with two tool functions:

| Tool | Role |
|------|------|
| `fetch_live_market_floor` | Checks the live floor price of an asset to validate snipe profitability |
| `search_macro_trends` | Queries macro market conditions (patch notes, news, trends) |

These tools are registered via **FastMCP** and passed to the Gemini SDK as callable functions.
Gemini uses them to independently verify whether a trade was a genuine snipe or a bad bet.

The CFO's verdict (confidence score 0-100 + reasoning trace) is logged immutably into **MLflow**,
then newly-evaluated baselines are synced back to the Edge Redis cache for the next hot-path cycle.

## Key Design Decisions

- **Edge-first processing** — Anomaly detection is synchronous and local. No round-trip to the cloud.
- **Agentic AI on the cold path** — LLMs are too slow and expensive for real-time trading.
  The CFO audits *after* the fact to prevent circular feedback loops.
- **O(1) cache** — Edge Redis stores sliding-window price history and ML baselines.
  No full-history queries needed on the hot path.
- **Deterministic Rules Engine** — Z-score based, no ML inference on the hot path.
  Baselines are computed offline and synced to the edge.
- **Hybrid Docker stacks** — The server stack bundles everything for a single machine;
  the edge stack is minimal (Redis + listener) for constrained devices.
