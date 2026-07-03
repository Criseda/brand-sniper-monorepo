# Brand Sniper: Algorithmic Market Sniping & Macro Arbitrage Engine

A production-grade, distributed data and AI system engineered to detect real-time market pricing anomalies and forecast long-term macroeconomic asset trends. The system architecture is built as a high-velocity Python monorepo split across a localized hybrid-network topology (Raspberry Pi 5 edge node and Windows PC compute engine). 

**The platform's mission has evolved:** We utilize a lightning-fast **Deterministic Rules Engine (DRE)** on the hot-path to instantly paper-trade statistical anomalies, while an offline **Agentic AI Pipeline (The Adversarial CFO)** leverages Live Tools to independently audit those trades asynchronously.

---

## 🏗️ System Architecture & Hardware Topology

The infrastructure is explicitly decoupled across two physical nodes to replicate an enterprise-grade hybrid-cloud network topology:

```mermaid
flowchart TD
    %% Define styles
    classDef edgeNode fill:#1E293B,stroke:#38BDF8,stroke-width:2px,color:#F8FAFC
    classDef computeNode fill:#1E293B,stroke:#A78BFA,stroke-width:2px,color:#F8FAFC
    classDef dataStore fill:#334155,stroke:#94A3B8,stroke-width:1px,color:#E2E8F0
    classDef process fill:#0F172A,stroke:#64748B,stroke-width:1px,color:#E2E8F0,shape:rect
    classDef alert fill:#7F1D1D,stroke:#FCA5A5,stroke-width:2px,color:#FEE2E2

    %% Live Market
    Market[LIVE MARKET DATA STREAM]

    %% Edge Node Subgraph
    subgraph Edge[RASPBERRY PI 5 Edge Node / Ingestion]
        direction TB
        Listener[/apps/listener Async Polling/]:::process
        Redis[(Redis Cache Hot 5-Min Window)]:::dataStore
        Postgres[(PostgreSQL Database SQLModel/Alembic)]:::dataStore
        Prometheus[(Prometheus / Grafana Observability Core)]:::dataStore
        
        Listener -->|High-Frequency Ticks| Redis
        Listener -->|Bulk Batches| Postgres
    end
    Edge:::edgeNode

    Market -->|WebSockets / REST| Listener

    %% Compute Node Subgraph
    subgraph Compute[WINDOWS PC Compute & Agentic Engine]
        direction TB
        Backend[/apps/backend FastAPI & DRE/]:::process
        Analytics[/apps/analytics Prefect CFO Pipeline/]:::process
        MLflow[(MLflow Server Model Registry)]:::dataStore
        Gemini[Google Gemini API Adversarial Agent]:::process

        Backend -->|Executes synchronous trades| Postgres
        Analytics -->|Fetches trades to Audit| Postgres
        Analytics <-->|Agentic Reasoning Loop| Gemini
        Analytics -->|Logs Audits & Rants| MLflow
    end
    Compute:::computeNode

    %% Cross-Network Connections
    Edge <-.-> |Local Network LAN| Compute
    Backend -.->|Scrapes Metrics| Prometheus
    Backend -.->|O(1) Baseline Lookup| Redis
    
    %% Alerts
    Discord[DISCORD WEBHOOK ALERT]:::alert
    Backend -->|Notifies| Discord
```

### 📡 1. The Short-Term Anomaly Path (The Edge)
Running 24/7 inside Docker on the **Raspberry Pi 5**, the `/apps/listener` service consumes real-time market data ticks. It writes incoming vectors concurrently to a Redis hot-cache window and a cold PostgreSQL history database. 

### ⚡ 2. The Deterministic Rules Engine (The Hot Path)
When `/apps/backend` receives a signal, the **Deterministic Rules Engine (DRE)** queries Redis in `O(1)` time (sub-millisecond latency) to construct a localized baseline. If the mathematical Z-score indicates a severe anomaly, the `PaperExecutor` immediately executes a simulated trade synchronously. **Average latency: <20ms.**

### 📈 3. Observability (Grafana & Prometheus)
Zero blocking external dependencies. The hot path increments memory-safe `prometheus_client` gauges and histograms. Prometheus scrapes this data on a schedule, and **Grafana** provides a stunning real-time visualization of simulated PnL and DRE execution latency.

### 🧠 4. The Adversarial CFO (The Cold Path)
To prevent Circular Feedback Loops (where the AI grades trades using the same stale database data that triggered them), we built the **Adversarial CFO**.
Orchestrated by **Prefect**, this daily offline pipeline feeds the bot's simulated trades to Google's **Gemini**. The AI is armed with **FastMCP** tools (`fetch_live_market_floor`, `search_macro_trends`), allowing it to scrape the *actual live internet* to prove the bot wrong (e.g. detecting falling knives or market crashes). 
Its final grade and reasoning trace are logged immutably into **MLflow**.

---

## 🛠️ The Tech Stack

* **Runtime:** Python 3.12 (Locked via `uv`)
* **Package Management:** `uv Workspaces` (Unified root lockfile, independent microservice dependency resolutions)
* **Database & Migrations:** PostgreSQL (`psycopg2`), **SQLModel**, and **Alembic** 
* **Cache:** Redis (Async key-value data-windowing)
* **Orchestration & Tracking:** Prefect Server & MLflow
* **AI Graph & Protocols:** Google Gemini & Model Context Protocol (FastMCP)
* **Observability:** Prometheus & Grafana Stack

---

## 🚀 Setup & Installation Guide

This monorepo is designed to be plug-and-play using `.env.example` templates and Docker Compose.

### 1. Prerequisites
Ensure you have `uv` installed globally, along with `Docker` and `Docker Compose`.
*(Windows PowerShell installation for uv)*: `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`

### 2. Environment Variables
To make setup seamless, copy the `.env.example` file in the root directory to `.env` and fill in your keys:

```bash
cp .env.example .env
```
*(Repeat this for `apps/analytics/.env.example` and `apps/backend/.env.example` if specific microservice configuration is needed).*

### 3. Workspace Initialization
Run `uv sync` from the project root. This command analyzes the entire workspace tree, creates a localized `.venv`, and links the internal `shared-utils` library across all applications instantly:

```bash
uv sync
```

### 4. Spin Up Infrastructure (Docker)
Initialize the foundational database, cache, tracking servers, and observability metrics layers.

**Windows Compute Node Stack:**
```bash
cd deployments/windows-stack
docker compose up -d
```
This single command spins up:
- **Grafana** (Port `3000`) - *Default Login: admin / admin*
- **Prometheus** (Port `9090`)
- **Prefect Server** (Port `4200`)
- **MLflow Tracking Server** (Port `5000`)

### 5. Database Migrations (Alembic)
Apply the SQL schemas to the live PostgreSQL instance:
```bash
cd deployments
uv run alembic upgrade head
```

---

## 🖥️ Running the Applications

### Start the Backend (DRE & API)
```bash
cd apps/backend
uv run fastapi dev main.py --port 8080
```
- API Docs: `http://localhost:8080/docs`
- Prometheus Metrics: `http://localhost:8080/metrics`

### Run the Agentic CFO Pipeline
Once trades exist in the database, execute the adversarial evaluation:
```bash
cd apps/analytics
uv run python evaluate_performance.py
```
View the AI's reasoning artifact in **MLflow** at `http://localhost:5000`.