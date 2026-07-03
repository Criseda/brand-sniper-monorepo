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
    subgraph Edge[RASPBERRY PI 5 Edge Node / Hot Path]
        direction TB
        Listener[/apps/listener Async Polling/]:::process
        DRE[/Deterministic Rules Engine & Executor/]:::process
        EdgeRedis[(Edge Redis Cache 6380)]:::dataStore
        
        Listener -->|High-Frequency Ticks| DRE
        DRE <-->|"O(1) Baseline Lookup"| EdgeRedis
    end
    Edge:::edgeNode

    Market -->|WebSockets / REST| Listener

    %% Compute Node Subgraph
    subgraph Compute[WINDOWS PC Command Center / Cold Path]
        direction TB
        Backend[/apps/backend FastAPI Core/]:::process
        Analytics[/apps/analytics Prefect Macro & CFO/]:::process
        Postgres[(PostgreSQL Database SQLModel)]:::dataStore
        Prometheus[(Prometheus / Grafana Observability)]:::dataStore
        MLflow[(MLflow Server Model Registry)]:::dataStore
        Gemini[Google Gemini API Adversarial Agent]:::process

        Backend -->|Logs simulated trades| Postgres
        Backend -->|"Scrapes Metrics"| Prometheus
        Analytics -->|Fetches trades to Audit| Postgres
        Analytics <-->|Agentic Reasoning Loop| Gemini
        Analytics -->|Logs Audits & Rants| MLflow
        Analytics -->|Syncs offline ML models to Edge| EdgeRedis
    end
    Compute:::computeNode

    %% Cross-Network Connections
    DRE -->|Async POST Trade Logs| Backend
    Listener -->|Async POST Batched Ticks| Backend
```

### 📡 1. The Short-Term Anomaly Path (The Edge)
Running 24/7 inside Docker on the **Raspberry Pi 5**, the `/apps/listener` service consumes real-time market data ticks. It writes incoming vectors concurrently to a local Edge Redis hot-cache window, and instantly evaluates them. 

### ⚡ 2. The Deterministic Rules Engine (The Hot Path)
When a statistical anomaly is detected, the **Deterministic Rules Engine (DRE)**—now residing entirely on the Edge Node—queries the Edge Redis in `O(1)` time (sub-millisecond latency) to validate the drop against long-term ML baselines. If verified, the local `PaperExecutor` executes the trade synchronously, totally bypassing the network boundary. **Average latency: <5ms.**

### 📈 3. Observability (Grafana & Prometheus)
The Edge Node asynchronously POSTs the execution logs to the Windows backend. The backend increments memory-safe `prometheus_client` gauges and histograms. Prometheus scrapes this data on a schedule, and **Grafana** provides a stunning real-time visualization of simulated PnL.

### 🧠 4. The Adversarial CFO (The Cold Path)
To prevent Circular Feedback Loops, we built the **Adversarial CFO**.
Orchestrated by **Prefect**, this daily offline pipeline feeds the bot's simulated trades to Google's **Gemini**. The AI is armed with **FastMCP** tools (`fetch_live_market_floor`, `search_macro_trends`), allowing it to scrape the *actual live internet* to prove the bot wrong (e.g. detecting falling knives or market crashes). 
Its final grade and reasoning trace are logged immutably into **MLflow**. Prefect then syncs the newly evaluated ML baselines back across the network to the Edge Redis cache.

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
Run `uv sync --all-packages` from the project root. Because this is a monorepo workspace, you must use the `--all-packages` flag so `uv` resolves dependencies across all microservices simultaneously. This command creates a localized `.venv` and links the internal `shared-utils` library across all applications instantly:

```bash
uv sync --all-packages
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

### Start the Backend (API Core)
```bash
cd apps/backend
uv run python main.py
```
- API Docs: `http://localhost:8080/docs`
- Prometheus Metrics: `http://localhost:8080/metrics`

### Start the Edge Listener (DRE Hot Path)
```bash
cd apps/listener
uv run python main.py
```

### Run the Agentic CFO Pipeline
Once trades exist in the database, execute the adversarial evaluation:
```bash
cd apps/analytics
uv run python evaluate_performance.py
```
View the AI's reasoning artifact in **MLflow** at `http://localhost:5000`.