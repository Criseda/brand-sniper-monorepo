# Algorithmic Market Sniping & Macro Arbitrage Engine

A production-grade, distributed data and AI system engineered to detect real-time market pricing anomalies and forecast long-term macroeconomic asset trends. The system architecture is built as a high-velocity Python monorepo split across a localized hybrid-network topology (Raspberry Pi 5 edge node and Windows PC compute engine). 

The platform monitors secondary digital asset marketplaces (using Counter-Strike 2 / Steam skins as a high-throughput proxy commodity) to identify deep mispricings using statistical filters, validating opportunities through an isolated multi-agent LLM reasoning pipeline before triggering trade executions.

---

## 🏗️ System Architecture & Hardware Topology

The infrastructure is explicitly decoupled across two physical nodes to replicate an enterprise-grade hybrid-cloud network topology:


```
           [ LIVE MARKET DATA STREAM ]
                       │
                       ▼

┌────────────────────────────────────────────────────────┐
│          RASPBERRY PI 5 (Edge / 24/7 Ingestion Node)   │
│                                                        │
│  ┌───────────────────────┐    ┌─────────────────────┐  │
│  │   /apps/listener      │───>│     Redis Cache     │  │
│  │ (Async Polling Loop)  │    │  (Hot 5-Min Window) │  │
│  └───────────┬───────────┘    └─────────────────────┘  │
│              │                                         │
│              ▼ (Bulk Batches)                          │
│  ┌───────────────────────┐    ┌─────────────────────┐  │
│  │  PostgreSQL Database  │<───┤ Prometheus/Grafana  │  │
│  │  (SQLModel/Alembic)   │    │ (Observability Core)│  │
│  └───────────────────────┘    └──────────▲──────────┘  │
└──────────────────────────────────────────┼─────────────┘
│
Local Network LAN │ Scrapes Metrics
│
┌──────────────────────────────────────────┴─────────────┐
│          WINDOWS PC (Compute & Core Reasoning Engine)  │
│                                                        │
│  ┌───────────────────────┐    ┌─────────────────────┐  │
│  │   /apps/analytics     │    │   /apps/backend     │  │
│  │   (Prefect Pipeline)  │    │ (FastAPI & FastMCP) │  │
│  └───────────┬───────────┘    └──────────┬──────────┘  │
│              │                           │             │
│              ▼                           ▼             │
│  ┌───────────────────────┐    ┌─────────────────────┐  │
│  │     MLflow Server     │    │   Google ADK Loop   │  │
│  │   (Model Registry)    │    │   (Gemini Pro)      │  │
│  └───────────────────────┘    └──────────┬──────────┘  │
│                                          │             │
│                                          ▼             │
│                               [ DISCORD WEBHOOK ALERT ]
└────────────────────────────────────────────────────────┘

```

### 📡 1. The Short-Term Anomaly Path (The Edge)
Running 24/7 inside Docker on the **Raspberry Pi 5**, the `/apps/listener` service consumes real-time market data ticks. It writes incoming vectors concurrently to a Redis hot-cache window and a cold PostgreSQL history database. For every asset tick, it calculates a rolling mathematical standard deviation (Z-score):

$$Z = \frac{X - \mu}{\sigma}$$

Where $X$ is the live price, $\mu$ is the rolling mean, and $\sigma$ is the rolling standard deviation. If a pricing tick triggers a threshold of $Z < -2.5$ (indicating an extreme, sudden flash-crash or human typing error), the Pi instantly dispatches an asynchronous HTTP POST event over the local network to the Windows Compute Engine.

### 📈 2. The Long-Term Macro Path (The Compute Core)
Running on the **Windows PC**, the `/apps/analytics` engine leverages **Prefect** to orchestrate long-term trend analysis. It extracts year-over-year cyclical market patterns from millions of historical transactions (e.g., localized holiday demand shocks like the Chinese New Year market rally, or structural dips during seasonal platform events). 

The generated macroeconomic baseline forecasts are registered inside an **MLflow** tracking server and exposed back to the database as an expected baseline curve projection.

### 🧠 3. The Multi-Agent Verification Loop
When an anomaly or macro-accumulation signal hits the Windows **FastAPI** layer (`/apps/backend`), it triggers a structured **Google Agent Development Kit (ADK 2.0)** workflow graph. 

Instead of executing raw programmatic buys on dirty data, **Gemini Pro** initializes as a core validator. Using the **Model Context Protocol (MCP)** powered by `FastMCP`, the agent dynamically runs isolated tools to read asset wear factors, evaluate market volume history, and parse listing descriptions to protect capital from fraud or illiquid traps before firing a checkout payload to a client webhook.

---

## 🛠️ The Tech Stack

* **Runtime:** Python 3.12 (Locked via `uv`)
* **Package Management:** `uv Workspaces` (Unified root lockfile, independent microservice dependency resolutions)
* **Database & Migrations:** PostgreSQL (`asyncpg` driver), **SQLModel** (Unified SQLAlchemy ORM + Pydantic core types), and **Alembic** (Async version-controlled schema migrations)
* **Cache:** Redis (Async key-value data-windowing)
* **Orchestration & Tracking:** Prefect Server & MLflow
* **AI Graph & Protocols:** Google ADK 2.0 & Model Context Protocol (FastMCP)
* **Observability:** Prometheus & Grafana Stack

---

## 📂 Repository Structure

```text
/brand-sniper-monorepo
│   .gitignore
│   .python-version           # Fixed Python 3.12 runtime configuration
│   pyproject.toml            # Global uv Workspace declaration
│   README.md                 # System documentation
│   uv.lock                   # Deterministic workspace dependency lockfile
│
├── /apps                     # Independent runtime services
│   ├── /analytics            # Model training & time-series extraction (Windows)
│   │   ├── Dockerfile
│   │   ├── long_term_macro.py
│   │   └── short_term_zscore.py
│   │
│   ├── /backend              # Core REST API + FastMCP Agent Server (Windows)
│   │   ├── /api
│   │   ├── /mcp_server
│   │   ├── Dockerfile
│   │   └── main.py
│   │
│   └── /listener             # Always-on 24/7 asset scraping stream (Pi 5)
│       ├── Dockerfile
│       └── main.py
│
├── /deployments              # DevOps infrastructure configurations
│   ├── alembic.ini           # Async migration orchestrator configuration
│   ├── /migrations           # Database schema migration version files
│   ├── /pi5-stack            # Docker-Compose definition for Postgres, Redis, Prometheus, Grafana
│   └── /windows-stack        # Docker-Compose definition for Backend, Prefect, MLflow
│
└── /packages                 # Shares internal system code libraries
    └── /shared_utils         # Editable internal dependency
        ├── __init__.py
        ├── db_connection.py  # Asynchronous SQL connection pool management
        ├── models.py         # Declarative SQLModel tables shared across apps
        └── pyproject.toml    # Shared utility library packaging config

```

---

## 🚀 Local Development Setup

### 1. Prerequisites

Ensure you have `uv` installed globally on your machine, along with `Docker` and `Docker Compose`.

To install `uv` on Windows (PowerShell):

```powershell
powershell -c "irm [https://astral.sh/uv/install.ps1](https://astral.sh/uv/install.ps1) | iex"

```

### 2. Workspace Initialization

Clone the repository and run `uv sync` from the project root. This command analyzes the entire workspace tree, creates a localized `.venv`, and links the internal `shared-utils` library across all applications instantly:

```bash
cd brand-sniper-monorepo
uv sync

```

### 3. Spin Up Infrastructure (Docker)

To initialize the foundational database, cache, and metrics layers:

For the Pi 5 Node (or local emulation):

```bash
cd deployments/pi5-stack
docker compose up -d

```

For the Windows Node:

```bash
cd deployments/windows-stack
docker compose up -d

```

### 4. Database Migrations (Alembic)

Database migrations are fully decoupled and run natively using an async bridge. Navigate to the deployments directory to generate and apply schemas:

```bash
cd deployments
# Generate initial migration version script
uv run alembic revision --autogenerate -m "initial_schema"

# Apply tables to the live PostgreSQL instance
uv run alembic upgrade head

```

---

## 📊 Observability & Telemetry

Every microservice exposes an independent `/metrics` endpoint via the Python `prometheus_client`. The metrics are automatically scraped by Prometheus across the local area network interfaces.

Core System Performance Indicators tracked:

* `market_ticks_processed_total` (Counter tracking ingestion speed)
* `gemini_inference_latency_seconds` (Histogram monitoring AI verification overhead)
* `active_arbitrage_signals` (Gauge tracking real-time market opportunities)
* `model_prediction_drift_error` (Tracking skew between historical calculations and live pricing)

## Datasets

Linked here: [Counter Strike Market Sale Data](https://www.kaggle.com/datasets/kieranpoc/counter-strike-market-sale-data)