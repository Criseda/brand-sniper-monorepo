# Shared Utilities Package

This package acts as the connective tissue between the decoupled edge node (Listener) and the server node services (Backend & Analytics). 

Because the monorepo utilizes `uv Workspaces`, this package is included as a localized, editable dependency across all apps. This ensures that a single database change or connection optimization instantly propagates to every microservice without needing to publish to an external PyPI registry.

## Core Modules

### 1. Unified SQLModel Schemas (`models.py`)
All PostgreSQL tables are defined here declaratively using **SQLModel** (which merges SQLAlchemy and Pydantic). 
- `MarketItem`: The master catalog of all tracked digital assets (name, type, rarity).
- `LiveMarketTick`: High-velocity real-time price updates from live market endpoints.
- `ItemMacroBaseline`: Persisted results of the long-term macro trend pipeline (rolling averages, volatility, support floors). Computed once from the Kaggle Steam history, which has since been dropped; frozen CFO context only (see `docs/data_sources.md`).
- `BaselineBuild`, `VenueBaseline`: Dated baseline builds per venue and their per item rows, the live baselines the listener loads. How each field is built is in `baselines.py`.
- `SimulatedTrade`: The audit log of every paper-trade executed by the Edge node's `PaperExecutor`.

*Note: Alembic uses this exact file in the `/deployments` directory to auto-generate schema migrations.*

### 2. Async Connection Pools (`db_connection.py`)
Provides highly optimized, thread-safe asynchronous PostgreSQL (`asyncpg`) engines to ensure the ingestion and DRE layers never block waiting for a database socket.
