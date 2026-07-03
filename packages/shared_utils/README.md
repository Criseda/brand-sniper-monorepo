# Shared Utilities Package

This package acts as the connective tissue between the decoupled edge hardware (Pi 5 Listener) and the Windows Compute nodes (Backend & Analytics). 

Because the monorepo utilizes `uv Workspaces`, this package is included as a localized, editable dependency across all apps. This ensures that a single database change or connection optimization instantly propagates to every microservice without needing to publish to an external PyPI registry.

## 📦 Core Modules

### 1. Unified SQLModel Schemas (`models.py`)
All PostgreSQL tables are defined here declaratively using **SQLModel** (which merges SQLAlchemy and Pydantic). 
- `MarketItem`: The master catalog of all traded assets.
- `SimulatedTrade`: The audit log of every paper-trade executed by the backend's `PaperExecutor`.

*Note: Alembic uses this exact file in the `/deployments` directory to auto-generate schema migrations.*

### 2. Async Connection Pools (`db_connection.py`)
Provides highly optimized, thread-safe asynchronous PostgreSQL (`asyncpg`) engines to ensure the ingestion and DRE layers never block waiting for a database socket.
