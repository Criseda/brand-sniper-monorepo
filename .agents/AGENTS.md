# Brand Sniper Coding Rules

This document outlines structural guidelines, clean code principles, and behavioral rules for writing code in the Brand Sniper monorepo. All AI agents and developers must strictly adhere to these instructions.

---

## 1. Logging & Messaging Standards
* **No Emojis:** Do not use emojis in source code files, logs, print statements, or comment structures. Always use standard, clean ASCII/Unicode text.
* **Prefix-Based Logging:** Use standardized prefix tags for stdout and logging statements to make debugging multi-node traces easier:
  * `[AGENT]` for backend AI generative actions and decisions.
  * `[ALERT APPROVED]` for confirmed snipes.
  * `[LAN]` / `[BATCH FLUSH]` for edge listener networking actions.
  * `[SKINPORT]` / `[CSFLOAT]` for scraper telemetry.
  * `[SKINPORT API]` for backend live queries.

---

## 2. Dependency & Package Management
* **Workspace Boundaries:** This repository uses `uv` workspaces.
  * **Workspace Syncing:** Because this is a monorepo workspace, you MUST always run `uv sync --all-packages` from the root directory to properly resolve dependencies across all microservices. Running a bare `uv sync` will uninstall microservice dependencies.
  * Never install packages globally or using standard `pip`. Always use `uv add <package-name>` inside the targeted directory or update the package-specific `pyproject.toml` and run `uv sync --all-packages`.
  * Share utility functions across nodes via the `shared-utils` workspace package (e.g., `packages/shared_utils`).

---

## 3. Asynchronous & Performance Architecture
* **Non-Blocking I/O:** The edge scraper (`apps/listener`) must remain non-blocking. Avoid any synchronous network requests (`requests` library) or blocking calls. Use `aiohttp` or `httpx` with correct timeout settings.
* **Raspberry Pi Optimizations:** 
  * Keep the memory footprint minimal. Avoid loading huge packages on the edge scraper node unless necessary.
  * Keep Redis memory configurations volatile on the Pi 5. Never enable disk persistence (`RDB`/`AOF`) for short-term caching.
* **Thread/Task-Safe Telemetry:**
  * Never store transaction or run state in global dictionaries. Use Python's `contextvars.ContextVar` for thread-safe and async-safe context propagation during verification workflows.

---

## 4. Database & ORM Guidelines
* **SQLModel Reference Directory:**
  * All database models are defined in [models.py](file:///c:/Users/ilaur/git/brand-sniper-monorepo/packages/shared_utils/src/shared_utils/models.py). Do not declare tables or models locally within services.
  * Every model must use native typing hints (`int | None` instead of `Optional[int]` for modern Python, except where SQLModel field definitions require it).
  * Do not bypass SQLModel migrations. If database schemas shift, developers must generate and execute an Alembic migration from the `deployments` root.

---

## 5. Agent Verification & Tool Call Conventions
* **Strict MCP Registration:** Expose tool functionalities to the Gemini verifier exclusively via FastMCP tools (`@mcp.tool()`). Do not write custom prompt parse strings or ad-hoc JSON execution layers.
* **Separation of Reasoning and Action:** Keep tools focused on returning structured context data (floats, history averages). The LLM analyst holds the responsibility of weighing this data and formulating a verdict.
