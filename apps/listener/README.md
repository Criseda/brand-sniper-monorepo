# Listener: Edge Node Ingestion Stream

The Listener application is designed to run 24/7 on edge hardware (e.g., a Raspberry Pi, low-power VPS, or any edge device). Its sole responsibility is to consume massive firehoses of live market data from secondary marketplaces via REST polling and a **Node.js WebSocket sidecar** (for Socket.IO-based push feeds).

## The Edge Compute Architecture

Because the ingestion node must never block, it routes incoming data ticks concurrently into two fast pipelines:

1. **The Edge Redis Hot Cache & DRE (The Hot Path):**
   Maintains a rolling 5-minute mathematical window of current market floors and synchronized long-term ML baselines. The **Deterministic Rules Engine (DRE)** queries this Edge Redis in `O(1)` time instantly to execute `SimulatedTrades` using the local `PaperExecutor`. Execution logs are then asynchronously POSTed over the network to the server backend to prevent blocking.

2. **The Batched Ingestion (The Cold Path):**
   Streams bulk batches of market ticks over the network to the server backend REST API (`/api/v1/ingest/bulk`) to be saved into the permanent SQL database. This historical data is later mined by the Analytics pipeline to train the AI baselines.

### Node.js WebSocket Sidecar

The `SkinportScraper` spawns a Node.js subprocess (`scrapers/skinport_websocket/sidecar.js`) that connects to Skinport's Socket.IO feed for real-time sale listings. The sidecar publishes parsed listings to the local Redis Pub/Sub channel `skinport:live_listings`, which the main Python process subscribes to and feeds into the anomaly detection pipeline. (The subprocess stdout/stderr are captured solely for application logging).

## Setup & Execution

### 1. Environment Configuration
Copy the example environment file:
```bash
cp .env.example .env
```

### 2. Run the Node
Assuming the `edge-stack` or local Docker Compose databases are running:

```bash
uv run python main.py
```
