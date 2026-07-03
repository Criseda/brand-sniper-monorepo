# Listener: Edge Node Ingestion Stream

The Listener application is designed to run 24/7 on edge hardware (e.g., a Raspberry Pi 5). Its sole responsibility is to consume massive firehoses of live market data from secondary marketplaces via WebSockets or high-frequency polling.

## 📡 The Dual-Write Architecture

Because the ingestion node must never block, it routes incoming data ticks concurrently into two separate storage mechanisms:

1. **The Redis Hot Cache (The Fast Path):**
   Maintains a rolling 5-minute mathematical window of current market floors. This allows the Windows Backend Compute Node to lookup a baseline in `O(1)` time instantly without running heavy SQL aggregations.

2. **The Postgres Cold Storage (The Slow Path):**
   Streams bulk batches of market ticks into the permanent SQL database. This historical data is later mined by the Analytics Prefect pipeline to discover cyclical macro trends and train the AI baselines.

## 🚀 Setup & Execution

### 1. Environment Configuration
Copy the example environment file:
```bash
cp .env.example .env
```

### 2. Run the Node
Assuming the `pi5-stack` or local Docker Compose databases are running:

```bash
uv run python main.py
```
