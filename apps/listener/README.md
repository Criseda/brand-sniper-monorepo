# Listener: Edge Node Ingestion Stream

The Listener application is designed to run 24/7 on edge hardware (e.g., a Raspberry Pi, low-power VPS, or any edge device). Its sole responsibility is to consume massive firehoses of live market data from secondary marketplaces via REST polling and a **Node.js WebSocket sidecar** (for Socket.IO-based push feeds).

## The Edge Compute Architecture

Because the ingestion node must never block, it routes incoming data ticks concurrently into two fast pipelines:

1. **The Edge Redis Hot Cache & DRE (The Hot Path):**
   Maintains a rolling 5-minute mathematical window of current market floors and the venue's baselines, which the listener loads from the backend when it starts and refreshes every 15 minutes (see [`docs/data_sources.md`](../../docs/data_sources.md)). The **Deterministic Rules Engine (DRE)** queries this Edge Redis in `O(1)` time instantly to execute `SimulatedTrades` using the local `PaperExecutor`. Execution logs are then asynchronously POSTed over the network to the server backend to prevent blocking.

2. **The Batched Ingestion (The Cold Path):**
   Streams bulk batches of market ticks over the network to the server backend REST API (`/api/v1/ingest/bulk`) to be saved into the permanent SQL database. This historical data is later mined by the Analytics pipeline to train the AI baselines.

Both paths run through bounded worker pools owned by `asyncio.TaskGroup`. The
tick queue, anomaly queue, and batch-flush queue apply asynchronous backpressure
at configurable limits, preventing burst traffic from creating an unbounded
number of tasks. Shutdown stops producers, drains queued work within
`LISTENER_SHUTDOWN_GRACE_SECONDS`, and then closes network resources. Each stage has its own
grace window, so keep the container's `stop_grace_period` (120 s in both compose
stacks) above three times `LISTENER_SHUTDOWN_GRACE_SECONDS`.

Each executor buys a listing (or a REST item and price, once a day) once: `ExecutionService.execute`
skips a purchase the process already made, and after a restart the backend answers a repeat as already
recorded (`outcome="already_recorded"`), because the unique indexes on `simulated_trades` are the guarantee.

Paper-trade submission is awaited inside the anomaly worker. Backend rejection,
timeouts, and connection failures therefore reach the worker supervisor and are
reported through `listener_trade_submissions_total` and
`listener_background_jobs_total` instead of becoming orphaned task failures.

### Node.js WebSocket Sidecar

The `SkinportScraper` spawns a Node.js subprocess (`scrapers/skinport_websocket/sidecar.js`) that connects to Skinport's Socket.IO `saleFeed`. The sidecar forwards every event type (`listed`, `sold`, ...) untouched, wrapped with a receive timestamp, to the local Redis Pub/Sub channel `skinport:sale_feed`. The main Python process records each raw event (`feed_events`) and every sale as a listing-level tick; only `listed` sales (and REST snapshots) enter the anomaly detection pipeline. See [`docs/skinport_feed.md`](../../docs/skinport_feed.md) and the official [Sale Feed](https://docs.skinport.com/websocket/sale-feed) and [Items](https://docs.skinport.com/items) docs. (The subprocess stdout/stderr are captured solely for application logging).

### Waxpeer Feed

With `LISTENER_VENUE=waxpeer` the listener runs `WaxpeerScraper`, which reads Waxpeer's public Socket.IO feed directly over an aiohttp websocket (no sidecar) and has no REST poll. It records and scores only listings priced below the item's Skinport price, then follows them for price cuts and removal, and judges them against the Skinport baselines it loads. Set `WAXPEER_API_KEY` in `.env`. See [`docs/waxpeer_feed.md`](../../docs/waxpeer_feed.md).

### Trade Alerts

There is no automated buying, so when the DRE approves a listing the listener sends me a message on Discord, Telegram or both (`alerts.py`, #18). The message names the item with its float, pattern and stickers, gives the listing price and the estimated net margin after fees (the same estimate the paper trade records), says which rule approved it with the Z score and its source, and links to the listing. A Skinport feed listing links to the item page, because the feed sends no sale ID. A Waxpeer listing links to the listing itself. A REST snapshot names no listing, so its alert links to a Skinport market search for the item.

A channel is on when its credentials are set in `.env`: `DISCORD_WEBHOOK_URL` for a channel webhook, or `TELEGRAM_BOT_TOKEN` with `TELEGRAM_CHAT_ID`. Setting `DISCORD_ALERTS_ENABLED` or `TELEGRAM_ALERTS_ENABLED` to `false` turns a channel off and keeps its credentials. With no credentials the listener sends nothing.

Alerts never hold up tick processing. The anomaly worker hands an alert over without waiting, and each channel has its own small queue and a single sender. When a queue is full the alert is dropped and counted. Each sender stays under `ALERT_MAX_PER_MINUTE` messages (20, below Discord's webhook limit and Telegram's group limit), waits out the `retry_after` of a 429 and tries again, up to `ALERT_MAX_ATTEMPTS` requests. It gives up when it would have to wait longer than `ALERT_MAX_WAIT_SECONDS`. A listing is alerted once per listener process, like a paper trade, and so is the REST snapshot that shows it as the lowest ask later that day. Webhook URLs and bot tokens never appear in the logs.

Every attempt, sent or not, goes to the backend and is stored in the `trade_alerts` table with the listing, venue, channel, outcome, estimate, rule and latency. I keep it in PostgreSQL rather than only in Prometheus because the point is to join alerts to `listing_outcomes` later and see whether the alerted listings were worth buying, and Prometheus cannot hold a listing ID per alert. `listener_alerts_total` counts outcomes per channel, including drops, and `listener_alert_latency_seconds` measures the time from building the tick to the channel accepting the message.

## Replay & Backtests

`python -m backtest` replays recorded feed events and REST snapshots through the same decision code
(`detection.py`, `zscore.py`, `rules_engine.py`) against an in-memory edge store, and writes a
deterministic decision log. See [docs/backtesting.md](../../docs/backtesting.md).

## Setup & Execution

### 1. Environment Configuration
Copy the example environment file:
```bash
cp .env.example .env
```

The listener also requires the shared `BACKEND_API_KEY` from the repository root
`.env`. It adds the key to ingestion requests automatically. Its Docker health
probe uses the container-internal `127.0.0.1:9101/health` endpoint, which is
served by the main asyncio event loop; an unexpected WebSocket sidecar exit is
fatal so the container restart policy can recover it.

### 2. Run the Node
Assuming the `edge-stack` or local Docker Compose databases are running:

```bash
uv run python main.py
```
