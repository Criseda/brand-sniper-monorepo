from prometheus_client import Counter, Gauge, Histogram

ticks_processed_total = Counter(
    "listener_ticks_processed_total",
    "Total number of market ticks processed (after dedup)",
)

feed_events_received_total = Counter(
    "listener_feed_events_received_total",
    "Total number of raw venue feed events captured for the feed_events table",
    labelnames=["event_type"],
)

ticks_deduplicated_total = Counter(
    "listener_ticks_deduplicated_total",
    "Total number of ticks filtered as duplicates",
)

snapshots_unchanged_total = Counter(
    "listener_snapshots_unchanged_total",
    "REST snapshots whose price had not changed since the item's previous snapshot: windowed, not scored",
)

# Labels on the anomaly counters: `source` is the Z-score source (local, hybrid, macro), `tick_kind`
# tells a REST lowest ask (rest_snapshot) from a live feed listing (listing), and `reason` is the DRE
# rule that approved the anomaly.
anomalies_detected_total = Counter(
    "listener_anomalies_detected_total",
    "Total number of anomalies flagged by Z-score analysis",
    labelnames=["source", "tick_kind"],
)

anomalies_confirmed_total = Counter(
    "listener_anomalies_confirmed_total",
    "Total number of anomalies approved by the DRE",
    labelnames=["source", "tick_kind", "reason"],
)

anomalies_rejected_total = Counter(
    "listener_anomalies_rejected_total",
    "Total number of anomalies filtered out by the DRE",
    labelnames=["source", "tick_kind"],
)

dedup_cache_size = Gauge(
    "listener_dedup_cache_size",
    "Current number of items in the venue's LRU deduplication cache",
    labelnames=["venue"],
)

dedup_cache_evictions_total = Counter(
    "listener_dedup_cache_evictions_total",
    "Items evicted from the venue's full deduplication cache; an evicted unchanged REST snapshot is scored again",
    labelnames=["venue"],
)

batch_buffer_size = Gauge(
    "listener_batch_buffer_size",
    "Current number of ticks waiting in the batch buffer",
)

rules_engine_latency_seconds = Histogram(
    "listener_rules_engine_latency_seconds",
    "Latency of the DRE evaluation in seconds",
    buckets=(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.5, 1.0, 2.0),
)

redis_operation_latency_seconds = Histogram(
    "listener_redis_operation_latency_seconds",
    "Latency of Redis operations in seconds",
    buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.5),
)

batch_flush_total = Counter(
    "listener_batch_flush_total",
    "Total number of batch flushes dispatched to the backend",
    labelnames=["status"],
)

batch_delivery_retries_total = Counter(
    "listener_batch_delivery_retries_total",
    "Total number of transient bulk-ingestion delivery retries",
)

batch_delivery_dead_letters_total = Counter(
    "listener_batch_delivery_dead_letters_total",
    "Total number of bulk-ingestion batches moved to the dead-letter stream",
)

batch_delivery_malformed_total = Counter(
    "listener_batch_delivery_malformed_total",
    "Total number of malformed bulk-ingestion records moved to quarantine",
)

batch_delivery_pending = Gauge(
    "listener_batch_delivery_pending",
    "Current number of batches waiting in the Redis delivery stream",
)

background_jobs_total = Counter(
    "listener_background_jobs_total",
    "Total number of listener background jobs by type and outcome",
    labelnames=["job_type", "outcome"],
)

background_jobs_active = Gauge(
    "listener_background_jobs_active",
    "Current number of active listener background jobs",
    labelnames=["job_type"],
)

background_jobs_queued = Gauge(
    "listener_background_jobs_queued",
    "Current number of queued listener background jobs",
    labelnames=["job_type"],
)

trade_submissions_total = Counter(
    "listener_trade_submissions_total",
    "Total number of paper-trade submissions to the backend by outcome",
    labelnames=["outcome"],
)

tick_queue_size = Gauge(
    "listener_tick_queue_size",
    "Current number of ticks waiting for listener processing",
)

baselines_loaded = Gauge(
    "listener_baselines_loaded",
    "Number of item baselines in the edge Redis for the venue (0 means the DRE rejects every anomaly)",
    labelnames=["venue"],
)

baseline_build_age_seconds = Gauge(
    "listener_baseline_build_age_seconds",
    "Age of the baseline build loaded in the edge Redis, from its build time",
    labelnames=["venue"],
)
