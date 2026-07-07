from prometheus_client import Counter, Gauge, Histogram

ticks_processed_total = Counter(
    "listener_ticks_processed_total",
    "Total number of market ticks processed (after dedup)",
)

ticks_deduplicated_total = Counter(
    "listener_ticks_deduplicated_total",
    "Total number of ticks filtered as duplicates",
)

anomalies_detected_total = Counter(
    "listener_anomalies_detected_total",
    "Total number of anomalies flagged by Z-score analysis",
    labelnames=["source"],
)

anomalies_confirmed_total = Counter(
    "listener_anomalies_confirmed_total",
    "Total number of anomalies approved by the DRE (paper trades executed)",
)

anomalies_rejected_total = Counter(
    "listener_anomalies_rejected_total",
    "Total number of anomalies filtered out by the DRE",
)

dedup_cache_size = Gauge(
    "listener_dedup_cache_size",
    "Current number of entries in the LRU deduplication cache",
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
