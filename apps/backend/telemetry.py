from prometheus_client import Gauge, Histogram, Counter

# Total estimated profit of all successful paper trades
paper_trading_estimated_profit_total = Gauge(
    'paper_trading_estimated_profit_total',
    'Total estimated profit in cents from successful paper trades'
)

# Number of successful paper trades
paper_trades_executed_total = Counter(
    'paper_trades_executed_total',
    'Total number of successful paper trades executed'
)

# Latency of the Deterministic Rules Engine
rules_engine_latency_seconds = Histogram(
    'rules_engine_latency_seconds',
    'Latency of the Deterministic Rules Engine in seconds',
    buckets=(0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.5, 1.0, 2.0)
)
