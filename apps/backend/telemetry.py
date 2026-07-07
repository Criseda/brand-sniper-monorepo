from prometheus_client import Counter, Gauge

# Total estimated profit of all successful paper trades
paper_trading_estimated_profit_total = Gauge(
    "paper_trading_estimated_profit_total", "Total estimated profit in cents from successful paper trades"
)

# Number of successful paper trades
paper_trades_executed_total = Counter("paper_trades_executed_total", "Total number of successful paper trades executed")
