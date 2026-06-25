from contextvars import ContextVar

# Async-safe ContextVar to hold execution logs for the current verification run
# Type: ContextVar[dict | None]
run_telemetry = ContextVar("run_telemetry", default=None)
