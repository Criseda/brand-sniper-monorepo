import logging
import os
import sys

_LOG_LEVEL_MAP: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def _resolve_log_level() -> int:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    return _LOG_LEVEL_MAP.get(level_name, logging.INFO)


def get_logger(name: str) -> logging.Logger:
    """Returns a pre-configured logger, attaching a stdout handler on first use.

    Raises TypeError if name is not a string. Falls back to a handler-less
    logger if stdout stream configuration fails, so logging never crashes callers.
    """
    if not isinstance(name, str):
        raise TypeError(f"get_logger expects a string name, got {type(name).__name__}.")

    logger = logging.getLogger(name)

    if not logger.handlers:
        if logger.level == logging.NOTSET:
            logger.setLevel(_resolve_log_level())

        try:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s [%(name)s] %(levelname)s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            logger.addHandler(handler)
        except (OSError, ValueError) as exc:
            logging.warning("[LOGGING] Failed to configure stdout handler for logger '%s': %s", name, exc)

    return logger
