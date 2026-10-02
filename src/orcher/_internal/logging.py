"""SDK logging utilities.

This module provides logging configuration and utilities for the ORCHER SDK.
"""

from __future__ import annotations

import logging
import sys

__all__ = [
    "get_logger",
    "configure_logging",
    "SDK_LOGGER_NAME",
]

SDK_LOGGER_NAME = "orcher"

# Default format for SDK logs
DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def get_logger(name: str | None = None) -> logging.Logger:
    """Get a logger for the SDK.

    Args:
        name: Optional sub-logger name. If provided, returns a child logger
            under the SDK namespace (e.g., "orcher.client").

    Returns:
        A configured logger instance.

    Example:
        >>> logger = get_logger("client")
        >>> logger.info("Connected to server")
    """
    if name:
        return logging.getLogger(f"{SDK_LOGGER_NAME}.{name}")
    return logging.getLogger(SDK_LOGGER_NAME)


def configure_logging(
    level: int = logging.INFO,
    format: str = DEFAULT_FORMAT,
    handler: logging.Handler | None = None,
) -> None:
    """Configure SDK logging.

    This sets up logging for all SDK components. Call this early in your
    application if you want to see SDK log messages.

    Args:
        level: The logging level (default: INFO).
        format: The log message format.
        handler: Optional custom handler. If not provided, logs to stderr.

    Example:
        >>> import logging
        >>> from orcher._internal import configure_logging
        >>> configure_logging(level=logging.DEBUG)
    """
    logger = logging.getLogger(SDK_LOGGER_NAME)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    if handler is None:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(format))

    logger.addHandler(handler)

    # Don't propagate to root logger
    logger.propagate = False
