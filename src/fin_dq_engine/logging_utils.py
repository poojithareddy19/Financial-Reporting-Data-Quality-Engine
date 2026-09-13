"""Structured JSON logging shared by the CLI, local runner and Lambda handlers."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import structlog


def configure_logging(level: str = "INFO", json_output: bool = True) -> None:
    """Configure structlog to emit one JSON object per line (CloudWatch friendly)."""
    logging.basicConfig(level=level, format="%(message)s")
    renderer: Any = structlog.processors.JSONRenderer() if json_output else structlog.dev.ConsoleRenderer()
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        logger_factory=structlog.PrintLoggerFactory(),  # resolves sys.stdout per logger, safe under capture
        cache_logger_on_first_use=False,
    )


def get_logger(name: str) -> Any:
    """Return a bound structlog logger."""
    return structlog.get_logger(name)


@contextmanager
def timed_stage(logger: Any, stage: str, **context: Any) -> Iterator[dict[str, Any]]:
    """Log stage start/finish and expose a dict where the caller can drop metrics.

    Example:
        with timed_stage(log, "transform", batch_id=bid) as info:
            info["rows_out"] = 42
    """
    start = time.perf_counter()
    logger.info("stage_started", stage=stage, **context)
    info: dict[str, Any] = {}
    try:
        yield info
    except Exception as exc:
        info["duration_seconds"] = round(time.perf_counter() - start, 3)
        logger.error("stage_failed", stage=stage, error=str(exc), **{**context, **info})
        raise
    info["duration_seconds"] = round(time.perf_counter() - start, 3)
    logger.info("stage_finished", stage=stage, **{**context, **info})
