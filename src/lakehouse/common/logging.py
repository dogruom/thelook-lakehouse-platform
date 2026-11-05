"""Structured logging for every component.

Configures structlog with:
  - human-readable coloured output when ``ENVIRONMENT`` is ``development``,
  - JSON output otherwise (``local``, ``aws``, CI) for log aggregation,
  - consistent fields: timestamp, level, logger, event, bound context (e.g. ``brand_id``),
  - a stdlib logging bridge so third-party logs (boto3, Airflow, ...) share the format.

Never pass secrets or raw PII as log fields.
"""

from __future__ import annotations

import logging
import os
import sys

import structlog


# ---------------------------------------------------------------------------
# Shared processors (applied to every log event regardless of renderer)
# ---------------------------------------------------------------------------

SHARED_PROCESSORS: list[structlog.types.Processor] = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.TimeStamper(fmt="iso", utc=True),
    structlog.processors.StackInfoRenderer(),
]


def configure_logging(
    level: str | None = None,
    environment: str | None = None,
    force: bool = False,
) -> None:
    """
    Configure structlog and stdlib logging for the project.

    Call once at application startup:
        from lakehouse.common.logging import configure_logging
        configure_logging()

    Args:
        level:       Log level string (e.g. "INFO", "DEBUG"). Defaults to LOG_LEVEL env var.
        environment: "development" | "production". Defaults to ENVIRONMENT env var.
        force:       Re-configure even if already set up (useful in tests).
    """
    if structlog.is_configured() and not force:
        return

    resolved_level = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    resolved_env = (environment or os.getenv("ENVIRONMENT") or "development").lower()

    # --- Stdlib bridge ---
    # This ensures that logs from boto3, kafka-python, etc. go through structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=resolved_level,
        force=True,
    )

    # Silence overly verbose loggers from dependencies
    for noisy_logger in [
        "boto3",
        "botocore",
        "s3transfer",
        "urllib3",
        "kafka",
        "pika",
    ]:
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)

    # --- Renderer selection ---
    renderer: structlog.types.Processor
    if resolved_env == "development":
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        # JSON everywhere else (local stack, AWS, CI) — parseable by log aggregators
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            *SHARED_PROCESSORS,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Apply formatter to root handler
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=SHARED_PROCESSORS,
    )
    root_handler = logging.StreamHandler(sys.stdout)
    root_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(root_handler)
    root_logger.setLevel(resolved_level)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Get a bound structlog logger.

    Usage:
        from lakehouse.common.logging import get_logger
        log = get_logger(__name__)
        log.info("event.name", key="value")
    """
    configure_logging()  # safe: no-op if already configured
    logger: structlog.stdlib.BoundLogger = structlog.stdlib.get_logger(name)
    return logger
