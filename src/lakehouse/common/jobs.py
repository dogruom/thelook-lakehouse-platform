"""Job framework: one entry point, one run id, one log context for every Spark job.

A job is a :class:`SparkJob` subclass exposed as ``JOB`` in the module
``lakehouse.<component>.jobs.<name>`` (component in ``ingestion``, ``quality``, ``silver``,
``ops``). The short name ``<name>`` is resolved by this convention with ``importlib``, so
``common`` keeps no static import of, and no shared registry for, any component (ADR-0009).

``main`` is what ``scripts/spark/job_entry.py`` calls under ``spark-submit``. Every log line of a
run carries ``job``, ``run_id`` and ``env`` through structlog context variables.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

import structlog

from lakehouse.common.logging import configure_logging, get_logger
from lakehouse.common.spark import JOB_NAME_PATTERN, SparkSettings, get_spark_session, new_run_id


if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from pyspark.sql import SparkSession


JOB_COMPONENTS = ("ingestion", "quality", "silver", "ops")
_EXIT_USAGE = 2


@dataclass(frozen=True)
class JobContext:
    """Per-run facts handed to a job."""

    job_name: str
    run_id: str
    settings: SparkSettings


class SparkJob(ABC):
    """Base class of every Spark job."""

    name: ClassVar[str]

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:  # noqa: B027
        """Declare job-specific CLI arguments (default: none)."""

    @abstractmethod
    def run(
        self, spark: SparkSession, args: argparse.Namespace, ctx: JobContext
    ) -> Mapping[str, Any]:
        """Execute the job and return metrics (logged when the job succeeds)."""


class UnknownJobError(ValueError):
    """No unique job module for the requested name."""


def _find_modules(name: str) -> list[str]:
    found: list[str] = []
    for component in JOB_COMPONENTS:
        module = f"lakehouse.{component}.jobs.{name}"
        try:
            spec = importlib.util.find_spec(module)
        except ModuleNotFoundError as exc:
            # A missing parent package means "no such job"; a broken dependency must surface.
            if exc.name and (module == exc.name or module.startswith(f"{exc.name}.")):
                continue
            raise
        if spec is not None:
            found.append(module)
    return found


def resolve_job(name: str) -> type[SparkJob]:
    """Resolve ``<name>`` to its job class by convention (see module docstring)."""
    if not JOB_NAME_PATTERN.match(name):
        raise UnknownJobError(f"invalid job name {name!r}")
    modules = _find_modules(name)
    if not modules:
        raise UnknownJobError(f"unknown job {name!r}")
    if len(modules) > 1:
        raise UnknownJobError(f"ambiguous job {name!r}: {', '.join(modules)}")
    cls = getattr(importlib.import_module(modules[0]), "JOB", None)
    if not (isinstance(cls, type) and issubclass(cls, SparkJob)):
        raise TypeError(f"{modules[0]}.JOB must be a SparkJob subclass")
    if cls.name != name:
        raise TypeError(f"{modules[0]}.JOB.name is {cls.name!r}, expected {name!r}")
    return cls


def main(
    argv: Sequence[str] | None = None,
    *,
    session_factory: Callable[..., Any] | None = None,
    settings: SparkSettings | None = None,
    stop_session: bool = True,
) -> int:
    """Run ``argv[0]`` with ``argv[1:]`` as its arguments. Exit codes: 0 ok, 1 failed, 2 usage."""
    configure_logging()
    log = get_logger(__name__)
    args_in = list(sys.argv[1:] if argv is None else argv)
    if not args_in:
        log.error("job.missing_name")
        return _EXIT_USAGE
    name, rest = args_in[0], args_in[1:]

    try:
        job = resolve_job(name)()
    except UnknownJobError as exc:
        log.error("job.unknown", job=name, reason=str(exc))
        return _EXIT_USAGE

    parser = argparse.ArgumentParser(prog=name)
    job.add_arguments(parser)
    try:
        args = parser.parse_args(rest)
    except SystemExit as exc:  # argparse already printed usage; no traceback for bad input
        return exc.code if isinstance(exc.code, int) else _EXIT_USAGE

    settings = settings or SparkSettings()
    run_id = new_run_id(name)
    factory = session_factory or get_spark_session
    structlog.contextvars.bind_contextvars(job=name, run_id=run_id, env=settings.env)
    session = None
    try:
        log.info("job.started")
        started = time.monotonic()
        session = factory(settings, app_name=f"lakehouse-{name}", run_id=run_id)
        metrics = job.run(session, args, JobContext(name, run_id, settings))
        log.info(
            "job.succeeded", duration_s=round(time.monotonic() - started, 3), metrics=dict(metrics)
        )
        return 0
    except Exception as exc:
        log.exception("job.failed", error_type=type(exc).__name__)
        return 1
    finally:
        if session is not None and stop_session:
            session.stop()
        structlog.contextvars.unbind_contextvars("job", "run_id", "env")


if __name__ == "__main__":
    raise SystemExit(main())
