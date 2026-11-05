"""Unit tests for the shared structured logging module."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
import structlog

from lakehouse.common.logging import configure_logging, get_logger


if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def _reset_structlog() -> Iterator[None]:
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    yield
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


def _last_json_line(text: str) -> dict[str, object]:
    lines = [line for line in text.splitlines() if line.strip()]
    return json.loads(lines[-1])


def test_non_development_environment_emits_json_with_context(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level="INFO", environment="local", force=True)
    structlog.contextvars.bind_contextvars(brand_id="brand_alpha")

    get_logger("tests.logging").info("batch.completed", rows=42)

    record = _last_json_line(capsys.readouterr().out)
    assert record["event"] == "batch.completed"
    assert record["rows"] == 42
    assert record["brand_id"] == "brand_alpha"
    assert record["level"] == "info"
    assert "timestamp" in record


def test_development_environment_renders_human_readable_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level="INFO", environment="development", force=True)

    get_logger("tests.logging").info("dev.event")

    output = capsys.readouterr().out
    assert "dev.event" in output
    with pytest.raises(json.JSONDecodeError):
        json.loads(output.strip().splitlines()[-1])


def test_level_filters_lower_severity(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(level="WARNING", environment="local", force=True)

    log = get_logger("tests.logging")
    log.info("hidden.event")
    log.warning("visible.event")

    output = capsys.readouterr().out
    assert "visible.event" in output
    assert "hidden.event" not in output


def test_configure_without_force_keeps_existing_configuration(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level="INFO", environment="local", force=True)
    configure_logging(level="INFO", environment="development")  # no-op: already configured

    get_logger("tests.logging").info("still.json")

    assert _last_json_line(capsys.readouterr().out)["event"] == "still.json"
