"""PII-safe validation error summaries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from pydantic import ValidationError


def safe_errors(exc: ValidationError) -> list[dict[str, Any]]:
    """Return only ``{"loc": tuple, "type": str}`` per error: no input values, messages or URLs.

    DLQ and logging code must use this, never ``exc.errors()``: error inputs and messages can
    contain raw PII (e.g. an invalid email address).
    """
    return [
        {"loc": tuple(e["loc"]), "type": e["type"]}
        for e in exc.errors(include_input=False, include_context=False, include_url=False)
    ]
