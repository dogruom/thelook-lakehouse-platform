"""``_await_all`` failure handling with fake query objects (no Spark)."""

from __future__ import annotations

from typing import Any

import pytest

from lakehouse.ingestion.jobs.bronze_stream import _await_all


class FakeQuery:
    """Minimal StreamingQuery stand-in; ``await_results`` are consumed one per call."""

    def __init__(
        self,
        await_results: list[bool | Exception],
        *,
        active: bool = True,
        error: BaseException | None = None,
        stop_raises: Exception | None = None,
    ) -> None:
        self._results = list(await_results)
        self.isActive = active
        self._error = error
        self._stop_raises = stop_raises
        self.stop_calls = 0

    def awaitTermination(self, _timeout: float | None = None) -> bool:
        result = self._results.pop(0)
        if isinstance(result, Exception):
            self.isActive = False
            raise result
        if result:
            self.isActive = False
        return result

    def exception(self) -> BaseException | None:
        return self._error

    def stop(self) -> None:
        self.stop_calls += 1
        if self._stop_raises is not None:
            raise self._stop_raises
        self.isActive = False


def _queries(**kwargs: FakeQuery) -> Any:
    return dict(kwargs)


def test_all_queries_succeeding_returns_without_stopping_anything() -> None:
    a, b = FakeQuery([True]), FakeQuery([False, True])
    _await_all(_queries(raw_customers=a, raw_clickstream=b))
    assert (a.stop_calls, b.stop_calls) == (0, 0)


def test_failure_of_one_query_stops_the_other_and_propagates_the_original_error() -> None:
    a = FakeQuery([False, False, False])  # still running
    b = FakeQuery([ValueError("boom")])
    with pytest.raises(ValueError, match="boom"):
        _await_all(_queries(raw_customers=a, raw_clickstream=b))
    assert a.stop_calls == 1


def test_stop_failure_does_not_mask_the_original_error() -> None:
    a = FakeQuery([False, False], stop_raises=OSError("stop failed"))
    b = FakeQuery([ValueError("original")])
    with pytest.raises(ValueError, match="original"):
        _await_all(_queries(raw_customers=a, raw_clickstream=b))
    assert a.stop_calls == 1


def test_recorded_exception_after_clean_termination_raises_naming_the_table() -> None:
    ok = FakeQuery([True])
    bad = FakeQuery([True], error=Exception("late failure"))
    with pytest.raises(RuntimeError, match="raw_clickstream"):
        _await_all(_queries(raw_customers=ok, raw_clickstream=bad))


def test_negative_a_query_without_exception_does_not_raise() -> None:
    """Guards the previous test: no recorded exception means no RuntimeError."""
    _await_all(_queries(raw_customers=FakeQuery([True], error=None)))
