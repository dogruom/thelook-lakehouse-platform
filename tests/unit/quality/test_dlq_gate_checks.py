"""dlq_gate post-write checks: each one can fail on its own (review finding: no vacuous check)."""

from __future__ import annotations

import pytest

from lakehouse.quality.jobs.dlq_gate import post_write_problems


CONSISTENT = {"rows_before": 10, "rows_after": 15, "inserted": 5, "missing": 0, "rejected_rows": 7}


def test_consistent_write_has_no_problems() -> None:
    assert post_write_problems(**CONSISTENT) == []


def test_rerun_inserting_nothing_is_consistent() -> None:
    assert (
        post_write_problems(rows_before=15, rows_after=15, inserted=0, missing=0, rejected_rows=7)
        == []
    )


@pytest.mark.parametrize(
    ("change", "fragment"),
    [
        ({"rows_after": 14}, "grew by 4 but the MERGE reported 5"),
        ({"missing": 2}, "2 rejected rows are not in the DLQ table"),
        ({"inserted": 8, "rows_after": 18}, "dlq_inserted=8 > rejected_rows=7"),
    ],
)
def test_each_check_fails_on_its_own(change: dict[str, int], fragment: str) -> None:
    problems = post_write_problems(**{**CONSISTENT, **change})
    assert len(problems) == 1
    assert fragment in problems[0]
