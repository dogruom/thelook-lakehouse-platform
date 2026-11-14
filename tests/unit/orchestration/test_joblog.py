"""Job log interface: parse JSON records out of mixed Spark output (offline)."""

from __future__ import annotations

import json

from lakehouse.orchestration import joblog


LINES = [
    "26/11/13 10:00:00 INFO SparkContext: Running Spark version 3.5.9",
    json.dumps({"event": "job.started", "job": "reconcile"}),
    "some prefix " + json.dumps({"event": "job.succeeded", "metrics": {"reconcile_rows": 3}}),
    "{not json",
    json.dumps(["not", "a", "dict"]),
]


def test_parse_records_ignores_noise() -> None:
    records = joblog.parse_records(LINES)
    assert [r["event"] for r in records] == ["job.started", "job.succeeded"]


def test_job_metrics_of_last_success() -> None:
    assert joblog.job_metrics(joblog.parse_records(LINES)) == {"reconcile_rows": 3}
    assert joblog.job_metrics([]) is None


def test_job_metrics_without_a_dict_is_empty() -> None:
    assert joblog.job_metrics([{"event": "job.succeeded"}]) == {}


def test_failure_reason_has_error_type_but_no_traceback() -> None:
    records = [
        {"event": "job.failed", "error_type": "ReconciliationMismatchError", "exception": "TRACE"}
    ]
    reason = joblog.failure_reason("reconcile", 1, records)
    assert (
        reason == "spark job reconcile exited with code 1, error_type=ReconciliationMismatchError"
    )
    assert "TRACE" not in reason
    assert joblog.failure_reason("reconcile", 2, []) == "spark job reconcile exited with code 2"


def test_breached_brands_from_log() -> None:
    records = [
        {"event": "dlq_rate_gate.breach", "brands": ["brand_beta", 7]},
        {"event": "dlq_rate_gate.breach", "brands": ["brand_alpha", "brand_beta"]},
        {"event": "other", "brands": ["brand_gamma"]},
    ]
    assert joblog.breached_brands_from_log(records) == ["brand_alpha", "brand_beta"]
    assert joblog.breached_brands_from_log([]) == []
