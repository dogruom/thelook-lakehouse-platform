"""Crash mid-batch, re-submit, compare with a clean reference (W3.D18, D-083/D-084).

Two namespaces read the same topic and the same landed files. The reference runs every job once.
The crash namespace kills ``silver_customers`` and ``silver_orders`` (external SIGKILL of the driver
at the start of batch 1, ``maxFilesPerTrigger=1``) and re-submits them with the same checkpoint.
The final Silver content must equal the reference. Comparison is twofold: content hashes from the
``content_hash`` job (customers: identical lineage; orders/items: the landed ``_bronze_ref``
embeds the namespace, so the host-side comparison normalises it) and a host-side row multiset.

No sleeps for ordering: events are awaited with timeouts. Whether the kill really landed
mid-batch is proven from the checkpoint (planned batches > committed batches); otherwise the test
is skipped with that reason. Budget: about 16 job runs, at most 10 minutes.
"""

from __future__ import annotations

import uuid
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from .delta_io import temp_topics
from .order_flow import Row, run, snapshot
from .pipeline_flow import build_world, checkpoint_progress, load_bronze, produce_world
from .spark_jobs import (
    kill_job_processes,
    metrics,
    run_ok,
    submit_streaming,
)


if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

TABLES = ("orders", "order_items", "customers")
KILL_AT_BATCH = 1
BATCH_WAIT_S = 240.0
VOLATILE = {"_updated_at"}


@dataclass
class Outcome:
    skipped_reason: str | None
    ref_hash: dict[str, str]
    crash_hash: dict[str, str]
    crash_hash_again: dict[str, str]
    ref_rows: dict[str, list[Row]]
    crash_rows: dict[str, list[Row]]
    kills: int
    ref_ns: str
    crash_ns: str


def _hashes(output: str) -> dict[str, str]:
    """``{table: hash}`` from the ``content_hash`` metrics (accepts ``silver.`` prefixed keys)."""
    m = metrics(output)
    found: Any = m.get("hashes", m)
    return {
        str(k).removeprefix("silver."): str(v)
        for k, v in dict(found).items()
        if str(k).removeprefix("silver.") in TABLES
    }


def _content_hashes(namespace: str) -> dict[str, str]:
    args = ["content_hash", "--namespace", namespace]
    for table in TABLES:
        args += ["--table", table]
    return _hashes(run(args))


def _normalise(namespace: str, rows: list[Row]) -> list[str]:
    out = []
    for r in rows:
        clean = {
            k: (v.replace(namespace, "NS") if isinstance(v, str) else v)
            for k, v in r.items()
            if k not in VOLATILE
        }
        out.append(repr(sorted(clean.items(), key=lambda kv: kv[0])))
    return sorted(out)


def _crash(job: str, table: str, namespace: str) -> bool:
    """Kill ``job`` at the start of batch ``KILL_AT_BATCH``, re-submit it; True when mid-batch."""
    streaming = submit_streaming(
        ["--skip-build", job, "--namespace", namespace, "--max-files-per-trigger", "1"],
        timeout_s=600,
        namespace=namespace,
    )
    try:
        while True:
            event = streaming.wait_for_event(f"{job}.batch_started", BATCH_WAIT_S)
            assert event is not None, f"{job} never reached batch {KILL_AT_BATCH}"
            if int(event["batch_id"]) >= KILL_AT_BATCH:
                break
        kill_job_processes(namespace)
        code, out = streaming.wait(timeout_s=300)
    finally:
        streaming.close()
    assert code != 0 and "job.succeeded" not in out, "job finished before the kill (vacuous)"
    planned, committed = checkpoint_progress(job, table, namespace)
    run_ok(["--skip-build", job, "--namespace", namespace])
    return planned > committed


@pytest.fixture(scope="module")
def outcome(bootstrap_servers: str, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Outcome]:
    tmp = tmp_path_factory.mktemp("rerun")
    world = build_world(
        brands=2, cdc_events=160, click_events=0, order_batches=6, orders_per_batch=15
    )
    ref, crash = (f"it_{uuid.uuid4().hex[:8]}" for _ in range(2))
    with temp_topics(bootstrap_servers, "ir_cdc", "ir_click") as (cdc_topic, click_topic):
        produce_world(bootstrap_servers, world, cdc_topic, click_topic)
        for ns in (ref, crash):
            load_bronze(world, ns, cdc_topic, click_topic, tmp, max_offsets=40)

    run(["silver_customers", "--namespace", ref])
    run(["silver_orders", "--namespace", ref])

    mid = _crash("silver_customers", "customers", crash)
    mid_orders = _crash("silver_orders", "orders", crash) if mid else False
    reason = None
    if not (mid and mid_orders):
        reason = "the kill did not land between offset planning and commit (not a mid-batch crash)"

    yield Outcome(
        reason,
        _content_hashes(ref),
        _content_hashes(crash),
        _content_hashes(crash),
        {t: snapshot("silver", t, ref, tmp, f"ref_{t}") for t in TABLES},
        {t: snapshot("silver", t, crash, tmp, f"crash_{t}") for t in TABLES},
        2,
        ref,
        crash,
    )


def _require(o: Outcome) -> None:
    if o.skipped_reason:
        pytest.skip(o.skipped_reason)


def test_crash_then_resubmit_equals_the_fresh_namespace_reference_by_content_hash(
    outcome: Outcome,
) -> None:
    _require(outcome)
    assert set(outcome.ref_hash) == set(TABLES)
    assert outcome.crash_hash["customers"] == outcome.ref_hash["customers"]


def test_content_hash_is_deterministic_for_unchanged_data(outcome: Outcome) -> None:
    assert outcome.crash_hash == outcome.crash_hash_again


def test_crash_then_resubmit_equals_the_reference_row_for_row(outcome: Outcome) -> None:
    _require(outcome)
    ref_ns, crash_ns = outcome.ref_ns, outcome.crash_ns
    for table in TABLES:
        assert outcome.ref_rows[table], f"reference {table} is empty (vacuous)"
        assert _normalise(crash_ns, outcome.crash_rows[table]) == _normalise(
            ref_ns, outcome.ref_rows[table]
        ), table


def test_no_duplicate_business_keys_after_the_crash(outcome: Outcome) -> None:
    _require(outcome)
    for table, key in (
        ("orders", ("brand_id", "order_id")),
        ("order_items", ("brand_id", "order_id", "item_id")),
        ("customers", ("brand_id", "customer_id")),
    ):
        counts = Counter(tuple(r[k] for k in key) for r in outcome.crash_rows[table])
        assert max(counts.values()) == 1, table


# ---- negative: the comparison detects a difference ------------------------------------------


def test_normalised_comparison_detects_a_changed_a_missing_and_a_duplicated_row() -> None:
    base = [
        {"brand_id": "a", "order_id": "o1", "x": 1},
        {"brand_id": "a", "order_id": "o2", "x": 2},
    ]
    assert _normalise("n", base) == _normalise("n", [dict(r) for r in reversed(base)])
    assert _normalise("n", base) != _normalise("n", [base[0], {**base[1], "x": 3}])
    assert _normalise("n", base) != _normalise("n", base[:1])
    assert _normalise("n", base) != _normalise("n", [*base, base[0]])
    ts = {"brand_id": "a", "order_id": "o1", "x": 1, "_updated_at": 1}
    assert _normalise("n", [ts]) == _normalise("n", [{**ts, "_updated_at": 2}])
