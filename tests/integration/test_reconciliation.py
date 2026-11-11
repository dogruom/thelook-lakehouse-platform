"""Reconciliation end to end (W3.D18, D-081/D-082) against the core stack.

Flow: seeded CDC + clickstream (Kafka) and landed products/orders -> Bronze -> ``dlq_gate`` ->
``silver_customers`` / ``silver_clickstream`` / ``silver_orders`` -> ``reconcile``. Expectations
come from the generator manifests and the landed bytes. ``ops.pipeline_metrics`` is copied out
through the ``seaweedfs-init`` container and read with pyarrow.

Checks: every batch row balances; data-derived totals equal the sum of the batch terms; totals per
(table, brand) equal the manifest on this clean run; ``reconcile`` exits non-zero on a tampered
metrics row (negative). Budget: about 8 jobs and 3 table copies, at most 8 minutes.
"""

from __future__ import annotations

import json
import subprocess
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from .delta_io import DOCKER_TIMEOUT_S, download_table, live_parquet_files, read_rows, temp_topics
from .pipeline_flow import World, build_world, load_bronze, produce_world, silver_all
from .spark_jobs import REPO_ROOT, log_events, submit


if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from .order_flow import Row

pytestmark = pytest.mark.integration

BRANDS = 3
TERMS = (
    "silver_accepted",
    "dlq",
    "duplicates_removed",
    "stale_cdc_noop",
    "late_beyond_window",
)
TABLES = ("raw_customers", "raw_clickstream", "raw_orders")
JOBS = ("silver_customers", "silver_clickstream", "silver_orders")


def batch_violations(rows: list[Row]) -> list[str]:
    """Batch rows whose terms do not add up to ``bronze`` (re-derived, not the stored flag)."""
    problems = []
    for r in rows:
        total = sum(int(r[t] or 0) for t in TERMS)
        if total != int(r["bronze"]):
            problems.append(f"{r['job']} batch {r['batch_id']}: {total} != bronze {r['bronze']}")
        if r["balanced"] is not True:
            problems.append(f"{r['job']} batch {r['batch_id']}: balanced flag is {r['balanced']}")
    return problems


@dataclass
class Scenario:
    world: World
    namespace: str
    metrics_rows: list[Row]
    reconcile_exit: int
    reconcile_out: str


def _metrics(namespace: str, root: Path, tag: str) -> list[Row]:
    dest = root / f"{tag}_ops_pipeline_metrics"
    download_table("ops", "pipeline_metrics", namespace, dest)
    return read_rows(dest)


@pytest.fixture(scope="module")
def scenario(
    bootstrap_servers: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Scenario]:
    namespace = f"it_{uuid.uuid4().hex[:8]}"
    tmp = tmp_path_factory.mktemp("reconciliation")
    world = build_world(
        brands=BRANDS, cdc_events=240, click_events=180, order_batches=4, orders_per_batch=20
    )
    with temp_topics(bootstrap_servers, "rc_cdc", "rc_click") as (cdc_topic, click_topic):
        produce_world(bootstrap_servers, world, cdc_topic, click_topic)
        load_bronze(world, namespace, cdc_topic, click_topic, tmp, max_offsets=120)
    silver_all(namespace)
    code, out = submit(["--skip-build", "reconcile", "--namespace", namespace])
    yield Scenario(world, namespace, _metrics(namespace, tmp, "m"), code, out)


def _batch(sc: Scenario) -> list[Row]:
    return [r for r in sc.metrics_rows if r["scope"] == "batch"]


def _recon(sc: Scenario) -> list[Row]:
    return [r for r in sc.metrics_rows if r["scope"] == "reconcile"]


def test_every_silver_job_recorded_at_least_one_batch_row(scenario: Scenario) -> None:
    assert {r["job"] for r in _batch(scenario)} >= set(JOBS)
    assert all(r["brand_id"] is None for r in _batch(scenario))


def test_every_batch_row_is_balanced(scenario: Scenario) -> None:
    assert batch_violations(_batch(scenario)) == []


def test_clean_run_reconcile_succeeds(scenario: Scenario) -> None:
    assert scenario.reconcile_exit == 0, scenario.reconcile_out[-2000:]
    assert not log_events(scenario.reconcile_out, "reconcile.mismatch")


def test_data_totals_equal_the_sum_of_batch_terms(scenario: Scenario) -> None:
    batch: dict[tuple[str, str], int] = defaultdict(int)
    for r in _batch(scenario):
        for term in ("bronze", "parse_errors", "dlq", "silver_accepted"):
            batch[(r["source_table"], term)] += int(r[term] or 0)
    data: dict[tuple[str, str], int] = defaultdict(int)
    for r in _recon(scenario):
        for term in ("bronze", "parse_errors", "dlq", "silver_accepted"):
            data[(r["source_table"], term)] += int(r[term] or 0)
    for table in TABLES:
        for term in ("bronze", "parse_errors", "dlq"):
            assert data[(table, term)] == batch[(table, term)], (table, term)
    for table in ("raw_orders", "raw_clickstream"):
        assert data[(table, "silver_accepted")] == batch[(table, "silver_accepted")], table


def _totals(sc: Scenario, table: str, term: str) -> dict[str | None, int]:
    out: dict[str | None, int] = defaultdict(int)
    for r in _recon(sc):
        if r["source_table"] == table:
            out[r["brand_id"]] += int(r[term] or 0)
    return dict(out)


def _nonzero(counts: Any) -> dict[str, int]:
    return {k: v for k, v in dict(counts).items() if v}


def test_totals_per_table_and_brand_equal_the_manifests(scenario: Scenario) -> None:
    w, sc = scenario.world, scenario
    truth = w.orders_truth()
    expected = {
        ("raw_customers", "bronze"): w.cdc_emitted(),
        ("raw_customers", "dlq"): w.cdc_dlq(),
        ("raw_clickstream", "bronze"): w.click_emitted(parseable_only=True),
        ("raw_clickstream", "dlq"): w.click_dlq(),
        ("raw_orders", "bronze"): truth["bronze"],
        ("raw_orders", "silver_accepted"): truth["silver_accepted"],
        ("raw_orders", "duplicates_removed"): truth["duplicates_removed"],
        ("raw_orders", "dlq"): truth["dlq"],
    }
    for (table, term), want in expected.items():
        got = {k: v for k, v in _totals(sc, table, term).items() if k is not None and v}
        assert got == _nonzero(want), (table, term)


def test_parse_errors_equal_the_malformed_clickstream_messages(scenario: Scenario) -> None:
    got = sum(_totals(scenario, "raw_clickstream", "parse_errors").values())
    assert got == scenario.world.click_malformed() > 0


def test_reconcile_rows_are_scoped_per_run_and_brand_dimension(scenario: Scenario) -> None:
    keys = [(r["run_id"], r["source_table"], r["brand_id"]) for r in _recon(scenario)]
    assert keys, "reconcile wrote no scope='reconcile' rows"
    assert len(keys) == len(set(keys)), "duplicate (run_id, source_table, brand_id) rows"


# ---- negative: the checks detect a violation ---------------------------------------------------


def _good_row() -> Row:
    return {
        "job": "j",
        "batch_id": 0,
        "bronze": 10,
        "silver_accepted": 6,
        "dlq": 2,
        "duplicates_removed": 1,
        "stale_cdc_noop": 1,
        "late_beyond_window": 0,
        "balanced": True,
    }


def test_batch_violation_check_accepts_a_balanced_row_and_flags_the_broken_ones() -> None:
    assert batch_violations([_good_row()]) == []
    assert batch_violations([{**_good_row(), "silver_accepted": 7}])
    assert batch_violations([{**_good_row(), "balanced": False}])


_AWS_CP = 'aws --endpoint-url "http://$S3_HOST:$S3_PORT" --no-cli-pager --only-show-errors s3 cp'


def _tamper_and_upload(namespace: str, tmp: Path) -> None:
    """Add one to ``silver_accepted`` of a batch row (parquet + patched Delta log) and upload."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = tmp / "tamper_ops_pipeline_metrics"
    download_table("ops", "pipeline_metrics", namespace, root)
    done = False
    for file in live_parquet_files(root):
        table = pq.read_table(file)
        rows = table.to_pylist()
        for row in rows:
            if row["scope"] == "batch" and row["job"] == "silver_orders" and not done:
                row["silver_accepted"] = int(row["silver_accepted"] or 0) + 1
                done = True
        if done:
            pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), file)
            size = file.stat().st_size
            for log in (root / "_delta_log").glob("*.json"):
                lines = []
                for line in log.read_text(encoding="utf-8").splitlines():
                    action = json.loads(line) if line.strip() else {}
                    if "add" in action and file.name in action["add"]["path"]:
                        action["add"]["size"] = size
                        action["add"].pop("stats", None)
                    lines.append(json.dumps(action) if action else line)
                log.write_text("\n".join(lines) + "\n", encoding="utf-8")
            break
    assert done, "no silver_orders batch row to tamper with"
    script = (
        f'{_AWS_CP} /data "s3://$LAKEHOUSE_BUCKET/ops/{namespace}/pipeline_metrics" --recursive'
    )
    proc = subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "core",
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "-v",
            f"{root.resolve().as_posix()}:/data",
            "--entrypoint",
            "/bin/sh",
            "seaweedfs-init",
            "-c",
            script,
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=DOCKER_TIMEOUT_S,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr[-400:]


def test_reconcile_exits_nonzero_on_a_tampered_metrics_row(
    scenario: Scenario, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """Runs last in the module: the tamper changes the namespace's metrics table."""
    assert scenario.reconcile_exit == 0  # the untampered state is clean (not a vacuous failure)
    _tamper_and_upload(scenario.namespace, tmp_path_factory.mktemp("tamper"))
    code, out = submit(["--skip-build", "reconcile", "--namespace", scenario.namespace])
    assert code != 0, f"tampered metrics were accepted; tail:\n{out[-2000:]}"
    assert log_events(out, "reconcile.mismatch"), out[-2000:]
