#!/usr/bin/env python3
"""Strict smoke test v2 for the local ``core`` stack (W1.D2.4).

Run after ``docker compose --profile core up -d``. Exit code 0 only if EVERY check passes; a
missing topic, bucket, prefix or database is a failure, never a warning.

Checks
  1. .env exists (compose cannot start without it)
  2. core containers report ``healthy``
  3. host-published endpoints answer on 127.0.0.1 (Spark master/worker UI, SeaweedFS master)
  4. Kafka topics ``customer_cdc`` and ``clickstream_events`` exist
  5. bucket and prefixes landing/ bronze/ silver/ gold/ dead_letter/ ops/ checkpoints/ exist
  6. PostgreSQL databases ``airflow_metadata`` and ``hive_metastore`` exist
  7. Delta write + read round trip through s3a, executed inside the Spark container on the
     cluster (driver/executor Python parity and Spark version are asserted as well)
  8. information: memory per container (``docker stats --no-stream``) and host spec

Credentials are never read or printed by this script: every command that needs them runs
inside a container that already has them in its environment.

Usage (repo root):  python scripts/smoke_test.py
"""

from __future__ import annotations

import os
import platform
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_PROJECT = "crm_lakehouse"

REQUIRED_TOPICS = ("customer_cdc", "clickstream_events")
REQUIRED_PREFIXES = ("landing", "bronze", "silver", "gold", "dead_letter", "ops", "checkpoints")
REQUIRED_DATABASES = ("airflow_metadata", "hive_metastore")
CORE_CONTAINERS = (
    "crm_postgres",
    "crm_seaweedfs",
    "crm_kafka",
    "crm_spark_master",
    "crm_spark_worker",
)
HTTP_ENDPOINTS = (
    ("SeaweedFS master API", "http://127.0.0.1:9333/cluster/status"),
    ("Spark master UI", "http://127.0.0.1:8181"),
    ("Spark worker UI", "http://127.0.0.1:8182"),
)
EXPECTED_SPARK = "3.5.9"
EXPECTED_PYTHON = "3.11"

# Executed inside the Spark master container (``python3 -`` reads it from stdin). It contains no
# credentials: S3A takes them from the container environment.
SPARK_ROUNDTRIP = r"""
import os
import sys
import uuid

from pyspark.sql import SparkSession

bucket = os.environ["LAKEHOUSE_BUCKET"]
path = "s3a://" + bucket + "/ops/smoke/" + uuid.uuid4().hex

spark = (
    SparkSession.builder.appName("smoke-delta-roundtrip")
    .master("spark://spark-master:7077")
    .config("spark.driver.host", "spark-master")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.executor.memory", "512m")
    .config("spark.cores.max", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
ok = False
try:
    driver_py = "%d.%d" % sys.version_info[:2]
    executor_py = (
        spark.sparkContext.parallelize([0], 1)
        .map(lambda _: "%d.%d" % sys.version_info[:2])
        .collect()[0]
    )
    rows = [(1, "alpha"), (2, "beta"), (3, "gamma")]
    spark.createDataFrame(rows, "id int, name string").write.format("delta").save(path)
    got = sorted((r["id"], r["name"]) for r in spark.read.format("delta").load(path).collect())
    ok = got == rows and driver_py == executor_py
    print(
        "SMOKE_DELTA_RESULT ok=%s rows=%d spark=%s driver_py=%s executor_py=%s"
        % (ok, len(got), spark.version, driver_py, executor_py)
    )
finally:
    # Remove the scratch table (ops/smoke/ is test scratch space, not a pipeline dataset).
    hpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    hpath.getFileSystem(spark._jsc.hadoopConfiguration()).delete(hpath, True)
    spark.stop()
sys.exit(0 if ok else 1)
"""

_results: list[bool] = []


def _short(text: str, limit: int = 300) -> str:
    one_line = " ".join(text.split())
    return one_line if len(one_line) <= limit else one_line[: limit - 3] + "..."


def record(name: str, ok: bool, detail: str = "") -> bool:
    _results.append(ok)
    suffix = f" - {detail}" if detail else ""
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{suffix}")
    return ok


def info(text: str) -> None:
    print(f"  [info] {text}")


def run(
    cmd: list[str], timeout: int = 60, stdin: str | None = None
) -> subprocess.CompletedProcess[str]:
    """Run a command from the repo root; never raises on a non-zero exit code."""
    try:
        return subprocess.run(
            cmd,
            input=stdin,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            cwd=REPO_ROOT,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return subprocess.CompletedProcess(cmd, 124, "", f"{type(exc).__name__}: {exc}")


def compose(*args: str) -> list[str]:
    # Explicit project name: a stale COMPOSE_PROJECT_NAME in .env once made plain
    # ``docker compose`` act on a legacy project instead of this stack.
    return ["docker", "compose", "-p", COMPOSE_PROJECT, "--profile", "core", *args]


def section(title: str) -> None:
    print(f"\n{title}")


def check_env_file() -> bool:
    section("[1/8] Environment file")
    if (REPO_ROOT / ".env").is_file():
        return record(".env present", True)
    return record(".env present", False, "run: python scripts/bootstrap_env.py")


def check_container_health() -> None:
    section("[2/8] Container health")
    for name in CORE_CONTAINERS:
        proc = run(["docker", "inspect", "--format", "{{.State.Health.Status}}", name], 15)
        status = proc.stdout.strip() or _short(proc.stderr, 120) or "unknown"
        record(name, proc.returncode == 0 and status == "healthy", status)


def check_http() -> None:
    section("[3/8] Host endpoints (127.0.0.1)")
    for name, url in HTTP_ENDPOINTS:
        try:
            with urllib.request.urlopen(url, timeout=8) as resp:  # - fixed local URLs
                record(name, resp.status == 200, f"{url} -> HTTP {resp.status}")
        except urllib.error.HTTPError as exc:
            record(name, False, f"{url} -> HTTP {exc.code}")
        except (urllib.error.URLError, OSError) as exc:
            record(name, False, f"{url} -> {type(exc).__name__}")


def check_kafka() -> None:
    section("[4/8] Kafka topics")
    proc = run(
        compose(
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--list",
        ),
        60,
    )
    if proc.returncode != 0:
        record("kafka-topics --list", False, _short(proc.stderr))
        return
    topics = {line.strip() for line in proc.stdout.splitlines() if line.strip()}
    for topic in REQUIRED_TOPICS:
        record(f"topic {topic}", topic in topics)


def check_storage() -> None:
    section("[5/8] Object storage (bucket and prefixes)")
    # One-off container of the seaweedfs-init service: it already has the S3 credentials in its
    # environment. Nothing is pulled or built: its image exists after the first `up`.
    script = 'aws --endpoint-url "http://$S3_HOST:$S3_PORT" --no-cli-pager s3 ls "s3://$LAKEHOUSE_BUCKET/"'
    proc = run(
        compose(
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "--entrypoint",
            "/bin/sh",
            "seaweedfs-init",
            "-c",
            script,
        ),
        120,
    )
    if proc.returncode != 0:
        record("bucket listing", False, _short(proc.stderr))
        return
    record("bucket reachable", True)
    present = {
        parts[1].rstrip("/")
        for parts in (line.split() for line in proc.stdout.splitlines())
        if len(parts) == 2 and parts[0] == "PRE"
    }
    for prefix in REQUIRED_PREFIXES:
        record(f"prefix {prefix}/", prefix in present)


def check_postgres() -> None:
    section("[6/8] PostgreSQL databases")
    proc = run(
        compose(
            "exec",
            "-T",
            "postgres",
            "sh",
            "-c",
            'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT datname FROM pg_database"',
        ),
        60,
    )
    if proc.returncode != 0:
        record("pg_database listing", False, _short(proc.stderr))
        return
    databases = {line.strip() for line in proc.stdout.splitlines() if line.strip()}
    for database in REQUIRED_DATABASES:
        record(f"database {database}", database in databases)


def check_spark_delta() -> None:
    section("[7/8] Spark: Delta write + read via s3a (inside the container, on the cluster)")
    proc = run(compose("exec", "-T", "spark-master", "python3", "-"), 420, stdin=SPARK_ROUNDTRIP)
    line = next(
        (ln for ln in proc.stdout.splitlines() if ln.startswith("SMOKE_DELTA_RESULT")), None
    )
    if proc.returncode != 0 or line is None:
        tail = [ln for ln in proc.stderr.splitlines() if ln.strip()][-3:]
        record("Delta round trip", False, _short(" | ".join(tail) or proc.stdout))
        return
    fields = dict(part.split("=", 1) for part in line.split()[1:])
    record("Delta round trip", fields.get("ok") == "True", f"rows={fields.get('rows')}")
    record(
        "Spark version",
        fields.get("spark") == EXPECTED_SPARK,
        f"{fields.get('spark')} (expected {EXPECTED_SPARK})",
    )
    record(
        "Python parity driver/executor",
        fields.get("driver_py") == fields.get("executor_py") == EXPECTED_PYTHON,
        f"driver={fields.get('driver_py')} executor={fields.get('executor_py')}",
    )


def report_resources() -> None:
    section("[8/8] Resources (information, not pass/fail)")
    stats = run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}",
        ],
        60,
    )
    if stats.returncode == 0:
        rows = [ln.split("\t") for ln in stats.stdout.splitlines() if ln.startswith("crm_")]
        info("memory per container (usage / limit, share of limit):")
        for row in sorted(rows):
            if len(row) == 3:
                info(f"  {row[0]:<24} {row[1]:<22} {row[2]}")
        if not rows:
            info("  no crm_* container is running")
    else:
        info(f"docker stats unavailable: {_short(stats.stderr, 120)}")
    daemon = run(
        [
            "docker",
            "info",
            "--format",
            "{{.NCPU}} CPUs, {{.MemTotal}} bytes RAM, Docker {{.ServerVersion}}, {{.OperatingSystem}}",
        ],
        30,
    )
    info(
        f"host: {platform.platform()}, {os.cpu_count()} logical CPUs, Python {platform.python_version()}"
    )
    if daemon.returncode == 0:
        info(f"docker daemon: {daemon.stdout.strip()}")


def main() -> int:
    print("CRM Lakehouse - strict smoke test v2 (profile: core)")
    if check_env_file():
        check_container_health()
        check_http()
        check_kafka()
        check_storage()
        check_postgres()
        check_spark_delta()
    report_resources()

    failed = _results.count(False)
    print()
    if failed or not _results:
        print(f"RESULT: {failed} check(s) FAILED ({len(_results) - failed}/{len(_results)} passed)")
        print(
            "Debug: docker compose --profile core logs <service>; init containers: crm_kafka_init, crm_seaweedfs_init"
        )
        return 1
    print(f"RESULT: all {len(_results)} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
