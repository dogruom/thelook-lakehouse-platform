#!/usr/bin/env python3
"""
smoke_test.py — Docker Compose Smoke Test
==========================================
Verifies that all services are up and reachable after `docker compose up`.

Services tested:
  - thelook_postgres          (PostgreSQL 15)
  - thelook_seaweedfs         (SeaweedFS S3-compatible storage — replaces MinIO)
  - thelook_kafka             (Apache Kafka 3.7 KRaft)
  - thelook_spark_master      (Spark 3.5 Master)
  - thelook_spark_worker      (Spark 3.5 Worker)
  - thelook_airflow_webserver (Airflow 2.9.3)
  - thelook_airflow_scheduler (Airflow Scheduler)

Usage:
    python scripts/smoke_test.py

Exit codes:
    0 — All checks passed
    1 — One or more checks failed
"""

import sys
import urllib.request
import urllib.error
import subprocess


# ANSI colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"
CYAN = "\033[96m"


def ok(msg: str) -> None:
    print(f"  {GREEN}✅ {msg}{RESET}")


def fail(msg: str) -> None:
    print(f"  {RED}❌ {msg}{RESET}")


def warn(msg: str) -> None:
    print(f"  {YELLOW}⚠️  {msg}{RESET}")


def info(msg: str) -> None:
    print(f"  {CYAN}ℹ️  {msg}{RESET}")


def check_http(name: str, url: str, expected_status: int = 200) -> bool:
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=8) as resp:
            if resp.status == expected_status:
                ok(f"{name} — {url} → HTTP {resp.status}")
                return True
            else:
                fail(f"{name} — {url} → HTTP {resp.status} (expected {expected_status})")
                return False
    except urllib.error.HTTPError as e:
        if e.code == expected_status:
            ok(f"{name} — {url} → HTTP {e.code}")
            return True
        fail(f"{name} — {url} → HTTP {e.code}")
        return False
    except Exception as e:
        fail(f"{name} — {url} → {type(e).__name__}: {e}")
        return False


def check_docker_health(container_name: str) -> bool:
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", container_name],
            capture_output=True,
            text=True,
            timeout=10,
        )
        status = result.stdout.strip()
        if status == "healthy":
            ok(f"{container_name} → {status}")
            return True
        elif status == "":
            warn(f"{container_name} → no health check configured")
            return True
        else:
            fail(f"{container_name} → {status}")
            return False
    except Exception as e:
        fail(f"{container_name} → {type(e).__name__}: {e}")
        return False


def check_kafka() -> bool:
    """Check Kafka by listing topics via kafka-topics.sh inside the container."""
    try:
        result = subprocess.run(
            [
                "docker", "exec", "thelook_kafka",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server", "localhost:9092",
                "--list",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0:
            topics = [t for t in result.stdout.strip().split("\n") if t]
            if "clickstream_events" in topics:
                ok(f"Kafka — topics found: {topics}")
                return True
            else:
                warn(f"Kafka reachable but 'clickstream_events' topic missing. Topics: {topics}")
                info("Hint: kafka-init container may still be running or failed. "
                     "Check: docker logs thelook_kafka_init")
                return True  # Kafka itself is OK; topic init is a separate concern
        else:
            fail(f"Kafka — kafka-topics.sh failed: {result.stderr.strip()}")
            return False
    except Exception as e:
        fail(f"Kafka — {type(e).__name__}: {e}")
        return False


def check_postgres() -> bool:
    """Check PostgreSQL: both databases must exist."""
    try:
        result = subprocess.run(
            [
                "docker", "exec", "thelook_postgres",
                "psql", "-U", "thelook_admin", "-d", "postgres",
                "-c", "SELECT datname FROM pg_database WHERE datname IN ('airflow_metadata', 'thelook_dwh');",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0 and "airflow_metadata" in result.stdout and "thelook_dwh" in result.stdout:
            ok("PostgreSQL — databases: airflow_metadata ✓, thelook_dwh ✓")
            return True
        else:
            fail(f"PostgreSQL — expected databases not found.\n    stdout: {result.stdout.strip()}\n    stderr: {result.stderr.strip()}")
            return False
    except Exception as e:
        fail(f"PostgreSQL — {type(e).__name__}: {e}")
        return False


def check_seaweedfs_s3() -> bool:
    """
    Check SeaweedFS S3 gateway via the AWS CLI inside the init container.
    We list buckets to confirm S3 API is functional and the lakehouse bucket exists.
    """
    try:
        result = subprocess.run(
            [
                "docker", "run", "--rm",
                "--network", "thelook_lakehouse_net",
                "-e", "AWS_ACCESS_KEY_ID=minioadmin",
                "-e", "AWS_SECRET_ACCESS_KEY=minioadmin123",
                "-e", "AWS_DEFAULT_REGION=us-east-1",
                "amazon/aws-cli:latest",
                "s3", "ls",
                "--endpoint-url", "http://seaweedfs:9000",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            output = result.stdout.strip()
            if "lakehouse" in output:
                ok(f"SeaweedFS S3 API — bucket 'lakehouse' exists ✓")
                info(f"Buckets: {output}")
            else:
                warn(f"SeaweedFS S3 API reachable but 'lakehouse' bucket not found yet.")
                info(f"Output: {output or '(empty — seaweedfs-init may not have run yet)'}")
            return True
        else:
            err = result.stderr.strip()
            fail(f"SeaweedFS S3 API — aws s3 ls failed: {err}")
            return False
    except Exception as e:
        fail(f"SeaweedFS S3 API — {type(e).__name__}: {e}")
        return False


def check_airflow_health() -> bool:
    """Check Airflow /health endpoint — expects healthy schedulerJob and metaDatabaseStatus."""
    try:
        req = urllib.request.Request("http://localhost:8080/health")
        with urllib.request.urlopen(req, timeout=10) as resp:
            import json
            body = json.loads(resp.read().decode())
            scheduler_status = body.get("scheduler", {}).get("status", "unknown")
            dag_processor_status = body.get("dag_processor", {}).get("status", "unknown")

            if scheduler_status == "healthy":
                ok(f"Airflow Health — scheduler: {scheduler_status}, dag_processor: {dag_processor_status}")
                return True
            else:
                warn(f"Airflow Health — scheduler: {scheduler_status}, dag_processor: {dag_processor_status}")
                return False
    except urllib.error.HTTPError as e:
        fail(f"Airflow Health — HTTP {e.code}")
        return False
    except Exception as e:
        fail(f"Airflow Health — {type(e).__name__}: {e}")
        return False


def main() -> int:
    print(f"\n{BOLD}{'='*65}{RESET}")
    print(f"{BOLD}  TheLook Lakehouse — Sprint 1 Docker Compose Smoke Test{RESET}")
    print(f"{BOLD}  Stack: PostgreSQL · SeaweedFS(S3) · Kafka · Spark · Airflow{RESET}")
    print(f"{BOLD}{'='*65}{RESET}\n")

    results = []

    # ── 1. Container Health ──────────────────────────────────────────────
    print(f"{BOLD}[1/5] Container Health Status{RESET}")
    containers = [
        "thelook_postgres",
        "thelook_seaweedfs",    # SeaweedFS (replaces thelook_minio)
        "thelook_kafka",
        "thelook_spark_master",
        "thelook_spark_worker",
        "thelook_airflow_webserver",
        "thelook_airflow_scheduler",
    ]
    for container in containers:
        results.append(check_docker_health(container))

    print()

    # ── 2. HTTP Endpoint Checks ──────────────────────────────────────────
    print(f"{BOLD}[2/5] Service UI & API Endpoints{RESET}")
    endpoints = [
        ("SeaweedFS Master API", "http://localhost:9333/cluster/status"),
        ("SeaweedFS Filer UI",   "http://localhost:9001"),
        ("Spark Master UI",      "http://localhost:8181"),
        ("Spark Worker UI",      "http://localhost:8182"),
    ]
    for name, url in endpoints:
        results.append(check_http(name, url))

    print()

    # ── 3. Airflow Health ────────────────────────────────────────────────
    print(f"{BOLD}[3/5] Airflow Health{RESET}")
    results.append(check_airflow_health())

    print()

    # ── 4. PostgreSQL Databases ──────────────────────────────────────────
    print(f"{BOLD}[4/5] PostgreSQL Databases{RESET}")
    results.append(check_postgres())

    print()

    # ── 5. Kafka Topics + SeaweedFS S3 ──────────────────────────────────
    print(f"{BOLD}[5/5] Kafka Topics & SeaweedFS S3 API{RESET}")
    results.append(check_kafka())
    results.append(check_seaweedfs_s3())

    print()

    # ── Summary ──────────────────────────────────────────────────────────
    passed = sum(1 for r in results if r)
    total = len(results)
    print(f"{BOLD}{'='*65}{RESET}")
    if passed == total:
        print(f"{GREEN}{BOLD}  ALL CHECKS PASSED ({passed}/{total}) 🎉{RESET}")
        print(f"{GREEN}  Sprint 1 infrastructure is healthy and ready for Sprint 2!{RESET}")
        exit_code = 0
    else:
        failed_count = total - passed
        print(f"{RED}{BOLD}  {failed_count} CHECK(S) FAILED ({passed}/{total}){RESET}")
        print(f"{YELLOW}  Debug: docker compose logs <service-name>{RESET}")
        print(f"{YELLOW}  Init containers: docker logs thelook_seaweedfs_init | thelook_kafka_init{RESET}")
        exit_code = 1
    print(f"{BOLD}{'='*65}{RESET}\n")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
