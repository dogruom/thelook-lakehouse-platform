"""Client-mode submit helpers: same zip identity as the host wrapper, command shape (offline)."""

from __future__ import annotations

import importlib.util
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from lakehouse.orchestration import submit


if TYPE_CHECKING:
    from types import ModuleType

REPO = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="module")
def host_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "_host_submit_job", REPO / "scripts/submit_job.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_entry_source_is_the_host_entry_file() -> None:
    on_disk = (REPO / "scripts" / "spark" / "job_entry.py").read_bytes().replace(b"\r\n", b"\n")
    assert submit.ENTRY_SOURCE.encode("utf-8") == on_disk


def test_digest_and_zip_name_match_the_host_wrapper(host_script: ModuleType) -> None:
    items = submit.collect_sources(REPO / "src")
    assert items == host_script._collect_sources(REPO / "src")
    assert submit.digest(items) == host_script.source_hash(REPO / "src")
    assert submit.zip_name(submit.digest(items)) == host_script.zip_name(submit.digest(items))


def _fake_src(root: Path, body: str = "X = 1\n") -> Path:
    pkg = root / "lakehouse" / "sub"
    pkg.mkdir(parents=True)
    (root / "lakehouse" / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "mod.py").write_text(body, encoding="utf-8")
    (pkg / "data.json").write_text("{}", encoding="utf-8")
    (pkg / "skip.txt").write_text("no", encoding="utf-8")
    (root / "lakehouse" / "__pycache__").mkdir()
    (root / "lakehouse" / "__pycache__" / "x.py").write_text("no", encoding="utf-8")
    return root


def test_stage_app_is_content_addressed_and_reused(tmp_path: Path) -> None:
    src = _fake_src(tmp_path / "src")
    dist = tmp_path / "dist"
    zip1, entry = submit.stage_app(src, dist)
    zip2, _ = submit.stage_app(src, dist)
    assert zip1 == zip2
    assert entry.read_text(encoding="utf-8") == submit.ENTRY_SOURCE
    with zipfile.ZipFile(zip1) as zf:
        assert zf.namelist() == [
            "lakehouse/__init__.py",
            "lakehouse/sub/data.json",
            "lakehouse/sub/mod.py",
        ]
    assert not list(dist.glob(".*.tmp"))


def test_changed_source_gives_a_new_zip(tmp_path: Path) -> None:
    src = _fake_src(tmp_path / "src")
    first, _ = submit.stage_app(src, tmp_path / "dist")
    (src / "lakehouse" / "sub" / "mod.py").write_text("X = 2\n", encoding="utf-8")
    second, _ = submit.stage_app(src, tmp_path / "dist")
    assert first != second


def test_stage_app_without_sources_fails(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        submit.stage_app(tmp_path / "empty", tmp_path / "dist")


def test_submit_command_is_client_mode_without_secrets() -> None:
    cmd = submit.build_submit_command(
        "silver_orders",
        ["--namespace", "it_1"],
        app_zip="/d/lakehouse_app-abc.zip",
        entry="/d/job_entry.py",
        driver_host="airflow-scheduler",
        marker="m1",
    )
    assert cmd[0] == "/opt/spark/bin/spark-submit"
    assert cmd[cmd.index("--master") + 1] == "spark://spark-master:7077"
    assert cmd[cmd.index("--deploy-mode") + 1] == "client"
    assert "spark.driver.host=airflow-scheduler" in cmd
    assert "spark.driver.bindAddress=0.0.0.0" in cmd
    assert cmd[cmd.index("--py-files") + 1] == "/d/lakehouse_app-abc.zip"
    assert cmd[-4:] == ["/d/job_entry.py", "silver_orders", "--namespace", "it_1"][-4:]
    assert cmd[cmd.index("/d/job_entry.py") + 1] == "silver_orders"
    assert not any(w in " ".join(cmd).lower() for w in ("password", "secret", "key="))


@pytest.mark.parametrize("job", ["Bad", "x;rm", "", "a b"])
def test_invalid_job_name_is_rejected(job: str) -> None:
    with pytest.raises(ValueError, match="job"):
        submit.build_submit_command(job, [], app_zip="z", entry="e", driver_host="h", marker="m")


def test_invalid_driver_host_is_rejected() -> None:
    with pytest.raises(ValueError, match="host"):
        submit.build_submit_command(
            "reconcile", [], app_zip="z", entry="e", driver_host="a b;c", marker="m"
        )


def test_dlq_rate_gate_args_and_dbt_command() -> None:
    assert submit.dlq_rate_gate_args("brand_beta", 0.15) == [
        "--brand",
        "brand_beta",
        "--max-dlq-rate",
        "0.15",
    ]
    cmd = submit.dbt_build_command("/x/dbt", ("staging", "marts"), target_path="/t", log_path="/l")
    assert cmd[:5] == ["/x/dbt", "build", "--select", "staging", "marts"]
    assert cmd[cmd.index("--target-path") + 1] == "/t"
    assert cmd[cmd.index("--profiles-dir") + 1] == "profiles"


def test_dbt_subprocess_env_drops_secret_looking_variables() -> None:
    from lakehouse.orchestration.submit import dbt_subprocess_env

    kept = {
        "PATH": "/usr/bin",
        "HOME": "/home/airflow",
        "DBT_SPARK_HOST": "spark-thrift",
        "DBT_SPARK_PORT": "10000",
    }
    dropped = {
        name: "x"
        for name in (
            "PII_HMAC_KEY",
            "S3_ACCESS_KEY",
            "S3_SECRET_KEY",
            "AIRFLOW__CORE__FERNET_KEY",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
            "AIRFLOW__API_AUTH__JWT_SECRET",
            "POSTGRES_PASSWORD",
            "ALERT_WEBHOOK_URL",
            "aws_session_token",
        )
    }
    assert dbt_subprocess_env({**kept, **dropped}) == kept
