"""Host-side spark-submit wrapper: pure helpers only (no Docker, no subprocess)."""

from __future__ import annotations

import importlib.util
import inspect
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest


if TYPE_CHECKING:
    from types import ModuleType

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "submit_job.py"
WHEEL = "multitenant_crm_lakehouse-0.1.0-py3-none-any.whl"


@pytest.fixture(scope="module")
def submit() -> ModuleType:
    if not SCRIPT.exists():
        pytest.skip("scripts/submit_job.py not implemented yet")
    spec = importlib.util.spec_from_file_location("_script_submit_job", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _build(submit: ModuleType, job: str, job_args: list[str]) -> list[str]:
    """Call build_submit_command binding by parameter-name keywords.

    The card names the helper but not its parameters, so bind by meaning (job / job args /
    wheel-or-zip artefact) and leave container/master at their defaults.
    """
    sig = inspect.signature(submit.build_submit_command)
    kwargs: dict[str, Any] = {}
    for name, p in sig.parameters.items():
        low = name.lower()
        if "arg" in low:
            kwargs[name] = job_args
        elif "job" in low:
            kwargs[name] = job
        elif any(k in low for k in ("wheel", "zip", "py_file", "artifact", "package")):
            kwargs[name] = WHEEL if "wheel" in low else "lakehouse_app.zip"
        elif p.default is inspect.Parameter.empty:
            pytest.fail(f"cannot bind required parameter {name!r} of build_submit_command")
    return submit.build_submit_command(**kwargs)


def _pairs(cmd: list[str], flag: str) -> list[str]:
    return [cmd[i + 1] for i, tok in enumerate(cmd[:-1]) if tok == flag]


def test_command_runs_spark_submit_in_the_master_container(submit: ModuleType) -> None:
    cmd = _build(submit, "hello_delta", ["--expect-packaged"])
    assert cmd[:2] == ["docker", "exec"]
    assert "crm_spark_master" in cmd
    assert "/opt/spark/bin/spark-submit" in cmd
    assert _pairs(cmd, "--master") == ["spark://spark-master:7077"]
    assert _pairs(cmd, "--deploy-mode") == ["client"]


def test_command_ships_zip_as_py_files_and_entry_script(submit: ModuleType) -> None:
    """Executors must receive the code via --py-files (zip), not the source mount."""
    cmd = _build(submit, "hello_delta", [])
    py_files = _pairs(cmd, "--py-files")
    assert len(py_files) == 1 and py_files[0].startswith("/opt/spark-apps/dist/")
    assert "/opt/spark-apps/dist/job_entry.py" in cmd


def test_job_name_and_args_come_after_entry_script_in_order(submit: ModuleType) -> None:
    cmd = _build(submit, "hello_delta", ["--rows", "5", "--expect-packaged"])
    i = cmd.index("/opt/spark-apps/dist/job_entry.py")
    assert cmd[i + 1 :] == ["hello_delta", "--rows", "5", "--expect-packaged"]


def test_command_always_pins_driver_host_cores_and_executor_memory(submit: ModuleType) -> None:
    cmd = _build(submit, "hello_delta", [])
    confs = _pairs(cmd, "--conf")
    assert "spark.driver.host=spark-master" in confs
    assert "spark.cores.max=1" in confs
    assert "spark.executor.memory=512m" in confs


def test_command_has_no_env_passthrough_or_secret_material(submit: ModuleType) -> None:
    """Negative: no -e / --env to docker exec, no credential-looking tokens."""
    cmd = _build(submit, "hello_delta", [])
    assert "-e" not in cmd and "--env" not in cmd
    assert not [t for t in cmd if any(w in t.lower() for w in ("secret", "password", "token"))]


_BAD = (SystemExit, LookupError, ValueError, RuntimeError, OSError)


def test_find_wheel_returns_the_single_wheel(submit: ModuleType, tmp_path: Path) -> None:
    (tmp_path / WHEEL).write_bytes(b"x")
    (tmp_path / "job_entry.py").write_text("x")
    assert Path(submit.find_wheel(tmp_path)).name == WHEEL


def test_find_wheel_with_no_wheel_fails(submit: ModuleType, tmp_path: Path) -> None:
    with pytest.raises(_BAD):
        submit.find_wheel(tmp_path)


def test_find_wheel_with_two_wheels_fails_instead_of_guessing(
    submit: ModuleType, tmp_path: Path
) -> None:
    """Stale wheels must not be shipped silently."""
    (tmp_path / WHEEL).write_bytes(b"x")
    (tmp_path / "multitenant_crm_lakehouse-0.0.9-py3-none-any.whl").write_bytes(b"x")
    with pytest.raises(_BAD):
        submit.find_wheel(tmp_path)
