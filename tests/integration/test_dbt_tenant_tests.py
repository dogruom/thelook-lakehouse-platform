"""dbt tenant-safe test primitives catch cross-tenant key matches (W4.D19.2).

The fixture view models hold one cross-tenant leak (``o3``) and one plain orphan (``o4``). The
tenant-safe generic test must flag both, the singular macro only the leak, and dbt's built-in
``relationships`` only the orphan, which is exactly the gap the primitives close.

Note: the fixtures are views in the shared ``gold_fixtures`` schema of the Thrift warehouse
(location-free, so reruns are safe; seeds were not: LOCATION_ALREADY_EXISTS on Delta/S3).
"""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
from pathlib import Path
from typing import Any

import pytest


pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT = REPO_ROOT / "dbt_project"
THRIFT_ADDR = ("127.0.0.1", 10000)
SELECT = "tag:tenant_fixture"
DBT_TIMEOUT_S = 600
FIXTURE_NODES = (
    "fx_tenant_parent",
    "fx_tenant_child",
    "fx_relationships_by_brand",
    "fx_plain_relationships",
    "fx_assert_cross_tenant_detected",
)


def _dbt_executable() -> Path | None:
    override = os.environ.get("DBT_EXECUTABLE")
    candidates = [Path(override)] if override else []
    candidates += [
        REPO_ROOT / ".venv-dbt" / "Scripts" / "dbt.exe",
        REPO_ROOT / ".venv-dbt" / "Scripts" / "dbt",
        REPO_ROOT / ".venv-dbt" / "bin" / "dbt",
    ]
    return next((c for c in candidates if c.is_file()), None)


@pytest.fixture(scope="module")
def dbt_exe() -> Path:
    exe = _dbt_executable()
    if exe is None:
        pytest.skip("dbt executable not found (set DBT_EXECUTABLE or create .venv-dbt)")
    return exe


@pytest.fixture(scope="module")
def thrift_up() -> None:
    try:
        with socket.create_connection(THRIFT_ADDR, timeout=3):
            return
    except OSError as exc:
        pytest.skip(f"Spark Thrift Server {THRIFT_ADDR} unreachable: {exc}")


def _run_dbt(exe: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(exe), *args, "--profiles-dir", "profiles"],
        cwd=DBT_PROJECT,
        capture_output=True,
        text=True,
        timeout=DBT_TIMEOUT_S,
        check=False,
    )


@pytest.fixture(scope="module")
def build_results(
    dbt_exe: Path, thrift_up: None, tmp_path_factory: pytest.TempPathFactory
) -> dict[str, dict[str, Any]]:
    target = tmp_path_factory.mktemp("dbt_target")
    proc = _run_dbt(
        dbt_exe,
        [
            *("build", "--select", SELECT, "--vars", "{tenant_fixtures: true}"),
            *("--target-path", str(target)),
        ],
    )
    assert proc.returncode != 0, "fixture build must fail: the leaky fixtures are detected"
    run_results = target / "run_results.json"
    assert run_results.is_file(), f"no run_results.json: {proc.stdout[-2000:]}"
    payload = json.loads(run_results.read_text(encoding="utf-8"))
    return {r["unique_id"]: r for r in payload["results"]}


def _find(results: dict[str, dict[str, Any]], fragment: str) -> dict[str, Any]:
    matches = [r for k, r in results.items() if fragment in k]
    assert len(matches) == 1, f"expected exactly one node matching {fragment!r}: {list(results)}"
    return matches[0]


def test_fixture_models_build_successfully(build_results: dict[str, dict[str, Any]]) -> None:
    for model in ("fx_tenant_parent", "fx_tenant_child"):
        res = _find(build_results, f"model.crm_lakehouse.{model}")
        assert res["status"] == "success", res


def test_relationships_by_brand_flags_leak_and_orphan(
    build_results: dict[str, dict[str, Any]],
) -> None:
    res = _find(build_results, "fx_relationships_by_brand")
    assert res["status"] == "fail"
    assert res["failures"] == 2  # o3 (cross-tenant) and o4 (orphan)


def test_plain_relationships_misses_the_cross_tenant_leak(
    build_results: dict[str, dict[str, Any]],
) -> None:
    res = _find(build_results, "fx_plain_relationships")
    assert res["status"] == "fail"
    assert res["failures"] == 1  # only the orphan o4; the o3 leak is invisible to it


def test_cross_tenant_macro_flags_only_the_leak(
    build_results: dict[str, dict[str, Any]],
) -> None:
    res = _find(build_results, "fx_assert_cross_tenant_detected")
    assert res["status"] == "fail"
    assert res["failures"] == 1  # o3 only; the plain orphan has no key in any brand


def _ls_names(exe: Path, extra: list[str]) -> set[str]:
    """Node names selected by ``dbt ls``; keeps only bare identifier lines (drops log noise)."""
    proc = _run_dbt(exe, ["ls", "--quiet", "--select", SELECT, "--output", "name", *extra])
    assert proc.returncode == 0, proc.stderr
    return {ln.strip() for ln in proc.stdout.splitlines() if re.fullmatch(r"\w+", ln.strip())}


def test_fixtures_are_selected_only_with_the_var(dbt_exe: Path) -> None:
    assert _ls_names(dbt_exe, []) == set()
    assert _ls_names(dbt_exe, ["--vars", "{tenant_fixtures: true}"]) == set(FIXTURE_NODES)
