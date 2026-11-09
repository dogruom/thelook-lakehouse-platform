"""Pure tests of the app-zip build and source hash in scripts/submit_job.py."""

from __future__ import annotations

import importlib.util
import os
import re
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    from types import ModuleType

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "submit_job.py"
ZIP_NAME = re.compile(r"^lakehouse_app-[0-9a-f]{12}\.zip$")


@pytest.fixture(scope="module")
def submit() -> ModuleType:
    spec = importlib.util.spec_from_file_location("_script_submit_build", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    pkg = tmp_path / "src" / "lakehouse"
    (pkg / "common").mkdir(parents=True)
    (pkg / "contracts" / "schemas").mkdir(parents=True)
    (pkg / "__pycache__").mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "common" / "spark.py").write_text("X = 1\n")
    (pkg / "contracts" / "schemas" / "customer.json").write_text('{"a": 1}')
    (pkg / "__pycache__" / "junk.cpython-311.pyc").write_bytes(b"\x00junk")
    return tmp_path


def _names(zip_path: Path) -> list[str]:
    with zipfile.ZipFile(zip_path) as zf:
        return zf.namelist()


def test_zip_is_named_after_the_source_hash(submit: ModuleType, repo: Path) -> None:
    out = submit.build_app_zip(repo, repo / "dist")
    assert out.parent == repo / "dist"
    assert ZIP_NAME.match(out.name), out.name


def test_zip_entries_are_relative_to_src_and_include_non_python_files(
    submit: ModuleType, repo: Path
) -> None:
    names = _names(submit.build_app_zip(repo, repo / "dist"))
    assert "lakehouse/__init__.py" in names
    assert "lakehouse/common/spark.py" in names
    assert "lakehouse/contracts/schemas/customer.json" in names
    assert not [n for n in names if n.startswith(("src/", "/"))]


def test_zip_never_contains_pycache(submit: ModuleType, repo: Path) -> None:
    """Negative: bytecode caches are host-specific noise and break determinism."""
    names = _names(submit.build_app_zip(repo, repo / "dist"))
    assert not [n for n in names if "__pycache__" in n or n.endswith(".pyc")]


def test_zip_and_hash_contain_only_python_and_json_files(submit: ModuleType, repo: Path) -> None:
    """Negative: editor backups and notes must neither ship nor change the content hash."""
    before = submit.source_hash(repo / "src")
    pkg = repo / "src" / "lakehouse"
    (pkg / "notes.md").write_text("scratch")
    (pkg / "common" / "spark.py.bak").write_text("old")
    (pkg / "common" / ".swp").write_bytes(b"\x00")
    assert submit.source_hash(repo / "src") == before
    names = _names(submit.build_app_zip(repo, repo / "dist"))
    assert all(n.endswith((".py", ".json")) for n in names)


def test_zip_bytes_are_deterministic_across_rebuilds_and_mtimes(
    submit: ModuleType, repo: Path
) -> None:
    first = submit.build_app_zip(repo, repo / "dist")
    first_bytes = first.read_bytes()
    first.unlink()
    for path in (repo / "src").rglob("*"):
        os.utime(path, (1_000_000_000, 1_000_000_000))
    second = submit.build_app_zip(repo, repo / "dist")
    assert second.name == first.name
    assert second.read_bytes() == first_bytes


def test_source_hash_is_stable_for_unchanged_sources(submit: ModuleType, repo: Path) -> None:
    src = repo / "src"
    assert submit.source_hash(src) == submit.source_hash(src)


def test_source_hash_changes_when_a_python_file_changes(submit: ModuleType, repo: Path) -> None:
    src = repo / "src"
    before = submit.source_hash(src)
    (src / "lakehouse" / "common" / "spark.py").write_text("X = 2\n")
    assert submit.source_hash(src) != before


def test_source_hash_changes_when_a_non_python_file_changes(submit: ModuleType, repo: Path) -> None:
    src = repo / "src"
    before = submit.source_hash(src)
    (src / "lakehouse" / "contracts" / "schemas" / "customer.json").write_text('{"a": 2}')
    assert submit.source_hash(src) != before


def test_source_hash_changes_when_a_file_is_added_or_removed(
    submit: ModuleType, repo: Path
) -> None:
    src = repo / "src"
    base = submit.source_hash(src)
    extra = src / "lakehouse" / "new.py"
    extra.write_text("")
    assert submit.source_hash(src) != base
    extra.unlink()
    assert submit.source_hash(src) == base


def test_changed_source_produces_a_differently_named_zip(submit: ModuleType, repo: Path) -> None:
    first = submit.build_app_zip(repo, repo / "dist")
    (repo / "src" / "lakehouse" / "common" / "spark.py").write_text("X = 3\n")
    assert submit.build_app_zip(repo, repo / "dist").name != first.name
