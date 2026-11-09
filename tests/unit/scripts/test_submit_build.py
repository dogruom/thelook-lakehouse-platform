"""Pure tests of the stale-module guard in scripts/submit_job.py."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    from types import ModuleType

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "submit_job.py"


@pytest.fixture(scope="module")
def submit() -> ModuleType:
    spec = importlib.util.spec_from_file_location("_script_submit_build", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_no_stale_modules_when_wheel_matches_source(submit: ModuleType) -> None:
    members = ["lakehouse/__init__.py", "lakehouse/common/spark.py"]
    assert submit.stale_modules(members, list(members)) == []


def test_module_deleted_from_source_is_reported(submit: ModuleType) -> None:
    members = ["lakehouse/__init__.py", "lakehouse/old/gone.py", "lakehouse/a.py"]
    source = ["lakehouse/__init__.py", "lakehouse/a.py"]
    assert submit.stale_modules(members, source) == ["lakehouse/old/gone.py"]


def test_non_python_and_foreign_members_are_ignored(submit: ModuleType) -> None:
    members = [
        "lakehouse/contracts/schemas/x.json",
        "multitenant_crm_lakehouse-0.1.0.dist-info/METADATA",
        "other/thing.py",
    ]
    assert submit.stale_modules(members, []) == []
