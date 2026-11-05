"""Loader for the standalone ``scripts/*.py`` modules (they are not a package)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    from types import ModuleType

SCRIPTS_DIR = Path(__file__).resolve().parents[3] / "scripts"

EXAMPLE = """\
# Local stack configuration
POSTGRES_USER=crm
POSTGRES_PASSWORD=change-me

# Object store
S3_ACCESS_KEY=change-me
S3_SECRET_KEY=change-me
KAFKA_PORT=9092
EMPTY_KEY=
"""


def load_script(name: str) -> ModuleType:
    path = SCRIPTS_DIR / f"{name}.py"
    if not path.exists():
        pytest.skip(f"scripts/{name}.py not implemented yet (blocked)")
    spec = importlib.util.spec_from_file_location(f"_script_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def bootstrap_env() -> ModuleType:
    return load_script("bootstrap_env")


@pytest.fixture(scope="session")
def check_env() -> ModuleType:
    return load_script("check_env")


@pytest.fixture
def example_path(tmp_path: Path) -> Path:
    path = tmp_path / ".env.example"
    path.write_text(EXAMPLE, encoding="utf-8")
    return path


def parse_env(path: Path) -> dict[str, str]:
    """Minimal KEY=VALUE parser (ignores blanks and comments)."""
    result: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        result[key.strip()] = value.strip()
    return result
