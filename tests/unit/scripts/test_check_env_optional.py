"""check_env semantics for example-empty (optional) keys."""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pathlib import Path


def _example(tmp_path: Path) -> Path:
    path = tmp_path / ".env.example"
    path.write_text("REQ=value\nOPT=\n", encoding="utf-8")
    return path


def test_optional_key_present_but_empty_is_not_reported(check_env, tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text("REQ=x\nOPT=\n", encoding="utf-8")
    assert check_env.missing_keys(env, _example(tmp_path)) == []


def test_optional_key_absent_is_reported(check_env, tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text("REQ=x\n", encoding="utf-8")
    assert check_env.missing_keys(env, _example(tmp_path)) == ["OPT"]


def test_required_key_empty_in_env_is_reported(check_env, tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text("REQ=\nOPT=\n", encoding="utf-8")
    assert check_env.missing_keys(env, _example(tmp_path)) == ["REQ"]


def test_optional_key_with_value_is_not_reported(check_env, tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text("REQ=x\nOPT=y\n", encoding="utf-8")
    assert check_env.missing_keys(env, _example(tmp_path)) == []
