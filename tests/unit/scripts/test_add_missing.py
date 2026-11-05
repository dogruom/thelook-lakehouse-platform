"""Spec tests for bootstrap_env.add_missing and FERNET_KEY generation."""

from __future__ import annotations

import base64
import contextlib
from typing import TYPE_CHECKING

from .conftest import parse_env


if TYPE_CHECKING:
    from pathlib import Path

    import pytest

EXAMPLE = (
    "# header\n"
    "A_USER=crm\n"
    "B_PASSWORD=change-me\n"
    "C_FERNET_KEY=change-me\n"
    "D_PORT=9092\n"
    "E_TOKEN=change-me\n"
)


def _example(tmp_path: Path) -> Path:
    path = tmp_path / ".env.example"
    path.write_text(EXAMPLE, encoding="utf-8")
    return path


def _assert_fernet(value: str) -> None:
    assert len(value) == 44
    assert len(base64.urlsafe_b64decode(value)) == 32


def test_add_missing_appends_only_absent_keys_and_keeps_existing_bytes(
    bootstrap_env, tmp_path: Path
) -> None:
    example = _example(tmp_path)
    env = tmp_path / ".env"
    original = b"# mine\r\nA_USER=custom\r\nD_PORT=1111\r\n"
    env.write_bytes(original)
    added = bootstrap_env.add_missing(env, example)
    assert added == ["B_PASSWORD", "C_FERNET_KEY", "E_TOKEN"]
    assert env.read_bytes().startswith(original)
    values = parse_env(env)
    assert values["A_USER"] == "custom"
    assert values["D_PORT"] == "1111"


def test_add_missing_adds_newline_when_file_lacks_trailing_newline(
    bootstrap_env, tmp_path: Path
) -> None:
    example = _example(tmp_path)
    env = tmp_path / ".env"
    env.write_bytes(b"A_USER=custom")
    bootstrap_env.add_missing(env, example)
    data = env.read_bytes()
    assert data.startswith(b"A_USER=custom\n")
    assert parse_env(env)["A_USER"] == "custom"
    assert "B_PASSWORD" in parse_env(env)


def test_add_missing_replaces_placeholders_and_copies_plain_values(
    bootstrap_env, tmp_path: Path
) -> None:
    example = _example(tmp_path)
    env = tmp_path / ".env"
    env.write_text("A_USER=x\n", encoding="utf-8")
    bootstrap_env.add_missing(env, example)
    values = parse_env(env)
    assert values["D_PORT"] == "9092"
    for key in ("B_PASSWORD", "E_TOKEN"):
        assert values[key] not in ("", "change-me")
    assert values["B_PASSWORD"] != values["E_TOKEN"]
    assert "change-me" not in env.read_text(encoding="utf-8")


def test_add_missing_is_noop_when_nothing_absent(bootstrap_env, tmp_path: Path) -> None:
    example = _example(tmp_path)
    env = tmp_path / ".env"
    content = b"A_USER=1\nB_PASSWORD=2\nC_FERNET_KEY=3\nD_PORT=4\nE_TOKEN=5\n"
    env.write_bytes(content)
    assert bootstrap_env.add_missing(env, example) == []
    assert env.read_bytes() == content


def test_add_missing_is_idempotent(bootstrap_env, tmp_path: Path) -> None:
    example = _example(tmp_path)
    env = tmp_path / ".env"
    env.write_text("A_USER=x\n", encoding="utf-8")
    assert bootstrap_env.add_missing(env, example)
    after_first = env.read_bytes()
    assert bootstrap_env.add_missing(env, example) == []
    assert env.read_bytes() == after_first


def test_add_missing_raises_when_env_file_missing(bootstrap_env, tmp_path: Path) -> None:
    import pytest

    with pytest.raises(FileNotFoundError):
        bootstrap_env.add_missing(tmp_path / "nope.env", _example(tmp_path))


def test_add_missing_does_not_create_env_when_missing(bootstrap_env, tmp_path: Path) -> None:
    env = tmp_path / "nope.env"
    with contextlib.suppress(FileNotFoundError):
        bootstrap_env.add_missing(env, _example(tmp_path))
    assert not env.exists()


def test_fernet_key_from_bootstrap_is_valid_and_random(bootstrap_env, tmp_path: Path) -> None:
    example = _example(tmp_path)
    first, second = tmp_path / "a.env", tmp_path / "b.env"
    bootstrap_env.bootstrap(first, example)
    bootstrap_env.bootstrap(second, example)
    a = parse_env(first)["C_FERNET_KEY"]
    b = parse_env(second)["C_FERNET_KEY"]
    _assert_fernet(a)
    _assert_fernet(b)
    assert a != b


def test_fernet_key_from_add_missing_is_valid(bootstrap_env, tmp_path: Path) -> None:
    example = _example(tmp_path)
    env = tmp_path / ".env"
    env.write_text("A_USER=x\n", encoding="utf-8")
    bootstrap_env.add_missing(env, example)
    _assert_fernet(parse_env(env)["C_FERNET_KEY"])


def test_main_add_missing_prints_key_names_only(
    bootstrap_env, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    example = _example(tmp_path)
    env = tmp_path / ".env"
    env.write_text("A_USER=x\n", encoding="utf-8")
    rc = bootstrap_env.main(["--add-missing", "--env", str(env), "--example", str(example)])
    out = capsys.readouterr()
    text = out.out + out.err
    assert rc == 0
    values = parse_env(env)
    for key in ("B_PASSWORD", "C_FERNET_KEY", "E_TOKEN"):
        assert values[key] not in text
    assert "B_PASSWORD" in text
