"""Spec tests for scripts/bootstrap_env.py (secret generation, never overwrite)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .conftest import parse_env


if TYPE_CHECKING:
    from pathlib import Path

    import pytest

SECRET_KEYS = ["POSTGRES_PASSWORD", "S3_ACCESS_KEY", "S3_SECRET_KEY"]


def test_existing_env_is_never_overwritten(
    bootstrap_env, tmp_path: Path, example_path: Path
) -> None:
    env = tmp_path / ".env"
    original = b"POSTGRES_PASSWORD=mine\r\nCUSTOM=1\n"
    env.write_bytes(original)
    assert bootstrap_env.bootstrap(env, example_path) is False
    assert env.read_bytes() == original


def test_existing_empty_env_is_still_not_overwritten(
    bootstrap_env, tmp_path: Path, example_path: Path
) -> None:
    env = tmp_path / ".env"
    env.write_bytes(b"")
    assert bootstrap_env.bootstrap(env, example_path) is False
    assert env.read_bytes() == b""


def test_creates_env_and_returns_true(bootstrap_env, tmp_path: Path, example_path: Path) -> None:
    env = tmp_path / ".env"
    assert bootstrap_env.bootstrap(env, example_path) is True
    assert env.exists()


def test_placeholders_are_replaced_with_real_secrets(
    bootstrap_env, tmp_path: Path, example_path: Path
) -> None:
    env = tmp_path / ".env"
    bootstrap_env.bootstrap(env, example_path)
    values = parse_env(env)
    for key in SECRET_KEYS:
        assert values[key] != "change-me"
        assert len(values[key]) >= 32  # token_urlsafe(32) is 43 chars
    assert "change-me" not in env.read_text(encoding="utf-8")


def test_secrets_are_distinct_per_key(bootstrap_env, tmp_path: Path, example_path: Path) -> None:
    env = tmp_path / ".env"
    bootstrap_env.bootstrap(env, example_path)
    values = parse_env(env)
    generated = [values[k] for k in SECRET_KEYS]
    assert len(set(generated)) == len(generated)


def test_secrets_differ_between_runs(bootstrap_env, tmp_path: Path, example_path: Path) -> None:
    first, second = tmp_path / "a.env", tmp_path / "b.env"
    bootstrap_env.bootstrap(first, example_path)
    bootstrap_env.bootstrap(second, example_path)
    a, b = parse_env(first), parse_env(second)
    for key in SECRET_KEYS:
        assert a[key] != b[key]


def test_non_placeholder_values_are_kept(bootstrap_env, tmp_path: Path, example_path: Path) -> None:
    env = tmp_path / ".env"
    bootstrap_env.bootstrap(env, example_path)
    values = parse_env(env)
    assert values["POSTGRES_USER"] == "crm"
    assert values["KAFKA_PORT"] == "9092"
    assert values["EMPTY_KEY"] == ""


def test_comments_are_preserved(bootstrap_env, tmp_path: Path, example_path: Path) -> None:
    env = tmp_path / ".env"
    bootstrap_env.bootstrap(env, example_path)
    text = env.read_text(encoding="utf-8")
    assert "# Local stack configuration" in text
    assert "# Object store" in text


def test_all_example_keys_are_present(bootstrap_env, tmp_path: Path, example_path: Path) -> None:
    env = tmp_path / ".env"
    bootstrap_env.bootstrap(env, example_path)
    assert set(parse_env(env)) == set(parse_env(example_path))


def test_example_file_is_not_modified(bootstrap_env, tmp_path: Path, example_path: Path) -> None:
    before = example_path.read_bytes()
    bootstrap_env.bootstrap(tmp_path / ".env", example_path)
    assert example_path.read_bytes() == before


def test_main_creates_env_and_does_not_leak_values(
    bootstrap_env, tmp_path: Path, example_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    env = tmp_path / ".env"
    rc = bootstrap_env.main(["--env", str(env), "--example", str(example_path)])
    out = capsys.readouterr()
    assert rc == 0
    values = parse_env(env)
    for key in SECRET_KEYS:
        assert values[key] not in out.out
        assert values[key] not in out.err


def test_main_on_existing_env_is_noop_and_exit_zero(
    bootstrap_env, tmp_path: Path, example_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    env = tmp_path / ".env"
    env.write_text("POSTGRES_PASSWORD=keepme-secret\n", encoding="utf-8")
    rc = bootstrap_env.main(["--env", str(env), "--example", str(example_path)])
    out = capsys.readouterr()
    assert rc == 0
    assert env.read_text(encoding="utf-8") == "POSTGRES_PASSWORD=keepme-secret\n"
    assert "keepme-secret" not in out.out + out.err


def test_leak_detector_would_catch_a_printing_implementation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Negative control: the no-leak assertion style fails when a value is printed."""
    secret = "s3cr3t-value"
    print(f"POSTGRES_PASSWORD={secret}")
    out = capsys.readouterr()
    assert secret in out.out
