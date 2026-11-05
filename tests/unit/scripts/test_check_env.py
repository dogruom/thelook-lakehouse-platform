"""Spec tests for scripts/check_env.py (missing/empty key detection, no value output)."""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pathlib import Path

    import pytest

ALL_KEYS = sorted(
    [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "S3_ACCESS_KEY",
        "S3_SECRET_KEY",
        "KAFKA_PORT",
        "EMPTY_KEY",
    ]
)


def _complete_env(path: Path) -> Path:
    path.write_text(
        "POSTGRES_USER=u\nPOSTGRES_PASSWORD=p\nS3_ACCESS_KEY=a\n"
        "S3_SECRET_KEY=s\nKAFKA_PORT=9092\nEMPTY_KEY=filled\n",
        encoding="utf-8",
    )
    return path


def test_complete_env_has_no_missing_keys(check_env, tmp_path: Path, example_path: Path) -> None:
    env = _complete_env(tmp_path / ".env")
    assert check_env.missing_keys(env, example_path) == []


def test_absent_key_is_reported(check_env, tmp_path: Path, example_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text(
        "POSTGRES_USER=u\nPOSTGRES_PASSWORD=p\nS3_ACCESS_KEY=a\nKAFKA_PORT=1\nEMPTY_KEY=x\n",
        encoding="utf-8",
    )
    assert check_env.missing_keys(env, example_path) == ["S3_SECRET_KEY"]


def test_empty_value_is_reported(check_env, tmp_path: Path, example_path: Path) -> None:
    env = _complete_env(tmp_path / ".env")
    text = env.read_text(encoding="utf-8").replace("KAFKA_PORT=9092", "KAFKA_PORT=")
    env.write_text(text, encoding="utf-8")
    assert check_env.missing_keys(env, example_path) == ["KAFKA_PORT"]


def test_result_is_sorted(check_env, tmp_path: Path, example_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text("POSTGRES_USER=u\n", encoding="utf-8")
    result = check_env.missing_keys(env, example_path)
    assert result == sorted(result)
    assert result == sorted(set(ALL_KEYS) - {"POSTGRES_USER"})


def test_missing_env_file_reports_all_example_keys(
    check_env, tmp_path: Path, example_path: Path
) -> None:
    assert check_env.missing_keys(tmp_path / "nope.env", example_path) == ALL_KEYS


def test_extra_env_keys_are_not_reported(check_env, tmp_path: Path, example_path: Path) -> None:
    env = _complete_env(tmp_path / ".env")
    with env.open("a", encoding="utf-8") as fh:
        fh.write("UNRELATED=1\n")
    assert check_env.missing_keys(env, example_path) == []


def test_commented_key_does_not_count_as_present(
    check_env, tmp_path: Path, example_path: Path
) -> None:
    env = tmp_path / ".env"
    env.write_text("# S3_SECRET_KEY=abc\n", encoding="utf-8")
    assert "S3_SECRET_KEY" in check_env.missing_keys(env, example_path)


def test_main_exit_zero_when_complete(
    check_env, tmp_path: Path, example_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    env = _complete_env(tmp_path / ".env")
    assert check_env.main(["--env", str(env), "--example", str(example_path)]) == 0
    capsys.readouterr()


def test_main_exit_one_and_lists_key_names_only(
    check_env, tmp_path: Path, example_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    env = tmp_path / ".env"
    env.write_text("POSTGRES_PASSWORD=topsecret-pw\nS3_ACCESS_KEY=topsecret-ak\n", encoding="utf-8")
    rc = check_env.main(["--env", str(env), "--example", str(example_path)])
    out = capsys.readouterr()
    text = out.out + out.err
    assert rc == 1
    assert "S3_SECRET_KEY" in text
    assert "topsecret-pw" not in text
    assert "topsecret-ak" not in text


def test_main_exit_one_when_env_file_missing(
    check_env, tmp_path: Path, example_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = check_env.main(["--env", str(tmp_path / "nope.env"), "--example", str(example_path)])
    out = capsys.readouterr()
    assert rc == 1
    assert "POSTGRES_PASSWORD" in out.out + out.err


def test_main_does_not_print_values_when_complete(
    check_env, tmp_path: Path, example_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    env = tmp_path / ".env"
    env.write_text(
        "POSTGRES_USER=u\nPOSTGRES_PASSWORD=topsecret-pw\nS3_ACCESS_KEY=a\n"
        "S3_SECRET_KEY=s\nKAFKA_PORT=9092\nEMPTY_KEY=x\n",
        encoding="utf-8",
    )
    check_env.main(["--env", str(env), "--example", str(example_path)])
    out = capsys.readouterr()
    assert "topsecret-pw" not in out.out + out.err
