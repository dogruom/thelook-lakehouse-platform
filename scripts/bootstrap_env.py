#!/usr/bin/env python3
"""Create a local ``.env`` from ``.env.example`` with random secrets, only if it is absent.

Every value equal to ``change-me`` is replaced by ``secrets.token_urlsafe(32)`` (a valid Fernet
key for names ending in ``FERNET_KEY``); all other values, comments and blank lines are copied
unchanged. An existing ``.env`` is never overwritten. With ``--add-missing`` the keys that the
example has but ``.env`` lacks are appended; existing lines are never modified or reordered.
Secret values are never printed.

Usage (repo root):
    python scripts/bootstrap_env.py [--env PATH] [--example PATH] [--add-missing]

Exit codes: 0 = done, 2 = a required file could not be read.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import os
import secrets
import sys
from pathlib import Path


PLACEHOLDER = "change-me"
REPO_ROOT = Path(__file__).resolve().parent.parent


def _generate(key: str) -> str:
    """Random value for a ``change-me`` placeholder; Fernet keys need 32 bytes, 44 characters."""
    if key.strip().endswith("FERNET_KEY"):
        return base64.urlsafe_b64encode(os.urandom(32)).decode()
    return secrets.token_urlsafe(32)


def _split_line(line: str) -> tuple[str, str, str, str] | None:
    """Split ``KEY=value`` into (key, separator, value, line ending); None for non-assignments."""
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in line:
        return None
    key, sep, rest = line.partition("=")
    value = rest.rstrip("\r\n")
    return key, sep, value, rest[len(value) :]


def _render(example_text: str) -> tuple[str, int]:
    """Return the new file content and the number of generated secrets."""
    out: list[str] = []
    generated = 0
    for line in example_text.splitlines(keepends=True):
        parts = _split_line(line)
        if parts is None:
            out.append(line)
            continue
        key, sep, value, eol = parts
        if value == PLACEHOLDER:
            value = _generate(key)
            generated += 1
        out.append(f"{key}{sep}{value}{eol}")
    return "".join(out), generated


def _key_names(text: str) -> set[str]:
    """Key names of the assignments in ``text`` (values are not kept)."""
    names: set[str] = set()
    for line in text.splitlines():
        parts = _split_line(line)
        if parts is not None:
            names.add(parts[0].strip().removeprefix("export ").strip())
    return names


def _bootstrap(env_path: Path, example_path: Path) -> tuple[bool, int]:
    if env_path.exists():
        return False, 0
    with example_path.open(encoding="utf-8", newline="") as handle:
        content, generated = _render(handle.read())
    try:
        # "x" fails instead of truncating if .env appears between the check and the write.
        with env_path.open("x", encoding="utf-8", newline="") as handle:
            handle.write(content)
    except FileExistsError:
        return False, 0
    with contextlib.suppress(OSError):
        env_path.chmod(0o600)  # best effort; a no-op for POSIX modes on Windows
    return True, generated


def bootstrap(env_path: Path, example_path: Path) -> bool:
    """Create ``env_path`` from ``example_path``; return False (and touch nothing) if it exists."""
    created, _ = _bootstrap(env_path, example_path)
    return created


def add_missing(env_path: Path, example_path: Path) -> list[str]:
    """Append the example keys that ``env_path`` lacks; return their names, sorted.

    Existing lines are never modified or reordered and keep their line endings. Raises
    ``FileNotFoundError`` if ``env_path`` does not exist. Only key names are parsed from the
    existing file; its values are neither inspected nor reported.
    """
    if not env_path.is_file():
        raise FileNotFoundError(str(env_path))
    existing_bytes = env_path.read_bytes()
    existing_text = existing_bytes.decode("utf-8")
    present = _key_names(existing_text)
    eol = "\r\n" if "\r\n" in existing_text else "\n"

    with example_path.open(encoding="utf-8", newline="") as handle:
        example_lines = handle.read().splitlines()

    appended: dict[str, str] = {}
    for line in example_lines:
        parts = _split_line(line)
        if parts is None:
            continue
        key, sep, value, _ = parts
        name = key.strip().removeprefix("export ").strip()
        if name in present or name in appended:
            continue
        appended[name] = f"{key}{sep}{_generate(name) if value == PLACEHOLDER else value}"

    if appended:
        prefix = "" if not existing_bytes or existing_bytes.endswith(b"\n") else eol
        addition = prefix + "".join(f"{line}{eol}" for line in appended.values())
        with env_path.open("ab") as handle:
            handle.write(addition.encode("utf-8"))
    return sorted(appended)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n", maxsplit=1)[0])
    parser.add_argument("--env", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--example", type=Path, default=REPO_ROOT / ".env.example")
    parser.add_argument(
        "--add-missing",
        action="store_true",
        help="append keys that the example has and the existing env file lacks",
    )
    args = parser.parse_args(argv)

    if args.add_missing:
        try:
            names = add_missing(args.env, args.example)
        except FileNotFoundError:
            print(f"ERROR: {args.env} does not exist (or the example is missing).", file=sys.stderr)
            return 2
        except OSError as exc:
            print(f"ERROR: cannot read {exc.filename}: {exc.strerror}", file=sys.stderr)
            return 2
        if names:
            print(f"Appended {len(names)} key(s) to {args.env.name}; values not shown:")
            for name in names:
                print(f"  {name}")
        else:
            print(f"{args.env.name}: no keys missing - left untouched.")
        return 0

    try:
        created, generated = _bootstrap(args.env, args.example)
    except OSError as exc:
        print(f"ERROR: cannot read example file {args.example}: {exc.strerror}", file=sys.stderr)
        return 2

    if created:
        print(f"Created {args.env.name} with {generated} generated secret(s). Values not shown.")
    else:
        print(f"{args.env.name} already exists - left untouched.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
