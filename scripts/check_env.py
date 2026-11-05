#!/usr/bin/env python3
"""Report which keys of ``.env.example`` are missing or empty in ``.env`` (key names only).

A key is reported when it is absent from ``.env``, or present with an empty value while the
example gives it a value. Keys that are intentionally empty in the example (optional settings)
are accepted as empty. If ``.env`` does not exist, every example key is reported. Values are
never printed.

Usage (repo root):
    python scripts/check_env.py [--env PATH] [--example PATH]

Exit codes: 0 = nothing missing, 1 = at least one key missing, 2 = example file unreadable.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent


def _parse(path: Path) -> dict[str, str]:
    """Parse ``KEY=value`` lines; comments and blank lines are ignored."""
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().removeprefix("export ").strip()
        if key:
            values[key] = value.strip().strip("\"'").strip()
    return values


def missing_keys(env_path: Path, example_path: Path) -> list[str]:
    """Return sorted key names present in the example but absent or empty in ``env_path``."""
    example = _parse(example_path)
    if not env_path.exists():
        return sorted(example)
    env = _parse(env_path)
    missing = [
        key
        for key, example_value in example.items()
        if key not in env or (env[key] == "" and example_value != "")
    ]
    return sorted(missing)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n", maxsplit=1)[0])
    parser.add_argument("--env", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument("--example", type=Path, default=REPO_ROOT / ".env.example")
    args = parser.parse_args(argv)

    try:
        missing = missing_keys(args.env, args.example)
    except OSError as exc:
        print(f"ERROR: cannot read {exc.filename}: {exc.strerror}", file=sys.stderr)
        return 2

    if missing:
        print(f"{len(missing)} key(s) missing or empty in {args.env.name}:")
        for key in missing:
            print(f"  {key}")
        print("Run: python scripts/bootstrap_env.py (creates .env only if it does not exist).")
        return 1
    print(f"{args.env.name}: all keys from {args.example.name} are set.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
