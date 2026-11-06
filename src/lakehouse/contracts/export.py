"""Regenerate the committed JSON Schema snapshots: ``python -m lakehouse.contracts.export``."""

from __future__ import annotations

import sys

from lakehouse.contracts.schema import SCHEMA_DIR, build_schema, export_schemas


__all__ = ["build_schema", "export_schemas", "main"]


def main() -> int:
    for path in export_schemas(SCHEMA_DIR):
        sys.stdout.write(f"wrote {path.name}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
