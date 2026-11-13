#!/usr/bin/env python3
"""Fail when a staging or marts model, or one of its catalog columns, has no description.

Usage (after ``dbt docs generate`` in ``dbt_project/``)::

    python scripts/check_dbt_docs.py [--target-dir dbt_project/target]

Reads ``manifest.json`` (descriptions) and ``catalog.json`` (the columns that really exist), so a
column added to the SQL but not to the YAML is reported. Exit 0 = complete, 1 = gaps (listed),
2 = artefacts missing. W4.D21.3.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


LAYERS = ("models/staging/", "models/marts/")


def find_gaps(manifest: dict, catalog: dict) -> tuple[int, list[str]]:  # type: ignore[type-arg]
    """(models checked, ``model`` or ``model.column`` entries without a description)."""
    checked = 0
    gaps: list[str] = []
    for uid, node in sorted(manifest["nodes"].items()):
        path = node.get("original_file_path", "").replace("\\", "/")
        if node.get("resource_type") != "model" or not path.startswith(LAYERS):
            continue
        checked += 1
        if not node.get("description", "").strip():
            gaps.append(node["name"])
        documented = {k.lower(): v for k, v in node.get("columns", {}).items()}
        for column in catalog.get("nodes", {}).get(uid, {}).get("columns", {}):
            if not documented.get(column.lower(), {}).get("description", "").strip():
                gaps.append(f"{node['name']}.{column}")
    return checked, gaps


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target-dir", type=Path, default=Path("dbt_project/target"))
    args = parser.parse_args(argv)
    try:
        manifest = json.loads((args.target_dir / "manifest.json").read_text(encoding="utf-8"))
        catalog = json.loads((args.target_dir / "catalog.json").read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        print(f"error: {exc.filename} not found; run `dbt docs generate` first", file=sys.stderr)
        return 2
    checked, gaps = find_gaps(manifest, catalog)
    print(f"models checked: {checked}; missing descriptions: {len(gaps)}")
    for gap in gaps:
        print(f"  - {gap}")
    return 1 if gaps else 0


if __name__ == "__main__":
    sys.exit(main())
