"""Canonical, byte-stable file output of a CDC run (same seed and config -> identical bytes)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from pathlib import Path

    from lakehouse.generator.cdc import CdcRun


EVENTS_FILE = "cdc_events.ndjson"
MANIFEST_FILE = "cdc_manifest.json"


def canonical_json(obj: Any) -> str:
    """Sorted keys, no whitespace, UTF-8 characters kept as is."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def write_run(run: CdcRun, out_dir: Path) -> tuple[Path, Path]:
    """Write ``cdc_events.ndjson`` (one line per emission) and ``cdc_manifest.json``."""
    out_dir.mkdir(parents=True, exist_ok=True)
    events_path = out_dir / EVENTS_FILE
    manifest_path = out_dir / MANIFEST_FILE
    # Bytes, not text mode: Windows text mode would turn "\n" into "\r\n".
    events_path.write_bytes(
        "".join(canonical_json(line) + "\n" for line in run.lines).encode("utf-8")
    )
    manifest_path.write_bytes((canonical_json(run.manifest.to_dict()) + "\n").encode("utf-8"))
    return events_path, manifest_path
