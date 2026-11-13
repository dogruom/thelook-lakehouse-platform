"""check_dbt_docs (W4.D21.3): model and catalog-column descriptions must be present."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .conftest import load_script


if TYPE_CHECKING:
    from pathlib import Path

mod = load_script("check_dbt_docs")


def _node(name: str, path: str, desc: str, cols: dict[str, str]) -> dict[str, Any]:
    return {
        "resource_type": "model",
        "name": name,
        "original_file_path": path,
        "description": desc,
        "columns": {c: {"description": d} for c, d in cols.items()},
    }


MANIFEST = {
    "nodes": {
        "model.p.ok": _node("ok", "models/marts/ok.sql", "fine", {"a": "x"}),
        "model.p.gap": _node("gap", "models\\staging\\gap.sql", "", {"a": ""}),
        "model.p.spike": _node("spike", "models/spike/s.sql", "", {}),
        "test.p.t": {"resource_type": "test", "original_file_path": "models/marts/x.yml"},
    }
}
CATALOG = {
    "nodes": {
        "model.p.ok": {"columns": {"A": {}}},
        "model.p.gap": {"columns": {"a": {}, "undocumented": {}}},
        "model.p.spike": {"columns": {"z": {}}},
    }
}


def test_find_gaps_checks_staging_and_marts_only() -> None:
    checked, gaps = mod.find_gaps(MANIFEST, CATALOG)
    assert checked == 2
    assert gaps == ["gap", "gap.a", "gap.undocumented"]


def test_main_exit_codes(tmp_path: Path) -> None:
    assert mod.main(["--target-dir", str(tmp_path)]) == 2
    (tmp_path / "manifest.json").write_text(json.dumps(MANIFEST), encoding="utf-8")
    (tmp_path / "catalog.json").write_text(json.dumps(CATALOG), encoding="utf-8")
    assert mod.main(["--target-dir", str(tmp_path)]) == 1
    ok = {"nodes": {"model.p.ok": MANIFEST["nodes"]["model.p.ok"]}}
    (tmp_path / "manifest.json").write_text(json.dumps(ok), encoding="utf-8")
    assert mod.main(["--target-dir", str(tmp_path)]) == 0
