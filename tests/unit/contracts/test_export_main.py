"""``python -m lakehouse.contracts.export`` regenerates one snapshot per contract (offline)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from lakehouse.contracts import export
from lakehouse.contracts.schema import CONTRACT_MODELS


if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_main_writes_every_schema_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(export, "SCHEMA_DIR", tmp_path)
    assert export.main() == 0
    assert {p.name for p in tmp_path.glob("*.json")} == {f"{n}.json" for n in CONTRACT_MODELS}
    out = capsys.readouterr().out
    assert all(f"wrote {n}.json" in out for n in CONTRACT_MODELS)
    text = (tmp_path / f"{next(iter(CONTRACT_MODELS))}.json").read_text(encoding="utf-8")
    assert isinstance(json.loads(text), dict)
    assert text.endswith("}\n")
