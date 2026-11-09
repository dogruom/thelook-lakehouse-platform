"""docker/spark/render_hive_site.py: password handling, XML escaping and DataNucleus flags."""

from __future__ import annotations

import importlib.util
import os
import stat
from pathlib import Path
from typing import TYPE_CHECKING
from xml.etree import ElementTree

import pytest


if TYPE_CHECKING:
    from types import ModuleType

SCRIPT = Path(__file__).resolve().parents[3] / "docker" / "spark" / "render_hive_site.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("render_hive_site", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _properties(xml: str) -> dict[str, str]:
    root = ElementTree.fromstring(xml)
    return {p.findtext("name") or "": p.findtext("value") or "" for p in root.iter("property")}


def test_no_password_renders_nothing() -> None:
    render = _load().render
    assert render({}) is None
    assert render({"HIVE_METASTORE_DB_PASSWORD": ""}) is None


def test_password_with_xml_metacharacters_round_trips_escaped() -> None:
    password = "p<a>&\"s'"
    xml = _load().render({"HIVE_METASTORE_DB_PASSWORD": password})
    assert xml is not None
    assert "<a>" not in xml  # raw markup from the value must not reach the document
    props = _properties(xml)  # parses: the document is well-formed
    assert props["javax.jdo.option.ConnectionPassword"] == password


def test_defaults_and_overrides_for_connection_settings() -> None:
    render = _load().render
    default = _properties(render({"HIVE_METASTORE_DB_PASSWORD": "x"}) or "")
    assert (
        default["javax.jdo.option.ConnectionURL"]
        == "jdbc:postgresql://postgres:5432/hive_metastore"
    )
    assert default["javax.jdo.option.ConnectionUserName"] == "hive"
    custom = _properties(
        render(
            {
                "HIVE_METASTORE_DB_PASSWORD": "x",
                "HIVE_METASTORE_DB_HOST": "db:5433",
                "HIVE_METASTORE_DB_NAME": "meta",
                "HIVE_METASTORE_DB_USER": "u",
            }
        )
        or ""
    )
    assert custom["javax.jdo.option.ConnectionURL"] == "jdbc:postgresql://db:5433/meta"
    assert custom["javax.jdo.option.ConnectionUserName"] == "u"


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("datanucleus.schema.autoCreateConstraints", "false"),
        ("datanucleus.schema.autoCreateAll", "false"),
        ("datanucleus.schema.autoCreateTables", "true"),
        ("datanucleus.schema.autoCreateColumns", "true"),
        ("hive.metastore.schema.verification", "false"),
    ],
)
def test_datanucleus_flags(key: str, value: str) -> None:
    props = _properties(_load().render({"HIVE_METASTORE_DB_PASSWORD": "x"}) or "")
    assert props[key] == value


# ---- main(): file handling, no value printed, exec ------------------------------------------


def _run_main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, env: dict[str, str]) -> Path:
    module = _load()
    conf_dir = tmp_path / "hive-conf"
    for key in ("HIVE_METASTORE_DB_PASSWORD", "HIVE_METASTORE_DB_USER"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HADOOP_CONF_DIR", str(conf_dir))
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    module.main(["render_hive_site.py"])  # no command: nothing to exec
    return conf_dir / "hive-site.xml"


def test_main_writes_the_file_and_never_prints_the_password(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    target = _run_main(monkeypatch, tmp_path, {"HIVE_METASTORE_DB_PASSWORD": "s3cr3t-Value"})
    assert "s3cr3t-Value" in target.read_text(encoding="utf-8")
    out = capsys.readouterr()
    assert "s3cr3t-Value" not in out.out + out.err


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits; run in the Spark image")
def test_main_file_is_owner_read_write_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = _run_main(monkeypatch, tmp_path, {"HIVE_METASTORE_DB_PASSWORD": "x"})
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert stat.S_IMODE(target.parent.stat().st_mode) == 0o700


def test_main_without_password_removes_a_stale_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = _run_main(monkeypatch, tmp_path, {"HIVE_METASTORE_DB_PASSWORD": "x"})
    assert target.exists()
    monkeypatch.delenv("HIVE_METASTORE_DB_PASSWORD")
    module = _load()
    module.main(["render_hive_site.py"])
    assert not target.exists()


def test_main_execs_the_container_command(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load()
    monkeypatch.setenv("HADOOP_CONF_DIR", str(tmp_path / "c"))
    monkeypatch.delenv("HIVE_METASTORE_DB_PASSWORD", raising=False)
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(module.os, "execvp", lambda f, a: calls.append((f, list(a))))
    module.main(["render_hive_site.py", "/opt/spark/bin/spark-class", "Master"])
    assert calls == [("/opt/spark/bin/spark-class", ["/opt/spark/bin/spark-class", "Master"])]
