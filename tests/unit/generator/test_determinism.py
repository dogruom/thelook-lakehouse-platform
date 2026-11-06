"""Determinism (D-027): same seed -> identical bytes, regardless of PYTHONHASHSEED."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.io import write_run


REPO = Path(__file__).resolve().parents[3]
FILES = ("cdc_events.ndjson", "cdc_manifest.json")


def make(**kwargs: Any) -> GeneratorConfig:
    return GeneratorConfig(_env_file=None, **kwargs)  # type: ignore[call-arg]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(seed: int, out: Path) -> dict[str, str]:
    run = generate_cdc(make(seed=seed), brands=2, events=300)
    events, manifest = write_run(run, out)
    assert (events.name, manifest.name) == FILES
    return {p.name: digest(p) for p in (events, manifest)}


def test_same_seed_produces_identical_file_hashes(tmp_path: Path) -> None:
    assert write(42, tmp_path / "a") == write(42, tmp_path / "b")


def test_different_seed_produces_different_files(tmp_path: Path) -> None:
    a, b = write(42, tmp_path / "a"), write(43, tmp_path / "b")
    assert a["cdc_events.ndjson"] != b["cdc_events.ndjson"]
    assert a["cdc_manifest.json"] != b["cdc_manifest.json"]


def test_output_files_are_canonical_utf8_lf(tmp_path: Path) -> None:
    write(42, tmp_path)
    ndjson = (tmp_path / FILES[0]).read_bytes()
    manifest = (tmp_path / FILES[1]).read_bytes()
    assert b"\r" not in ndjson + manifest
    assert ndjson.endswith(b"\n")
    for line in ndjson.decode("utf-8").splitlines():
        obj = json.loads(line)
        assert json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False) == line
    text = manifest.decode("utf-8")
    assert json.dumps(
        json.loads(text), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ) == text.rstrip("\n")


def _cli(out: Path, hashseed: str) -> None:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GEN_")}
    env["PYTHONHASHSEED"] = hashseed
    env["PYTHONPATH"] = os.pathsep.join([str(REPO / "src"), env.get("PYTHONPATH", "")])
    proc = subprocess.run(
        [sys.executable, "-m", "lakehouse.generator", "cdc", "--brands", "3", "--events", "300",
         "--seed", "42", "--dry-run", "--out-dir", str(out)],
        cwd=out.parent, env=env, capture_output=True, text=True, timeout=120, check=False,
    )  # fmt: skip
    assert proc.returncode == 0, proc.stderr[-500:]


def test_cli_output_is_identical_across_python_hash_seeds(tmp_path: Path) -> None:
    out0, out1 = tmp_path / "h0", tmp_path / "h12345"
    _cli(out0, "0")
    _cli(out1, "12345")
    for name in FILES:
        assert (out0 / name).read_bytes() == (out1 / name).read_bytes(), name


def test_negative_hash_seed_comparison_detects_a_seed_change(tmp_path: Path) -> None:
    """The byte comparison above can fail: a different --seed must differ."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("GEN_")}
    env["PYTHONPATH"] = str(REPO / "src")
    out = tmp_path / "s"
    _cli(tmp_path / "base", "0")
    subprocess.run(
        [sys.executable, "-m", "lakehouse.generator", "cdc", "--brands", "3", "--events", "300",
         "--seed", "99", "--dry-run", "--out-dir", str(out)],
        cwd=tmp_path, env=env, capture_output=True, check=True, timeout=120,
    )  # fmt: skip
    assert (out / FILES[0]).read_bytes() != (tmp_path / "base" / FILES[0]).read_bytes()


def test_email_slugs_are_ascii_lowercase() -> None:
    run = generate_cdc(make(), brands=3, events=300)
    for line in run.lines:
        email = line["value"]["email"]
        if email is None or "@" not in email:
            continue  # defect payloads
        local = email.split("@")[0]
        assert local.isascii()
        assert local == local.lower()
