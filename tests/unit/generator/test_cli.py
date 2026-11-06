"""CLI: python -m lakehouse.generator cdc ... (W1.D4)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[3]


def cli(
    cwd: Path, *args: str, env_extra: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GEN_")}
    env["PYTHONPATH"] = os.pathsep.join([str(REPO / "src"), env.get("PYTHONPATH", "")])
    env.update(env_extra or {})
    return subprocess.run(
        [sys.executable, "-m", "lakehouse.generator", *args],
        cwd=cwd, env=env, capture_output=True, text=True, timeout=120, check=False,
    )  # fmt: skip


def test_dry_run_writes_events_and_manifest(tmp_path: Path) -> None:
    out = tmp_path / "out"
    proc = cli(
        tmp_path, "cdc", "--brands", "3", "--events", "300", "--dry-run", "--out-dir", str(out)
    )
    assert proc.returncode == 0, proc.stderr[-500:]
    lines = (out / "cdc_events.ndjson").read_text(encoding="utf-8").splitlines()
    manifest = json.loads((out / "cdc_manifest.json").read_text(encoding="utf-8"))
    assert len(lines) == manifest["emitted_events"] == len(manifest["entries"])
    assert manifest["base_events"] == 300
    assert set(json.loads(lines[0])) == {"key", "headers", "value"}


def test_default_out_dir_is_build_generated_cdc(tmp_path: Path) -> None:
    proc = cli(tmp_path, "cdc", "--brands", "2", "--events", "100", "--dry-run")
    assert proc.returncode == 0, proc.stderr[-500:]
    assert (tmp_path / "build" / "generated" / "cdc" / "cdc_events.ndjson").is_file()


def test_single_brand_run_with_zero_collision_rate_works(tmp_path: Path) -> None:
    proc = cli(
        tmp_path, "cdc", "--brands", "1", "--events", "300", "--dry-run", "--out-dir", str(tmp_path / "o"),
        env_extra={"GEN_CDC_CROSS_TENANT_COLLISION_RATE": "0"},
    )  # fmt: skip
    assert proc.returncode == 0, proc.stderr[-500:]
    assert (tmp_path / "o" / "cdc_manifest.json").is_file()


def test_single_brand_with_default_collision_rate_fails_with_a_message(tmp_path: Path) -> None:
    out = tmp_path / "o"
    proc = cli(
        tmp_path, "cdc", "--brands", "1", "--events", "300", "--dry-run", "--out-dir", str(out)
    )
    assert proc.returncode != 0
    assert proc.stderr.strip()
    assert "infeasible" in proc.stderr.lower()
    assert not (out / "cdc_events.ndjson").exists()


def test_brands_above_tenant_count_is_an_error_and_writes_nothing(tmp_path: Path) -> None:
    out = tmp_path / "o"
    proc = cli(
        tmp_path, "cdc", "--brands", "4", "--events", "300", "--dry-run", "--out-dir", str(out)
    )
    assert proc.returncode != 0
    assert proc.stderr.strip()
    assert not out.exists() or not any(out.iterdir())


def test_missing_dry_run_is_rejected_pointing_at_w1_d5(tmp_path: Path) -> None:
    out = tmp_path / "o"
    proc = cli(tmp_path, "cdc", "--brands", "2", "--events", "100", "--out-dir", str(out))
    assert proc.returncode != 0
    assert "W1.D5" in proc.stderr + proc.stdout
    assert not out.exists() or not any(out.iterdir())


def test_usage_error_exits_with_code_2(tmp_path: Path) -> None:
    assert cli(tmp_path, "cdc", "--dry-run").returncode == 2  # required args missing
    assert cli(tmp_path, "cdc", "--brands", "x", "--events", "10", "--dry-run").returncode == 2
    assert cli(tmp_path, "nonsense").returncode == 2


def test_stdout_has_no_ansi_and_logs_go_to_stderr(tmp_path: Path) -> None:
    proc = cli(
        tmp_path,
        "cdc",
        "--brands",
        "2",
        "--events",
        "100",
        "--dry-run",
        "--out-dir",
        str(tmp_path / "o"),
    )
    assert proc.returncode == 0, proc.stderr[-500:]
    assert "\x1b[" not in proc.stdout
    assert "cdc.generated" not in proc.stdout
    assert "cdc.generated" in proc.stderr  # structured logs are on stderr


# --- clickstream (W1.D5) -----------------------------------------------------------------


def test_clickstream_dry_run_writes_events_and_manifest(tmp_path: Path) -> None:
    out = tmp_path / "out"
    proc = cli(
        tmp_path, "clickstream", "--brands", "3", "--events", "5000", "--seed", "42",
        "--dry-run", "--out-dir", str(out),
    )  # fmt: skip
    assert proc.returncode == 0, proc.stderr[-500:]
    lines = (out / "clickstream_events.ndjson").read_text(encoding="utf-8").splitlines()
    manifest = json.loads((out / "clickstream_manifest.json").read_text(encoding="utf-8"))
    counts = manifest.get("counts", manifest)
    assert counts["base"] == 5000
    assert counts["emitted"] == len(lines) == 5200
    assert "\x1b[" not in proc.stdout


def test_clickstream_brands_out_of_range_is_an_error_and_writes_nothing(tmp_path: Path) -> None:
    out = tmp_path / "o"
    proc = cli(
        tmp_path, "clickstream", "--brands", "4", "--events", "100", "--dry-run",
        "--out-dir", str(out),
    )  # fmt: skip
    assert proc.returncode != 0
    assert proc.stderr.strip()
    assert not out.exists() or not any(out.iterdir())


def test_clickstream_usage_error_exits_with_code_2(tmp_path: Path) -> None:
    assert cli(tmp_path, "clickstream", "--dry-run").returncode == 2
    assert cli(tmp_path, "produce", "--brands", "x").returncode == 2


def test_produce_with_too_few_events_fails_cleanly_before_connecting(tmp_path: Path) -> None:
    # unreachable broker: the generation error must be reported before any producer is created
    proc = cli(tmp_path, "produce", "--events", "3", "--bootstrap-servers", "127.0.0.1:1")
    assert proc.returncode == 2
    assert "error:" in proc.stderr
    assert "Traceback" not in proc.stderr
