"""submit_job: confirmed driver stop, run_submit exit codes and zip pruning.

Docker and subprocess are always faked; nothing here touches the real stack.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    from types import ModuleType

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "submit_job.py"
ZIP = "lakehouse_app-0123456789ab.zip"
MARKER_CONF = "spark.lakehouse.submit_marker"


@pytest.fixture(scope="module")
def submit() -> ModuleType:
    spec = importlib.util.spec_from_file_location("_script_submit_stop", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def no_sleep(submit: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(submit.time, "sleep", lambda _s: None)


# ---- kill_driver: only a confirmed stop counts -----------------------------------------------


def _ps_table(*rows: tuple[int, int, str]) -> str:
    return "  PID  PPID COMMAND\n" + "".join(f"{pid:5d} {ppid:5d} {a}\n" for pid, ppid, a in rows)


def _driver_rows(marker: str) -> tuple[tuple[int, int, str], ...]:
    jvm = f"java org.apache.spark.deploy.SparkSubmit --conf {MARKER_CONF}={marker}"
    return (
        (10, 1, jvm),
        (11, 10, "python3 /opt/spark-apps/dist/job_entry.py bronze_stream"),
    )


class _FakeExec:
    """Scripted stand-in for ``_docker_exec``: ``ps`` snapshots in order, ``kill`` recorded."""

    def __init__(self, snapshots: list[str | None], *, kill_ok: bool = True) -> None:
        self.snapshots = snapshots
        self.kill_ok = kill_ok
        self.kills: list[list[str]] = []

    def __call__(
        self, _container: str, command: list[str], _timeout: float = 30
    ) -> subprocess.CompletedProcess[str] | None:
        if command[0] == "ps":
            snap = self.snapshots.pop(0) if len(self.snapshots) > 1 else self.snapshots[0]
            if snap is None:
                return subprocess.CompletedProcess(command, 1, "", "boom")
            return subprocess.CompletedProcess(command, 0, snap, "")
        self.kills.append(list(command))
        return subprocess.CompletedProcess(command, 0, "", "") if self.kill_ok else None


def _kill(submit: ModuleType) -> bool:
    # Zero waits: the search and the grace period end after one pass each.
    return bool(
        submit.kill_driver("m1", "c", grace_seconds=0.0, find_seconds=0.0, poll_seconds=0.0)
    )


def test_kill_driver_confirms_when_every_pid_is_gone_after_sigterm(
    submit: ModuleType, monkeypatch: pytest.MonkeyPatch, no_sleep: None
) -> None:
    fake = _FakeExec([_ps_table(*_driver_rows("m1")), _ps_table((1, 0, "/sbin/init"))])
    monkeypatch.setattr(submit, "_docker_exec", fake)
    clock = iter([0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
    monkeypatch.setattr(submit.time, "monotonic", lambda: next(clock, 0.0))
    assert submit.kill_driver("m1", "c", grace_seconds=5.0, find_seconds=0.0, poll_seconds=0.0)
    assert fake.kills[0][:2] == ["kill", "-TERM"]
    assert sorted(fake.kills[0][2:]) == ["10", "11"]  # the JVM and its python child


def test_kill_driver_without_a_process_table_is_unconfirmed_and_warns(
    submit: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    no_sleep: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(submit, "_docker_exec", _FakeExec([None]))
    assert _kill(submit) is False
    err = capsys.readouterr().err
    assert "NOT confirmed" in err
    assert "docker exec crm_spark_master ps -eo pid,args" in err


def test_kill_driver_with_unknown_marker_is_unconfirmed_and_kills_nothing(
    submit: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    no_sleep: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fake = _FakeExec([_ps_table((1, 0, "/sbin/init"))])
    monkeypatch.setattr(submit, "_docker_exec", fake)
    assert _kill(submit) is False
    assert fake.kills == []
    assert "NOT confirmed" in capsys.readouterr().err


def test_kill_driver_unusable_kill_is_unconfirmed(
    submit: ModuleType, monkeypatch: pytest.MonkeyPatch, no_sleep: None
) -> None:
    monkeypatch.setattr(
        submit, "_docker_exec", _FakeExec([_ps_table(*_driver_rows("m1"))], kill_ok=False)
    )
    assert _kill(submit) is False


def test_kill_driver_survivors_after_sigkill_are_unconfirmed(
    submit: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    no_sleep: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Negative: signals were sent, but the processes are still listed afterwards."""
    fake = _FakeExec([_ps_table(*_driver_rows("m1"))])  # the same table every time
    monkeypatch.setattr(submit, "_docker_exec", fake)
    assert _kill(submit) is False
    assert any(k[:2] == ["kill", "-KILL"] for k in fake.kills)
    assert "survived SIGKILL" in capsys.readouterr().err


# ---- run_submit exit codes -------------------------------------------------------------------


class _FakeProc:
    def __init__(self, behaviour: str) -> None:
        self.behaviour = behaviour
        self.terminated = False

    def wait(self, timeout: float | None = None) -> int:
        if not self.terminated and self.behaviour == "timeout":
            raise subprocess.TimeoutExpired("docker", timeout or 0)
        if not self.terminated and self.behaviour == "interrupt":
            raise KeyboardInterrupt
        return 0 if self.behaviour == "ok" else -15

    def poll(self) -> int | None:
        return 0 if self.behaviour == "ok" or self.terminated else None

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.terminated = True


@pytest.mark.parametrize(
    ("behaviour", "confirmed", "expected"),
    [
        ("timeout", True, 124),
        ("timeout", False, 125),
        ("interrupt", True, 130),
        ("interrupt", False, 130),
    ],
)
def test_run_submit_exit_codes_after_timeout_or_interrupt(
    submit: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    behaviour: str,
    confirmed: bool,
    expected: int,
) -> None:
    proc = _FakeProc(behaviour)
    monkeypatch.setattr(submit.subprocess, "Popen", lambda *_a, **_k: proc)
    monkeypatch.setattr(submit, "kill_driver", lambda _m: confirmed)
    assert submit.run_submit(["docker", "exec"], "m1", 1.0) == expected
    assert proc.terminated  # the host client is always stopped as well
    err = capsys.readouterr().err
    if behaviour == "interrupt":
        # Never an unconditional "was stopped": the message states which outcome happened.
        assert ("was NOT confirmed" in err) == (not confirmed)
        assert ("stop was confirmed" in err) == confirmed


def test_run_submit_returns_the_exit_code_of_a_finished_command_without_killing(
    submit: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    killed: list[str] = []
    monkeypatch.setattr(submit.subprocess, "Popen", lambda *_a, **_k: _FakeProc("ok"))
    monkeypatch.setattr(submit, "kill_driver", lambda m: killed.append(m) or True)
    assert submit.run_submit(["docker", "exec"], "m1", None) == 0
    assert killed == []


class _FinishedAtDeadline(_FakeProc):
    """wait() times out, but the command has exited by the time poll() is called."""

    def wait(self, timeout: float | None = None) -> int:
        raise subprocess.TimeoutExpired("docker", timeout or 0)

    def poll(self) -> int | None:
        return 3


def test_run_submit_keeps_the_real_code_when_the_command_ends_at_the_deadline(
    submit: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    killed: list[str] = []
    monkeypatch.setattr(submit.subprocess, "Popen", lambda *_a, **_k: _FinishedAtDeadline("x"))
    monkeypatch.setattr(submit, "kill_driver", lambda m: killed.append(m) or True)
    assert submit.run_submit(["docker", "exec"], "m1", 1.0) == 3
    assert killed == []


# ---- pruning ---------------------------------------------------------------------------------


def _make_zips(dist: Path, count: int) -> list[Path]:
    dist.mkdir(exist_ok=True)
    zips: list[Path] = []
    for i in range(count):
        path = dist / f"lakehouse_app-{i:012x}.zip"
        path.write_bytes(b"z")
        os.utime(path, (1_000_000 + i, 1_000_000 + i))
        zips.append(path)
    return zips  # oldest first


def test_prune_keeps_the_newest_five_and_deletes_the_rest(
    submit: ModuleType, tmp_path: Path
) -> None:
    zips = _make_zips(tmp_path / "dist", 8)
    submit._prune_zips(tmp_path / "dist", zips[-1], "  1 /sbin/init\n")
    assert [p.exists() for p in zips] == [False] * 3 + [True] * 5


def test_prune_never_deletes_a_zip_a_running_job_uses(submit: ModuleType, tmp_path: Path) -> None:
    zips = _make_zips(tmp_path / "dist", 8)
    ps = f"  77 java SparkSubmit --py-files /opt/spark-apps/dist/{zips[0].name} job_entry.py\n"
    submit._prune_zips(tmp_path / "dist", zips[-1], ps)
    assert zips[0].exists()  # oldest, but in use
    assert not zips[1].exists()


def test_prune_is_skipped_entirely_when_ps_is_unavailable(
    submit: ModuleType, tmp_path: Path
) -> None:
    zips = _make_zips(tmp_path / "dist", 8)
    submit._prune_zips(tmp_path / "dist", zips[-1], None)
    assert all(p.exists() for p in zips)


def _patch_main(
    submit: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, preflight: tuple[int, str]
) -> list[tuple[str, object]]:
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(submit, "_master_running", lambda: True)
    monkeypatch.setattr(submit, "DIST_DIR", tmp_path / "dist")
    monkeypatch.setattr(submit, "build_app_zip", lambda *_a: tmp_path / "dist" / ZIP)
    monkeypatch.setattr(submit, "stage_entry", lambda *_a: None)
    monkeypatch.setattr(submit, "_preflight", lambda *_a: preflight)
    monkeypatch.setattr(submit, "_prune_zips", lambda _d, _k, ps: calls.append(("prune", ps)))
    monkeypatch.setattr(submit, "run_submit", lambda *_a: calls.append(("run", None)) or 0)
    return calls


def test_a_refused_preflight_neither_prunes_nor_submits(
    submit: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls = _patch_main(submit, monkeypatch, tmp_path, (3, "ps"))
    assert submit.main(["hello_delta"]) == 3
    assert calls == []


def test_a_passed_preflight_prunes_with_the_ps_output_then_submits(
    submit: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls = _patch_main(submit, monkeypatch, tmp_path, (0, "ps-output"))
    assert submit.main(["hello_delta"]) == 0
    assert calls == [("prune", "ps-output"), ("run", None)]
