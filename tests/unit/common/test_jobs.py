# ruff: noqa: ARG002
"""Job framework: convention-based resolution and the ``main`` runner (no pyspark needed)."""

from __future__ import annotations

import json
import subprocess
import sys
import types
from importlib.machinery import ModuleSpec
from typing import TYPE_CHECKING, Any

import pytest
import structlog

from lakehouse.common import jobs
from lakehouse.common.jobs import JobContext, SparkJob, UnknownJobError, resolve_job
from lakehouse.common.spark import SparkSettings


if TYPE_CHECKING:
    import argparse
    from collections.abc import Callable, Iterator, Mapping


class FakeSession:
    def __init__(self) -> None:
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True


class Factory:
    def __init__(self) -> None:
        self.sessions: list[FakeSession] = []
        self.calls: list[dict[str, Any]] = []

    def __call__(self, settings: SparkSettings, *, app_name: str, run_id: str) -> FakeSession:
        self.calls.append({"app_name": app_name, "run_id": run_id})
        s = FakeSession()
        self.sessions.append(s)
        return s


class OkJob(SparkJob):
    name = "fake_ok"

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--rows", type=int, default=3)

    def run(self, spark: Any, args: argparse.Namespace, ctx: JobContext) -> Mapping[str, Any]:
        return {"rows": args.rows, "seen_run_id": ctx.run_id}


class BoomJob(SparkJob):
    name = "fake_boom"

    def run(self, spark: Any, args: argparse.Namespace, ctx: JobContext) -> Mapping[str, Any]:
        raise RuntimeError("kaboom")


def _fake_resolver(name: str) -> type[SparkJob]:
    table: dict[str, type[SparkJob]] = {"fake_ok": OkJob, "fake_boom": BoomJob}
    if name not in table:
        raise UnknownJobError(name)
    return table[name]


@pytest.fixture(autouse=True)
def _logging(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    # Not configured here: the handler binds sys.stdout when configured, and a stream from the
    # setup phase is closed by the call phase. jobs.main() configures it inside the test.
    monkeypatch.setenv("ENVIRONMENT", "local")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    yield
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


@pytest.fixture
def fake_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobs, "resolve_job", _fake_resolver)


def _records(text: str) -> list[dict[str, Any]]:
    return [json.loads(ln) for ln in (s.strip() for s in text.splitlines()) if ln.startswith("{")]


# ---- main ------------------------------------------------------------------------------------


@pytest.mark.usefixtures("fake_resolver")
def test_success_returns_0_logs_metrics_and_stops_session(
    capsys: pytest.CaptureFixture[str],
) -> None:
    f = Factory()
    rc = jobs.main(["fake_ok", "--rows", "7"], session_factory=f, settings=SparkSettings())
    assert rc == 0
    assert f.sessions[0].stopped is True
    assert f.calls[0]["app_name"] == "lakehouse-fake_ok"
    done = [r for r in _records(capsys.readouterr().out) if r["event"] == "job.succeeded"]
    assert len(done) == 1
    assert done[0]["metrics"] == {"rows": 7, "seen_run_id": f.calls[0]["run_id"]}
    assert "duration_s" in done[0]


@pytest.mark.usefixtures("fake_resolver")
def test_every_log_line_of_the_run_carries_the_run_id(
    capsys: pytest.CaptureFixture[str],
) -> None:
    f = Factory()
    jobs.main(["fake_ok"], session_factory=f, settings=SparkSettings())
    recs = _records(capsys.readouterr().out)
    assert {"job.started", "job.succeeded"} <= {r["event"] for r in recs}
    assert all(r["run_id"] == f.calls[0]["run_id"] for r in recs)
    assert all(r["job"] == "fake_ok" for r in recs)


@pytest.mark.usefixtures("fake_resolver")
def test_run_context_is_unbound_after_the_run() -> None:
    jobs.main(["fake_ok"], session_factory=Factory(), settings=SparkSettings())
    assert "run_id" not in structlog.contextvars.get_contextvars()


@pytest.mark.usefixtures("fake_resolver")
def test_job_exception_returns_1_logs_failure_and_still_stops_session(
    capsys: pytest.CaptureFixture[str],
) -> None:
    f = Factory()
    rc = jobs.main(["fake_boom"], session_factory=f, settings=SparkSettings())
    assert rc == 1
    assert f.sessions[0].stopped is True
    recs = _records(capsys.readouterr().out)
    failed = [r for r in recs if r["event"] == "job.failed"]
    assert len(failed) == 1 and failed[0]["error_type"] == "RuntimeError"
    assert not [r for r in recs if r["event"] == "job.succeeded"]


@pytest.mark.usefixtures("fake_resolver")
def test_stop_session_false_leaves_session_running() -> None:
    f = Factory()
    jobs.main(["fake_ok"], session_factory=f, settings=SparkSettings(), stop_session=False)
    assert f.sessions[0].stopped is False


@pytest.mark.usefixtures("fake_resolver")
def test_unknown_job_returns_2_without_creating_a_session() -> None:
    f = Factory()
    assert jobs.main(["nope"], session_factory=f, settings=SparkSettings()) == 2
    assert f.sessions == []


@pytest.mark.usefixtures("fake_resolver")
def test_bad_job_arguments_return_2_without_creating_a_session() -> None:
    f = Factory()
    assert jobs.main(["fake_ok", "--rows", "x"], session_factory=f, settings=SparkSettings()) == 2
    assert f.sessions == []


@pytest.mark.usefixtures("fake_resolver")
def test_invalid_settings_return_2_without_leaking_the_value(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("LAKEHOUSE_ENV", "user:hunter2@nowhere")
    f = Factory()
    assert jobs.main(["fake_ok"], session_factory=f) == 2
    assert f.sessions == []
    out = capsys.readouterr().out
    assert "job.invalid_settings" in out
    assert "hunter2" not in out


@pytest.mark.usefixtures("fake_resolver")
def test_failing_session_stop_does_not_replace_the_job_result(
    capsys: pytest.CaptureFixture[str],
) -> None:
    class BadStop(FakeSession):
        def stop(self) -> None:
            raise RuntimeError("stop failed")

    rc = jobs.main(
        ["fake_ok"], session_factory=lambda *_, **__: BadStop(), settings=SparkSettings()
    )
    assert rc == 0
    events = {r["event"] for r in _records(capsys.readouterr().out)}
    assert {"job.succeeded", "job.stop_failed"} <= events


# ---- resolve_job -----------------------------------------------------------------------------


class _Job(SparkJob):
    name = "placeholder"

    def run(self, spark: Any, args: argparse.Namespace, ctx: JobContext) -> Mapping[str, Any]:
        return {}


def _job_named(job_name: str) -> type[SparkJob]:
    return type("J", (_Job,), {"name": job_name})


@pytest.fixture
def add_module() -> Iterator[Callable[..., None]]:
    created: list[str] = []

    def add(component: str, name: str, **attrs: Any) -> None:
        full = f"lakehouse.{component}.jobs.{name}"
        mod = types.ModuleType(full)
        mod.__spec__ = ModuleSpec(full, loader=None)
        for k, v in attrs.items():
            setattr(mod, k, v)
        sys.modules[full] = mod
        created.append(full)

    yield add
    for m in created:
        sys.modules.pop(m, None)


def test_resolve_job_finds_module_by_convention(add_module: Callable[..., None]) -> None:
    cls = _job_named("conv_job")
    add_module("quality", "conv_job", JOB=cls)
    assert resolve_job("conv_job") is cls


def test_resolve_job_unknown_raises_unknown_job_error() -> None:
    with pytest.raises(UnknownJobError):
        resolve_job("definitely_missing_job")


def test_resolve_job_ambiguous_across_components_raises(add_module: Callable[..., None]) -> None:
    cls = _job_named("dup_job")
    add_module("silver", "dup_job", JOB=cls)
    add_module("ops", "dup_job", JOB=cls)
    with pytest.raises(UnknownJobError, match="ambiguous"):
        resolve_job("dup_job")


def test_resolve_job_rejects_non_sparkjob_target(add_module: Callable[..., None]) -> None:
    add_module("silver", "bad_type_job", JOB=int)
    with pytest.raises(TypeError):
        resolve_job("bad_type_job")


def test_resolve_job_rejects_name_mismatch(add_module: Callable[..., None]) -> None:
    add_module("silver", "mismatch_job", JOB=_job_named("other_name"))
    with pytest.raises(TypeError):
        resolve_job("mismatch_job")


def test_hello_delta_resolves_by_convention() -> None:
    assert resolve_job("hello_delta").name == "hello_delta"


def test_job_modules_import_with_pyspark_blocked() -> None:
    """Host venv has no pyspark: common + hello_delta modules must import it lazily."""
    code = (
        "import sys; sys.modules['pyspark'] = None\n"
        "import lakehouse.common.jobs\n"
        "import lakehouse.common.spark\n"
        "import lakehouse.ops.jobs.hello_delta as m\n"
        "assert m.JOB.name == 'hello_delta'\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=60, check=False
    )
    assert proc.returncode == 0, proc.stderr[-800:]
