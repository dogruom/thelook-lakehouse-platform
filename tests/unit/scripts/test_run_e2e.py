"""run_e2e.plan (W4.D24.1): step order, seed passing, resume, unknown step."""

from __future__ import annotations

import pytest

from .conftest import load_script


mod = load_script("run_e2e")


def _names(**kwargs: object) -> list[str]:
    defaults: dict[str, object] = {
        "python": "py",
        "dbt": "dbt",
        "seed": None,
        "skip_generate": False,
    }
    defaults.update(kwargs)
    return [s.name for s in mod.plan(**defaults)]


def test_full_plan_order() -> None:
    assert _names() == ["produce", "land", *mod.SPARK_JOBS, "dbt_build"]
    assert mod.SPARK_JOBS[0] == "bronze_stream"
    assert mod.SPARK_JOBS[-1] == "reconcile"


def test_skip_generate_drops_produce_and_land() -> None:
    assert _names(skip_generate=True) == [*mod.SPARK_JOBS, "dbt_build"]


def test_seed_is_passed_to_both_generator_steps() -> None:
    steps = mod.plan(python="py", dbt="dbt", seed=7, skip_generate=False)
    for step in steps[:2]:
        assert step.argv[-2:] == ("--seed", "7")
    assert all("--seed" not in s.argv for s in steps[2:])


def test_only_the_first_spark_job_builds_the_zip() -> None:
    steps = mod.plan(python="py", dbt="dbt", seed=None, skip_generate=True)
    spark = [s for s in steps if "scripts/submit_job.py" in s.argv]
    assert "--skip-build" not in spark[0].argv
    assert all("--skip-build" in s.argv for s in spark[1:])


def test_dbt_runs_in_dbt_project_with_local_profiles() -> None:
    step = mod.plan(python="py", dbt="dbt", seed=None, skip_generate=True)[-1]
    assert step.cwd.name == "dbt_project"
    assert step.argv == (
        "dbt",
        "build",
        "--select",
        "staging",
        "marts",
        "--profiles-dir",
        "profiles",
    )


def test_from_step_resumes_and_unknown_step_raises() -> None:
    assert _names(from_step="reconcile") == ["reconcile", "dbt_build"]
    with pytest.raises(ValueError, match="unknown step"):
        _names(from_step="nope")


def test_dry_run_prints_plan_and_runs_nothing(capsys: pytest.CaptureFixture[str]) -> None:
    assert mod.main(["--dry-run", "--skip-generate"]) == 0
    out = capsys.readouterr().out
    assert "bronze_stream" in out
    assert "dbt_build" in out


def test_dlq_gate_names_all_four_bronze_tables() -> None:
    steps = {s.name: s for s in mod.plan(python="py", dbt="dbt", seed=None, skip_generate=True)}
    argv = steps["dlq_gate"].argv
    tables = [argv[i + 1] for i, a in enumerate(argv) if a == "--table"]
    assert tables == ["raw_customers", "raw_clickstream", "raw_orders", "raw_products"]


def test_skip_land_keeps_produce() -> None:
    assert _names(skip_land=True)[:2] == ["produce", mod.SPARK_JOBS[0]]


def test_jobs_and_dlq_gate_args_match_the_airflow_dag_plan() -> None:
    """One sequence for both runners: a drift here caused V-28 (dlq_gate skipped the file tables)."""
    from lakehouse.orchestration.plan import DLQ_GATE_JOB, pipeline_job_names

    assert pipeline_job_names() == mod.SPARK_JOBS
    assert mod.JOB_ARGS["dlq_gate"] == DLQ_GATE_JOB.args
