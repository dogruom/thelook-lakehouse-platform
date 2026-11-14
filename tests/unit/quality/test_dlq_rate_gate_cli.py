"""dlq_rate_gate job: name, CLI contract and the breach path with stubbed reads (offline)."""

from __future__ import annotations

import argparse
from typing import Any

import pytest

from lakehouse.quality.dlq_rate import DlqRateBreachError
from lakehouse.quality.jobs import dlq_rate_gate
from lakehouse.quality.jobs.dlq_rate_gate import JOB


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="dlq_rate_gate")
    JOB().add_arguments(parser)
    return parser.parse_args(list(argv))


def test_job_name_matches_module_and_resolves() -> None:
    from lakehouse.common.jobs import resolve_job

    assert JOB.name == "dlq_rate_gate"
    assert resolve_job("dlq_rate_gate") is JOB


def test_brand_is_repeatable_and_both_args_are_required() -> None:
    ns = _parse("--max-dlq-rate", "0.15", "--brand", "brand_alpha", "--brand", "brand_beta")
    assert ns.brand == ["brand_alpha", "brand_beta"]
    assert ns.max_dlq_rate == 0.15
    assert ns.namespace is None
    with pytest.raises(SystemExit):
        _parse("--brand", "brand_alpha")
    with pytest.raises(SystemExit):
        _parse("--max-dlq-rate", "0.1")


@pytest.mark.parametrize("rate", ["-0.1", "1.5", "abc", "nan"])
def test_bad_rate_is_a_usage_error(rate: str) -> None:
    with pytest.raises(SystemExit):
        _parse("--max-dlq-rate", rate, "--brand", "brand_alpha")


@pytest.mark.parametrize("brand", ["alpha", "brand_A", "brand_x\n", "jane@example.com"])
def test_bad_brand_is_a_usage_error(brand: str) -> None:
    with pytest.raises(SystemExit):
        _parse("--max-dlq-rate", "0.1", "--brand", brand)


def _run(
    monkeypatch: pytest.MonkeyPatch, counts: dict[tuple[str, str], tuple[int, int]], *argv: str
) -> Any:
    monkeypatch.setattr(dlq_rate_gate, "read_counts", lambda *_a, **_k: counts)
    return JOB().run(object(), _parse(*argv), object())  # type: ignore[arg-type]


def test_run_returns_report_when_all_brands_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    counts = {("brand_alpha", "raw_customers"): (100, 5)}
    out = _run(monkeypatch, counts, "--max-dlq-rate", "0.15", "--brand", "brand_alpha")
    assert out["max_dlq_rate"] == 0.15
    assert out["brands"]["brand_alpha"]["raw_customers"]["dlq_rate"] == 0.05


def test_run_raises_naming_the_breaching_brand(monkeypatch: pytest.MonkeyPatch) -> None:
    counts = {("brand_beta", "raw_orders"): (100, 40), ("brand_alpha", "raw_orders"): (100, 1)}
    with pytest.raises(DlqRateBreachError, match="brand_beta") as err:
        _run(
            monkeypatch,
            counts,
            "--max-dlq-rate",
            "0.15",
            "--brand",
            "brand_alpha",
            "--brand",
            "brand_beta",
        )
    assert "brand_alpha" not in str(err.value).split("(")[0]
