"""Freshness guard for the Spark differential-test fixtures (``tests/fixtures/rules_diff``).

The Spark image has no Faker, so ``tests/spark/test_rules.py`` replays generator output that is
frozen in JSON files. This host test regenerates the same data and fails when a fixture is stale.
Regenerate with ``REGEN_RULES_FIXTURES=1 pytest tests/unit/quality -q``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.clickstream import generate_clickstream
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.messages import OutboundMessage, cdc_messages


FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "rules_diff"
SEEDS = [1, 42]
BRANDS = 3
EVENTS = 300


def _messages(messages: list[OutboundMessage]) -> list[dict[str, Any]]:
    return [
        {
            "key": m.key,
            "value": m.value.decode("utf-8"),
            "headers": [[name, value.decode("utf-8")] for name, value in m.headers],
        }
        for m in messages
    ]


def build_fixture(seed: int) -> dict[str, Any]:
    config = GeneratorConfig(_env_file=None, seed=seed)  # type: ignore[call-arg]
    cdc_run = generate_cdc(config, brands=BRANDS, events=EVENTS)
    click_run = generate_clickstream(
        config, brands=BRANDS, events=EVENTS, topic="clickstream_events"
    )
    entries = cdc_run.manifest.entries
    return {
        "seed": seed,
        "cdc": {
            "messages": _messages(cdc_messages(cdc_run, "customer_cdc")),
            "dlq_indexes": [e.emission_index for e in entries if e.expected_outcome == "DLQ"],
            "defect_by_index": {
                str(e.emission_index): e.defect.value for e in entries if e.defect is not None
            },
            "counts": {
                k: cdc_run.manifest.counts[k]
                for k in ("invalid_email", "null_email", "future_signup")
            },
        },
        "click": {
            "messages": _messages(click_run.messages),
            "dlq_indexes": [
                e["index"] for e in click_run.manifest.entries if e["defect"] == "null_session"
            ],
            "parse_error_indexes": [
                e["index"] for e in click_run.manifest.entries if e["defect"] == "malformed_json"
            ],
            "defect_by_index": {str(e["index"]): e["defect"] for e in click_run.manifest.entries},
        },
    }


def _dump(data: dict[str, Any]) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":")) + "\n"


@pytest.mark.parametrize("seed", SEEDS)
def test_spark_differential_fixture_matches_the_generator(seed: int) -> None:
    path = FIXTURE_DIR / f"seed_{seed}.json"
    fresh = _dump(build_fixture(seed))
    if os.environ.get("REGEN_RULES_FIXTURES") == "1":
        path.write_text(fresh, encoding="utf-8", newline="\n")
    assert path.exists(), "run with REGEN_RULES_FIXTURES=1 to create the fixture"
    assert path.read_text(encoding="utf-8") == fresh, "stale fixture: regenerate"


@pytest.mark.parametrize("seed", SEEDS)
def test_fixture_contains_every_defect_kind(seed: int) -> None:
    data = build_fixture(seed)
    assert data["cdc"]["counts"] == {"invalid_email": 9, "null_email": 6, "future_signup": 6}
    assert len(data["cdc"]["dlq_indexes"]) == 21
    assert len(data["click"]["dlq_indexes"]) == 3
    assert len(data["click"]["parse_error_indexes"]) == 2
