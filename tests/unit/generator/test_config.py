"""Unit tests for the generator configuration (GeneratorConfig v1).

The model is rewritten in W1.D3.2 (contracts alignment); these tests pin the current behaviour:
defaults, environment overrides with the GEN_ prefix, and bound validation.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lakehouse.generator.config import GeneratorConfig


def test_defaults_instantiate() -> None:
    config = GeneratorConfig()

    assert config.batch_size == 500
    assert config.kafka_topic == "clickstream_events"
    assert 0 <= config.click_late_arrival_rate <= 50


def test_environment_variable_overrides_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEN_BATCH_SIZE", "1000")

    assert GeneratorConfig().batch_size == 1000


@pytest.mark.parametrize("value", [5, 20_000])
def test_batch_size_outside_bounds_is_rejected(value: int) -> None:
    with pytest.raises(ValidationError):
        GeneratorConfig(batch_size=value)


def test_dirty_rate_above_fifty_percent_is_rejected() -> None:
    with pytest.raises(ValidationError):
        GeneratorConfig(user_invalid_email_rate=51.0)


def test_every_dirty_rate_default_is_a_valid_percentage() -> None:
    config = GeneratorConfig()
    rates = {name: value for name, value in config.model_dump().items() if name.endswith("_rate")}

    assert rates, "expected dirty-data rate fields"
    assert all(0 <= value <= 50 for value in rates.values())
