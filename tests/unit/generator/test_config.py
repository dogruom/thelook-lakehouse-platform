"""Unit tests for GeneratorConfig v2 (W1.D3): defaults, GEN_ env overrides, validators, defect_count."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from lakehouse.generator.config import GeneratorConfig, defect_count


RATE_DEFAULTS = {
    "cdc_invalid_email_rate": 3.0,
    "cdc_null_email_rate": 2.0,
    "cdc_future_signup_rate": 2.0,
    "cdc_out_of_order_rate": 5.0,
    "cdc_duplicate_rate": 2.0,
    "cdc_cross_tenant_collision_rate": 1.0,
    "order_orphan_fk_rate": 3.0,
    "order_late_parent_rate": 2.0,
    "order_duplicate_rate": 4.0,
    "order_future_ts_rate": 2.0,
    "order_negative_amount_rate": 1.0,
    "click_late_rate": 10.0,
    "click_duplicate_rate": 4.0,
    "click_null_session_rate": 1.0,
    "click_malformed_json_rate": 0.5,
}


DLQ_RATES = ("cdc_invalid_email_rate", "cdc_null_email_rate", "cdc_future_signup_rate")


@pytest.fixture(autouse=True)
def _clean_gen_env(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    for key in [k for k in os.environ if k.startswith("GEN_")]:
        monkeypatch.delenv(key)


def make(**kwargs: Any) -> GeneratorConfig:
    """Build a config without reading any .env file."""
    return GeneratorConfig(_env_file=None, **kwargs)  # type: ignore[call-arg]


# --------------------------------------------------------------------------- defaults


def test_defaults_match_the_card() -> None:
    config = make()

    assert config.seed == 42
    assert config.tenants == ["brand_alpha", "brand_beta", "brand_gamma"]
    assert config.tenant_weights == [0.6, 0.3, 0.1]
    assert config.kafka_bootstrap_servers == "localhost:9094"
    assert config.topic_customer_cdc == "customer_cdc"
    assert config.topic_clickstream == "clickstream_events"


@pytest.mark.parametrize(("name", "expected"), sorted(RATE_DEFAULTS.items()))
def test_rate_defaults(name: str, expected: float) -> None:
    assert getattr(make(), name) == expected


def test_no_unexpected_rate_fields_exist() -> None:
    rate_fields = {n for n in GeneratorConfig.model_fields if n.endswith("_rate")}

    assert rate_fields == set(RATE_DEFAULTS)


def test_no_s3_fields_remain() -> None:
    assert [n for n in GeneratorConfig.model_fields if n.startswith("s3_")] == []


def test_v1_fields_are_removed() -> None:
    legacy = {"batch_size", "kafka_topic", "click_late_arrival_rate", "user_invalid_email_rate"}

    assert legacy.isdisjoint(GeneratorConfig.model_fields)


def test_secret_like_fields_have_no_non_empty_default() -> None:
    offenders = []
    for name, field in GeneratorConfig.model_fields.items():
        if name.startswith("topic_"):
            continue  # Kafka topic names that contain "key" are not credentials
        secret_like = any(w in name.lower() for w in ("secret", "password", "key", "token"))
        if secret_like and not field.is_required() and field.default not in (None, ""):
            offenders.append(name)
    assert offenders == []


# --------------------------------------------------------------------------- env overrides


def test_env_overrides_scalar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEN_SEED", "7")
    monkeypatch.setenv("GEN_KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("GEN_CDC_INVALID_EMAIL_RATE", "1.5")

    config = make()

    assert (config.seed, config.kafka_bootstrap_servers) == (7, "broker:9092")
    assert config.cdc_invalid_email_rate == 1.5


def test_env_overrides_lists_as_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEN_TENANTS", '["brand_a","brand_b"]')
    monkeypatch.setenv("GEN_TENANT_WEIGHTS", "[0.5, 0.5]")

    config = make()

    assert config.tenants == ["brand_a", "brand_b"]
    assert config.tenant_weights == [0.5, 0.5]


def test_env_value_violating_bounds_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEN_CLICK_LATE_RATE", "75")

    with pytest.raises(ValidationError):
        make()


# --------------------------------------------------------------------------- tenants / weights


@pytest.mark.parametrize(
    "tenants",
    [["brand_a", "brand_a"], ["Brand_A"], ["alpha"], ["brand_"], [""], []],
)
def test_invalid_tenants_are_rejected(tenants: list[str]) -> None:
    weights = [1.0] * len(tenants)

    with pytest.raises(ValidationError):
        make(tenants=tenants, tenant_weights=weights)


def test_single_valid_tenant_is_accepted() -> None:
    config = make(tenants=["brand_solo"], tenant_weights=[1.0])

    assert config.tenants == ["brand_solo"]


def test_weights_length_must_match_tenants() -> None:
    with pytest.raises(ValidationError):
        make(tenants=["brand_a", "brand_b"], tenant_weights=[1.0])
    with pytest.raises(ValidationError):
        make(tenants=["brand_a"])  # default 3 weights vs 1 tenant


@pytest.mark.parametrize("weights", [[0.0, 1.0], [-0.1, 1.0]])
def test_non_positive_weight_is_rejected(weights: list[float]) -> None:
    with pytest.raises(ValidationError):
        make(tenants=["brand_a", "brand_b"], tenant_weights=weights)


# --------------------------------------------------------------------------- rates


@pytest.mark.parametrize("name", sorted(RATE_DEFAULTS))
@pytest.mark.parametrize("value", [-0.01, 50.01])
def test_rate_outside_zero_to_fifty_is_rejected(name: str, value: float) -> None:
    with pytest.raises(ValidationError):
        make(**{name: value})


@pytest.mark.parametrize("name", sorted(RATE_DEFAULTS))
@pytest.mark.parametrize("value", [0.0, 50.0])
def test_rate_boundaries_are_accepted_individually(name: str, value: float) -> None:
    kwargs: dict[str, float] = {name: value}
    if value == 50.0:  # keep the DLQ-sum rule satisfied when one DLQ rate sits at its bound
        kwargs |= {k: 0.0 for k in DLQ_RATES if k != name}
    assert getattr(make(**kwargs), name) == value


def test_dlq_rate_sum_of_exactly_fifty_is_accepted() -> None:
    config = make(cdc_invalid_email_rate=25.0, cdc_null_email_rate=20.0, cdc_future_signup_rate=5.0)

    assert config.cdc_invalid_email_rate == 25.0


def test_dlq_rate_sum_above_fifty_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make(cdc_invalid_email_rate=25.0, cdc_null_email_rate=20.0, cdc_future_signup_rate=5.01)


def test_non_dlq_rates_do_not_count_toward_the_sum_rule() -> None:
    config = make(cdc_out_of_order_rate=50.0, cdc_duplicate_rate=50.0, click_late_rate=50.0)

    assert config.cdc_out_of_order_rate == 50.0


# --------------------------------------------------------------------------- defect_count


@pytest.mark.parametrize(
    ("rate", "n", "expected"),
    [
        (2.5, 100, 2),  # 2.5 -> even 2
        (3.5, 100, 4),  # 3.5 -> even 4
        (0.5, 100, 0),  # 0.5 -> even 0
        (1.5, 100, 2),
        (12.5, 10, 1),  # 1.25 -> 1
        (12.5, 12, 2),  # 1.5 -> 2
        (12.5, 4, 0),  # 0.5 -> 0
        (2.5, 10, 0),  # 0.25 -> 0
        (3.0, 1000, 30),
        (0.5, 1000, 5),
        (1.1, 1000, 11),  # exact via str(): no binary-float drift
        (0.0, 1000, 0),
        (50.0, 7, 4),  # 3.5 -> 4
        (4.0, 0, 0),
    ],
)
def test_defect_count_rounds_half_even(rate: float, n: int, expected: int) -> None:
    result = defect_count(rate, n)

    assert result == expected
    assert isinstance(result, int)


def test_defect_count_accepts_decimal_rate() -> None:
    assert defect_count(Decimal("2.5"), 100) == 2
    assert defect_count(Decimal("3.5"), 100) == 4


def test_defect_count_negative_n_raises_value_error() -> None:
    with pytest.raises(ValueError):
        defect_count(1.0, -1)


def test_defect_count_half_even_differs_from_half_up() -> None:
    """Negative: the expectations above would fail under round-half-up."""
    assert defect_count(2.5, 100) != 3
    assert defect_count(0.5, 100) != 1
