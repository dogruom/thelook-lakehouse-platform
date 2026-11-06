"""Generator configuration (v2, DATA_CONTRACTS.md §2).

All values can be overridden with ``GEN_``-prefixed environment variables; list values are JSON,
e.g. ``GEN_TENANTS='["brand_alpha","brand_beta"]'``. Defect rates are percentages: the generator
injects ``defect_count(rate, n)`` defects exactly and records them in the manifest.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Annotated

from pydantic import AwareDatetime, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from lakehouse.contracts import BRAND_ID_PATTERN


RatePct = Annotated[float, Field(ge=0, le=50)]

_BRAND_ID_RE = re.compile(BRAND_ID_PATTERN)


def defect_count(rate_pct: float | Decimal, n: int) -> int:
    """Number of defects for ``n`` records at ``rate_pct`` percent (round half to even)."""
    if n < 0:
        raise ValueError("n must be >= 0")
    exact = Decimal(str(rate_pct)) * n / 100
    return int(exact.quantize(Decimal(1), rounding=ROUND_HALF_EVEN))


class GeneratorConfig(BaseSettings):
    """Seeded, deterministic generator settings."""

    model_config = SettingsConfigDict(env_prefix="GEN_")

    seed: int = 42
    tenants: list[str] = ["brand_alpha", "brand_beta", "brand_gamma"]
    tenant_weights: list[float] = [0.6, 0.3, 0.1]

    kafka_bootstrap_servers: str = "localhost:9094"
    topic_customer_cdc: str = "customer_cdc"
    topic_clickstream: str = "clickstream_events"

    # Simulated clock origin: every generated timestamp is derived from it, never from wall time
    sim_start: AwareDatetime = datetime(2026, 1, 1, tzinfo=UTC)

    # Customer CDC stream
    cdc_max_updates: int = Field(default=4, ge=0, le=20)
    cdc_delete_rate: RatePct = 5.0
    cdc_invalid_email_rate: RatePct = 3.0
    cdc_null_email_rate: RatePct = 2.0
    cdc_future_signup_rate: RatePct = 2.0
    cdc_out_of_order_rate: RatePct = 5.0
    cdc_duplicate_rate: RatePct = 2.0
    cdc_cross_tenant_collision_rate: RatePct = 1.0

    # orders
    order_orphan_fk_rate: RatePct = 3.0
    order_late_parent_rate: RatePct = 2.0
    order_duplicate_rate: RatePct = 4.0
    order_future_ts_rate: RatePct = 2.0
    order_negative_amount_rate: RatePct = 1.0

    # clickstream
    click_late_rate: RatePct = 10.0
    click_duplicate_rate: RatePct = 4.0
    click_null_session_rate: RatePct = 1.0
    click_malformed_json_rate: RatePct = 0.5

    @field_validator("tenants")
    @classmethod
    def _valid_tenants(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("at least one tenant is required")
        if len(set(value)) != len(value):
            raise ValueError("tenants must be unique")
        bad = [t for t in value if not _BRAND_ID_RE.fullmatch(t)]
        if bad:
            raise ValueError(f"tenant ids must match {BRAND_ID_PATTERN}: {bad}")
        return value

    @field_validator("tenant_weights")
    @classmethod
    def _positive_weights(cls, value: list[float]) -> list[float]:
        if any(w <= 0 for w in value):
            raise ValueError("tenant_weights must all be > 0")
        return value

    @model_validator(mode="after")
    def _consistent(self) -> GeneratorConfig:
        if len(self.tenant_weights) != len(self.tenants):
            raise ValueError("tenant_weights must have the same length as tenants")
        dlq_share = (
            self.cdc_invalid_email_rate + self.cdc_null_email_rate + self.cdc_future_signup_rate
        )
        if dlq_share > 50:
            raise ValueError("cdc invalid_email + null_email + future_signup rates must be <= 50")
        return self
