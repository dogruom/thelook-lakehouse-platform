"""Generator configuration model.

Centralized configuration for the e-commerce data generator.
Uses pydantic-settings for environment variable support with sensible defaults.
All dirty data injection rates are configurable here.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GeneratorConfig(BaseSettings):
    """Configuration for the e-commerce data generator.

    All settings can be overridden via environment variables prefixed with ``GEN_``.
    Example: ``GEN_BATCH_SIZE=1000 python -m lakehouse.generator``.
    Version 1 — aligned with the data contracts in W1.D3.2.
    """

    model_config = SettingsConfigDict(env_prefix="GEN_")

    # ── Batch settings ──────────────────────────────────────────────────────
    batch_size: int = Field(default=500, ge=10, le=10_000, description="Records per batch")
    num_batches: int = Field(default=15, ge=1, le=100, description="Number of batches to generate")
    product_seed_count: int = Field(
        default=500, ge=50, le=5_000, description="Number of seed products"
    )
    schema_evolution_batch: int = Field(
        default=10, ge=1, description="Batch number after which schema evolution kicks in"
    )

    # ── SeaweedFS (S3-compatible) ───────────────────────────────────────────
    s3_endpoint_url: str = Field(
        default="http://localhost:9000", description="SeaweedFS S3 API endpoint"
    )
    s3_access_key: str = Field(default="any", description="S3 access key (SeaweedFS dev mode)")
    s3_secret_key: str = Field(default="any", description="S3 secret key (SeaweedFS dev mode)")
    s3_bucket: str = Field(default="lakehouse", description="Target S3 bucket")
    s3_region: str = Field(
        default="us-east-1", description="AWS region (required by boto3, ignored by SeaweedFS)"
    )

    # ── Kafka ───────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = Field(
        default="localhost:9094", description="Kafka bootstrap servers"
    )
    kafka_topic: str = Field(
        default="clickstream_events", description="Kafka topic for clickstream events"
    )
    kafka_events_per_batch: int = Field(
        default=5_000, ge=100, le=100_000, description="Clickstream events per batch"
    )

    # ── Dirty data injection rates (percentages 0.0 to 100.0) ──────────────
    # Users
    user_null_email_rate: float = Field(
        default=2.0, ge=0, le=50, description="% of users with null email"
    )
    user_invalid_email_rate: float = Field(
        default=3.0, ge=0, le=50, description="% of users with invalid email format"
    )
    user_future_date_rate: float = Field(
        default=2.0, ge=0, le=50, description="% of users with future signup_date"
    )
    user_duplicate_rate: float = Field(
        default=2.0, ge=0, le=50, description="% of duplicate user_id records"
    )
    user_null_country_rate: float = Field(
        default=1.0, ge=0, le=50, description="% of users with null country"
    )

    # Orders
    order_orphan_fk_rate: float = Field(
        default=3.0, ge=0, le=50, description="% of orders with orphan user_id"
    )
    order_duplicate_rate: float = Field(
        default=4.0, ge=0, le=50, description="% of duplicate order rows"
    )
    order_future_date_rate: float = Field(
        default=2.0, ge=0, le=50, description="% of orders with future order_date"
    )
    order_invalid_currency_rate: float = Field(
        default=1.0, ge=0, le=50, description="% of orders with invalid currency code"
    )

    # Order Items
    item_negative_price_rate: float = Field(
        default=1.0, ge=0, le=50, description="% of items with negative unit_price"
    )
    item_orphan_order_rate: float = Field(
        default=2.0, ge=0, le=50, description="% of items with orphan order_id"
    )
    item_zero_quantity_rate: float = Field(
        default=0.5, ge=0, le=50, description="% of items with zero quantity"
    )
    item_invalid_discount_rate: float = Field(
        default=0.5, ge=0, le=50, description="% of items with discount > 100"
    )

    # Products
    product_negative_price_rate: float = Field(
        default=1.0, ge=0, le=50, description="% of products with negative price"
    )
    product_null_category_rate: float = Field(
        default=1.0, ge=0, le=50, description="% of products with null category"
    )

    # Clickstream
    click_duplicate_rate: float = Field(
        default=4.0, ge=0, le=50, description="% of duplicate clickstream events"
    )
    click_late_arrival_rate: float = Field(
        default=10.0, ge=0, le=50, description="% of late-arriving events"
    )
    click_future_date_rate: float = Field(
        default=2.0, ge=0, le=50, description="% of events with future timestamp"
    )
    click_orphan_user_rate: float = Field(
        default=2.0, ge=0, le=50, description="% of events with orphan user_id"
    )
    click_null_session_rate: float = Field(
        default=1.0, ge=0, le=50, description="% of events with null session_id"
    )
