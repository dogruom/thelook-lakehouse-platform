"""One way to configure a SparkSession for every job (local standalone cluster, AWS later).

Configuration is layered, later wins:

1. the image's ``docker/spark/spark-defaults.conf`` (S3A endpoint, Delta extension; no secrets),
2. the profile built here from :class:`SparkSettings` (``LAKEHOUSE_ENV`` = ``local`` | ``aws``),
3. explicit ``extra`` conf passed by the caller.

Rules:

* Credentials never enter Spark conf. Locally S3A reads ``AWS_*`` from the process environment via
  ``EnvironmentVariableCredentialsProvider``; this module never reads ``AWS_*`` itself.
* The ``aws`` profile is untested until W5: Glue 5.0 uses EMRFS ``s3://`` and ignores static
  confs when the runtime pre-creates the session, so only the non-S3A parts are set here.
* PySpark is imported lazily (inside functions) so hosts without PySpark can import this module.
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlsplit

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from lakehouse.contracts.models import CONTRACT_MODELS
from lakehouse.contracts.spark_schema import spark_schema_json


if TYPE_CHECKING:
    from collections.abc import Mapping

    from pydantic import BaseModel
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType


Layer = Literal[
    "landing", "bronze", "silver", "gold", "dead_letter", "ops", "checkpoints", "warehouse"
]

JOB_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{1,62}\Z")
_TOKEN_PATTERN = re.compile(r"^[0-9a-f]{8}\Z")
_PATH_PART_PATTERN = re.compile(r"^[A-Za-z0-9._=-]+\Z")
_BUCKET_PATTERN = r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$"
_ALLOWED_CREDENTIAL_KEY = "spark.hadoop.fs.s3a.aws.credentials.provider"
_CREDENTIAL_MARKERS = (
    "secret",
    "password",
    "access.key",
    "accesskey",
    "apikey",
    "token",
    "credential",
)
_GLUE_FACTORY = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"


class SparkSettings(BaseSettings):
    """Spark/storage profile read from ``LAKEHOUSE_*`` environment variables (no secrets)."""

    model_config = SettingsConfigDict(
        env_prefix="LAKEHOUSE_",
        extra="ignore",
        populate_by_name=True,
        hide_input_in_errors=True,
    )

    env: Literal["local", "aws"] = "local"
    bucket: str = Field(default="lakehouse", pattern=_BUCKET_PATTERN)
    s3_endpoint: str = Field(
        default="http://seaweedfs:9000",
        validation_alias=AliasChoices("S3_ENDPOINT", "LAKEHOUSE_S3_ENDPOINT", "s3_endpoint"),
    )
    # Single alias on purpose: a bare "master" alias would also read Spark's legacy MASTER env var.
    master: str | None = Field(default=None, validation_alias="LAKEHOUSE_SPARK_MASTER")
    catalog: Literal["in_memory", "hive", "glue"] | None = None
    # Internal (container network) listener. Never the host's KAFKA_BOOTSTRAP_SERVERS.
    kafka_bootstrap_servers: str = Field(
        default="kafka:9092",
        validation_alias=AliasChoices("LAKEHOUSE_KAFKA_BOOTSTRAP", "kafka_bootstrap_servers"),
    )

    @field_validator("s3_endpoint")
    @classmethod
    def _endpoint_without_userinfo(cls, value: str) -> str:
        # Never echo the value: it is exactly the string that would carry the credentials.
        if "@" in urlsplit(value).netloc:
            raise ValueError("s3_endpoint must not contain credentials (userinfo)")
        return value

    @model_validator(mode="after")
    def _glue_needs_aws(self) -> SparkSettings:
        if self.env == "local" and self.catalog == "glue":
            raise ValueError("catalog 'glue' requires env 'aws'")
        return self

    @property
    def effective_catalog(self) -> Literal["in_memory", "hive", "glue"]:
        """Explicit catalog, else in-memory locally / Glue on AWS (compose sets ``hive``, D-060)."""
        if self.catalog is not None:
            return self.catalog
        return "in_memory" if self.env == "local" else "glue"

    @property
    def storage_scheme(self) -> str:
        """``s3a`` for Hadoop S3A locally, ``s3`` (EMRFS) on AWS."""
        return "s3a" if self.env == "local" else "s3"

    def layer_uri(self, layer: Layer, *parts: str) -> str:
        """URI of a bucket layer, optionally with path parts (each one a single safe segment)."""
        for part in parts:
            if part in {".", ".."} or not _PATH_PART_PATTERN.match(part):
                raise ValueError(f"invalid path part {part!r}")
        suffix = "".join(f"/{p}" for p in parts)
        return f"{self.storage_scheme}://{self.bucket}/{layer}{suffix}"


def new_run_id(job_name: str, *, now: datetime | None = None, token: str | None = None) -> str:
    """``<job>_<UTC timestamp>_<8 hex>``: sortable, unique per run, safe in paths and logs."""
    if not JOB_NAME_PATTERN.match(job_name):
        raise ValueError(f"invalid job name {job_name!r}")
    if now is None:
        now = datetime.now(UTC)
    elif now.tzinfo is None:
        raise ValueError("'now' must be timezone-aware")
    if token is None:
        token = uuid.uuid4().hex[:8]
    elif not _TOKEN_PATTERN.match(token):
        raise ValueError(f"invalid token {token!r} (expected 8 lowercase hex characters)")
    return f"{job_name}_{now.astimezone(UTC):%Y%m%dT%H%M%SZ}_{token}"


def build_spark_conf(
    settings: SparkSettings,
    *,
    app_name: str,
    run_id: str,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Spark conf for the profile; ``extra`` wins over the profile. Credential keys are refused."""
    for key in extra or {}:
        lowered = key.lower()
        if key != _ALLOWED_CREDENTIAL_KEY and any(m in lowered for m in _CREDENTIAL_MARKERS):
            raise ValueError(f"credentials never go into Spark conf (key {key!r})")

    conf: dict[str, str] = {
        "spark.app.name": app_name,
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.session.timeZone": "UTC",
        "spark.lakehouse.run_id": run_id,
        "spark.lakehouse.env": settings.env,
    }
    if settings.env == "local":
        https = settings.s3_endpoint.startswith("https://")
        conf |= {
            "spark.hadoop.fs.s3a.endpoint": settings.s3_endpoint,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "true" if https else "false",
            _ALLOWED_CREDENTIAL_KEY: "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        }

    catalog = settings.effective_catalog
    conf["spark.sql.catalogImplementation"] = "in-memory" if catalog == "in_memory" else "hive"
    if catalog == "hive":
        # Same warehouse as the Thrift Server: whichever client creates a database first fixes
        # its location in the metastore, and a container-local path would be invisible elsewhere.
        conf["spark.sql.warehouse.dir"] = settings.layer_uri("warehouse")
    if catalog == "glue":
        conf["spark.hadoop.hive.metastore.client.factory.class"] = _GLUE_FACTORY

    conf.update(extra or {})
    return conf


def get_spark_session(
    settings: SparkSettings | None = None,
    *,
    app_name: str,
    run_id: str,
    extra_conf: Mapping[str, str] | None = None,
) -> SparkSession:
    """Create or reuse the session for ``settings`` and tag it with ``run_id``."""
    from pyspark.sql import SparkSession

    settings = settings or SparkSettings()
    builder = SparkSession.builder
    for key, value in build_spark_conf(
        settings, app_name=app_name, run_id=run_id, extra=extra_conf
    ).items():
        builder = builder.config(key, value)
    if settings.master:
        builder = builder.master(settings.master)
    if settings.effective_catalog in ("hive", "glue"):
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    spark.sparkContext.setLocalProperty("lakehouse.run_id", run_id)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def struct_type_for(model_or_name: str | type[BaseModel]) -> StructType:
    """Spark ``StructType`` of a contract (by ``CONTRACT_MODELS`` key or model class)."""
    from pyspark.sql.types import StructType

    model = CONTRACT_MODELS[model_or_name] if isinstance(model_or_name, str) else model_or_name
    return StructType.fromJson(spark_schema_json(model))
