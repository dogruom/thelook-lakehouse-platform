"""SparkSettings, run ids and Spark conf layering (offline, no pyspark)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from lakehouse.common.spark import SparkSettings, build_spark_conf, new_run_id


CRED_PROVIDER = "spark.hadoop.fs.s3a.aws.credentials.provider"
RUN_ID = "job_20261106T120000Z_abcdef01"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "LAKEHOUSE_ENV",
        "LAKEHOUSE_BUCKET",
        "LAKEHOUSE_S3_ENDPOINT",
        "S3_ENDPOINT",
        "LAKEHOUSE_SPARK_MASTER",
        "LAKEHOUSE_CATALOG",
    ):
        monkeypatch.delenv(key, raising=False)


def _conf(
    settings: SparkSettings | None = None, extra: dict[str, str] | None = None
) -> dict[str, str]:
    return build_spark_conf(settings or SparkSettings(), app_name="app", run_id=RUN_ID, extra=extra)


@pytest.mark.parametrize("key", ["spark.x.token", "spark.x.apikey", "spark.x.session.token"])
def test_token_and_api_key_conf_keys_are_refused(key: str) -> None:
    with pytest.raises(ValueError, match="credentials"):
        _conf(extra={key: "x"})


def test_run_id_rejects_trailing_newline_in_job_and_token() -> None:
    with pytest.raises(ValueError, match="job name"):
        new_run_id("job\n")
    with pytest.raises(ValueError, match="token"):
        new_run_id("job", token="abcdef12\n")


def test_legacy_master_env_var_is_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MASTER", "spark://wrong:7077")
    assert SparkSettings().master is None
    assert SparkSettings(master="local[2]").master == "local[2]"


# ---- layer_uri -------------------------------------------------------------------------------


def test_layer_uri_local_uses_s3a_and_joins_parts() -> None:
    assert SparkSettings().layer_uri("bronze", "customers", "v1") == (
        "s3a://lakehouse/bronze/customers/v1"
    )


def test_layer_uri_without_parts_has_no_trailing_slash() -> None:
    assert SparkSettings().layer_uri("silver") == "s3a://lakehouse/silver"


def test_layer_uri_aws_uses_s3_scheme() -> None:
    s = SparkSettings(env="aws", bucket="my-bucket")
    assert s.storage_scheme == "s3"
    assert s.layer_uri("gold", "x") == "s3://my-bucket/gold/x"


@pytest.mark.parametrize("part", ["", "a/b", "a\\b", ".", "..", "has space", "tab\t", "new\nline"])
def test_layer_uri_rejects_unsafe_parts(part: str) -> None:
    """Path traversal / ambiguous segments must never reach an object-store URI."""
    with pytest.raises(ValueError):
        SparkSettings().layer_uri("ops", part)


@pytest.mark.parametrize(
    "part", ["a?b", "a#b", "a\n", "a\x00b", "a\x1bb", "a b", "a/b", "a%2Fb", "a:b", "ä"]
)
def test_layer_uri_rejects_uri_syntax_and_control_characters(part: str) -> None:
    with pytest.raises(ValueError):
        SparkSettings().layer_uri("bronze", part)


@pytest.mark.parametrize("part", ["it_ab12cd34", "raw_customers", "v1.2", "k=v", "a-b.c_d"])
def test_layer_uri_accepts_allowlisted_parts(part: str) -> None:
    assert SparkSettings().layer_uri("bronze", part) == f"s3a://lakehouse/bronze/{part}"


def test_s3_endpoint_with_userinfo_is_rejected_without_echoing_the_secret() -> None:
    with pytest.raises(ValueError) as info:
        SparkSettings(s3_endpoint="http://user:hunter2-secret@seaweedfs:9000")
    assert "hunter2-secret" not in str(info.value)
    assert "hunter2-secret" not in repr(info.value)


def test_s3_endpoint_with_userinfo_from_env_is_rejected_without_echoing_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("S3_ENDPOINT", "http://user:hunter2-secret@seaweedfs:9000")
    with pytest.raises(ValueError) as info:
        SparkSettings()
    assert "hunter2-secret" not in str(info.value)


def test_invalid_bucket_name_is_rejected() -> None:
    with pytest.raises(ValidationError):
        SparkSettings(bucket="Bad_Bucket")


# ---- catalog / aliases ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({}, "in_memory"),
        ({"env": "aws"}, "glue"),
        ({"catalog": "hive"}, "hive"),
        ({"env": "aws", "catalog": "hive"}, "hive"),
    ],
)
def test_effective_catalog(kwargs: dict[str, Any], expected: str) -> None:
    assert SparkSettings(**kwargs).effective_catalog == expected


def test_local_env_with_glue_catalog_is_invalid() -> None:
    with pytest.raises(ValidationError):
        SparkSettings(env="local", catalog="glue")


def test_env_vars_populate_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAKEHOUSE_SPARK_MASTER", "local[2]")
    monkeypatch.setenv("S3_ENDPOINT", "http://other:9000")
    monkeypatch.setenv("LAKEHOUSE_ENV", "aws")
    s = SparkSettings()
    assert s.master == "local[2]"
    assert s.s3_endpoint == "http://other:9000"
    assert s.env == "aws"


def test_kwargs_populate_settings_by_field_name() -> None:
    s = SparkSettings(master="local[1]", s3_endpoint="http://k:1")
    assert s.master == "local[1]" and s.s3_endpoint == "http://k:1"


def test_lakehouse_prefixed_endpoint_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAKEHOUSE_S3_ENDPOINT", "http://pref:9000")
    assert SparkSettings().s3_endpoint == "http://pref:9000"


# ---- new_run_id -----------------------------------------------------------------------------


def test_new_run_id_format_is_deterministic_with_injected_clock_and_token() -> None:
    now = datetime(2026, 11, 6, 12, 0, 0, tzinfo=UTC)
    assert (
        new_run_id("hello_delta", now=now, token="abcdef01")
        == "hello_delta_20261106T120000Z_abcdef01"
    )


def test_new_run_id_default_token_is_8_hex_and_unique() -> None:
    a, b = new_run_id("hello_delta"), new_run_id("hello_delta")
    assert a != b
    token = a.rsplit("_", 1)[1]
    assert len(token) == 8 and all(c in "0123456789abcdef" for c in token)


@pytest.mark.parametrize("name", ["", "Hello", "1job", "a", "has-dash", "x" * 64, "sp ace"])
def test_new_run_id_rejects_bad_job_names(name: str) -> None:
    with pytest.raises(ValueError):
        new_run_id(name)


def test_new_run_id_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError):
        new_run_id("job_x", now=datetime(2026, 11, 6, 12, 0, 0))


@pytest.mark.parametrize("token", ["ABCDEF01", "abc", "abcdef012", "ghijklmn"])
def test_new_run_id_rejects_bad_token(token: str) -> None:
    with pytest.raises(ValueError):
        new_run_id("job_x", token=token)


# ---- build_spark_conf -----------------------------------------------------------------------


def test_local_conf_has_baseline_and_s3a_endpoint() -> None:
    c = _conf()
    assert c["spark.app.name"] == "app"
    assert c["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    assert c["spark.sql.catalog.spark_catalog"] == "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    assert c["spark.sql.session.timeZone"] == "UTC"
    assert c["spark.lakehouse.run_id"] == RUN_ID
    assert c["spark.lakehouse.env"] == "local"
    assert c["spark.hadoop.fs.s3a.endpoint"] == "http://seaweedfs:9000"
    assert c["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert c["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert c[CRED_PROVIDER] == "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
    assert c["spark.sql.catalogImplementation"] == "in-memory"


def test_https_endpoint_enables_ssl() -> None:
    c = _conf(SparkSettings(s3_endpoint="https://s3.example.com"))
    assert c["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "true"


def test_aws_conf_has_no_s3a_keys_and_uses_glue_catalog() -> None:
    """aws profile leaves storage config to the runtime (EMRFS); only env + catalog are set."""
    c = _conf(SparkSettings(env="aws"))
    assert c["spark.lakehouse.env"] == "aws"
    assert not [k for k in c if k.startswith("spark.hadoop.fs.s3a.")]
    assert c["spark.sql.catalogImplementation"] == "hive"
    assert c["spark.hadoop.hive.metastore.client.factory.class"] == (
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    )


def test_hive_catalog_sets_hive_without_glue_factory() -> None:
    c = _conf(SparkSettings(catalog="hive"))
    assert c["spark.sql.catalogImplementation"] == "hive"
    assert "spark.hadoop.hive.metastore.client.factory.class" not in c


def test_extra_overrides_profile_values() -> None:
    c = _conf(extra={"spark.sql.session.timeZone": "Europe/Berlin", "spark.custom": "1"})
    assert c["spark.sql.session.timeZone"] == "Europe/Berlin"
    assert c["spark.custom"] == "1"


@pytest.mark.parametrize(
    "key",
    [
        "spark.hadoop.fs.s3a.secret.key",
        "spark.hadoop.fs.s3a.access.key",
        "spark.hadoop.fs.s3a.accesskey",
        "spark.hadoop.fs.s3a.session.token",
        "spark.my.PASSWORD",
        "spark.some.credential",
    ],
)
def test_credential_like_extra_keys_are_rejected(key: str) -> None:
    """Negative: secrets must never enter Spark conf (visible in UI / event logs)."""
    with pytest.raises(ValueError):
        _conf(extra={key: "x"})


def test_credentials_provider_key_is_the_allowed_exception() -> None:
    provider = "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
    assert _conf(extra={CRED_PROVIDER: provider})[CRED_PROVIDER] == provider


def test_aws_env_secrets_never_leak_into_conf(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "sekret-value-xyz")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAFAKEFAKEFAKE")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "tok-value-xyz")
    for s in (SparkSettings(), SparkSettings(env="aws")):
        blob = " ".join(f"{k}={v}" for k, v in _conf(s).items())
        for leaked in ("sekret-value-xyz", "AKIAFAKEFAKEFAKE", "tok-value-xyz"):
            assert leaked not in blob
