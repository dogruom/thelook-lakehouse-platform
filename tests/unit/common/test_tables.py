"""Catalog-name addressing: resolve(), identifier validation and the catalog table set (offline)."""

from __future__ import annotations

import dataclasses

import pytest

from lakehouse.common.spark import SparkSettings
from lakehouse.common.tables import CATALOG_TABLES, ResolvedTable, TableSpec, resolve


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


RAW = CATALOG_TABLES["raw_customers"]


def test_catalog_tables_are_the_charter_names() -> None:
    assert {k: s.qualified_name for k, s in CATALOG_TABLES.items()} == {
        "raw_customers": "bronze.raw_customers",
        "raw_orders": "bronze.raw_orders",
        "raw_products": "bronze.raw_products",
        "raw_clickstream": "bronze.raw_clickstream",
        "dlq_customers": "dead_letter.dlq_customers",
        "dlq_clickstream": "dead_letter.dlq_clickstream",
        "customers": "silver.customers",
    }
    assert all(s.layer == s.database for s in CATALOG_TABLES.values())


def test_catalog_tables_mapping_is_read_only() -> None:
    with pytest.raises(TypeError):
        CATALOG_TABLES["x"] = RAW  # type: ignore[index]


def test_table_spec_is_frozen() -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        RAW.name = "other"  # type: ignore[misc]


@pytest.mark.parametrize("bad", ["", "1abc", "Raw", "a-b", "a.b", "a b", "a\n", "x" * 129, "a`b"])
def test_identifiers_are_validated(bad: str) -> None:
    with pytest.raises(ValueError, match="invalid"):
        TableSpec("bronze", "bronze", bad)
    with pytest.raises(ValueError, match="invalid"):
        TableSpec("bronze", bad, "ok")


def test_identifier_length_limit_is_128() -> None:
    assert TableSpec("bronze", "bronze", "a" + "b" * 127).name.startswith("ab")
    with pytest.raises(ValueError):
        TableSpec("bronze", "bronze", "a" + "b" * 128)


@pytest.mark.parametrize(
    ("kwargs", "namespace", "name", "location"),
    [
        # in-memory (the local default): always path-addressed
        ({}, None, None, "s3a://lakehouse/bronze/raw_customers"),
        ({}, "ns1", None, "s3a://lakehouse/bronze/ns1/raw_customers"),
        # hive: named only without a namespace
        ({"catalog": "hive"}, None, "bronze.raw_customers", "s3a://lakehouse/bronze/raw_customers"),
        ({"catalog": "hive"}, "ns1", None, "s3a://lakehouse/bronze/ns1/raw_customers"),
        # glue on AWS (EMRFS scheme)
        ({"env": "aws"}, None, "bronze.raw_customers", "s3://lakehouse/bronze/raw_customers"),
        ({"env": "aws"}, "ns1", None, "s3://lakehouse/bronze/ns1/raw_customers"),
        # an explicit in_memory on AWS stays path-addressed
        ({"env": "aws", "catalog": "in_memory"}, None, None, "s3://lakehouse/bronze/raw_customers"),
    ],
)
def test_resolve(
    kwargs: dict[str, str], namespace: str | None, name: str | None, location: str
) -> None:
    settings = SparkSettings(**kwargs)  # type: ignore[arg-type]
    assert resolve(settings, RAW, namespace) == ResolvedTable(name, location)


def test_resolve_uses_the_spec_layer_for_the_location() -> None:
    dlq = CATALOG_TABLES["dlq_clickstream"]
    assert resolve(SparkSettings(catalog="hive"), dlq) == ResolvedTable(
        "dead_letter.dlq_clickstream", "s3a://lakehouse/dead_letter/dlq_clickstream"
    )


def test_resolve_empty_namespace_counts_as_none() -> None:
    assert resolve(SparkSettings(catalog="hive"), RAW, "").name == "bronze.raw_customers"


def test_resolve_rejects_unsafe_namespace() -> None:
    with pytest.raises(ValueError, match="invalid path part"):
        resolve(SparkSettings(catalog="hive"), RAW, "../x")
