"""Catalog-name addressing for Delta tables: jobs read ``bronze.raw_customers``, not a path.

Tables are registered in the Hive Metastore locally and in the Glue Data Catalog on AWS
(ADR-0007). Storage paths still come from :meth:`SparkSettings.layer_uri`: the location is the
write contract and the table registration only points at it. This module resolves, per run, whether
a table is addressed by catalog name or by location:

* **named**: the effective catalog is ``hive`` or ``glue`` and the run has no namespace. Readers use
  ``spark.read.table("<database>.<name>")``.
* **path-addressed**: the catalog is ``in_memory`` (nothing is registered across sessions) or the
  run has a ``--namespace``. Namespaced runs are test isolation (one throw-away path segment per
  test); they are deliberately never registered in the shared catalog, so they cannot collide with
  or pollute the real tables. Behaviour on those runs is the path-based behaviour of before.

Registering never re-points an existing table: when the catalog already knows the table at another
location, :class:`CatalogLocationMismatchError` is raised (a silent re-point would hide data).

PySpark is imported lazily (inside functions) so hosts without PySpark can import this module.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal


if TYPE_CHECKING:
    from collections.abc import Mapping

    from pyspark.sql import DataFrame, SparkSession

    from lakehouse.common.spark import Layer, SparkSettings


_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,127}\Z")


class CatalogLocationMismatchError(RuntimeError):
    """The catalog knows the table at a different location than the configured one."""


@dataclass(frozen=True)
class TableSpec:
    """A table of the lakehouse: the bucket layer, the catalog database and the table name."""

    layer: Layer
    database: str
    name: str

    def __post_init__(self) -> None:
        for label, value in (("database", self.database), ("table name", self.name)):
            if not _IDENTIFIER.match(value):
                raise ValueError(f"invalid {label} {value!r}")

    @property
    def qualified_name(self) -> str:
        return f"{self.database}.{self.name}"


@dataclass(frozen=True)
class ResolvedTable:
    """How one run addresses a table: ``name`` (catalog) or ``None`` (path), plus the location."""

    name: str | None
    location: str


# Key = table name (unique across layers). Database == layer name (charter table names).
CATALOG_TABLES: Mapping[str, TableSpec] = MappingProxyType(
    {
        spec.name: spec
        for spec in (
            TableSpec("bronze", "bronze", "raw_customers"),
            TableSpec("bronze", "bronze", "raw_clickstream"),
            TableSpec("bronze", "bronze", "raw_orders"),
            TableSpec("bronze", "bronze", "raw_products"),
            TableSpec("dead_letter", "dead_letter", "dlq_customers"),
            TableSpec("dead_letter", "dead_letter", "dlq_clickstream"),
            TableSpec("dead_letter", "dead_letter", "dlq_orders"),
            TableSpec("dead_letter", "dead_letter", "dlq_products"),
            TableSpec("silver", "silver", "customers"),
            TableSpec("silver", "silver", "customers_history"),
            TableSpec("silver", "silver", "orders"),
            TableSpec("silver", "silver", "order_items"),
            TableSpec("silver", "silver", "clickstream"),
        )
    }
)

# Operational metrics (W3.D18, D-081): written path-based by the Silver jobs and ``reconcile``.
# Deliberately not in ``CATALOG_TABLES``: it is not a pipeline dataset and is never registered
# by ``register_tables`` (that mapping is the charter table list).
PIPELINE_METRICS = TableSpec("ops", "ops", "pipeline_metrics")


def resolve(
    settings: SparkSettings, spec: TableSpec, namespace: str | None = None
) -> ResolvedTable:
    """Location of ``spec`` and, when the run uses the catalog, its qualified name."""
    ns_parts = [namespace] if namespace else []
    location = settings.layer_uri(spec.layer, *ns_parts, spec.name)
    named = settings.effective_catalog != "in_memory" and not namespace
    return ResolvedTable(spec.qualified_name if named else None, location)


def read_delta(spark: SparkSession, table: ResolvedTable) -> DataFrame:
    """Read the table by catalog name when it has one, else by Delta location."""
    if table.name is not None:
        return spark.read.table(table.name)
    return spark.read.format("delta").load(table.location)


def table_exists(spark: SparkSession, table: ResolvedTable) -> bool:
    """Named: the catalog knows the table. Path-addressed: a Delta table exists at the location."""
    if table.name is not None:
        return bool(spark.catalog.tableExists(table.name))
    return delta_exists_at_location(spark, table)


def delta_exists_at_location(spark: SparkSession, table: ResolvedTable) -> bool:
    """A Delta table exists at ``table.location`` (regardless of catalog registration)."""
    from delta.tables import DeltaTable

    return bool(DeltaTable.isDeltaTable(spark, table.location))


def _quoted(spec: TableSpec) -> tuple[str, str]:
    return f"`{spec.database}`", f"`{spec.database}`.`{spec.name}`"


def _same_location(a: str, b: str) -> bool:
    return a.rstrip("/") == b.rstrip("/")


def _check_registered_location(
    spark: SparkSession, spec_name: str, quoted: str, expected: str
) -> None:
    row = spark.sql(f"DESCRIBE DETAIL {quoted}").select("location").first()
    actual = str(row["location"]) if row is not None else ""
    if not _same_location(actual, expected):
        raise CatalogLocationMismatchError(
            f"{spec_name} is registered at {actual!r}, expected {expected!r}; "
            "refusing to use or re-point it"
        )


def verify_location(spark: SparkSession, spec: TableSpec, table: ResolvedTable) -> None:
    """Named tables only: the catalog entry must point at ``table.location`` (the write contract).

    Guards readers against a table registered by hand elsewhere, which would be read silently
    while the writers keep writing to the configured location. Path-addressed tables: no-op.
    """
    if table.name is None:
        return
    _check_registered_location(spark, spec.qualified_name, _quoted(spec)[1], table.location)


def register_table(
    spark: SparkSession, spec: TableSpec, table: ResolvedTable
) -> Literal["registered", "exists"]:
    """Register the Delta table at ``table.location`` as ``spec.qualified_name`` (idempotent).

    The database is created at the layer root (the location without the trailing ``/<name>``).
    An already registered table must point at the same location (a trailing slash is ignored; the
    ``s3a`` / ``s3`` scheme must match), otherwise :class:`CatalogLocationMismatchError`.
    """
    if table.name is None:
        raise ValueError("register_table needs a catalog-addressed table (name is None)")
    suffix = f"/{spec.name}"
    location = table.location.rstrip("/")
    if not location.endswith(suffix):
        raise ValueError(f"location {location!r} does not end with {suffix!r}")
    root = location.removesuffix(suffix)
    for value in (location, root):
        if "'" in value or "\\" in value:
            raise ValueError("location contains a quote or backslash")

    database, qualified = _quoted(spec)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database} LOCATION '{root}'")
    if spark.catalog.tableExists(spec.qualified_name):
        _check_registered_location(spark, spec.qualified_name, qualified, location)
        return "exists"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {qualified} USING DELTA LOCATION '{location}'")
    return "registered"
