"""Contract-derived StructTypes against real Spark ``from_json`` behaviour."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest


if TYPE_CHECKING:
    from pyspark.sql import SparkSession

CONTRACT_NAMES = ["customer_cdc.v1", "clickstream_event.v1", "order_event.v1", "product.v1"]
_SAMPLES_PATH = Path(__file__).resolve().parents[1] / "unit" / "contracts" / "_samples.py"


def _wire(name: str) -> dict[str, Any]:
    """Valid wire payload from the shared (faker-free) samples module, loaded by path."""
    spec = importlib.util.spec_from_file_location("_contract_samples", _SAMPLES_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    from lakehouse.contracts.models import CONTRACT_MODELS, to_wire

    model = CONTRACT_MODELS[name].model_validate(mod.SAMPLES[name]())
    return to_wire(model)


@pytest.mark.parametrize("name", CONTRACT_NAMES)
def test_struct_type_for_matches_contract_field_names(name: str) -> None:
    from lakehouse.common.spark import struct_type_for
    from lakehouse.contracts.spark_schema import spark_schema_json_for

    st = struct_type_for(name)
    assert st.fieldNames() == [f["name"] for f in spark_schema_json_for(name)["fields"]]


@pytest.mark.parametrize("name", CONTRACT_NAMES)
def test_from_json_of_valid_wire_payload_keeps_every_field(spark: SparkSession, name: str) -> None:
    """A valid contract payload must parse with no field dropped or nulled."""
    from pyspark.sql import functions as F

    from lakehouse.common.spark import struct_type_for

    wire = _wire(name)
    st = struct_type_for(name)
    df = spark.createDataFrame([(json.dumps(wire),)], "raw string").select(
        F.from_json("raw", st).alias("p")
    )
    parsed = df.select("p.*").collect()[0].asDict(recursive=True)
    assert set(parsed) == set(st.fieldNames())
    for key, value in wire.items():
        if value is not None:
            assert parsed[key] is not None, f"{name}.{key} lost in from_json"


def test_from_json_nulls_wrong_typed_field_but_keeps_row(spark: SparkSession) -> None:
    """Documents that from_json is lenient: type errors become nulls (gate must catch them)."""
    from pyspark.sql import functions as F

    from lakehouse.common.spark import struct_type_for

    wire = _wire("customer_cdc.v1")
    wire["_cdc_sequence"] = "not-a-number"
    row = (
        spark.createDataFrame([(json.dumps(wire),)], "raw string")
        .select(F.from_json("raw", struct_type_for("customer_cdc.v1")).alias("p"))
        .select("p.*")
        .collect()[0]
    )
    assert row["_cdc_sequence"] is None
    assert row["event_id"] == wire["event_id"]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("12.50", "12.50"),
        ("0.01", "0.01"),
        ("99999999.99", "99999999.99"),
        ("12.505", "12.51"),  # rounds: cast does not enforce exactly two decimals
        (" 12.50 ", "12.50"),  # trims: regex gate must run before the cast
        ("1e2", "100.00"),
        ("100000000.00", None),  # overflow of DECIMAL(10,2)
        ("12.5x", None),
    ],
)
def test_money_string_cast_to_decimal_10_2(
    spark: SparkSession, raw: str, expected: str | None
) -> None:
    from pyspark.sql import functions as F

    previous = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        df = spark.createDataFrame([(raw,)], "m string").select(
            F.col("m").cast("decimal(10,2)").alias("d")
        )
        assert dict(df.dtypes)["d"] == "decimal(10,2)"
        value = df.collect()[0]["d"]
    finally:
        spark.conf.set("spark.sql.ansi.enabled", previous)
    assert (str(value) if value is not None else None) == expected
