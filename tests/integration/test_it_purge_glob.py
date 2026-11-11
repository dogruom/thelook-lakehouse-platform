"""The session-end purge glob matches only ``it_<hex8>`` namespaces (W2.D12 review, D-066).

Pure ``fnmatchcase`` check (the aws CLI filters use fnmatch, case-sensitive on Linux); needs no
broker or stack.
"""

from __future__ import annotations

from fnmatch import fnmatchcase

import pytest

from .conftest import IT_NAMESPACE_GLOB


@pytest.mark.parametrize(
    "key",
    [
        "bronze/it_0a1b2c3d/raw_customers/part-00000.parquet",
        "dead_letter/it_deadbeef/dlq_customers/_delta_log/00000000000000000000.json",
        "checkpoints/bronze_stream/it_12345678/raw_clickstream/offsets/0",
    ],
)
def test_namespace_keys_are_purged(key: str) -> None:
    assert fnmatchcase(key, IT_NAMESPACE_GLOB)


@pytest.mark.parametrize(
    "key",
    [
        "bronze/raw_customers/brand_id=it_alpha/part-00000.parquet",
        "silver/it_orders/part-00000.parquet",
        "gold/report/it_summary.csv",
        "bronze/it_ABCDEF12/raw_customers/part-00000.parquet",
        "bronze/it_0a1b2c3/raw_customers/part-00000.parquet",
        "bronze/raw_customers/_delta_log/00000000000000000000.json",
    ],
)
def test_other_keys_are_kept(key: str) -> None:
    assert not fnmatchcase(key, IT_NAMESPACE_GLOB)
