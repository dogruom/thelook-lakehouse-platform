"""Deterministic content hash of a DataFrame (W3.D18, D-084).

Used to prove that two runs produced the same table content (crash + re-run versus a clean
reference). PySpark is imported lazily so hosts without it can import this module.
"""

from __future__ import annotations

from hashlib import sha256
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Iterable

    from pyspark.sql import DataFrame

DEFAULT_EXCLUDE = ("_updated_at",)
_FIELD_SEP = "\u001f"
_NULL_MARK = "\u0000NULL"


def content_hash(df: DataFrame, exclude: Iterable[str] = DEFAULT_EXCLUDE) -> str:
    """SHA-256 over the rows of ``df``: independent of row and column order, NULL-aware.

    Every column but those in ``exclude`` (which *replaces* the default ``_updated_at``, a wall
    clock) takes part, with its name. Each row is hashed on its own, the row hashes are sorted and
    hashed again, so duplicate rows change the result and row order does not. A NULL and an empty
    string differ. The row hashes (32 bytes each) are collected to the driver: fine for the
    table sizes of this project, not a general-purpose checksum for very large tables.
    """
    from pyspark.sql import functions as F

    dropped = set(exclude)
    columns = sorted(c for c in df.columns if c not in dropped)
    parts = [F.coalesce(F.col(c).cast("string"), F.lit(_NULL_MARK)) for c in columns]
    digest = F.sha2(F.concat_ws(_FIELD_SEP, *parts), 256)
    row_hashes = sorted(str(row[0]) for row in df.select(digest).collect())
    return sha256("\n".join([",".join(columns), *row_hashes]).encode("utf-8")).hexdigest()
