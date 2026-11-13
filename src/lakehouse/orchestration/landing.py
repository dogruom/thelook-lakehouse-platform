"""Landing-zone check of one brand (W4.D22.2): is there at least one complete batch per entity?

The landing layout is owned by the generator (``landing/{brand_id}/{entity}/{date}/...``, a
``*.manifest.json`` next to the data file marks a complete batch). Orchestration does not import
that component (ADR-0009); it reads the same paths through the S3 API. The check is deliberately
about the brand's whole landing prefix, not about the Airflow logical date: Bronze ingestion is
checkpoint-driven (``availableNow``), so what is new is decided by the checkpoint, not by the date.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Protocol

from lakehouse.orchestration.plan import is_valid_brand_id


if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

LANDING_ENTITIES: Final = ("orders", "products")
MANIFEST_SUFFIX: Final = ".manifest.json"


class S3ListClient(Protocol):
    """The one boto3 S3 client method used here (``list_objects_v2``)."""

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True)
class LandingStatus:
    """Result of one brand's landing check."""

    brand_id: str
    complete_batches: dict[str, int]  # entity -> number of manifests seen (capped by the scan)

    @property
    def missing_entities(self) -> list[str]:
        return [e for e in LANDING_ENTITIES if self.complete_batches.get(e, 0) == 0]

    @property
    def ok(self) -> bool:
        return not self.missing_entities


def landing_prefix(brand_id: str, entity: str) -> str:
    """Bucket-relative prefix ``landing/{brand_id}/{entity}/`` (``ValueError`` for a bad brand)."""
    if not is_valid_brand_id(brand_id):
        raise ValueError("brand_id is outside the tenant-id contract")
    if entity not in LANDING_ENTITIES:
        raise ValueError(f"unknown landing entity {entity!r}")
    return f"landing/{brand_id}/{entity}/"


def landing_prefixes(brand_id: str) -> tuple[str, ...]:
    """The prefixes that must each hold a complete batch, in ``LANDING_ENTITIES`` order."""
    return tuple(landing_prefix(brand_id, e) for e in LANDING_ENTITIES)


def is_complete_batch_key(key: str) -> bool:
    """True for a batch-complete marker (the manifest written after the data file)."""
    return key.endswith(MANIFEST_SUFFIX)


def iter_keys(client: S3ListClient, bucket: str, prefix: str) -> Iterator[str]:
    """All keys under ``prefix`` (follows ``NextContinuationToken``)."""
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for item in page.get("Contents", []):
            yield str(item["Key"])
        if not page.get("IsTruncated"):
            return
        token = page.get("NextContinuationToken")
        if not token:
            return


def count_complete_batches(keys: Iterable[str], *, stop_after: int = 1) -> int:
    """Number of manifest keys, stopping early once ``stop_after`` were seen."""
    seen = 0
    for key in keys:
        if is_complete_batch_key(key):
            seen += 1
            if seen >= stop_after:
                break
    return seen


def check_brand_landing(client: S3ListClient, bucket: str, brand_id: str) -> LandingStatus:
    """List the brand's landing prefixes and report complete batches per entity."""
    found = {
        entity: count_complete_batches(iter_keys(client, bucket, landing_prefix(brand_id, entity)))
        for entity in LANDING_ENTITIES
    }
    return LandingStatus(brand_id, found)


def split_landing_results(
    planned: Sequence[str], healthy: Iterable[object]
) -> tuple[list[str], list[str]]:
    """``(healthy brands, missing brands)`` in plan order.

    ``healthy`` is what the successful mapped checks returned; anything that is not a planned
    brand (for example ``None`` for a failed instance) is ignored.
    """
    ok = {h for h in healthy if isinstance(h, str)}
    return [b for b in planned if b in ok], [b for b in planned if b not in ok]
