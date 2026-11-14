"""Landing check: prefixes, pagination, complete-batch detection, result split (offline)."""

from __future__ import annotations

from typing import Any

import pytest

from lakehouse.orchestration import landing


class FakeS3:
    """Minimal ``list_objects_v2`` over a key list, with page size to exercise pagination."""

    def __init__(self, keys: list[str], page_size: int = 2) -> None:
        self.keys = sorted(keys)
        self.page_size = page_size
        self.calls = 0

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        self.calls += 1
        matching = [k for k in self.keys if k.startswith(kwargs["Prefix"])]
        start = int(kwargs.get("ContinuationToken", 0))
        page = matching[start : start + self.page_size]
        more = start + self.page_size < len(matching)
        out: dict[str, Any] = {"Contents": [{"Key": k} for k in page], "IsTruncated": more}
        if more:
            out["NextContinuationToken"] = str(start + self.page_size)
        return out


def test_prefixes_follow_the_landing_layout() -> None:
    assert landing.landing_prefixes("brand_alpha") == (
        "landing/brand_alpha/orders/",
        "landing/brand_alpha/products/",
    )


def test_prefix_rejects_bad_brand_and_entity() -> None:
    with pytest.raises(ValueError, match="brand_id"):
        landing.landing_prefix("../other", "orders")
    with pytest.raises(ValueError, match="entity"):
        landing.landing_prefix("brand_alpha", "customers")


def test_only_manifests_mark_a_complete_batch() -> None:
    assert landing.is_complete_batch_key("landing/b/orders/2026-01-01/batch-00001.manifest.json")
    assert not landing.is_complete_batch_key("landing/b/orders/2026-01-01/batch-00001.ndjson")


def test_iter_keys_follows_pagination() -> None:
    client = FakeS3([f"landing/brand_alpha/orders/d/batch-{i}.ndjson" for i in range(5)])
    assert len(list(landing.iter_keys(client, "lakehouse", "landing/brand_alpha/orders/"))) == 5
    assert client.calls == 3


def test_check_ok_when_every_entity_has_a_manifest() -> None:
    client = FakeS3(
        [
            "landing/brand_alpha/orders/2026-01-01/batch-00001.ndjson",
            "landing/brand_alpha/orders/2026-01-01/batch-00001.manifest.json",
            "landing/brand_alpha/products/2026-01-01/products.manifest.json",
        ]
    )
    status = landing.check_brand_landing(client, "lakehouse", "brand_alpha")
    assert status.ok
    assert status.missing_entities == []


def test_check_reports_missing_entity_and_ignores_other_brands() -> None:
    client = FakeS3(
        [
            "landing/brand_alpha/orders/2026-01-01/batch-00001.manifest.json",
            "landing/brand_beta/products/2026-01-01/products.manifest.json",
            "landing/brand_alpha/products/2026-01-01/products.ndjson",  # data without manifest
        ]
    )
    status = landing.check_brand_landing(client, "lakehouse", "brand_alpha")
    assert not status.ok
    assert status.missing_entities == ["products"]


def test_split_results_keeps_plan_order_and_ignores_none() -> None:
    healthy, missing = landing.split_landing_results(
        ["brand_alpha", "brand_beta", "brand_gamma"], ["brand_gamma", None, "brand_alpha", 3]
    )
    assert healthy == ["brand_alpha", "brand_gamma"]
    assert missing == ["brand_beta"]


def test_split_results_with_nothing_healthy() -> None:
    assert landing.split_landing_results(["brand_alpha"], []) == ([], ["brand_alpha"])
