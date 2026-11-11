"""Seeded per-brand product catalog (DATA_CONTRACTS §2.4): static seed, no defect injection.

Product ids are ``PRD-00001``..``PRD-{n:05d}`` in every brand; the business key is
``(brand_id, product_id)``, so the same id in two brands is two independent products with
independent names and prices. Every random draw comes from a per-brand sub-RNG of the run seed.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC
from decimal import ROUND_HALF_EVEN, Decimal
from typing import TYPE_CHECKING, Any

import structlog

from lakehouse.contracts import Product, ProductCategory, to_wire
from lakehouse.generator.determinism import sub_rng
from lakehouse.generator.io import canonical_json
from lakehouse.generator.landing import LandingFile, products_data_key, sha256_hex


if TYPE_CHECKING:
    from datetime import date

    from lakehouse.generator.config import GeneratorConfig


GENERATOR_VERSION = "products-1.0.0"
MANIFEST_VERSION = 1

_NAMES: dict[ProductCategory, tuple[tuple[str, ...], tuple[str, ...]]] = {
    ProductCategory.Apparel: (
        ("Classic", "Slim", "Relaxed", "Merino", "Organic"),
        ("Jacket", "Sneaker", "Hoodie", "Jeans", "Scarf"),
    ),
    ProductCategory.Electronics: (
        ("Compact", "Wireless", "Smart", "Portable", "Pro"),
        ("Speaker", "Headphones", "Charger", "Webcam", "Keyboard"),
    ),
    ProductCategory.Home: (
        ("Nordic", "Ceramic", "Linen", "Oak", "Cast-Iron"),
        ("Lamp", "Vase", "Cushion", "Shelf", "Pan"),
    ),
    ProductCategory.Beauty: (
        ("Hydrating", "Matte", "Repair", "Gentle", "Radiant"),
        ("Serum", "Lipstick", "Cleanser", "Cream", "Mask"),
    ),
}
_CATEGORIES = tuple(_NAMES)
_CENT = Decimal("0.01")
_COST_NULL_SHARE = 0.10
_INACTIVE_SHARE = 0.10

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class ProductsRun:
    """Products per brand, their landing files and the landing date (simulated)."""

    seed: int
    config_fingerprint: str
    batch_date: date
    products: dict[str, list[Product]]
    files: dict[str, LandingFile]

    def product_ids(self, *, active_only: bool = False) -> dict[str, list[str]]:
        """``brand_id -> product ids`` (what ``generate_orders`` references)."""
        return {
            brand: [p.product_id for p in items if p.is_active or not active_only]
            for brand, items in self.products.items()
        }


def products_fingerprint(config: GeneratorConfig, seed: int) -> str:
    """sha256 over the generation-relevant fields only (no connection settings, no secrets)."""
    relevant = {
        "seed": seed,
        "tenants": config.tenants,
        "sim_start": config.sim_start.astimezone(UTC).isoformat(),
        "products_per_brand": config.products_per_brand,
    }
    return hashlib.sha256(canonical_json(relevant).encode("utf-8")).hexdigest()


def _money(cents: int) -> Decimal:
    return (Decimal(cents) / 100).quantize(_CENT)


def _brand_products(seed: int, brand_id: str, count: int) -> list[Product]:
    rng = sub_rng(seed, f"products:{brand_id}")
    items: list[Product] = []
    for number in range(1, count + 1):
        category = rng.choice(_CATEGORIES)
        adjectives, nouns = _NAMES[category]
        retail = _money(rng.randint(500, 29999))
        cost: Decimal | None = None
        share = Decimal(rng.randint(40, 70)) / 100
        if rng.random() >= _COST_NULL_SHARE:
            cost = (retail * share).quantize(_CENT, rounding=ROUND_HALF_EVEN)
        items.append(
            Product.model_validate(
                {
                    "schema_version": 1,
                    "product_id": f"PRD-{number:05d}",
                    "brand_id": brand_id,
                    "product_name": f"{rng.choice(adjectives)} {rng.choice(nouns)}",
                    "category": category,
                    "retail_price": retail,
                    "cost_price": cost,
                    "is_active": rng.random() >= _INACTIVE_SHARE,
                }
            )
        )
    return items


def generate_products(config: GeneratorConfig, *, seed: int | None = None) -> ProductsRun:
    """Generate the catalog of every configured tenant (``config.products_per_brand`` each)."""
    seed = config.seed if seed is None else seed
    batch_date = config.sim_start.astimezone(UTC).date()
    date_str = batch_date.isoformat()
    fingerprint = products_fingerprint(config, seed)
    products: dict[str, list[Product]] = {}
    files: dict[str, LandingFile] = {}
    for brand_id in config.tenants:
        items = _brand_products(seed, brand_id, config.products_per_brand)
        data = "".join(canonical_json(to_wire(p)) + "\n" for p in items).encode("utf-8")
        manifest: dict[str, Any] = {
            "manifest_version": MANIFEST_VERSION,
            "entity": "products",
            "schema_version": 1,
            "generator_version": GENERATOR_VERSION,
            "config_fingerprint": fingerprint,
            "seed": seed,
            "brand_id": brand_id,
            "batch_date": date_str,
            "data_key": products_data_key(brand_id, date_str),
            "line_count": len(items),
            "sha256": sha256_hex(data),
            "counts": {
                "products": len(items),
                "active": sum(1 for p in items if p.is_active),
            },
        }
        products[brand_id] = items
        files[brand_id] = LandingFile(data=data, manifest=manifest)
    log.info("products.generated", brands=len(products), per_brand=config.products_per_brand)
    return ProductsRun(
        seed=seed,
        config_fingerprint=fingerprint,
        batch_date=batch_date,
        products=products,
        files=files,
    )
