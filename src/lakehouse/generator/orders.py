"""Seeded orders micro-batches with exact-count defects and a per-batch manifest (§2.2, §2.3).

Per brand and landing batch, ``orders_per_batch`` (``n``) base orders are generated. The defect
budget is exact at run level: per brand and defect the run total is
``defect_count(rate, batches * n)``, and batch ``k`` gets
``defect_count(rate, k * n) - defect_count(rate, (k - 1) * n)``. A small rate is therefore not
rounded to zero in every batch (1 % of 50 is 0.5), and a batch deviates by at most one from
``rate * n``. Defects live only in the manifest, never in a payload field.

Four defect kinds are generated (DATA_CONTRACTS section 2.2); a fifth, ``late_parent``, is deferred
(D-069):

* ``orphan_fk``: ``customer_id`` is a fresh UUID that is in no customer pool (DLQ, retryable);
* ``future_ts``: ``order_timestamp`` ten years after the batch date (DLQ);
* ``negative_amount``: ``gross_amount < 0`` and ``net = gross - discount`` (DLQ);
* ``duplicate``: a clean line re-emitted byte-identically at the end of the batch; half of a
  batch's duplicates replay a clean line of the previous batch of the same brand (at-least-once
  replay across files); Silver dedup removes and counts them;

``late_parent`` needs CDC timing (W3.D17); its count is always 0 here and the manifest lists it
under ``deferred``.

Same seed + config + pools -> identical bytes. Each (brand, batch) has its own RNG stream, so a
batch does not change when another batch is added to the run.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal
from typing import TYPE_CHECKING, Any

import structlog

from lakehouse.contracts import (
    Currency,
    OrderEvent,
    OrderStatus,
    PaymentMethod,
    SalesChannel,
    to_wire,
)
from lakehouse.generator.cdc import InfeasibleDefectPlan
from lakehouse.generator.config import defect_count
from lakehouse.generator.determinism import sub_rng, uuid4_from
from lakehouse.generator.io import canonical_json
from lakehouse.generator.landing import orders_data_key, sha256_hex
from lakehouse.generator.manifest import DLQ_DEFECTS


if TYPE_CHECKING:
    import random
    from collections.abc import Mapping, Sequence
    from datetime import date

    from lakehouse.generator.cdc import CdcRun
    from lakehouse.generator.config import GeneratorConfig


GENERATOR_VERSION = "orders-1.0.0"
MANIFEST_VERSION = 1
FUTURE_OFFSET_DAYS = 3650  # far enough that the order stays "future" for any plausible run date
DEFECTS = ("orphan_fk", "duplicate", "future_ts", "negative_amount")
DEFERRED = ("late_parent",)

_CURRENCIES = tuple(c.value for c in Currency)
_STATUSES = tuple(s.value for s in OrderStatus)
_STATUS_WEIGHTS = (60, 20, 12, 8)  # same order as OrderStatus
_PAYMENTS = tuple(p.value for p in PaymentMethod)
_CHANNELS = tuple(c.value for c in SalesChannel)
_ITEM_COUNTS = (1, 2, 3, 4, 5)
_ITEM_WEIGHTS = (40, 30, 15, 10, 5)
_CENT = Decimal("0.01")
_SECONDS_PER_DAY = 86_400

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class OrderBatch:
    """One landed file: ``batch_no`` is the 1-based run batch index (same across brands)."""

    brand_id: str
    batch_no: int
    batch_date: date
    data: bytes
    manifest: dict[str, Any]


@dataclass(frozen=True)
class OrdersRun:
    """All batches (ordered by ``batch_no``, then tenant order) and a run-level summary."""

    seed: int
    config_fingerprint: str
    batches: list[OrderBatch]
    summary: dict[str, Any]


def customer_pools_from_cdc(run: CdcRun) -> dict[str, list[str]]:
    """``brand_id -> sorted customer ids`` that have an insert (``I``) which passes the CDC contract.

    Inserts that the CDC manifest expects in the DLQ (invalid/null email, future signup) are left
    out: such a customer would not exist in Silver, so an order for it would be an unintended orphan.
    """
    pools: dict[str, set[str]] = {}
    for entry in run.manifest.entries:
        if entry.cdc_action == "I" and entry.defect not in DLQ_DEFECTS:
            pools.setdefault(entry.brand_id, set()).add(entry.customer_id)
    return {brand: sorted(ids) for brand, ids in sorted(pools.items())}


def _digest(items: Sequence[str]) -> str:
    return hashlib.sha256("\n".join(items).encode("utf-8")).hexdigest()


def orders_fingerprint(
    config: GeneratorConfig,
    *,
    seed: int,
    brands: Sequence[str],
    batches: int,
    orders_per_batch: int,
    customer_pools: Mapping[str, Sequence[str]],
    product_ids: Mapping[str, Sequence[str]],
) -> str:
    """sha256 over the generation-relevant fields and digests of the input pools (no secrets)."""
    relevant: dict[str, Any] = {
        "seed": seed,
        "tenants": list(brands),
        "sim_start": config.sim_start.astimezone(UTC).isoformat(),
        "batches": batches,
        "orders_per_batch": orders_per_batch,
        "customer_pools": {b: _digest(customer_pools[b]) for b in brands},
        "product_ids": {b: _digest(product_ids[b]) for b in brands},
    }
    for name in sorted(type(config).model_fields):
        if name.startswith("order_"):
            relevant[name] = getattr(config, name)
    return hashlib.sha256(canonical_json(relevant).encode("utf-8")).hexdigest()


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _money(value: Decimal) -> Decimal:
    return value.quantize(_CENT, rounding=ROUND_HALF_EVEN)


def _build_order(
    rng: random.Random,
    *,
    brand_id: str,
    currency: str,
    customer_ids: Sequence[str],
    product_ids: Sequence[str],
    moment: datetime,
    channel: bool,
) -> dict[str, Any]:
    order_id = uuid4_from(rng)
    items: list[dict[str, Any]] = []
    gross = Decimal(0)
    for _ in range(rng.choices(_ITEM_COUNTS, _ITEM_WEIGHTS)[0]):
        quantity = rng.randint(1, 4)
        # Order-time price: independent of the catalog list price (promotions, price changes).
        unit_price = _money(Decimal(rng.randint(500, 30000)) / 100)
        line_total = quantity * unit_price
        gross += line_total
        items.append(
            {
                "item_id": uuid4_from(rng),
                "order_id": order_id,
                "brand_id": brand_id,
                "product_id": rng.choice(product_ids),
                "quantity": quantity,
                "unit_price": unit_price,
                "line_total": line_total,
            }
        )
    discount = _money(gross * rng.randint(5, 25) / 100) if rng.random() < 0.4 else Decimal("0.00")
    raw: dict[str, Any] = {
        "schema_version": 1,
        "order_id": order_id,
        "brand_id": brand_id,
        "customer_id": rng.choice(customer_ids),
        "order_timestamp": moment,
        "order_status": rng.choices(_STATUSES, _STATUS_WEIGHTS)[0],
        "payment_method": rng.choice(_PAYMENTS),
        "currency": currency,
        "gross_amount": gross,
        "discount_amount": discount,
        "net_amount": gross - discount,
        "items": items,
        "sales_channel": rng.choice(_CHANNELS) if channel else None,
    }
    wire = to_wire(OrderEvent.model_validate(raw))
    if wire["sales_channel"] is None:
        del wire["sales_channel"]  # absent, not null, before the schema-evolution batch
    return wire


def _fresh_uuid(rng: random.Random, taken: set[str]) -> str:
    while True:
        candidate = uuid4_from(rng)
        if candidate not in taken:
            return candidate


def _build_batch(
    config: GeneratorConfig,
    *,
    seed: int,
    fingerprint: str,
    brand_id: str,
    currency: str,
    batch_no: int,
    orders_per_batch: int,
    customer_ids: Sequence[str],
    product_ids: Sequence[str],
    previous_clean: Sequence[str],
) -> tuple[OrderBatch, list[str]]:
    """One batch file; returns it plus the clean base lines (candidates for later replays)."""
    n = orders_per_batch
    batch_date = config.sim_start.astimezone(UTC).date() + timedelta(days=batch_no - 1)
    midnight = datetime(batch_date.year, batch_date.month, batch_date.day, tzinfo=UTC)
    stream = f"orders:{brand_id}:{batch_no}"
    rng = sub_rng(seed, f"{stream}:base")
    seconds = sorted(rng.randrange(_SECONDS_PER_DAY) for _ in range(n))
    channel = batch_no >= config.order_sales_channel_from_batch
    wires = [
        _build_order(
            rng,
            brand_id=brand_id,
            currency=currency,
            customer_ids=customer_ids,
            product_ids=product_ids,
            moment=midnight + timedelta(seconds=sec),
            channel=channel,
        )
        for sec in seconds
    ]

    rates = {
        "orphan_fk": config.order_orphan_fk_rate,
        "future_ts": config.order_future_ts_rate,
        "negative_amount": config.order_negative_amount_rate,
        "duplicate": config.order_duplicate_rate,
    }
    want = {
        name: defect_count(rate, n * batch_no) - defect_count(rate, n * (batch_no - 1))
        for name, rate in rates.items()
    }
    from_previous = min(want["duplicate"] // 2, len(previous_clean))
    from_current = want["duplicate"] - from_previous
    need = want["orphan_fk"] + want["future_ts"] + want["negative_amount"] + from_current
    if need > n:
        raise InfeasibleDefectPlan(
            f"{brand_id} batch {batch_no}: {need} defect/duplicate-source lines do not fit "
            f"into {n} orders"
        )
    plan = sub_rng(seed, f"{stream}:plan")
    picked = plan.sample(range(n), need)
    cursor = 0
    defect_at: dict[int, str] = {}
    for name in ("orphan_fk", "future_ts", "negative_amount"):
        for index in picked[cursor : cursor + want[name]]:
            defect_at[index] = name
        cursor += want[name]
    duplicate_sources = sorted(picked[cursor:])
    previous_sources = sorted(plan.sample(range(len(previous_clean)), from_previous))

    orphan_rng = sub_rng(seed, f"{stream}:orphan")
    taken = set(customer_ids)
    for index, name in sorted(defect_at.items()):
        wire = wires[index]
        if name == "orphan_fk":
            wire["customer_id"] = _fresh_uuid(orphan_rng, taken)
        elif name == "future_ts":
            future = datetime.fromisoformat(wire["order_timestamp"]) + timedelta(
                days=FUTURE_OFFSET_DAYS
            )
            wire["order_timestamp"] = _iso(future)
        else:
            gross = -Decimal(wire["gross_amount"])
            wire["gross_amount"] = f"{gross:.2f}"
            wire["net_amount"] = f"{gross - Decimal(wire['discount_amount']):.2f}"

    lines = [canonical_json(wire) for wire in wires]
    source_set = set(duplicate_sources)
    clean = [lines[i] for i in range(n) if i not in defect_at and i not in source_set]
    entries: list[dict[str, Any]] = [
        {"line_no": index + 1, "order_id": wires[index]["order_id"], "defect": name}
        for index, name in sorted(defect_at.items())
    ]
    replays: list[tuple[str, dict[str, Any]]] = [
        (previous_clean[i], {"batch_no": batch_no - 1}) for i in previous_sources
    ] + [(lines[i], {"batch_no": batch_no, "line_no": i + 1}) for i in duplicate_sources]
    for line, origin in replays:
        lines.append(line)
        entries.append(
            {
                "line_no": len(lines),
                "order_id": json.loads(line)["order_id"],
                "defect": "duplicate",
                "duplicate_of": origin,
            }
        )

    data = "".join(line + "\n" for line in lines).encode("utf-8")
    counts = {
        "clean": n - sum(want[k] for k in ("orphan_fk", "future_ts", "negative_amount")),
        **want,
        "late_parent": 0,
    }
    manifest: dict[str, Any] = {
        "manifest_version": MANIFEST_VERSION,
        "entity": "orders",
        "schema_version": 1,
        "generator_version": GENERATOR_VERSION,
        "config_fingerprint": fingerprint,
        "seed": seed,
        "brand_id": brand_id,
        "batch_no": batch_no,
        "batch_date": batch_date.isoformat(),
        "data_key": orders_data_key(brand_id, batch_date.isoformat(), batch_no),
        "line_count": len(lines),
        "sha256": sha256_hex(data),
        "sales_channel_present": channel,
        "counts": counts,
        "expected_outcomes": {
            "silver_accepted": counts["clean"],
            "dlq": want["orphan_fk"] + want["future_ts"] + want["negative_amount"],
            "duplicates_removed": want["duplicate"],
        },
        "deferred": list(DEFERRED),
        "entries": sorted(entries, key=lambda e: int(e["line_no"])),
    }
    batch = OrderBatch(
        brand_id=brand_id,
        batch_no=batch_no,
        batch_date=batch_date,
        data=data,
        manifest=manifest,
    )
    return batch, clean


def generate_orders(
    config: GeneratorConfig,
    *,
    customer_pools: Mapping[str, Sequence[str]],
    product_ids: Mapping[str, Sequence[str]],
    batches: int,
    orders_per_batch: int,
    seed: int | None = None,
) -> OrdersRun:
    """Generate ``batches`` landing batches of ``orders_per_batch`` base orders per brand.

    Brands are the configured tenants present in ``customer_pools`` (config order). Batch ``k``
    is dated ``sim_start + (k - 1)`` days. Raises ``InfeasibleDefectPlan`` when the exact defect
    counts do not fit into ``orders_per_batch``.
    """
    seed = config.seed if seed is None else seed
    if batches < 1:
        raise ValueError("batches must be >= 1")
    if orders_per_batch < 1:
        raise ValueError("orders_per_batch must be >= 1")
    unknown = sorted(set(customer_pools) - set(config.tenants))
    if unknown:
        raise ValueError(f"customer_pools has brands that are not configured tenants: {unknown}")
    brands = [t for t in config.tenants if t in customer_pools]
    if not brands:
        raise ValueError("customer_pools has no configured tenant")
    for brand_id in brands:
        if not customer_pools[brand_id]:
            raise ValueError(f"empty customer pool for {brand_id}")
        if not product_ids.get(brand_id):
            raise ValueError(f"no product ids for {brand_id}")
    fingerprint = orders_fingerprint(
        config,
        seed=seed,
        brands=brands,
        batches=batches,
        orders_per_batch=orders_per_batch,
        customer_pools=customer_pools,
        product_ids=product_ids,
    )
    produced: list[tuple[int, int, OrderBatch]] = []
    for brand_id in brands:
        tenant_idx = config.tenants.index(brand_id)
        previous: list[str] = []
        for batch_no in range(1, batches + 1):
            batch, previous = _build_batch(
                config,
                seed=seed,
                fingerprint=fingerprint,
                brand_id=brand_id,
                currency=_CURRENCIES[tenant_idx % len(_CURRENCIES)],
                batch_no=batch_no,
                orders_per_batch=orders_per_batch,
                customer_ids=customer_pools[brand_id],
                product_ids=product_ids[brand_id],
                previous_clean=previous,
            )
            produced.append((batch_no, tenant_idx, batch))
    ordered = [batch for _, _, batch in sorted(produced, key=lambda t: (t[0], t[1]))]
    summary = _summary(ordered)
    log.info(
        "orders.generated", brands=len(brands), batches=batches, lines=summary["totals"]["lines"]
    )
    return OrdersRun(seed=seed, config_fingerprint=fingerprint, batches=ordered, summary=summary)


def _summary(batches: Sequence[OrderBatch]) -> dict[str, Any]:
    totals: dict[str, int] = {"lines": 0, "clean": 0, "late_parent": 0, **{d: 0 for d in DEFECTS}}
    by_brand: dict[str, dict[str, int]] = {}
    for batch in batches:
        counts = batch.manifest["counts"]
        brand = by_brand.setdefault(batch.brand_id, {"lines": 0, "batches": 0})
        brand["lines"] += batch.manifest["line_count"]
        brand["batches"] += 1
        totals["lines"] += batch.manifest["line_count"]
        for name in ("clean", "late_parent", *DEFECTS):
            totals[name] += counts[name]
    return {"batches": len(batches), "totals": totals, "by_brand": by_brand}
