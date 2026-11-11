"""Seeded customer CDC generator with a ground-truth manifest (DATA_CONTRACTS §2.1, §4.1).

Pipeline: lifecycles per tenant -> interleaved base order -> defect plan -> emission order ->
payloads -> one state-machine pass (``classify_outcomes``) -> manifest. Defects are recorded only
in the manifest, never in a payload. Decisions D-022...D-027.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import ValidationError

from lakehouse.contracts import (
    CustomerCdcEvent,
    LoyaltyTier,
    cdc_headers,
    cdc_key,
    to_wire,
)
from lakehouse.generator.config import defect_count
from lakehouse.generator.determinism import (
    SimClock,
    apportion,
    make_fakers,
    slugify,
    sub_rng,
    uuid4_from,
)
from lakehouse.generator.manifest import (
    DLQ_DEFECTS,
    CdcDefect,
    Manifest,
    ManifestEntry,
    build_counts,
    classify_outcomes,
    expected_final_rows,
)


if TYPE_CHECKING:
    import random

    from faker import Faker

    from lakehouse.generator.config import GeneratorConfig


GENERATOR_VERSION = "cdc-1.1.0"
LOCALES = {"DE": "de_DE", "GB": "en_GB", "TR": "tr_TR"}
COUNTRIES = tuple(LOCALES)
# Phone numbers come only from ranges reserved for fiction, so no real subscriber can be hit:
# the UK Ofcom drama range +44 7700 900000-900999 is used for every country (a verified DE/TR
# equivalent was not confirmed, so the phone prefix does not follow the customer's country).
PHONE_DRAMA_RANGE = {"DE": "+447700900", "GB": "+447700900", "TR": "+447700900"}
TIERS = tuple(t.value for t in LoyaltyTier)
EMAIL_TLDS = ("com", "org", "net")  # RFC 2606: example.<tld> never resolves to a real mailbox
INVALID_EMAILS = (
    "user@",
    "missing-at.com",
    "@no-local.example",
    "two@@example.com",
    "spaces in@example.com",
    "no-tld@example",
)
MAX_LATE_POSITIONS = 30  # how far behind its natural position a late/replayed event may land
MAX_SIGNUP_AGE_DAYS = 1500
PHONE_NULL_SHARE = 0.15
LIFECYCLE_WINDOW = 60  # events a customer lifecycle is spread over (in base-order positions)

# Not configured here: the CLI (or any other caller) owns logging configuration.
log = structlog.get_logger(__name__)


class InfeasibleDefectPlan(ValueError):
    """The requested exact defect counts cannot be injected into the generated events."""


@dataclass(slots=True)
class _Customer:
    brand_idx: int
    brand_id: str
    customer_id: str
    email: str
    first_name: str
    last_name: str
    signup_back_days: int
    events: list[_Event] = field(default_factory=list)


@dataclass(slots=True)
class _Event:
    customer: _Customer
    event_id: str
    action: str
    seq: int
    city: str | None
    country: str
    tier: str
    phone: str | None
    pos: int = -1
    defect: CdcDefect | None = None
    protected: bool = False  # anchor of a moved event: must stay valid and in place
    anchor: _Event | None = None  # out_of_order only: later event of the customer it lands after


@dataclass(slots=True)
class _State:
    city: str | None
    country: str
    tier: str
    phone: str | None


@dataclass(frozen=True)
class CdcRun:
    """Emitted lines (``{"key", "headers", "value"}``, emission order) and their manifest."""

    lines: list[dict[str, Any]]
    manifest: Manifest


def config_fingerprint(config: GeneratorConfig, *, brands: int, events: int) -> str:
    """sha256 over the generation-relevant fields only (no connection settings, no secrets)."""
    relevant: dict[str, Any] = {
        "seed": config.seed,
        "tenants": config.tenants,
        "tenant_weights": config.tenant_weights,
        "sim_start": config.sim_start.astimezone(UTC).isoformat(),
        "brands": brands,
        "events": events,
    }
    for name in sorted(type(config).model_fields):
        if name.startswith("cdc_"):
            relevant[name] = getattr(config, name)
    canonical = json.dumps(relevant, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# --- lifecycles ----------------------------------------------------------------------------


def _phone(country: str, rng: random.Random) -> str | None:
    if rng.random() < PHONE_NULL_SHARE:
        return None
    return f"{PHONE_DRAMA_RANGE[country]}{rng.randrange(1000):03d}"


def _new_customer(
    brand_idx: int,
    brand_id: str,
    rng: random.Random,
    fakers: dict[str, Faker],
    serial: int,
) -> tuple[_Customer, _State]:
    country = rng.choice(COUNTRIES)
    faker = fakers[LOCALES[country]]
    first = faker.first_name()[:50] or "Alex"
    last = faker.last_name()[:50] or "Muster"
    email = (
        f"{slugify(first) or 'user'}.{slugify(last) or 'customer'}.{serial}"
        f"@example.{rng.choice(EMAIL_TLDS)}"
    )
    customer = _Customer(
        brand_idx=brand_idx,
        brand_id=brand_id,
        customer_id=uuid4_from(rng),
        email=email,
        first_name=first,
        last_name=last,
        signup_back_days=rng.randint(0, MAX_SIGNUP_AGE_DAYS),
    )
    state = _State(
        city=faker.city(), country=country, tier=rng.choice(TIERS), phone=_phone(country, rng)
    )
    return customer, state


def _mutate(state: _State, rng: random.Random, fakers: dict[str, Faker]) -> None:
    """Change one or two of city, country, loyalty tier, phone (an ``U`` row image)."""
    for name in rng.sample(("city", "country", "tier", "phone"), rng.randint(1, 2)):
        if name == "city":
            state.city = fakers[LOCALES[state.country]].city()
        elif name == "country":
            state.country = rng.choice([c for c in COUNTRIES if c != state.country])
            state.city = fakers[LOCALES[state.country]].city()
        elif name == "tier":
            state.tier = rng.choice([t for t in TIERS if t != state.tier])
        else:
            state.phone = _phone(state.country, rng)


def _customer_count(config: GeneratorConfig, n_events: int) -> int:
    """Customers ``C`` whose lifecycles can total ``n_events`` with ``defect_count`` D events.

    Feasible when ``C + d <= n_events <= C * (1 + max_updates) + d`` with ``d = defect_count(rate, C)``;
    picks the feasible ``C`` closest to the expected-lifecycle-length target.
    """
    rate, longest = config.cdc_delete_rate, 1 + config.cdc_max_updates
    target = round(n_events / (1 + config.cdc_max_updates / 2 + rate / 100))
    feasible = [
        c
        for c in range(1, n_events + 1)
        if c + defect_count(rate, c) <= n_events <= c * longest + defect_count(rate, c)
    ]
    if not feasible:
        raise InfeasibleDefectPlan(
            f"infeasible delete (D) plan: {n_events} events cannot be split into complete "
            f"lifecycles with cdc_delete_rate={rate} and cdc_max_updates={config.cdc_max_updates}"
        )
    return min(feasible, key=lambda c: (abs(c - target), c))


def _update_counts(
    config: GeneratorConfig, rng: random.Random, customers: int, deletes: int, n_events: int
) -> list[int]:
    """Updates per customer: uniform draw, then random +/-1 steps until the event total is exact."""
    longest = config.cdc_max_updates
    updates = [rng.randint(0, longest) for _ in range(customers)]
    diff = n_events - (customers + sum(updates) + deletes)
    step = 1 if diff > 0 else -1
    bound = longest if step == 1 else 0
    movable = [i for i, u in enumerate(updates) if u != bound]
    while diff != 0:
        slot = rng.randrange(len(movable))
        updates[movable[slot]] += step
        diff -= step
        if updates[movable[slot]] == bound:
            movable[slot] = movable[-1]
            movable.pop()
    return updates


def _generate_brand(
    config: GeneratorConfig,
    brand_idx: int,
    n_events: int,
    fakers: dict[str, Faker],
    serial_start: int,
) -> list[_Customer]:
    """Complete lifecycles; exactly ``defect_count(cdc_delete_rate, customers)`` end with ``D``."""
    brand_id = config.tenants[brand_idx]
    if n_events == 0:
        return []
    rng = sub_rng(config.seed, f"lifecycle:{brand_id}")
    n_customers = _customer_count(config, n_events)
    n_deleted = defect_count(config.cdc_delete_rate, n_customers)
    deleted = set(rng.sample(range(n_customers), n_deleted))
    updates = _update_counts(config, rng, n_customers, n_deleted, n_events)
    customers: list[_Customer] = []
    for idx in range(n_customers):
        customer, state = _new_customer(brand_idx, brand_id, rng, fakers, serial_start + idx)
        actions = ["I"] + ["U"] * updates[idx] + (["D"] if idx in deleted else [])
        for seq, action in enumerate(actions, start=1):
            if action == "U":
                _mutate(state, rng, fakers)
            customer.events.append(
                _Event(
                    customer=customer,
                    event_id=uuid4_from(rng),
                    action=action,
                    seq=seq,
                    city=state.city,
                    country=state.country,
                    tier=state.tier,
                    phone=state.phone,
                )
            )
        customers.append(customer)
    log.bind(brand_id=brand_id).info(
        "cdc.brand_generated", customers=n_customers, events=n_events, deleted=n_deleted
    )
    return customers


def _interleave(customers: list[_Customer], seed: int, total: int) -> list[_Event]:
    """Global base order: per-customer order kept, lifecycles overlap within a small window."""
    rng = sub_rng(seed, "interleave")
    window = min(1.0, LIFECYCLE_WINDOW / total)
    keyed: list[tuple[float, int, int, _Event]] = []
    for ci, customer in enumerate(customers):
        start = rng.random()
        offsets = sorted(rng.random() for _ in customer.events)
        keyed.extend(
            (start + window * offsets[j], ci, j, ev) for j, ev in enumerate(customer.events)
        )
    keyed.sort(key=lambda t: t[:3])
    order = [t[3] for t in keyed]
    for pos, ev in enumerate(order):
        ev.pos = pos
    return order


# --- defect plan ---------------------------------------------------------------------------


def _pick(rng: random.Random, pool: list[Any], k: int, what: str) -> list[Any]:
    if k > len(pool):
        raise InfeasibleDefectPlan(
            f"infeasible {what}: need {k} but only {len(pool)} eligible; use more --events/--brands "
            "or lower the defect rate"
        )
    return rng.sample(pool, k)


def _apply_collisions(config: GeneratorConfig, customers: list[_Customer], brands: int) -> None:
    """Customers of brands 2..N reuse ``customer_id`` and email of a customer in an earlier brand."""
    rate = config.cdc_cross_tenant_collision_rate
    if brands == 1:
        if rate > 0:
            raise InfeasibleDefectPlan(
                "infeasible cross_tenant_collision: needs at least 2 brands; set "
                "GEN_CDC_CROSS_TENANT_COLLISION_RATE=0 for a single-brand run"
            )
        return
    rng = sub_rng(config.seed, "defect:collision")
    candidates = [c for c in customers if c.brand_idx >= 1]
    targets = _pick(
        rng,
        candidates,
        defect_count(rate, len(candidates)),
        "cross_tenant_collision (target customers)",
    )
    index = {id(c): i for i, c in enumerate(customers)}
    target_ids = {id(c) for c in targets}
    used: set[int] = set()
    for target in sorted(targets, key=lambda c: index[id(c)]):
        sources = [
            c
            for c in customers
            if c.brand_idx < target.brand_idx and id(c) not in target_ids and id(c) not in used
        ]
        if not sources:
            raise InfeasibleDefectPlan(
                "infeasible cross_tenant_collision: no unused source customer left"
            )
        source = rng.choice(sources)
        used.add(id(source))
        target.customer_id, target.email = source.customer_id, source.email
        target.events[0].defect = CdcDefect.cross_tenant_collision


def _apply_out_of_order(config: GeneratorConfig, order: list[_Event], n: int) -> None:
    """Mark ``n`` events to be emitted after a later event (anchor) of the same customer.

    The anchor is drawn from the later events of the customer that were not themselves chosen;
    the last event is always eligible. Anchor < last = intermediate late event (backfill),
    anchor == last = late event after the last one. Every moved event stays a stale NOOP.
    """
    if n == 0:
        return
    if config.cdc_max_updates == 0:
        raise InfeasibleDefectPlan(
            "infeasible out_of_order: needs updates, cdc_max_updates must be > 0"
        )
    rng = sub_rng(config.seed, "defect:out_of_order")
    pool = [ev for ev in order if ev.defect is None and ev is not ev.customer.events[-1]]
    chosen = _pick(rng, pool, n, "out_of_order (events with a later event of the same customer)")
    for ev in chosen:
        ev.defect = CdcDefect.out_of_order
    for ev in chosen:
        later = [e for e in ev.customer.events[ev.seq :] if e.defect is not CdcDefect.out_of_order]
        ev.anchor = rng.choice(later)
        ev.anchor.protected = True


def _apply_dlq_defects(config: GeneratorConfig, order: list[_Event], n_by: dict[str, int]) -> None:
    rng = sub_rng(config.seed, "defect:dlq")
    pool = [ev for ev in order if ev.defect is None and ev.action != "D" and not ev.protected]
    total = sum(n_by.values())
    chosen = _pick(rng, pool, total, "invalid_email/null_email/future_signup (clean non-D events)")
    start = 0
    for defect in (CdcDefect.invalid_email, CdcDefect.null_email, CdcDefect.future_signup):
        for ev in chosen[start : start + n_by[defect.value]]:
            ev.defect = defect
        start += n_by[defect.value]


def _pick_duplicates(config: GeneratorConfig, order: list[_Event], n: int) -> list[int]:
    rng = sub_rng(config.seed, "defect:duplicate")
    pool = [ev.pos for ev in order if ev.defect is None]
    return sorted(_pick(rng, pool, n, "duplicate (clean events to replay)"))


def _emission_order(
    seed: int, order: list[_Event], duplicates: list[int]
) -> list[tuple[int, bool]]:
    """Final emission order as ``(base position, is_replay)``; late events land after their anchor."""
    rng = sub_rng(seed, "emission")
    keyed: list[tuple[float, int, int]] = []
    for ev in order:
        if ev.defect is CdcDefect.out_of_order:
            assert ev.anchor is not None
            key = ev.anchor.pos + rng.randint(0, MAX_LATE_POSITIONS) + rng.uniform(0.01, 0.99)
        else:
            key = float(ev.pos)
        keyed.append((key, ev.pos, 0))
    for pos in duplicates:
        key = pos + rng.randint(1, MAX_LATE_POSITIONS) + rng.uniform(0.01, 0.99)
        keyed.append((key, pos, 1))
    keyed.sort()
    return [(pos, bool(replay)) for _, pos, replay in keyed]


# --- payloads ------------------------------------------------------------------------------


def _clean_wire(ev: _Event, cdc_ts: datetime, produced_at: datetime, first_ts: datetime) -> Any:
    c = ev.customer
    raw = {
        "event_id": ev.event_id,
        "brand_id": c.brand_id,
        "schema_version": 1,
        "produced_at": produced_at,
        "customer_id": c.customer_id,
        "first_name": c.first_name,
        "last_name": c.last_name,
        "email": c.email,
        "phone": ev.phone,
        "city": ev.city,
        "country": ev.country,
        "loyalty_tier": ev.tier,
        "signup_date": first_ts.date() - timedelta(days=c.signup_back_days),
        "_cdc_action": ev.action,
        "_cdc_sequence": ev.seq,
        "_cdc_ts": cdc_ts,
    }
    return to_wire(CustomerCdcEvent.model_validate(raw))


def _defective_wire(
    wire: dict[str, Any], defect: CdcDefect, produced_at: datetime, rng: random.Random
) -> dict[str, Any]:
    out = dict(wire)
    if defect is CdcDefect.invalid_email:
        out["email"] = rng.choice(INVALID_EMAILS)
    elif defect is CdcDefect.null_email:
        out["email"] = None
    else:
        days = rng.randint(1, 365)
        out["signup_date"] = (produced_at.astimezone(UTC).date() + timedelta(days=days)).isoformat()
    try:
        CustomerCdcEvent.model_validate(out)
    except ValidationError:
        return out
    raise RuntimeError(f"generator bug: {defect.value} payload passed the contract")


def _build_payloads(
    config: GeneratorConfig, order: list[_Event], emission: list[tuple[int, bool]]
) -> list[dict[str, Any]]:
    """Wire payload per base position; ``produced_at`` follows the final emission order."""
    source_clock = SimClock(config.sim_start.astimezone(UTC), sub_rng(config.seed, "source-clock"))
    cdc_ts = [source_clock.tick() for _ in order]
    lag_rng = sub_rng(config.seed, "produced-lag")
    producer_clock = SimClock(
        config.sim_start.astimezone(UTC), sub_rng(config.seed, "producer-clock"), min_step_ms=1
    )
    payload_rng = sub_rng(config.seed, "defect-payload")
    produced: dict[int, datetime] = {}
    for pos, is_replay in emission:
        if not is_replay:
            lag = timedelta(milliseconds=lag_rng.randint(5, 200))
            produced[pos] = producer_clock.tick(not_before=cdc_ts[pos] + lag)
    payloads: list[dict[str, Any]] = []
    for ev in order:
        first_ts = cdc_ts[ev.customer.events[0].pos]
        wire = _clean_wire(ev, cdc_ts[ev.pos], produced[ev.pos], first_ts)
        if ev.defect is not None and ev.defect in DLQ_DEFECTS:
            wire = _defective_wire(wire, ev.defect, produced[ev.pos], payload_rng)
        payloads.append(wire)
    return payloads


def _line(payload: dict[str, Any]) -> dict[str, Any]:
    headers = {name: value.decode("utf-8") for name, value in cdc_headers(payload)}
    return {"key": cdc_key(payload), "headers": headers, "value": payload}


# --- entry point ---------------------------------------------------------------------------


def _defect_counts(config: GeneratorConfig, events: int) -> dict[str, int]:
    return {
        "invalid_email": defect_count(config.cdc_invalid_email_rate, events),
        "null_email": defect_count(config.cdc_null_email_rate, events),
        "future_signup": defect_count(config.cdc_future_signup_rate, events),
        "out_of_order": defect_count(config.cdc_out_of_order_rate, events),
        "duplicate": defect_count(config.cdc_duplicate_rate, events),
    }


def generate_cdc(config: GeneratorConfig, *, brands: int, events: int) -> CdcRun:
    """Generate ``events`` clean lifecycle events over the first ``brands`` tenants plus defects.

    Raises ``InfeasibleDefectPlan`` when the exact defect counts cannot be injected.
    """
    if not 1 <= brands <= len(config.tenants):
        raise ValueError(f"brands must be between 1 and {len(config.tenants)}")
    if events < 1:
        raise ValueError("events must be >= 1")
    shares = apportion(events, config.tenant_weights[:brands])
    fakers = make_fakers(config.seed, list(LOCALES.values()))
    customers: list[_Customer] = []
    for idx, share in enumerate(shares):
        customers += _generate_brand(config, idx, share, fakers, len(customers))
    order = _interleave(customers, config.seed, events)

    wanted = _defect_counts(config, events)
    _apply_collisions(config, customers, brands)
    _apply_out_of_order(config, order, wanted["out_of_order"])
    _apply_dlq_defects(config, order, wanted)
    duplicates = _pick_duplicates(config, order, wanted["duplicate"])

    emission = _emission_order(config.seed, order, duplicates)
    payloads = _build_payloads(config, order, emission)

    entries: list[ManifestEntry] = []
    lines: list[dict[str, Any]] = []
    values: list[dict[str, Any]] = []
    valid: list[bool] = []
    for index, (pos, is_replay) in enumerate(emission):
        ev = order[pos]
        payload = copy.deepcopy(payloads[pos]) if is_replay else payloads[pos]
        defect = CdcDefect.duplicate if is_replay else ev.defect
        entries.append(
            ManifestEntry(
                emission_index=index,
                event_id=ev.event_id,
                brand_id=ev.customer.brand_id,
                customer_id=ev.customer.customer_id,
                cdc_sequence=ev.seq,
                cdc_action=ev.action,
                defect=defect,
                late_after_sequence=ev.anchor.seq if ev.anchor is not None else None,
            )
        )
        lines.append(_line(payload))
        values.append(payload)
        valid.append(ev.defect not in DLQ_DEFECTS)

    outcomes = classify_outcomes(entries, valid)
    entries = [replace(e, expected_outcome=o) for e, o in zip(entries, outcomes, strict=True)]
    manifest = Manifest(
        seed=config.seed,
        brands=brands,
        base_events=events,
        emitted_events=len(entries),
        generator_version=GENERATOR_VERSION,
        config_fingerprint=config_fingerprint(config, brands=brands, events=events),
        entries=entries,
        counts=build_counts(entries),
        expected_final=expected_final_rows(entries, values, valid),
    )
    log.info("cdc.generated", brands=brands, base_events=events, emitted=len(entries))
    return CdcRun(lines=lines, manifest=manifest)
