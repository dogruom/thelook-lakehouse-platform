"""Seeded clickstream generator with exact-count defects and a ground-truth manifest.

Contract: DATA_CONTRACTS §2.0 and §2.5. Pipeline: funnel counts per tenant -> sessions (a session
is a prefix of the funnel) -> interleaved emission order -> payloads on a simulated producer clock
-> exact-count defect plan -> duplicates -> manifest. Every random draw comes from a sub-RNG of the
run seed and every timestamp from the simulated clock, so the same seed and config give identical
bytes. Defective payloads (null session, truncated JSON) deliberately bypass the contract model.

Identified sessions (D-096): with ``customer_pools`` (``brand_id -> customer ids`` from
``orders.customer_pools_from_cdc``) every identified session references a same-brand CDC customer,
as orders do; without pools the customer id is a fresh UUID (unit tests, standalone runs).
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, timedelta
from itertools import pairwise
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import ValidationError

from lakehouse.contracts import (
    ClickstreamEvent,
    clickstream_headers,
    clickstream_key,
    to_wire,
)
from lakehouse.generator.cdc import InfeasibleDefectPlan
from lakehouse.generator.config import defect_count
from lakehouse.generator.determinism import SimClock, apportion, sub_rng, uuid4_from
from lakehouse.generator.io import canonical_json
from lakehouse.generator.messages import OutboundMessage


if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from lakehouse.generator.config import GeneratorConfig


GENERATOR_VERSION = "click-1.1.0"
FUNNEL = ("page_view", "search", "add_to_cart", "checkout", "purchase")
# Share of base events per funnel step; strictly decreasing, so per-run counts are too.
FUNNEL_WEIGHTS = (0.50, 0.22, 0.14, 0.09, 0.05)
DEVICES = ("mobile", "desktop", "tablet")
DEVICE_WEIGHTS = (0.55, 0.35, 0.10)
PAGE_URLS = {
    "page_view": ("/", "/category/shoes", "/category/jackets", "/product/PRD-00042", "/sale"),
    "search": ("/search",),
    "add_to_cart": ("/cart",),
    "checkout": ("/checkout",),
    "purchase": ("/checkout/confirmation",),
}
ANONYMOUS_SHARE = 0.4  # sessions without a customer_id
NULL_PAGE_URL_SHARE = 0.03
LATE_MIN_S, LATE_MAX_S = 10 * 60, 60 * 60  # DATA_CONTRACTS §2.5: 10-60 min behind
ONTIME_LAG_MS = (5, 2000)
MAX_SESSION_GAP = 40  # positions between two events of one session in the emission order
MAX_DUPLICATE_GAP = 50  # positions between an event and its re-sent copy
MAX_EVENTS = 10_000_000
PRODUCER_START_OFFSET = timedelta(hours=2)  # keeps late event_timestamps after sim_start

# Not configured here: the CLI (or any other caller) owns logging configuration.
log = structlog.get_logger(__name__)

_OUTCOME = {
    "late": "ACCEPT",
    "duplicate": "DUPLICATE",
    "null_session": "DLQ",
    "malformed_json": "PARSE_ERROR",
}


@dataclass(frozen=True)
class ClickstreamManifest:
    """Ground truth of a run: exact defect counts and one entry per defective message."""

    counts: dict[str, int]
    entries: list[dict[str, Any]]
    generator_version: str = GENERATOR_VERSION
    seed: int = 0
    brands: int = 0
    config_fingerprint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "generator_version": self.generator_version,
            "seed": self.seed,
            "brands": self.brands,
            "config_fingerprint": self.config_fingerprint,
            "counts": dict(self.counts),
            "entries": [dict(entry) for entry in self.entries],
        }


@dataclass(frozen=True)
class ClickstreamRun:
    """Messages in emission order and their manifest."""

    messages: list[OutboundMessage]
    manifest: ClickstreamManifest


@dataclass(slots=True)
class _Session:
    brand_id: str
    session_id: str
    customer_id: str | None
    device: str
    depth: int


@dataclass(slots=True)
class _Step:
    session: _Session
    step: int
    sort_key: tuple[int, int, int] = field(default=(0, 0, 0))


def _digest(items: Sequence[str]) -> str:
    return hashlib.sha256("\n".join(items).encode("utf-8")).hexdigest()


def config_fingerprint(
    config: GeneratorConfig,
    *,
    brands: int,
    events: int,
    customer_pools: Mapping[str, Sequence[str]] | None = None,
) -> str:
    """sha256 over the generation-relevant fields only (no connection settings, no secrets)."""
    relevant: dict[str, Any] = {
        "seed": config.seed,
        "tenants": config.tenants,
        "tenant_weights": config.tenant_weights,
        "sim_start": config.sim_start.astimezone(UTC).isoformat(),
        "brands": brands,
        "events": events,
        "generator_version": GENERATOR_VERSION,
    }
    for name in sorted(type(config).model_fields):
        if name.startswith("click_"):
            relevant[name] = getattr(config, name)
    if customer_pools is not None:
        relevant["customer_pools"] = {b: _digest(customer_pools[b]) for b in sorted(customer_pools)}
    return hashlib.sha256(canonical_json(relevant).encode("utf-8")).hexdigest()


def _check_pools(
    config: GeneratorConfig, brands: int, customer_pools: Mapping[str, Sequence[str]]
) -> None:
    for brand_id in config.tenants[:brands]:
        if not customer_pools.get(brand_id):
            raise ValueError(f"empty or missing customer pool for {brand_id}")


def _plan_sessions(
    config: GeneratorConfig,
    brands: int,
    events: int,
    customer_pools: Mapping[str, Sequence[str]] | None = None,
) -> list[_Session]:
    """Sessions per tenant such that the funnel step counts of the run are strictly decreasing."""
    shares = apportion(events, config.tenant_weights[:brands])
    attr_rng = sub_rng(config.seed, "click-session-attrs")
    id_rng = sub_rng(config.seed, "click-session-ids")
    customer_rng = sub_rng(config.seed, "click-session-customers")
    sessions: list[_Session] = []
    for idx, share in enumerate(shares):
        counts = sorted(apportion(share, FUNNEL_WEIGHTS), reverse=True)
        depths: list[int] = []
        for depth in range(1, len(FUNNEL) + 1):
            deeper = counts[depth] if depth < len(FUNNEL) else 0
            depths += [depth] * (counts[depth - 1] - deeper)
        attr_rng.shuffle(depths)
        for depth in depths:
            anonymous = attr_rng.random() < ANONYMOUS_SHARE
            brand_id = config.tenants[idx]
            session_id = uuid4_from(id_rng)
            customer_id: str | None = None
            if not anonymous:
                customer_id = (
                    customer_rng.choice(customer_pools[brand_id])
                    if customer_pools is not None
                    else uuid4_from(id_rng)
                )
            sessions.append(
                _Session(
                    brand_id=brand_id,
                    session_id=session_id,
                    customer_id=customer_id,
                    device=attr_rng.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
                    depth=depth,
                )
            )
    return sessions


def _emission_order(config: GeneratorConfig, sessions: list[_Session], events: int) -> list[_Step]:
    """Interleave sessions; the steps of one session keep their funnel order."""
    rng = sub_rng(config.seed, "click-schedule")
    steps: list[_Step] = []
    for s_idx, session in enumerate(sessions):
        position = rng.randrange(events)
        for step in range(session.depth):
            if step:
                position += rng.randint(1, MAX_SESSION_GAP)
            steps.append(_Step(session, step, (position, s_idx, step)))
    steps.sort(key=lambda s: s.sort_key)
    counts = Counter(FUNNEL[s.step] for s in steps)
    series = [counts[name] for name in FUNNEL]
    if sum(series) != events or any(a <= b for a, b in pairwise(series)):
        raise ValueError(f"events={events} is too small for a strictly decreasing funnel: {series}")
    return steps


def _assert_contract_violation(wire: dict[str, Any]) -> None:
    try:
        ClickstreamEvent.model_validate(wire)
    except ValidationError:
        return
    raise RuntimeError("generator bug: defective clickstream payload passed the contract")


def _message(topic: str, wire: dict[str, Any], value: bytes) -> OutboundMessage:
    return OutboundMessage(
        topic=topic,
        key=clickstream_key(wire),
        value=value,
        headers=tuple(clickstream_headers(wire)),
        brand_id=wire["brand_id"],
    )


def _select_defects(
    config: GeneratorConfig, events: int
) -> tuple[dict[int, str], list[int], dict[str, int]]:
    """Disjoint, exact-count selection of base positions (late | null | malformed | dup source)."""
    n = {
        "late": defect_count(config.click_late_rate, events),
        "null_session": defect_count(config.click_null_session_rate, events),
        "malformed_json": defect_count(config.click_malformed_json_rate, events),
        "duplicate": defect_count(config.click_duplicate_rate, events),
    }
    if sum(n.values()) > events:
        raise InfeasibleDefectPlan(f"defect plan {n} does not fit into {events} base events")
    rng = sub_rng(config.seed, "click-defect-plan")
    order = list(range(events))
    rng.shuffle(order)
    kind_of: dict[int, str] = {}
    cursor = 0
    for kind in ("late", "null_session", "malformed_json"):
        kind_of.update({i: kind for i in order[cursor : cursor + n[kind]]})
        cursor += n[kind]
    duplicate_src = sorted(rng.sample(sorted(order[cursor:]), n["duplicate"]))
    return kind_of, duplicate_src, n


def _build_base(
    config: GeneratorConfig, steps: list[_Step], kind_of: dict[int, str], topic: str
) -> tuple[list[OutboundMessage], list[str]]:
    """One message per base event on the simulated producer clock, with in-place mutations."""
    id_rng = sub_rng(config.seed, "click-event-ids")
    lag_rng = sub_rng(config.seed, "click-lag")
    url_rng = sub_rng(config.seed, "click-pages")
    cut_rng = sub_rng(config.seed, "click-truncate")
    clock = SimClock(
        config.sim_start.astimezone(UTC) + PRODUCER_START_OFFSET,
        sub_rng(config.seed, "click-producer-clock"),
        min_step_ms=1,
    )
    base: list[OutboundMessage] = []
    event_ids: list[str] = []
    for i, planned in enumerate(steps):
        session = planned.session
        name = FUNNEL[planned.step]
        kind = kind_of.get(i)
        produced_at = clock.tick()
        if kind == "late":
            lag = timedelta(seconds=lag_rng.randint(LATE_MIN_S, LATE_MAX_S))
        else:
            lag = timedelta(milliseconds=lag_rng.randint(*ONTIME_LAG_MS))
        page_url = (
            None if url_rng.random() < NULL_PAGE_URL_SHARE else url_rng.choice(PAGE_URLS[name])
        )
        wire = to_wire(
            ClickstreamEvent.model_validate(
                {
                    "event_id": uuid4_from(id_rng),
                    "brand_id": session.brand_id,
                    "schema_version": 1,
                    "produced_at": produced_at,
                    "session_id": session.session_id,
                    "customer_id": session.customer_id,
                    "event_type": name,
                    "page_url": page_url,
                    "device_category": session.device,
                    "event_timestamp": produced_at - lag,
                }
            )
        )
        event_ids.append(wire["event_id"])
        if kind == "null_session":
            defective = {**wire, "session_id": None}
            _assert_contract_violation(defective)
            base.append(_message(topic, defective, canonical_json(defective).encode("utf-8")))
        elif kind == "malformed_json":
            text = canonical_json(wire)
            truncated = text[: cut_rng.randint(len(text) // 2, len(text) - 1)]
            _assert_unparseable(truncated)
            # Key and headers stay those of the original, well-formed event.
            base.append(_message(topic, wire, truncated.encode("utf-8")))
        else:
            base.append(_message(topic, wire, canonical_json(wire).encode("utf-8")))
    return base, event_ids


def _assert_unparseable(text: str) -> None:
    try:
        json.loads(text)
    except json.JSONDecodeError:
        return
    raise RuntimeError("generator bug: truncated clickstream JSON still parses")


def _with_duplicates(
    config: GeneratorConfig,
    base: list[OutboundMessage],
    event_ids: list[str],
    kind_of: dict[int, str],
    duplicate_src: list[int],
) -> tuple[list[OutboundMessage], list[dict[str, Any]]]:
    """Re-send duplicates after their original: slot ``(pos, 0)`` is original, ``(pos, 1)`` a copy."""
    rng = sub_rng(config.seed, "click-duplicate-gap")
    last = len(base) - 1
    slots = [(i, 0, 0, i) for i in range(len(base))]
    for k, src in enumerate(duplicate_src):
        slots.append((min(src + rng.randint(1, MAX_DUPLICATE_GAP), last), 1, k, src))
    slots.sort()
    messages: list[OutboundMessage] = []
    entries: list[dict[str, Any]] = []
    for _, is_copy, _, src in slots:
        defect = "duplicate" if is_copy else kind_of.get(src)
        if defect is not None:
            entries.append(
                {
                    "index": len(messages),
                    "event_id": event_ids[src],
                    "brand_id": base[src].brand_id,
                    "defect": defect,
                    "expected_outcome": _OUTCOME[defect],
                }
            )
        messages.append(base[src])
    return messages, entries


def generate_clickstream(
    config: GeneratorConfig,
    *,
    brands: int,
    events: int,
    topic: str | None = None,
    customer_pools: Mapping[str, Sequence[str]] | None = None,
) -> ClickstreamRun:
    """Generate ``events`` clean base events over the first ``brands`` tenants plus defects.

    ``customer_pools`` (``brand_id -> customer ids``) makes identified sessions reference
    same-brand customers (D-096). Raises ``ValueError`` for out-of-range arguments or a missing or
    empty pool for a generated brand, and ``InfeasibleDefectPlan`` when the exact
    defect counts do not fit into the base events.
    """
    if not 1 <= brands <= len(config.tenants):
        raise ValueError(f"brands must be between 1 and {len(config.tenants)}")
    if not 1 <= events <= MAX_EVENTS:
        raise ValueError(f"events must be between 1 and {MAX_EVENTS}")
    if customer_pools is not None:
        _check_pools(config, brands, customer_pools)
    target_topic = topic if topic is not None else config.topic_clickstream
    kind_of, duplicate_src, n = _select_defects(config, events)
    steps = _emission_order(config, _plan_sessions(config, brands, events, customer_pools), events)
    base, event_ids = _build_base(config, steps, kind_of, target_topic)
    messages, entries = _with_duplicates(config, base, event_ids, kind_of, duplicate_src)
    counts = {"base": events, **n, "emitted": len(messages)}
    manifest = ClickstreamManifest(
        counts=counts,
        entries=entries,
        seed=config.seed,
        brands=brands,
        config_fingerprint=config_fingerprint(
            config, brands=brands, events=events, customer_pools=customer_pools
        ),
    )
    log.info("clickstream.generated", brands=brands, base_events=events, emitted=len(messages))
    return ClickstreamRun(messages=messages, manifest=manifest)
