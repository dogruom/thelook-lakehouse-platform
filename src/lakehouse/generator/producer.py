"""Keyed, idempotent Kafka producer with delivery accounting (DATA_CONTRACTS §2.0).

Delivery semantics: at-least-once. ``enable.idempotence`` and ``acks=all`` only limit duplicates
caused by producer retries; deterministic dedup downstream remains the safety net. Payloads are
never logged, only counts, brand ids, topics and error codes.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol

import structlog


if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from lakehouse.generator.messages import OutboundMessage


# Not configured here: the CLI (or any other caller) owns logging configuration.
log = structlog.get_logger(__name__)

BUFFER_POLL_TIMEOUT_S = 0.1
MAX_BUFFER_RETRIES = 600  # about a minute of waiting for the local queue to drain


class ProducerLike(Protocol):
    """The subset of ``confluent_kafka.Producer`` used here (lets tests inject a fake)."""

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: Callable[[Any, Any], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


def producer_config(bootstrap_servers: str) -> dict[str, Any]:
    """Idempotent, all-replica-acknowledged producer settings (no credentials)."""
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "lakehouse-generator",
        "enable.idempotence": True,
        "acks": "all",
        # librdkafka defaults to CRC32 (consistent_random); the Java client, Spark and Kafka
        # Connect use murmur2. Same key -> same partition across clients only with murmur2.
        "partitioner": "murmur2_random",
        "linger.ms": 20,
        "compression.type": "lz4",
        "delivery.timeout.ms": 60_000,
    }


def create_producer(bootstrap_servers: str) -> ProducerLike:
    """Real ``confluent_kafka.Producer`` (imported lazily so unit tests need no broker client)."""
    import confluent_kafka

    producer: ProducerLike = confluent_kafka.Producer(producer_config(bootstrap_servers))
    return producer


@dataclass
class DeliveryStats:
    """Outcome of one ``produce_messages`` call."""

    attempted: int = 0
    delivered: int = 0
    failed: int = 0
    undelivered: int = 0
    by_brand: dict[str, int] = field(default_factory=dict)
    by_topic: dict[str, int] = field(default_factory=dict)
    interrupted: bool = False  # run stopped early (local queue never drained)

    @property
    def ok(self) -> bool:
        return (
            self.failed == 0
            and self.undelivered == 0
            and not self.interrupted
            and self.delivered == self.attempted
        )


def _error_code(err: Any) -> str:
    """Short, payload-free description of a delivery error."""
    name = getattr(err, "name", None)
    if callable(name):
        return str(name())
    code = getattr(err, "code", None)
    return str(code() if callable(code) else err)


def _enqueue(
    producer: ProducerLike,
    message: OutboundMessage,
    callback: Callable[[Any, Any], None],
    stats: DeliveryStats,
) -> bool:
    """Enqueue one message. ``False`` means the local queue stayed full and the run must stop."""
    for _ in range(MAX_BUFFER_RETRIES + 1):
        try:
            producer.produce(
                message.topic,
                value=message.value,
                key=message.key.encode("utf-8"),
                headers=list(message.headers),
                on_delivery=callback,
            )
        except BufferError:
            producer.poll(BUFFER_POLL_TIMEOUT_S)  # let delivery callbacks free queue space
        except Exception as exc:  # one bad message must not hide the rest
            stats.failed += 1
            log.error(
                "producer_enqueue_failed",
                brand_id=message.brand_id,
                topic=message.topic,
                error_type=type(exc).__name__,
            )
            return True
        else:
            return True
    stats.failed += 1
    log.error("producer_queue_full", brand_id=message.brand_id, topic=message.topic)
    return False


def produce_messages(
    messages: Iterable[OutboundMessage],
    *,
    producer: ProducerLike,
    rate_limit_per_s: float | None = None,
    # Longer than delivery.timeout.ms (60 s): librdkafka reports every message through its
    # callback (delivered or failed) before flush returns; "undelivered" = outcome unknown.
    flush_timeout_s: float = 65.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> DeliveryStats:
    """Send ``messages`` in order; always flush before returning.

    ``KeyboardInterrupt`` (and any other exception) is re-raised after the flush and the summary.
    """
    if rate_limit_per_s is not None and rate_limit_per_s <= 0:
        raise ValueError("rate_limit_per_s must be > 0")
    stats = DeliveryStats()
    interval = None if rate_limit_per_s is None else 1.0 / rate_limit_per_s

    def callback_for(message: OutboundMessage) -> Callable[[Any, Any], None]:
        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                stats.failed += 1
                log.error(
                    "producer_delivery_failed",
                    brand_id=message.brand_id,
                    topic=message.topic,
                    error_code=_error_code(err),
                )
                return
            stats.delivered += 1
            stats.by_brand[message.brand_id] = stats.by_brand.get(message.brand_id, 0) + 1
            stats.by_topic[message.topic] = stats.by_topic.get(message.topic, 0) + 1

        return on_delivery

    started = clock()
    try:
        for message in messages:
            if interval is not None:
                wait = started + stats.attempted * interval - clock()
                if wait > 0:
                    sleep(wait)
            # ``attempted`` is counted only once ``_enqueue`` has resolved the message (queued, or
            # counted as failed there). A message abandoned by an escaping exception (e.g.
            # KeyboardInterrupt inside ``produce``) was never queued, so it is in no counter and
            # ``attempted == delivered + failed + undelivered`` holds on every exit path, except a
            # signal landing in the few bytecodes between _enqueue returning and this increment.
            enqueued = _enqueue(producer, message, callback_for(message), stats)
            stats.attempted += 1
            if not enqueued:
                stats.interrupted = True  # local queue never drained: stop, do not skip silently
                break
            producer.poll(0)
    finally:
        stats.undelivered = producer.flush(flush_timeout_s)
        _log_summary(stats)
    return stats


def _log_summary(stats: DeliveryStats) -> None:
    for brand_id, delivered in sorted(stats.by_brand.items()):
        log.bind(brand_id=brand_id).info("producer_brand_delivery", delivered=delivered)
    log.info(
        "producer_delivery_summary",
        attempted=stats.attempted,
        delivered=stats.delivered,
        failed=stats.failed,
        undelivered=stats.undelivered,
        interrupted=stats.interrupted,
        by_brand=dict(sorted(stats.by_brand.items())),
        by_topic=dict(sorted(stats.by_topic.items())),
    )
