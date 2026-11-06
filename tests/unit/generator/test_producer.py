"""produce_messages / producer_config with a FakeProducer (offline, W1.D5 / D5.2)."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, Any

import pytest

from lakehouse.generator.cdc import generate_cdc
from lakehouse.generator.clickstream import generate_clickstream
from lakehouse.generator.config import GeneratorConfig
from lakehouse.generator.messages import OutboundMessage, cdc_messages
from lakehouse.generator.producer import (
    DeliveryStats,
    create_producer,
    produce_messages,
    producer_config,
)


if TYPE_CHECKING:
    from collections.abc import Callable


class FakeMsg:
    def __init__(self, topic: str, key: Any) -> None:
        self._topic, self._key = topic, key

    def topic(self) -> str:
        return self._topic

    def key(self) -> Any:
        return self._key


class FakeError(Exception):
    pass


class FakeProducer:
    """Delivery callbacks fire on poll()/flush(), like librdkafka."""

    def __init__(
        self,
        *,
        buffer_error_on_call: int | None = None,
        fail_when: Callable[[int], bool] | None = None,
        undelivered: int = 0,
        interrupt_on_call: int | None = None,
    ) -> None:
        self.buffer_error_on_call = buffer_error_on_call
        self.fail_when = fail_when or (lambda _i: False)
        self.undelivered = undelivered
        self.interrupt_on_call = interrupt_on_call
        self.calls = 0
        self.accepted: list[dict[str, Any]] = []
        self.pending: list[tuple[int, Callable[..., None] | None, FakeMsg]] = []
        self.poll_calls = 0
        self.flush_calls: list[Any] = []
        self.events: list[str] = []
        self._buffer_error_raised = False

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: Any = None,
        headers: Any = None,
        on_delivery: Callable[..., None] | None = None,
        **_: Any,
    ) -> None:
        self.calls += 1
        self.events.append("produce")
        if self.interrupt_on_call == self.calls:
            raise KeyboardInterrupt
        if self.buffer_error_on_call == self.calls and not self._buffer_error_raised:
            self._buffer_error_raised = True
            raise BufferError("queue full")
        index = len(self.accepted)
        self.accepted.append(
            {"topic": topic, "value": value, "key": key, "headers": list(headers or [])}
        )
        self.pending.append((index, on_delivery, FakeMsg(topic, key)))

    def _fire(self, items: list[tuple[int, Callable[..., None] | None, FakeMsg]]) -> None:
        for index, callback, msg in items:
            if callback is not None:
                callback(FakeError("boom") if self.fail_when(index) else None, msg)

    def poll(self, timeout: float = 0) -> int:  # noqa: ARG002
        self.poll_calls += 1
        self.events.append("poll")
        if self.undelivered:  # simulate a stuck queue: only flush() can drain
            return 0
        items, self.pending = self.pending, []
        self._fire(items)
        return len(items)

    def flush(self, timeout: float = -1) -> int:
        self.flush_calls.append(timeout)
        self.events.append("flush")
        keep = len(self.pending) - self.undelivered if self.undelivered else len(self.pending)
        deliver = self.pending[: max(keep, 0)]
        left = len(self.pending) - len(deliver)
        self.pending = []
        self._fire(deliver)
        return left


def msgs(n: int = 30, topic: str = "t") -> list[OutboundMessage]:
    brands = ["brand_alpha", "brand_beta", "brand_gamma"]
    out = []
    for i in range(n):
        b = brands[i % 3]
        out.append(
            OutboundMessage(
                topic=topic if i % 2 == 0 else topic + "_b",
                key=f"{b}:{i}",
                value=f'{{"i":{i}}}'.encode(),
                headers=(("brand_id", b.encode()), ("schema_version", b"1")),
                brand_id=b,
            )
        )
    return out


def _key(raw: Any) -> str:
    return raw.decode() if isinstance(raw, bytes) else str(raw)


# --- producer_config ------------------------------------------------------------------------


def test_producer_config_is_idempotent_acks_all() -> None:
    cfg = producer_config("broker:9094")
    assert cfg["enable.idempotence"] is True
    assert cfg["acks"] == "all"
    assert cfg["bootstrap.servers"] == "broker:9094"


def test_producer_config_has_no_secret_defaults() -> None:
    assert not any("password" in k or "secret" in k for k in producer_config("h:1"))


def test_create_producer_returns_producer_like_without_connecting() -> None:
    producer = create_producer("127.0.0.1:1")  # lazy connect; nothing listens here
    assert callable(producer.produce) and callable(producer.poll) and callable(producer.flush)
    producer.flush(0)


# --- happy path -----------------------------------------------------------------------------


def test_all_messages_delivered_and_counted() -> None:
    producer = FakeProducer()
    batch = msgs(30)
    stats = produce_messages(batch, producer=producer)
    assert isinstance(stats, DeliveryStats)
    assert (stats.attempted, stats.delivered, stats.failed, stats.undelivered) == (30, 30, 0, 0)
    assert stats.ok is True
    assert len(producer.accepted) == 30


def test_by_brand_and_by_topic_counts() -> None:
    stats = produce_messages(msgs(30), producer=FakeProducer())
    assert dict(stats.by_brand) == {"brand_alpha": 10, "brand_beta": 10, "brand_gamma": 10}
    assert dict(stats.by_topic) == {"t": 15, "t_b": 15}


def test_key_value_and_headers_are_passed_through_in_order() -> None:
    producer = FakeProducer()
    batch = msgs(9)
    produce_messages(batch, producer=producer)
    for sent, original in zip(producer.accepted, batch, strict=True):
        assert sent["topic"] == original.topic
        assert _key(sent["key"]) == original.key
        assert sent["value"] == original.value
        assert list(sent["headers"]) == list(original.headers)
        assert dict(sent["headers"])["brand_id"] == original.key.split(":")[0].encode()


def test_cdc_messages_keys_have_brand_customer_format_through_the_producer() -> None:
    config = GeneratorConfig(_env_file=None, seed=42)  # type: ignore[call-arg]
    run = generate_cdc(config, brands=3, events=100)
    producer = FakeProducer()
    stats = produce_messages(cdc_messages(run, "customer_cdc"), producer=producer)
    assert stats.ok and stats.attempted == len(run.lines)
    for sent, line in zip(producer.accepted, run.lines, strict=True):
        payload = line["value"]
        assert _key(sent["key"]) == f"{payload['brand_id']}:{payload['customer_id']}"
        headers = dict(sent["headers"])
        assert set(headers) == {"brand_id", "schema_version", "event_type"}
        assert headers["event_type"] == b"customer_cdc"


def test_clickstream_keys_have_brand_session_format_through_the_producer() -> None:
    config = GeneratorConfig(_env_file=None, seed=42)  # type: ignore[call-arg]
    run = generate_clickstream(config, brands=3, events=300)
    producer = FakeProducer()
    stats = produce_messages(run.messages, producer=producer)
    assert stats.ok and stats.attempted == run.manifest.counts["emitted"]
    assert Counter(stats.by_brand) == Counter(m.brand_id for m in run.messages)
    for sent, message in zip(producer.accepted, run.messages, strict=True):
        assert _key(sent["key"]) == message.key
        assert _key(sent["key"]).startswith(dict(sent["headers"])["brand_id"].decode() + ":")


def test_empty_input_still_flushes_and_is_ok() -> None:
    producer = FakeProducer()
    stats = produce_messages([], producer=producer)
    assert stats.attempted == 0 and stats.ok
    assert len(producer.flush_calls) >= 1


# --- flush ----------------------------------------------------------------------------------


def test_flush_is_called_after_the_last_produce_with_the_timeout() -> None:
    producer = FakeProducer()
    produce_messages(msgs(5), producer=producer, flush_timeout_s=12.5)
    assert producer.flush_calls
    assert 12.5 in producer.flush_calls or producer.flush_calls[-1] == 12.5
    last_produce = max(i for i, e in enumerate(producer.events) if e == "produce")
    assert "flush" in producer.events[last_produce:]


def test_keyboard_interrupt_still_flushes_and_reraises() -> None:
    producer = FakeProducer(interrupt_on_call=5)
    with pytest.raises(KeyboardInterrupt):
        produce_messages(msgs(20), producer=producer)
    assert producer.flush_calls, "flush must run even when interrupted"
    assert len(producer.accepted) == 4


def test_unexpected_enqueue_error_still_flushes_and_is_never_ok() -> None:
    class Boom(FakeProducer):
        def produce(self, *a: Any, **k: Any) -> None:
            if self.calls == 1:
                self.calls += 1
                raise RuntimeError("broker exploded")
            super().produce(*a, **k)

    producer = Boom()
    try:
        stats = produce_messages(msgs(10), producer=producer)
    except RuntimeError:
        stats = None  # raising is acceptable; silently reporting success is not
    assert producer.flush_calls
    assert stats is None or stats.ok is False


# --- failures / undelivered (negative: loss is detected) ------------------------------------


def test_delivery_errors_are_counted_as_failed_and_not_ok() -> None:
    producer = FakeProducer(fail_when=lambda i: i % 10 == 0)
    stats = produce_messages(msgs(30), producer=producer)
    assert stats.attempted == 30
    assert stats.failed == 3
    assert stats.delivered == 27
    assert stats.undelivered == 0
    assert stats.ok is False


def test_failed_delivery_does_not_inflate_by_brand_counts_as_success() -> None:
    producer = FakeProducer(fail_when=lambda _i: True)
    stats = produce_messages(msgs(6), producer=producer)
    assert (stats.delivered, stats.failed) == (0, 6)
    assert sum(stats.by_brand.values()) in (0, 6)  # delivered or attempted view, never mixed
    assert stats.ok is False


def test_messages_left_in_queue_after_flush_are_undelivered_and_not_ok() -> None:
    producer = FakeProducer(undelivered=3)
    stats = produce_messages(msgs(30), producer=producer)
    assert stats.undelivered == 3
    assert stats.delivered == 27
    assert stats.failed == 0
    assert stats.ok is False


def test_delivered_failed_undelivered_always_sum_to_attempted() -> None:
    producer = FakeProducer(fail_when=lambda i: i < 4, undelivered=2)
    stats = produce_messages(msgs(30), producer=producer)
    assert stats.delivered + stats.failed + stats.undelivered == stats.attempted == 30
    assert stats.ok is False


# --- BufferError retry ----------------------------------------------------------------------


def test_buffer_error_triggers_poll_and_retry_without_losing_the_message() -> None:
    producer = FakeProducer(buffer_error_on_call=3)
    batch = msgs(10)
    stats = produce_messages(batch, producer=producer)
    assert stats.attempted == 10 and stats.delivered == 10 and stats.ok
    assert [_key(a["key"]) for a in producer.accepted] == [m.key for m in batch]
    # a poll must occur between the failing produce (call 3) and its successful retry (call 4)
    produce_idx = [i for i, e in enumerate(producer.events) if e == "produce"]
    between = producer.events[produce_idx[2] : produce_idx[3]]
    assert "poll" in between


# --- rate limit -----------------------------------------------------------------------------


def test_rate_limit_sleeps_via_injected_sleep_and_clock() -> None:
    now = [0.0]
    slept: list[float] = []

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        now[0] += seconds

    stats = produce_messages(
        msgs(50),
        producer=FakeProducer(),
        rate_limit_per_s=100,
        sleep=sleep,
        clock=lambda: now[0],
    )
    assert stats.ok
    assert slept and all(s >= 0 for s in slept)
    assert sum(slept) >= 0.9 * (50 - 1) / 100


def test_no_rate_limit_never_sleeps() -> None:
    def sleep(seconds: float) -> None:
        raise AssertionError("sleep must not be called without a rate limit")

    stats = produce_messages(msgs(20), producer=FakeProducer(), sleep=sleep, clock=lambda: 0.0)
    assert stats.ok
