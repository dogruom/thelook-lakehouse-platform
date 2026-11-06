"""Transport-neutral outbound messages (what the producer sends), built from generator runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakehouse.generator.io import canonical_json


if TYPE_CHECKING:
    from lakehouse.generator.cdc import CdcRun


@dataclass(frozen=True)
class OutboundMessage:
    """One Kafka message: key ``{brand_id}:{entity_id}``, UTF-8 value, str/bytes headers."""

    topic: str
    key: str
    value: bytes
    headers: tuple[tuple[str, bytes], ...]
    brand_id: str


def cdc_messages(run: CdcRun, topic: str) -> list[OutboundMessage]:
    """One message per CDC line, in emission order; the value is the canonical-JSON payload."""
    messages: list[OutboundMessage] = []
    for line in run.lines:
        payload = line["value"]
        messages.append(
            OutboundMessage(
                topic=topic,
                key=line["key"],
                value=canonical_json(payload).encode("utf-8"),
                headers=tuple(
                    (name, value.encode("utf-8")) for name, value in line["headers"].items()
                ),
                brand_id=payload["brand_id"],
            )
        )
    return messages
