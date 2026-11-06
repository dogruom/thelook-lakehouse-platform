"""Deterministic building blocks of the generator (decision D-027).

Every random decision comes from a per-purpose ``random.Random`` seeded with the string
``"{seed}:{purpose}"`` (string seeds are hashed deterministically, independent of
``PYTHONHASHSEED``). No wall clock, no global RNG state, no set iteration.
"""

from __future__ import annotations

import random
import re
import unicodedata
import uuid
from datetime import timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from faker import Faker


if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime


_TRANSLITERATION = {
    "\u0131": "i",  # dotless i
    "\u0130": "i",  # dotted capital I
    "ş": "s",
    "Ş": "s",
    "ğ": "g",
    "Ğ": "g",
    "ç": "c",
    "Ç": "c",
    "ö": "o",
    "Ö": "o",
    "ü": "u",
    "Ü": "u",
    "ä": "a",
    "Ä": "a",
    "ß": "ss",
    "é": "e",
    "è": "e",
    "ê": "e",
    "É": "e",
    "à": "a",
    "á": "a",
    "â": "a",
    "ô": "o",
    "ñ": "n",
    "ø": "o",
    "æ": "ae",
    "œ": "oe",
}
_NON_SLUG = re.compile(r"[^a-z0-9]")


def sub_rng(seed: int, purpose: str) -> random.Random:
    """Independent RNG stream for one purpose, derived from the run seed."""
    return random.Random(f"{seed}:{purpose}")


def uuid4_from(rng: random.Random) -> str:
    """Lowercase RFC 4122 version-4 UUID drawn from ``rng`` (never ``uuid.uuid4()``)."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def apportion(total: int, weights: Sequence[float]) -> list[int]:
    """Largest-remainder split of ``total`` over ``weights``; the parts sum to ``total`` exactly.

    Uses ``Decimal`` arithmetic; ties on the remainder are broken by index (lower index first).
    """
    if total < 0:
        raise ValueError("total must be >= 0")
    if not weights or any(w <= 0 for w in weights):
        raise ValueError("weights must be non-empty and > 0")
    decimals = [Decimal(str(w)) for w in weights]
    whole = sum(decimals)
    quotas = [Decimal(total) * w / whole for w in decimals]
    parts = [int(q) for q in quotas]
    leftover = total - sum(parts)
    by_remainder = sorted(range(len(parts)), key=lambda i: (-(quotas[i] - parts[i]), i))
    for i in by_remainder[:leftover]:
        parts[i] += 1
    return parts


def slugify(text: str) -> str:
    """ASCII ``[a-z0-9]`` slug: explicit transliteration first, then drop everything else."""
    mapped = "".join(_TRANSLITERATION.get(ch, ch) for ch in text)
    ascii_only = unicodedata.normalize("NFKD", mapped).encode("ascii", "ignore").decode("ascii")
    return _NON_SLUG.sub("", ascii_only.lower())


def make_fakers(seed: int, locales: Sequence[str]) -> dict[str, Faker]:
    """One independently seeded ``Faker`` per locale (the locale is chosen by our own RNG)."""
    fakers: dict[str, Faker] = {}
    for locale in locales:
        faker = Faker(locale)
        faker.seed_instance(sub_rng(seed, f"faker:{locale}").getrandbits(64))
        fakers[locale] = faker
    return fakers


class SimClock:
    """Strictly increasing simulated UTC clock with millisecond steps."""

    def __init__(
        self,
        start: datetime,
        rng: random.Random,
        *,
        min_step_ms: int = 50,
        max_step_ms: int = 500,
    ) -> None:
        self._now = start
        self._rng = rng
        self._min = min_step_ms
        self._max = max_step_ms

    def tick(self, not_before: datetime | None = None) -> datetime:
        """Advance by a random step, but never to a time earlier than ``not_before``."""
        now = self._now + timedelta(milliseconds=self._rng.randint(self._min, self._max))
        if not_before is not None and not_before > now:
            now = not_before
        self._now = now
        return now
