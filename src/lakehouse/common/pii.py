"""PII pseudonymisation helpers (privacy controls, not anonymisation).

* ``email_token`` = ``HMAC-SHA256(key, lower(trim(email)))`` as 64 lowercase hex characters. The
  token is *pseudonymous* personal data: whoever holds the key can recompute it for a known email.
  Plain SHA-256 would be a lookup table for the same reason, which is why a key is mandatory.
* ``phone_last4`` keeps only the last four digits of a phone number.

The key comes from the process environment (``PII_HMAC_KEY``, local ``.env`` / secret store on AWS).
It is **never** put into Spark conf, CLI arguments, logs or error messages: :func:`load_pii_key`
reports the reason of a rejection but never the value. Spark has no built-in HMAC in 3.5, so the
token column is a plain Python UDF whose closure holds a *broadcast* of the key (no pandas UDF:
pandas is not in the Spark image). The UDF body imports only the standard library, so executors do
not need the ``lakehouse`` package for it.

PySpark is imported lazily so this module imports on hosts without it.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
from contextlib import contextmanager
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from pyspark import Broadcast
    from pyspark.sql import Column, SparkSession


PII_KEY_ENV = "PII_HMAC_KEY"
MIN_KEY_BYTES = 32
_PLACEHOLDERS = frozenset({"change-me", "changeme"})
_NON_DIGITS = re.compile(r"[^0-9]")


class PiiKeyError(RuntimeError):
    """The HMAC key is missing, a placeholder or too short. Never carries the key itself."""


def load_pii_key(environ: Mapping[str, str] | None = None) -> bytes:
    """The HMAC key as bytes: the UTF-8 bytes of ``PII_HMAC_KEY`` (no implicit base64 decoding).

    Rejected with :class:`PiiKeyError`: unset or blank, leading/trailing whitespace (a pasted
    newline or space would silently change every token), a placeholder (``change-me``), or shorter
    than 32 bytes. ``environ`` defaults to ``os.environ``.
    """
    env = os.environ if environ is None else environ
    value = env.get(PII_KEY_ENV)
    if value is None or not value.strip():
        raise PiiKeyError(f"{PII_KEY_ENV} is not set; set it in .env (see .env.example)")
    if value != value.strip():
        raise PiiKeyError(f"{PII_KEY_ENV} has leading or trailing whitespace; remove it")
    if value.strip().lower() in _PLACEHOLDERS:
        raise PiiKeyError(f"{PII_KEY_ENV} is still the placeholder; set a random secret")
    key = value.encode("utf-8")
    if len(key) < MIN_KEY_BYTES:
        raise PiiKeyError(f"{PII_KEY_ENV} must be at least {MIN_KEY_BYTES} bytes")
    return key


def email_token(key: bytes, email: str | None) -> str | None:
    """HMAC-SHA256 hex of ``lower(trim(email))``; ``None`` for a ``None`` email."""
    if email is None:
        return None
    return hmac.new(key, email.strip().lower().encode("utf-8"), hashlib.sha256).hexdigest()


def phone_last4(phone: str | None) -> str | None:
    """Last four digits of ``phone``; ``None`` when absent or with fewer than four digits."""
    if phone is None:
        return None
    digits = _NON_DIGITS.sub("", phone)
    return digits[-4:] if len(digits) >= 4 else None


def phone_last4_column(phone: Column) -> Column:
    """Spark-native equivalent of :func:`phone_last4` (no UDF)."""
    from pyspark.sql import functions as F

    digits = F.regexp_replace(phone, "[^0-9]", "")
    return F.when(F.length(digits) >= 4, F.substring(digits, -4, 4))


@contextmanager
def pii_key_broadcast(spark: SparkSession, key: bytes) -> Iterator[Broadcast[bytes]]:
    """Broadcast of the HMAC key that is destroyed on exit, also when the body raises.

    Every action that evaluates :func:`email_token_column` with this handle must finish inside the
    ``with`` block; afterwards executors and the driver no longer hold the key.
    """
    broadcast = spark.sparkContext.broadcast(key)
    try:
        yield broadcast
    finally:
        broadcast.destroy()


def email_token_column(spark: SparkSession, key: bytes | Broadcast[bytes], email: Column) -> Column:
    """Column of :func:`email_token` values. The key travels as a broadcast, not as Spark conf.

    Pass a handle from :func:`pii_key_broadcast` to control its lifetime. Passing raw ``bytes``
    creates a broadcast that lives until the SparkContext stops (tests and ad-hoc use only).
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    broadcast = spark.sparkContext.broadcast(key) if isinstance(key, bytes) else key

    def token(value: str | None) -> str | None:
        # Self-contained on purpose (stdlib only): see the module docstring.
        if value is None:
            return None
        return hmac.new(
            broadcast.value, value.strip().lower().encode("utf-8"), hashlib.sha256
        ).hexdigest()

    return F.udf(token, StringType())(email)
