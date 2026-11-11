"""PII helpers (DATA_CONTRACTS section 4): HMAC-SHA256 email token, phone mask, key loading.

The expected token is computed here with stdlib ``hmac`` so the test does not mirror the
implementation. The keys below are obvious test values, never real secrets.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from unittest.mock import MagicMock

import pytest

from lakehouse.common.pii import (
    PII_KEY_ENV,
    PiiKeyError,
    email_token,
    load_pii_key,
    phone_last4,
    pii_key_broadcast,
)


KEY = b"test-key-" * 4  # 36 bytes
OTHER_KEY = b"other-test-key-" * 3


def _expected(key: bytes, text: str) -> str:
    return hmac.new(key, text.encode("utf-8"), hashlib.sha256).hexdigest()


def test_env_var_name_is_the_documented_one() -> None:
    assert PII_KEY_ENV == "PII_HMAC_KEY"


def test_email_token_matches_independent_hmac_vector() -> None:
    assert email_token(KEY, "ada@example.com") == _expected(KEY, "ada@example.com")


def test_email_token_is_64_lowercase_hex() -> None:
    token = email_token(KEY, "ada@example.com")
    assert token is not None
    assert len(token) == 64
    assert token == token.lower()
    int(token, 16)


@pytest.mark.parametrize("raw", ["  Ada@Example.COM ", "ADA@EXAMPLE.COM", "\tada@example.com\n"])
def test_email_token_normalises_case_and_whitespace(raw: str) -> None:
    assert email_token(KEY, raw) == _expected(KEY, "ada@example.com")


def test_email_token_differs_for_different_email() -> None:
    assert email_token(KEY, "a@example.com") != email_token(KEY, "b@example.com")


def test_email_token_depends_on_key() -> None:
    assert email_token(KEY, "ada@example.com") != email_token(OTHER_KEY, "ada@example.com")


def test_email_token_is_not_plain_sha256() -> None:
    plain = hashlib.sha256(b"ada@example.com").hexdigest()
    assert email_token(KEY, "ada@example.com") != plain


def test_email_token_null_stays_null() -> None:
    assert email_token(KEY, None) is None


@pytest.mark.parametrize(
    ("phone", "expected"),
    [
        ("+447700900123", "0123"),
        ("+4915112345678", "5678"),
        ("+905321234567", "4567"),
        (None, None),
    ],
)
def test_phone_last4(phone: str | None, expected: str | None) -> None:
    assert phone_last4(phone) == expected


def test_phone_last4_never_returns_more_than_four_characters() -> None:
    result = phone_last4("+447700900123")
    assert result is not None
    assert len(result) == 4


def test_load_pii_key_accepts_a_long_raw_key() -> None:
    key = load_pii_key({PII_KEY_ENV: KEY.decode()})
    assert isinstance(key, bytes)
    assert len(key) >= 32


def test_load_pii_key_accepts_base64_of_32_bytes() -> None:
    encoded = base64.b64encode(bytes(range(32))).decode()
    key = load_pii_key({PII_KEY_ENV: encoded})
    assert isinstance(key, bytes)
    assert len(key) >= 32


@pytest.mark.parametrize(
    "environ",
    [
        {},
        {PII_KEY_ENV: ""},
        {PII_KEY_ENV: "change-me"},
        {PII_KEY_ENV: "short"},
        {PII_KEY_ENV: base64.b64encode(b"x" * 16).decode()},
    ],
    ids=["missing", "empty", "placeholder", "short_raw", "short_base64"],
)
def test_load_pii_key_rejects_unusable_keys(environ: dict[str, str]) -> None:
    with pytest.raises(PiiKeyError):
        load_pii_key(environ)


def test_load_pii_key_error_does_not_leak_the_value() -> None:
    with pytest.raises(PiiKeyError) as excinfo:
        load_pii_key({PII_KEY_ENV: "tiny-secret-value"})
    assert "tiny-secret-value" not in str(excinfo.value)


def test_load_pii_key_reads_process_environment_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(PII_KEY_ENV, KEY.decode())
    assert len(load_pii_key()) >= 32
    monkeypatch.delenv(PII_KEY_ENV)
    with pytest.raises(PiiKeyError):
        load_pii_key()


@pytest.mark.parametrize("pad", ["{} ", " {}", "{}\n", "\t{}"], ids=["trail", "lead", "nl", "tab"])
def test_load_pii_key_rejects_surrounding_whitespace_without_leaking_it(pad: str) -> None:
    value = pad.format("w" * 40)
    with pytest.raises(PiiKeyError, match="whitespace") as excinfo:
        load_pii_key({PII_KEY_ENV: value})
    assert "w" * 10 not in str(excinfo.value)


def test_load_pii_key_accepts_inner_whitespace() -> None:
    assert load_pii_key({PII_KEY_ENV: "a b " * 10 + "x"}) == ("a b " * 10 + "x").encode()


def test_pii_key_broadcast_is_destroyed_after_the_block() -> None:
    spark = MagicMock()
    with pii_key_broadcast(spark, KEY) as handle:
        spark.sparkContext.broadcast.assert_called_once_with(KEY)
        assert handle is spark.sparkContext.broadcast.return_value
        handle.destroy.assert_not_called()
    handle.destroy.assert_called_once_with()


def test_pii_key_broadcast_is_destroyed_when_the_block_raises() -> None:
    spark = MagicMock()
    with pytest.raises(RuntimeError, match="boom"), pii_key_broadcast(spark, KEY) as handle:
        raise RuntimeError("boom")
    handle.destroy.assert_called_once_with()


def _fake_pyspark(monkeypatch: pytest.MonkeyPatch) -> dict[str, list]:
    """Minimal pyspark stand-in: records column calls, keeps the UDF function."""
    import sys
    import types

    rec: dict[str, list] = {"udf": [], "calls": []}

    class _Len:
        def __ge__(self, n: int) -> tuple:
            return ("len>=", n)

    class _F:
        @staticmethod
        def regexp_replace(col, pat, rep):  # noqa: ARG004
            rec["calls"].append(("regexp_replace", pat, rep))
            return "digits"

        @staticmethod
        def length(col):  # noqa: ARG004
            return _Len()

        @staticmethod
        def substring(col, pos, ln):  # noqa: ARG004
            rec["calls"].append(("substring", pos, ln))
            return "sub"

        @staticmethod
        def when(cond, val):
            rec["calls"].append(("when", cond, val))
            return "when-col"

        @staticmethod
        def udf(fn, dtype):  # noqa: ARG004
            rec["udf"].append(fn)
            return lambda col: ("udf-col", col)

    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    sql.functions = _F  # type: ignore[attr-defined]
    types_mod = types.ModuleType("pyspark.sql.types")
    types_mod.StringType = lambda: "string"  # type: ignore[attr-defined]
    pyspark.sql = sql  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", types_mod)
    return rec


def test_phone_last4_column_keeps_only_digits_and_guards_short_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from lakehouse.common.pii import phone_last4_column

    rec = _fake_pyspark(monkeypatch)
    assert phone_last4_column("phone") == "when-col"  # type: ignore[arg-type]
    assert ("regexp_replace", "[^0-9]", "") in rec["calls"]
    assert ("substring", -4, 4) in rec["calls"]
    assert rec["calls"][-1][1] == ("len>=", 4)


def test_email_token_column_udf_matches_email_token_and_passes_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from lakehouse.common.pii import email_token_column

    rec = _fake_pyspark(monkeypatch)
    spark = MagicMock()
    spark.sparkContext.broadcast.return_value.value = KEY
    result = email_token_column(spark, KEY, "email-col")  # type: ignore[arg-type]
    assert result == ("udf-col", "email-col")
    spark.sparkContext.broadcast.assert_called_once_with(KEY)  # raw bytes -> new broadcast
    token = rec["udf"][0]
    assert token(None) is None
    assert token("  A@B.com ") == email_token(KEY, "a@b.com")
    assert token("a@b.com") != email_token(b"x" * 40, "a@b.com")


def test_email_token_column_reuses_a_given_broadcast_handle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from lakehouse.common.pii import email_token_column

    _fake_pyspark(monkeypatch)
    spark = MagicMock()
    email_token_column(spark, MagicMock(), "c")
    spark.sparkContext.broadcast.assert_not_called()
