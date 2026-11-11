"""replay job: run() preconditions, select_candidates wiring, replay_orders reconciliation (stubs)."""

from __future__ import annotations

import argparse
from typing import Any

import pytest

from lakehouse.common.jobs import JobContext
from lakehouse.common.spark import SparkSettings
from lakehouse.quality.jobs import replay
from lakehouse.quality.jobs.replay import ReplayError, ReplayJob
from tests.unit.quality import fake_spark
from tests.unit.quality.fake_spark import Perm


def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="replay")
    ReplayJob().add_arguments(parser)
    return parser.parse_args(list(argv))


def _ctx() -> JobContext:
    return JobContext("replay", "run1", SparkSettings(catalog="hive"))


# ---- argument validators ------------------------------------------------------------------------


@pytest.mark.parametrize("raw", ["1", "3", "99"])
def test_positive_int_accepts(raw: str) -> None:
    assert replay._positive_int(raw) == int(raw)


@pytest.mark.parametrize("raw", ["0", "-2", "x", "1.5", ""])
def test_positive_int_rejects(raw: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        replay._positive_int(raw)


@pytest.mark.parametrize("raw", ["brand_a", "brand_01"])
def test_brand_arg_accepts_valid_ids(raw: str) -> None:
    assert replay._brand_arg(raw) == raw


@pytest.mark.parametrize(
    "raw", ["", "acme", "brand_", "Brand_a", "brand_a b", "brand_x;y", "../x", "acme\n", "a/b"]
)
def test_brand_arg_rejects_invalid_ids(raw: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="not a valid brand_id"):
        replay._brand_arg(raw)


def test_only_orders_are_replayable_and_unknown_tables_are_rejected() -> None:
    assert {"orders": "raw_orders"} == replay.REPLAY_SOURCES
    with pytest.raises(SystemExit):
        _parse("--table", "customers")
    with pytest.raises(SystemExit):
        _parse()  # --table is required


def test_replay_error_is_a_runtime_error() -> None:
    assert issubclass(ReplayError, RuntimeError)


# ---- run() --------------------------------------------------------------------------------------


class Env:
    def __init__(self) -> None:
        self.table_exists: dict[str, bool] = {}
        self.delta_exists: dict[str, bool] = {}
        self.verified: list[str] = []
        self.calls: list[dict[str, Any]] = []


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> Env:
    e = Env()
    monkeypatch.setattr(
        replay, "table_exists", lambda _s, t: e.table_exists.get(t.location.split("/")[-1], True)
    )
    monkeypatch.setattr(
        replay,
        "delta_exists_at_location",
        lambda _s, t: e.delta_exists.get(t.location.split("/")[-1], True),
    )
    monkeypatch.setattr(
        replay, "verify_location", lambda _s, spec, _t: e.verified.append(spec.name)
    )
    monkeypatch.setattr(replay, "read_delta", lambda _s, t: f"DF:{t.location}")

    def fake_replay(_spark: Any, **kw: Any) -> dict[str, int]:
        e.calls.append(kw)
        return {"candidates": 2, "replayed": 2}

    monkeypatch.setattr(replay, "replay_orders", fake_replay)

    class Spark:
        class read:
            @staticmethod
            def format(_f: str) -> Any:
                class R:
                    @staticmethod
                    def load(path: str) -> str:
                        return f"DLQ:{path}"

                return R

    e.spark = Spark  # type: ignore[attr-defined]
    return e


def _run(env: Env, *argv: str) -> Any:
    return ReplayJob().run(env.spark, _parse("--table", "orders", *argv), _ctx())  # type: ignore[attr-defined]


def test_run_passes_locations_limits_and_brand_to_replay_orders(env: Env) -> None:
    out = _run(env, "--max-replays", "5", "--brand", "brand_a")
    kw = env.calls[0]
    assert kw["bronze"] == "DF:s3a://lakehouse/bronze/raw_orders"
    assert kw["products"] == "DF:s3a://lakehouse/bronze/raw_products"
    assert kw["customers"] == "DF:s3a://lakehouse/silver/customers"
    assert kw["dlq"] == "DLQ:s3a://lakehouse/dead_letter/dlq_orders"
    assert kw["dlq_path"] == "s3a://lakehouse/dead_letter/dlq_orders"
    assert kw["orders_path"] == "s3a://lakehouse/silver/orders"
    assert kw["items_path"] == "s3a://lakehouse/silver/order_items"
    assert (kw["max_replays"], kw["brand"]) == (5, "brand_a")
    assert out == {"table": "orders", "max_replays": 5, "counts": {"candidates": 2, "replayed": 2}}
    assert env.verified == ["raw_orders", "raw_products", "customers"]


def test_run_namespace_scopes_every_location(env: Env) -> None:
    _run(env, "--namespace", "ns1")
    kw = env.calls[0]
    for key in ("dlq_path", "orders_path", "items_path"):
        assert "/ns1/" in kw[key]
    assert kw["brand"] is None
    assert kw["max_replays"] == 3


@pytest.mark.parametrize("missing", ["raw_orders", "raw_products", "customers"])
def test_run_fails_fast_when_a_read_table_is_missing(env: Env, missing: str) -> None:
    env.table_exists[missing] = False
    with pytest.raises(ReplayError, match="does not exist"):
        _run(env)
    assert env.calls == []


@pytest.mark.parametrize("missing", ["orders", "order_items", "dlq_orders"])
def test_run_fails_fast_when_a_write_target_is_missing(env: Env, missing: str) -> None:
    env.delta_exists[missing] = False
    with pytest.raises(ReplayError, match="run silver_orders first"):
        _run(env)
    assert env.calls == []


def test_run_rejects_an_unsafe_namespace_before_any_probe(env: Env) -> None:
    with pytest.raises(ValueError, match="invalid path part"):
        _run(env, "--namespace", "..")
    assert env.verified == []


# ---- replay_orders ------------------------------------------------------------------------------


class Df(Perm):
    """Chainable DataFrame stand-in: returns itself, ``join`` returns ``joined`` when set."""

    def __init__(self, count: int = 0, rows: list[dict[str, Any]] | None = None) -> None:
        super().__init__("df")
        self.n = count
        self.rows = rows or []
        self.joined: Df | None = None
        self.unpersisted = 0

    def count(self) -> int:  # type: ignore[override]
        return self.n

    def collect(self) -> list[dict[str, Any]]:  # type: ignore[override]
        return self.rows

    def _self(self, *_a: Any, **_k: Any) -> Df:
        return self

    alias = persist = select = dropDuplicates = withColumn = groupBy = agg = _self

    def join(self, *_a: Any, **_k: Any) -> Df:
        return self.joined or self

    def unpersist(self) -> None:
        self.unpersisted += 1


class Replay:
    def __init__(
        self,
        *,
        candidates: int,
        bronze_found: int,
        recovered: int,
        orphan: int,
        status: dict[str, int],
        dlq_updated: int,
    ) -> None:
        self.cand = Df(candidates)
        self.outcomes = Df(rows=[{"_replay_status": k, "n": v} for k, v in status.items()])
        self.cand.joined = self.outcomes
        self.bronze = Df()
        self.bronze_rows = Df(bronze_found)
        self.bronze.joined = self.bronze_rows
        self.recovered = Df(recovered)
        self.orphan = Df(orphan)
        self.dlq_updated = dlq_updated
        self.merged: list[Any] = []
        self.selected: list[tuple[Any, ...]] = []

    def patch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_spark.install(monkeypatch)

        def select(_dlq: Any, source: str, max_replays: int, brand: Any) -> Df:
            self.selected.append((source, max_replays, brand))
            return self.cand

        monkeypatch.setattr(replay, "select_candidates", select)
        monkeypatch.setattr(replay, "recheck_fk", lambda *_a: (self.recovered, self.orphan))

        def merge(_s: Any, _o: str, _i: str, rows: Any) -> tuple[int, int]:
            self.merged.append(rows)
            return 4, 9

        monkeypatch.setattr(replay, "merge_orders", merge)
        monkeypatch.setattr(replay, "set_replay_outcome", lambda *_a: self.dlq_updated)

    def run(self, brand: str | None = None) -> dict[str, int]:
        return replay.replay_orders(
            None,  # type: ignore[arg-type]
            bronze=self.bronze,  # type: ignore[arg-type]
            products=Perm("p"),  # type: ignore[arg-type]
            customers=Perm("c"),  # type: ignore[arg-type]
            dlq=Perm("d"),  # type: ignore[arg-type]
            dlq_path="d",
            orders_path="o",
            items_path="i",
            max_replays=3,
            brand=brand,
        )


def test_replay_orders_without_candidates_is_all_zero_and_touches_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    r = Replay(candidates=0, bronze_found=0, recovered=0, orphan=0, status={}, dlq_updated=0)
    r.patch(monkeypatch)
    counts = r.run()
    assert set(counts.values()) == {0}
    assert len(counts) == 9
    assert r.merged == []
    assert r.cand.unpersisted == 1  # released even on the early return


def test_replay_orders_reconciles_and_reports_every_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    r = Replay(
        candidates=3,
        bronze_found=3,
        recovered=2,
        orphan=1,
        status={"REPLAYED": 2, "FAILED_AGAIN": 1},
        dlq_updated=3,
    )
    r.patch(monkeypatch)
    assert r.run("brand_a") == {
        "candidates": 3,
        "bronze_found": 3,
        "recovered": 2,
        "still_orphan": 1,
        "orders_inserted": 4,
        "items_inserted": 9,
        "replayed": 2,
        "failed_again": 1,
        "dlq_updated": 3,
    }
    assert r.selected == [("bronze.raw_orders", 3, "brand_a")]
    assert len(r.merged) == 1  # Silver is written once, before the DLQ update


def test_replay_orders_skips_the_silver_merge_when_nothing_recovered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    r = Replay(
        candidates=1,
        bronze_found=1,
        recovered=0,
        orphan=1,
        status={"FAILED_AGAIN": 1},
        dlq_updated=1,
    )
    r.patch(monkeypatch)
    counts = r.run()
    assert r.merged == []
    assert (counts["orders_inserted"], counts["items_inserted"]) == (0, 0)
    assert counts["failed_again"] == 1


@pytest.mark.parametrize(
    ("kw", "fragment"),
    [
        (  # outcome rows do not add up to the candidates
            {"status": {"REPLAYED": 1}, "dlq_updated": 2},
            "outcome rows do not add up",
        ),
        (  # DLQ MERGE updated a different number of rows than candidates
            {"status": {"REPLAYED": 1, "FAILED_AGAIN": 1}, "dlq_updated": 1},
            "DLQ MERGE updated 1",
        ),
        (  # recovered + still_orphan != bronze_found (a lost Bronze row)
            {"status": {"REPLAYED": 1, "FAILED_AGAIN": 1}, "dlq_updated": 2, "bronze_found": 5},
            "recovered + still_orphan (2) != bronze_found (5)",
        ),
    ],
)
def test_replay_orders_raises_when_a_reconciliation_check_fails(
    monkeypatch: pytest.MonkeyPatch, kw: dict[str, Any], fragment: str
) -> None:
    base: dict[str, Any] = {
        "candidates": 2,
        "bronze_found": 2,
        "recovered": 1,
        "orphan": 1,
        "status": {"REPLAYED": 1, "FAILED_AGAIN": 1},
        "dlq_updated": 2,
    }
    r = Replay(**{**base, **kw})
    r.patch(monkeypatch)
    with pytest.raises(ReplayError, match="candidates|outcome|bronze_found") as exc:
        r.run()
    assert fragment in str(exc.value)
