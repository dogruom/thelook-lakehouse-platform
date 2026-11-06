# Day Close Reports

> **Document Version**: 1.0
> **Last Updated**: 2026-11-06
> **Purpose**: one readable report per planned day (newest first), written by the `checkpoint` skill.
> Raw evidence lives in [PROGRESS.md](PROGRESS.md), the per-session file trace in
> [SESSION_LOG.md](SESSION_LOG.md), the reasons in [DECISION_LOG.md](DECISION_LOG.md).
> Days W1.D1–W1.D3 closed before this log existed; their evidence is in PROGRESS and SESSION_LOG.

---

## W1.D4 · Customer CDC generator and ground-truth manifest — closed 2026-11-06

**Executive summary.** The project can now generate deterministic, multi-tenant customer CDC events
with an exactly known set of defects, and a manifest that states the correct outcome for every event.
This is the measuring stick for the Silver MERGE in Week 2: pipeline output will be compared with it
by equality, not by eyeballing.

| Task | Status | Evidence |
|---|---|---|
| W1.D4.1 CDC lifecycle engine | ✅ | `848d9c8`; `pytest tests/unit/generator/test_cdc.py -q -k lifecycle` → 2 passed |
| W1.D4.2 Exact-count defects + manifest | ✅ | `848d9c8`; `-k manifest` → 2 passed; full `test_cdc.py` + `test_manifest.py` → 161 passed |
| W1.D4.3 Determinism | ✅ | `848d9c8`; `-k determinism` → 6 passed (incl. subprocess runs with different `PYTHONHASHSEED`); CLI seed 42 twice → identical hashes |
| W1.D2 carry-over: compose policy test | ✅ | `f72c2aa`; `pytest tests/unit/infra -q` → 48 passed |

- **Example run** (`--brands 3 --events 1000 --seed 42`): accepted 880 + DLQ 70 + duplicates 20 + stale 50 + late 0 = emitted 1020 = bronze.
- **Gates**: unit 709 passed · `lakehouse.generator` coverage 98 % · ruff / format / mypy (21 files) clean · import-linter 2 kept.
- **Reviews and audit**: design review CHANGES REQUIRED → 6 must-fix applied (D-022…D-027) · code review APPROVE WITH NITS → 1 MAJOR doc fix (D-029) + nits applied · `evidence-auditor`: all tasks OK, no MISMATCH.
- **Decisions**: D-021…D-033 ([DECISION_LOG](DECISION_LOG.md)) — key ones: gate order DLQ → dedup → MERGE (D-022); outcome precedence (D-023, D-029); batch-aware `classify_outcomes` (D-024); exact counts or error (D-026); fictional phones only (D-031); standing merge approval (D-032).
- **Not verified / known limitations**: `late = 0` by construction — CDC lateness and delays between intermediate sequences are not generated (D-030, extend before SCD2 tests); byte-identity verified only with the pinned Faker on Python 3.11; Spark parsing of money strings still unverified (W2).
- **Carry-overs**: none.
- **Risks**: `confluent-kafka` wheel on Windows untested (D5); 7.6 GB Docker memory for the D5 integration test.
- **Learning queued (batched, W1 gate)**: CDC lifecycle + sequencing, test oracle / reconciliation, deterministic generation; 6 questions; glossary + 4 entries.
- **Next day**: W1.D5 — clickstream generator, keyed idempotent producer, produce → consume round trip (not started, by owner instruction).
- **Merge status**: see the line below (filled after CI).
