# Session Log

> **Document Version**: 1.0
> **Last Updated**: 2026-11-06
> **Purpose**: append-only, per-session trace of what changed and where — the backward-looking
> complement to [PROGRESS.md](PROGRESS.md) `Current State` (which is overwritten each session).

**How the documents reference each other**

```
DELIVERY_PLAN day card (W?.D?) ──► PROGRESS task row + Evidence (commit hash, command output)
        │                                   │
        ▼                                   ▼
DECISION_LOG (D-NNN, why) ◄──── SESSION_LOG entry (commits, files, D-IDs, learning) ────► git / PR
        │                                   │
        ▼                                   ▼
ADR (architecture-level)          PORTFOLIO_NOTES (verified capability → evidence)
```

**Rules**: one entry per session, newest first, written by the `session-handoff` skill from
`git log --name-status` (facts from git, not from chat memory). Entries are never edited later except
to add a missing link. File lines are clickable repo-relative links.

---

## 2026-11-11 · W2.D12 M1 review, demo and release

- **Tasks**: W2.D12.1–D12.2 ✅, D12.3 🔄 (owner tag) ([PROGRESS D12](PROGRESS.md))
- **Commits / PR**: `0ba2cc2` feat(m1) · `782ec39` fix(m1) review nits · docs close commit · PR to `main` under D-032
- **Decisions**: D-062 … D-068 ([DECISION_LOG](DECISION_LOG.md))
- **Files**: [m1-streaming-demo.md](runbooks/m1-streaming-demo.md), [README.md](../README.md), [rules.py](../src/lakehouse/quality/rules.py), [brand_key.py](../src/lakehouse/common/brand_key.py), [smoke_test.py](../scripts/smoke_test.py), [Dockerfile](../docker/spark/Dockerfile), [ci.yml](../.github/workflows/ci.yml), [conftest.py](../tests/integration/conftest.py), [DATA_CONTRACTS.md](DATA_CONTRACTS.md), [VERIFICATION_REGISTER.md](VERIFICATION_REGISTER.md); tests [test_rules.py](../tests/spark/test_rules.py), [test_brand_key.py](../tests/spark/test_brand_key.py), [test_smoke_wait.py](../tests/unit/scripts/test_smoke_wait.py), [test_bronze_report_args.py](../tests/unit/ops/test_bronze_report_args.py), [test_it_purge_glob.py](../tests/integration/test_it_purge_glob.py)
- **Agents**: reviewer milestone (Sonnet) → implementer ×2 (Sonnet) → reviewer code (Sonnet) → evidence-auditor (Haiku)
- **Finding**: the first smoke run after `up -d` raced the init jobs; the integration purge glob `*/it_*` would also have matched any future `it_*` table
- **Learning**: M1 learning gate round 1 asked; answers pending (V-11)

## 2026-11-09 · W2.D10 Validation gate and DLQ

- **Tasks**: W2.D10.1–D10.3 ✅ ([PROGRESS D10](PROGRESS.md))
- **Commits / PR**: `5f64ad4` feat(quality) · `c91e03f` fix(quality) · day-close docs commit · PR to `main` under D-032
- **Decisions**: D-056 … D-059 ([DECISION_LOG](DECISION_LOG.md)); TASKS Q-001 queued
- **Files**: [rules.py](../src/lakehouse/quality/rules.py), [dlq_gate.py](../src/lakehouse/quality/jobs/dlq_gate.py), [dlq_report.py](../src/lakehouse/ops/jobs/dlq_report.py), [DATA_FLOW.md](DATA_FLOW.md), [VERIFICATION_REGISTER.md](VERIFICATION_REGISTER.md), [TASKS.md](TASKS.md); tests [test_rules.py](../tests/spark/test_rules.py), [test_dlq_reconciliation.py](../tests/integration/test_dlq_reconciliation.py), [test_rules_fixture.py](../tests/unit/quality/test_rules_fixture.py), [test_dlq_gate_checks.py](../tests/unit/quality/test_dlq_gate_checks.py), [test_dlq_report_masking.py](../tests/unit/ops/test_dlq_report_masking.py), [tests/fixtures/rules_diff/](../tests/fixtures/rules_diff/)
- **Agents**: reviewer design (Sonnet) → implementer ‖ test-author (Sonnet) → reviewer code + re-check (Sonnet) → evidence-auditor (Haiku)
- **Finding**: the first reconciliation check compared the MERGE metric with itself (vacuous); replaced by independent post-write checks with a negative unit test
- **Learning**: validation as data, error taxonomies, quarantine patterns, insert-only MERGE, null-safe join; tutor gate queued

## 2026-11-09 · W2.D9 Restart and checkpoint loss

- **Tasks**: W2.D9.1–D9.2 ✅ ([PROGRESS D9](PROGRESS.md))
- **Commits / PR**: `c23e322` feat(ingestion) · day-close docs commit · PR to `main` under D-032
- **Decisions**: D-049 … D-055 ([DECISION_LOG](DECISION_LOG.md)); [ADR-0006](adr/0006-delivery-semantics-restart-and-checkpoint-loss.md) draft
- **Files**: [bronze.py](../src/lakehouse/ingestion/bronze.py), [bronze_stream.py](../src/lakehouse/ingestion/jobs/bronze_stream.py), [bronze_stream_fault.py](../src/lakehouse/ingestion/jobs/bronze_stream_fault.py), [bronze_report.py](../src/lakehouse/ops/jobs/bronze_report.py), [submit_job.py](../scripts/submit_job.py), [spark.py](../src/lakehouse/common/spark.py), [jobs.py](../src/lakehouse/common/jobs.py), [docker-compose.yml](../docker-compose.yml), [VERIFICATION_REGISTER.md](VERIFICATION_REGISTER.md), [LOCAL_VS_CLOUD.md](LOCAL_VS_CLOUD.md); tests [test_stream_restart.py](../tests/integration/test_stream_restart.py), [test_submit_guards.py](../tests/integration/test_submit_guards.py), [spark_jobs.py](../tests/integration/spark_jobs.py), [test_bronze_rewound.py](../tests/spark/test_bronze_rewound.py), [tests/unit/scripts/](../tests/unit/scripts/)
- **Agents**: reviewer design (Sonnet) → implementer ‖ test-author (Sonnet) → reviewer code (Sonnet) → evidence-auditor (Haiku)
- **Finding**: partial checkpoint loss caused silent Bronze data loss; now refused (D-050)
- **Learning**: checkpoint anatomy, Delta idempotent writes and their blind spot, `failOnDataLoss`, fault injection; tutor gate queued

## 2026-11-09 · W2.D8 Kafka → Bronze streaming

- **Tasks**: W2.D8.1–D8.3 ✅ ([PROGRESS D8](PROGRESS.md))
- **Commits / PR**: `b67f155` feat(ingestion) · `78f3d60` test(ingestion) · `0792283` fix(logging) · day-close docs commit · PR to `main` under D-032
- **Decisions**: D-047 Bronze columns, idempotent batch key, parse codes · D-048 exception text in logs ([DECISION_LOG](DECISION_LOG.md))
- **Files**: [bronze.py](../src/lakehouse/ingestion/bronze.py), [bronze_stream.py](../src/lakehouse/ingestion/jobs/bronze_stream.py), [bronze_report.py](../src/lakehouse/ops/jobs/bronze_report.py), [spark.py](../src/lakehouse/common/spark.py), [logging.py](../src/lakehouse/common/logging.py), [.env.example](../.env.example), [DATA_CONTRACTS.md](DATA_CONTRACTS.md) §2.0/§3.1, [ADR-0002](adr/0002-event-interface-kafka-envelope.md), [DATA_FLOW.md](DATA_FLOW.md); tests [test_bronze_parse.py](../tests/spark/test_bronze_parse.py), [test_bronze_stream.py](../tests/integration/test_bronze_stream.py), [tests/unit/ingestion/](../tests/unit/ingestion/), [test_logging.py](../tests/unit/common/test_logging.py)
- **Agents**: reviewer design (Sonnet) → implementer ‖ test-author (Sonnet) → reviewer code (Sonnet) → evidence-auditor (Haiku)
- **Learning**: Kafka source options, `foreachBatch` vs sink, checkpoints, Delta `txnAppId`; tutor gate queued

## 2026-11-09 · W2.D7 Spark job framework

- **Tasks**: W2.D7.1–D7.3 ✅ ([PROGRESS D7](PROGRESS.md))
- **Commits / PR**: `306701c` docs(learning) · `e82f91b` feat(spark) · `5836709` test(spark) · `80a6e63` feat(spark) · `44b5ea7` fix(spark) · day-close docs commit · PR to `main` under D-032
- **Decisions**: D-043 explanation style · D-044 job framework and packaging · D-045 strings until Silver ([DECISION_LOG](DECISION_LOG.md))
- **Files**: [spark.py](../src/lakehouse/common/spark.py), [jobs.py](../src/lakehouse/common/jobs.py), [spark_schema.py](../src/lakehouse/contracts/spark_schema.py), [hello_delta.py](../src/lakehouse/ops/jobs/hello_delta.py), [submit_job.py](../scripts/submit_job.py), [job_entry.py](../scripts/spark/job_entry.py), [docker-compose.yml](../docker-compose.yml); tests in [tests/spark/](../tests/spark/), [tests/unit/common/](../tests/unit/common/), [tests/unit/ops/](../tests/unit/ops/), [tests/unit/scripts/](../tests/unit/scripts/)
- **Agents**: implementer (Sonnet) → reviewer code (Sonnet) → evidence-auditor (Haiku)
- **Incident**: stale `COMPOSE_PROJECT_NAME` removed the legacy `thelook_spark_master` container ([DAY_CLOSE_REPORTS](DAY_CLOSE_REPORTS.md))
- **Learning**: driver vs executor code shipping, config layering, run IDs, `from_json` leniency; tutor gate queued

## 2026-11-06 · W1.D5 producer + W1.D6 week-1 consolidation

- **Tasks**: W1.D5.1–D5.3 ✅ · W1.D6.2–D6.3 ✅ · W1.D6.1 🔄 (CI link) ([PROGRESS D5/D6](PROGRESS.md))
- **Commits / PR**: `03d02b4` feat(generator) · `12ac263` docs (ADR-0002, runbook, CI gate) · checkpoint docs commit · PR to `main` under D-032
- **Decisions**: D-034 clickstream constants · D-035 malformed manifest identity · D-036 producer settings · D-037 per-package coverage · D-038 ADR-0002 ([DECISION_LOG](DECISION_LOG.md))
- **Files**: [clickstream.py](../src/lakehouse/generator/clickstream.py), [producer.py](../src/lakehouse/generator/producer.py), [messages.py](../src/lakehouse/generator/messages.py), [__main__.py](../src/lakehouse/generator/__main__.py), [io.py](../src/lakehouse/generator/io.py); tests in [tests/unit/generator/](../tests/unit/generator/) and [test_producer_roundtrip.py](../tests/integration/test_producer_roundtrip.py); [ADR-0002](adr/0002-event-interface-kafka-envelope.md), [week1-demo.md](runbooks/week1-demo.md), [ci.yml](../.github/workflows/ci.yml), [DATA_CONTRACTS.md](DATA_CONTRACTS.md) §2.0/§2.5.1
- **Agents**: implementer ‖ test-author ‖ docs drafter (Sonnet) → reviewer code (Sonnet) → evidence-auditor (Haiku)
- **Learning**: key-hash partitioning, idempotent producer vs transactions, delivery callbacks, consumer lag; W1 learning summary + senior questions delivered in chat

## 2026-11-06 · W1.D4 customer CDC generator + ground-truth manifest

- **Tasks**: W1.D4.1–D4.3 ✅ ([PROGRESS D4](PROGRESS.md)) · W1.D2 follow-up compose policy test ✅
- **Commits / PR**: `f72c2aa` test(infra) · `848d9c8` feat(generator) · day-close docs commit · PR pending owner go
- **Decisions**: D-021 glossary + senior Q&A (owner) · D-022…D-027 oracle semantics, feasibility, determinism (design review) · D-028 new config · D-029 DUPLICATE vs Bronze history · D-030 known limitation · D-031 fictional phones ([DECISION_LOG](DECISION_LOG.md))
- **Files**
  - [src/lakehouse/generator/cdc.py](../src/lakehouse/generator/cdc.py) — new: lifecycle engine, exact defects, `InfeasibleDefectPlan`
  - [src/lakehouse/generator/manifest.py](../src/lakehouse/generator/manifest.py) — new: `Outcome`, `CdcDefect`, `classify_outcomes`, `Manifest`
  - [src/lakehouse/generator/determinism.py](../src/lakehouse/generator/determinism.py) — new: string-seeded sub-streams, sim clock, per-locale Faker
  - [src/lakehouse/generator/io.py](../src/lakehouse/generator/io.py), [\_\_main\_\_.py](../src/lakehouse/generator/__main__.py) — new: canonical NDJSON writer, `cdc --dry-run` CLI
  - [src/lakehouse/generator/config.py](../src/lakehouse/generator/config.py) — edited: `cdc_max_updates`, `cdc_delete_rate`, `sim_start`
  - [tests/unit/generator/](../tests/unit/generator/) — new: `_reference.py`, `test_cdc.py`, `test_manifest.py`, `test_determinism.py`, `test_cli.py`
  - [tests/unit/infra/test_compose_policy.py](../tests/unit/infra/test_compose_policy.py) — new
  - [DATA_CONTRACTS.md](DATA_CONTRACTS.md) v7.3 (§2.1.1 manifest, §4.1 gate order + precedence) · [DECISION_LOG.md](DECISION_LOG.md) · [PROGRESS.md](PROGRESS.md) · [PORTFOLIO_NOTES.md](PORTFOLIO_NOTES.md)
  - `requirements/base.txt` (faker 40.44.0), `requirements/dev.txt` (pyyaml 6.0.3), `pyproject.toml`, `.env.example`, [CLAUDE.md](../CLAUDE.md), tutor skill
- **Verification**: unit 709 passed · generator coverage 98 % · CLI seed 42 twice → identical hashes · reconciliation 880+70+20+50+0 = 1020
- **Learning (batched)**: 3 concept notes + 6 questions for D4; glossary 25 → 29 entries (+ data reconciliation, idempotent MERGE, largest-remainder apportionment, test oracle)
- **Portfolio**: + deterministic CDC generator with ground-truth oracle

## 2026-11-06 · W1.D3 data contracts (+ W1.D2 close earlier the same day)

- **Tasks**: W1.D3.1–D3.3 ✅ ([PROGRESS D3](PROGRESS.md)) · W1.D2 follow-up compose policy test → D4
- **Commits / PR**: `6889317` feat(contracts) · `2eaa8d5` docs(progress) · [PR #3](https://github.com/dogruom/thelook-lakehouse-platform/pull/3) · `ba79036` docs(traceability) · CI green (run 42141296077) · merged to `main` as `6b0a0fa`
- **Decisions**: D-001…D-018 ([DECISION_LOG](DECISION_LOG.md)); owner-approved: D-001 batched tutor, D-002 CDC duplicate rate, D-014 reply format
- **Files**
  - [src/lakehouse/contracts/models.py](../src/lakehouse/contracts/models.py) — new: envelope + 4 contract models, Money
  - [src/lakehouse/contracts/schema.py](../src/lakehouse/contracts/schema.py) — new: `build_schema`, `classify_change`
  - [src/lakehouse/contracts/errors.py](../src/lakehouse/contracts/errors.py) — new: `safe_errors`
  - [src/lakehouse/contracts/kafka.py](../src/lakehouse/contracts/kafka.py) — new: key/header helpers
  - [src/lakehouse/contracts/enums.py](../src/lakehouse/contracts/enums.py), [export.py](../src/lakehouse/contracts/export.py), [schemas/](../src/lakehouse/contracts/schemas/) — new
  - [src/lakehouse/generator/config.py](../src/lakehouse/generator/config.py) — rewritten: GeneratorConfig v2
  - [tests/unit/contracts/](../tests/unit/contracts/), [tests/unit/generator/test_config.py](../tests/unit/generator/test_config.py) — new / rewritten
  - [DATA_CONTRACTS.md](DATA_CONTRACTS.md) v7.2 · [DECISION_LOG.md](DECISION_LOG.md) new · [PORTFOLIO_NOTES.md](PORTFOLIO_NOTES.md) · [PROGRESS.md](PROGRESS.md)
  - [CLAUDE.md](../CLAUDE.md), tutor / checkpoint / session-handoff skills, `.gitattributes`, `.env.example`, `pyproject.toml`
- **Verification**: unit 470 passed · contracts coverage 96 % · negative snapshot test failed as intended · pre-commit clean
- **Learning (batched, for the W1 gate)**: 3 concept notes + 6 questions for D3; owner questions: `models.py`, `schema.py` deep dive
- **Portfolio**: + versioned contracts with breaking-change gate; + schema-evolution talking point
