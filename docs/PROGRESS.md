# Progress Tracker — Tasks, Statuses and Evidence

> **Document Version**: 9.0
> **Last Updated**: 2026-11-05 (v9.0: rebuilt to match [DELIVERY_PLAN.md](DELIVERY_PLAN.md) v8 — the v8 day structure contradicted the milestone sequence; checkpoint audit added)
> **Status Conventions**: ✅ Verified (commit hash + verification output or CI link) · 🔄 In progress / implemented without evidence · ⏳ Not started · ❌ Blocked
> **Tiers**: 🟩 Core · 🟦 Extended · 🟨 Stretch — see [PROJECT_CHARTER.md §1](PROJECT_CHARTER.md)
> **Active**: **W1 closed** (D5 3/3 ✅, D6 3/3 ✅; merged `3da43d1`) → next **W2.D7**

> [!IMPORTANT]
> **Evidence rule**: ✅ only when the Evidence line names a commit hash **and** the output of the task's verification command (or a CI run link). Files existing is not evidence. Checkpoints are run with the `checkpoint` skill (day / week / milestone) — protocol in DELIVERY_PLAN §1.

---

## Current State

- **Updated**: 2026-11-06 (Week 1 closed; evidence audit: no MISMATCH) · **Branch**: `main` — W1 merged (PR #5 → `3da43d1`; week-gate follow-ups PR #6 → `0c283c5`, CI green); the PR #6 merge line in DAY_CLOSE_REPORTS is edited on local `main`, uncommitted → commit with the W2.D7 branch
- **Last delivered**: W1.D5.1–D5.3 ✅ — clickstream generator + manifest, keyed idempotent producer + `produce` CLI, live round-trip test; W1.D6.2–D6.3 ✅ — ADR-0002, week-1 runbook, week checkpoint.
- **Unfinished**: tutor week-gate answers from the owner (learning summary + senior questions delivered in chat 2026-11-06).
- **Tests & results**: unit 797 passed; coverage total 95.6 % / generator 95 % / contracts 96 %; integration 3 passed (live broker); smoke 24/24; `produce` 6220/6220 delivered; ruff/mypy/pre-commit clean; import-linter 2 kept.
- **Owner action**: study `.claude/learning/W1_REHBER.md` + the three W1 artifacts (index `.claude/learning/artifacts/INDEX.md`), then take the W1 tutor re-test. Stack stopped (`docker compose --profile core stop`, verified 0 running).
- **Decisions to remember** (full list: [DECISION_LOG.md](DECISION_LOG.md)): Spark 3.5.9/Delta 3.3.3/Python 3.11; Glue 5.0 (ADR-0011); Databricks Free Edition W6 (ADR-0012); budget ≤ USD 50; contracts strict producer / tolerant consumer; Silver gate order DLQ → dedup → MERGE (D-022); outcome precedence DLQ → DUPLICATE (vs valid Bronze history) → NOOP → ACCEPT (D-023/D-029).
- **Risks / blockers**: 7.6 GB Docker memory; host Postgres on 5432 (container 15432); Spark string→DECIMAL parsing unverified (W2); generator does not produce intermediate-late CDC or `late_beyond_window` (D-030, extend before SCD2 tests); broker failing mid-run untested (fully unreachable broker is tested); Kafka container got SIGKILL (exit 137) on one stop — default 10 s grace may be short for a clean KRaft shutdown (check in W2 with `stop_grace_period`).
- **Way of working**: W1 tutor batched (D6 learning session); every decision in DECISION_LOG; per-session trace in [SESSION_LOG.md](SESSION_LOG.md); replies with executive/technical summary, file links, senior Q&A; personal glossary `.claude/learning/GLOSSARY.md`.
- **Next task ("Start today's task")**: **W2.D7** — Spark job framework (DELIVERY_PLAN D7 card); Spark tests run inside the image (`tools` profile).
- **Next mode**: coordinator plans; `reviewer` design review → `implementer` ‖ `test-author` → code review. Unattended option (D-041): queue in [TASKS.md](TASKS.md), runner `scripts/autonomous/run_queue.ps1`; pilot = W2.D7.1 with `--max-iterations 1` while the owner is present.
- **Autonomous run**: not started (setup on `chore/autonomous-run`, D-041/D-042; pre-flight verified: refuses to start with uncommitted guardrail files)

---

## Checkpoint audit (2026-11-05)

Question: were the planned daily/weekly checkpoints designed, documented, implemented and verified?

| # | Item | Finding | Class |
|---|---|---|---|
| A1 | Daily verification commands (v8 days 1–36) | Documented per task; none executed except Day 1 partially | Documented, not implemented |
| A2 | Daily Definition of Done | Present for 9 of 36 days (2, 3, 4, 5, 6, 10, 12, 17 and weeks 5–6); missing elsewhere | Partially documented |
| A3 | Weekly integration / regression tests and acceptance | Not defined for weeks 1–4; only milestone criteria in DELIVERY_PLAN v7 | Designed in chat, not documented |
| A4 | Milestone demos | Described in DELIVERY_PLAN v7; no runbook exists | Documented, not implemented |
| A5 | Day sequence | PROGRESS v8 (CI in W5, benchmarks in W4, Airflow in W5) contradicted DELIVERY_PLAN v7 (CI in M1, benchmarks stretch, Airflow in M2) | Inconsistent |
| A6 | Docker stack + smoke test (old W1.D1.1) | Implemented; never verified with recorded output; smoke test treats a missing topic as a pass; `customer_cdc` topic is never created; Spark image runs Python 3.8 | Implemented, not verified (and defective) |
| A7 | `GeneratorConfig` (old W1.D1.2) | Implemented; not aligned with contracts; no tests | Implemented, not verified |
| A8 | Documentation reorganisation (old W1.D1.3) | Commit `851a88a`, manual review | Verified (manual) |
| A9 | Checkpoint mechanism itself | Did not exist → now `checkpoint` skill + DELIVERY_PLAN §1 protocol + weekly summaries + this tracker's per-day Evidence lines | Documented now; verified when W1 checkpoint runs |

Result: every day in v9 has a deliverable, a verification command and a Done-when; every week has a regression set, acceptance criteria and a demo (DELIVERY_PLAN §3–§5).

---

## 📊 Macro Status (count of ✅ tasks / planned tasks)

```
M1 (D1–D12)   [███████████░░░░░░░░░] 20 / 37
M2 (D13–D24)  [░░░░░░░░░░░░░░░░░░░░]  0 / 35
M3 (D25–D36)  [░░░░░░░░░░░░░░░░░░░░]  0 / 35
```

## Pre-M1 (completed before this plan)
| Task | Status | Evidence |
|---|---|---|
| Documentation reorganised under `docs/`, aligned with Charter v7 (old W1.D1.3) | ✅ | commit `851a88a`; manual cross-reference review (2026-11-05) |
| Claude Code setup v1 (rules, skills, settings) | ✅ | commit `573b399` |
| Critical review + re-plan v8, Sonnet-only agent setup, guard hook | 🔄 | branch `docs/replan-v8`, uncommitted; hook self-test 34/34; Sonnet-only headless agent run verified — becomes ✅ with its commit |
| Docker stack definition and `smoke_test.py` (old W1.D1.1) | 🔄 → superseded by W1.D2 | defects A6 |
| `GeneratorConfig` v1 (old W1.D1.2) | 🔄 → superseded by W1.D3.2 | A7 |

---

## Milestone 1 — Reliable Streaming Ingestion 🟩

### Week 1

#### D1 · Python baseline and quality gates
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W1.D1.1 | Project venv + packaging | Python 3.11 venv; `pyproject` build backend `setuptools.build_meta`; `pip install -e .[dev]` succeeds | `python -c "import lakehouse, sys; print(sys.version)"` | ✅ |
| W1.D1.2 | Package layout + lint debt + first tests | `src/lakehouse/{common,contracts,generator}`; no `from src.` imports; ruff 0 findings; coverage gate only in CI command; tests for config + logging | `ruff check src tests scripts && mypy src && pytest tests/unit -q` | ✅ |
| W1.D1.3 | Pre-commit + import-linter | hooks: ruff, ruff-format, gitleaks, check-yaml, EOF; import-linter contracts per ADR-0009 | `pre-commit run --all-files && lint-imports` | ✅ |
| W1.D1.4 | CI job 1 | push/PR: ruff, mypy, unit tests + coverage, import-linter, gitleaks; minimal `permissions` | CI run link | ✅ `14a6a20` — CI run 42113731992 success (54 s); earlier run 42111019559 success |

**Evidence** (2026-11-06, commit `00df04e`, branch `feat/w1-baseline`): W1.D1.1 — `import lakehouse` → 0.1.0 on Python 3.11.9; editable install ok. W1.D1.2 — `ruff check` all passed, `ruff format --check` 18 files formatted, `mypy src` no issues in 10 files, `pytest tests/unit -q --cov=lakehouse --cov-fail-under=80` → 10 passed, coverage 100%; 0 `from src.` imports. W1.D1.3 — `pre-commit run --all-files` 10/10 hooks passed (also passed as the commit hook); `lint-imports` 2 kept, 0 broken; negative probe (`common` → `generator` import) broke the contract as expected. Secret scan: `gitleaks git` full history → no leaks (2 reviewed historical local-dev defaults in `.gitleaksignore`). Review: `reviewer` (code, security) APPROVE WITH NITS — both MAJOR items fixed (checksum-verified gitleaks, SHA-pinned actions, no persisted credentials). W1.D1.4 — pending: link of the first green CI run.

#### D2 · Local stack rebuild and hardening
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W1.D2.1 | Spark image per ADR-0007 | Python 3.11, Java 17, Spark 3.5.9, Delta 3.3.3, pinned JARs; `spark-defaults.conf` mounted, no credentials | `docker compose run --rm spark-master python3 --version` → 3.11.x; `spark-submit --version` → 3.5.9 | ✅ `f896e02` |
| W1.D2.2 | Compose profiles + secret hardening | profiles `core/sql/orchestration/tools`; ports on `127.0.0.1`; `${VAR:?}` for every secret; no `:latest`; SeaweedFS identities rendered from env; `bootstrap_env.py` never overwrites; `check_env.py` prints key names only | `docker compose config --quiet`; `python scripts/check_env.py`; `gitleaks detect --no-git -s docker` | ✅ `f896e02` |
| W1.D2.3 | Init scripts aligned | topics `customer_cdc`, `clickstream_events` (no Kafka DLQ); prefixes `landing/ bronze/ silver/ gold/ dead_letter/ ops/ checkpoints/`; DBs `airflow_metadata`, `hive_metastore`; no `crm_dwh`/`snapshots` | `kafka-topics.sh --list`; `aws s3 ls` (in container) | ✅ `f896e02` |
| W1.D2.4 | Strict smoke test v2 | fails on any missing topic/bucket/DB; Delta write + read via s3a in Spark; prints memory per container; host spec recorded | `python scripts/smoke_test.py` → all pass, exit 0 | ✅ `f896e02` |

**Evidence** (2026-11-06, commit `f896e02`, branch `feat/w1-local-stack`): W1.D2.1 — image `crm/spark:3.5.9` built (1.94 GB): Python 3.11.18, OpenJDK 17.0.21, Spark 3.5.9, `delta-spark_2.12-3.3.3`, `hadoop-aws-3.3.4` = `hadoop-client-api-3.3.4`; tarball SHA-512 and JAR SHA-1 verified at build; non-root `spark` user. W1.D2.2 — `check_env.py` → exit 0 ("all keys set", names only); `bootstrap_env.py --add-missing` appended 15 keys to an existing `.env` without printing values; `docker compose config --quiet` replaced by an offline PyYAML parse (profiles, pinned images, `127.0.0.1` ports, no secret fallbacks) because the guard hook blocks `compose config`; gitleaks via pre-commit on all staged files → passed; 38 unit tests for the env scripts. W1.D2.3 — init logs: topics `customer_cdc`, `clickstream_events` created; 7 prefixes ready; DBs verified by the smoke test. W1.D2.4 — `smoke_test.py` → **24/24 checks passed, exit 0** (health ×5, host endpoints ×3, topics ×2, bucket + 7 prefixes, DBs ×2, Delta s3a round trip rows=3, Spark 3.5.9, driver/executor Python 3.11); memory: kafka 326 MiB, spark-master 265 MiB, spark-worker 217 MiB, postgres 60 MiB, seaweedfs 38 MiB of 7.6 GiB; host: Windows 11 (10.0.26200), 8 logical CPUs, Docker 29.1.3. Gates: ruff/format clean, mypy 13 files clean, `pytest tests/unit` 48 passed, import-linter 2 kept. Review: `reviewer` (code, security) APPROVE WITH NITS, 0 blocker/major; MINOR 1 (Fernet comment) and init_postgres wording fixed; MINOR 2–3 documented in [LOCAL_VS_CLOUD.md](LOCAL_VS_CLOUD.md); open follow-ups: static compose policy test (no `:latest`, `127.0.0.1` ports, no secret fallbacks) and an `entrypoint.sh` test → W1.D3 backlog; SHA-256 for JARs and digest-pinned base image (NIT). Deviations: Postgres host port `${POSTGRES_HOST_PORT:-15432}` (native PostgreSQL 18 owns 5432); stack run with `COMPOSE_PROJECT_NAME=crm_lakehouse` set in the shell because the owner's `.env` still carries an old project name.

#### D3 · Data contracts and generator configuration
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W1.D3.1 | `lakehouse.contracts` v1 | envelope + CustomerCdcEvent, ClickstreamEvent, OrderEvent (embedded items), Product; JSON Schemas exported and committed | `pytest tests/unit/contracts -q` | ✅ |
| W1.D3.2 | `GeneratorConfig` v2 | tenants, weights, seed, topics, exact defect counts per DATA_CONTRACTS; no secret defaults; mirrored in `.env.example` | `pytest tests/unit/generator/test_config.py -q` | ✅ |
| W1.D3.3 | Contract tests + snapshot | valid/invalid fixtures; breaking change fails snapshot test (negative test demonstrated) | `pytest tests/unit/contracts -q` | ✅ |

**Evidence** (2026-11-06, commit `6889317`, branch `feat/w1-contracts`):
- D3.1/D3.3: `pytest tests/unit/contracts -q` → 315 passed; `--cov=lakehouse.contracts` → 96 %; `python -m lakehouse.contracts.export` twice → identical hashes.
- D3.3 negative test: `CustomerCdcEvent.first_name` changed to `int` → `test_committed_schema_matches_model[customer_cdc.v1]` FAILED (1 failed, 21 passed); reverted → 459 passed.
- D3.2: `pytest tests/unit/generator/test_config.py -q` → 113 passed (no `s3_*` fields, no secret defaults).
- Gates: `pytest tests/unit -q` → 470 passed; ruff check/format clean; `mypy src` clean (16 files); `lint-imports` 2 kept; `pre-commit run --all-files` all passed.
- Reviews: design review APPROVE WITH CHANGES (6 must-fix applied); code review CHANGES REQUIRED (2 must-fix: nested `classify_change`, PII in `errors()` → `safe_errors`) applied and re-verified. Decisions D-002…D-018 in [DECISION_LOG.md](DECISION_LOG.md).
- Done-when: coverage ≥ 90 % ✔; DATA_CONTRACTS v7.2 vs code field-by-field ✔ by independent code review only (no automated doc↔code check). Day-close audit (`evidence-auditor`): D3.1–D3.3 OK, re-ran contracts 315 / config 113 passed, coverage 96 %. **Carried over to D4**: W1.D2 follow-up static compose policy test (not started).

#### D4 · Customer CDC generator and manifest
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W1.D4.1 | CDC lifecycle engine | first event `I`, `D` terminal; `_cdc_sequence` strictly increasing per `(brand_id, customer_id)` | `pytest tests/unit/generator/test_cdc.py -q -k lifecycle` | ✅ |
| W1.D4.2 | Exact-count defects + manifest | counts equal config exactly; manifest maps every defective `event_id` → defect + expected outcome; defects not in payload | `pytest tests/unit/generator/test_cdc.py -q -k manifest` | ✅ |
| W1.D4.3 | Determinism | same seed + config → byte-identical output (hash) | `pytest tests/unit/generator -q -k determinism` | ✅ |

**Evidence** (2026-11-06, commits `f72c2aa` compose policy test, `848d9c8` generator; branch `feat/w1-cdc-generator`):
- D4.1: `-k lifecycle` → 2 passed; D4.2: `-k manifest` → 2 passed (the `-k` filters select few tests — full files `test_cdc.py` + `test_manifest.py` → 161 passed); D4.3: `-k determinism` → 6 passed (incl. subprocess runs with different `PYTHONHASHSEED`).
- CLI `python -m lakehouse.generator cdc --brands 3 --events 1000 --seed 42 --dry-run` twice → identical file hashes; counts: invalid_email 30, null_email 20, future_signup 20, out_of_order 50, duplicate 20, collision 1 → accepted 880 + dlq 70 + duplicates 20 + stale 50 + late 0 = emitted 1020 = bronze (late is 0 by construction — CDC lateness not generated, D-030). Day-close audit (`evidence-auditor`): D4.1–D4.3 + compose OK, CLI hashes identical, counts match.
- Oracle: independent test reference MERGE agrees with `expected_final`; `classify_outcomes` with batch sizes 1/7/50/all keeps the reconciliation sum and last-ACCEPT = `expected_final`.
- Gates: `pytest tests/unit -q` → 709 passed; `lakehouse.generator` coverage 98 %; ruff/format/mypy (21 files) clean; `lint-imports` 2 kept. Carried-over W1.D2 compose policy test: `pytest tests/unit/infra -q` → 48 passed.
- Reviews: design CHANGES REQUIRED (6 must-fix → D-022…D-027 applied); code APPROVE WITH NITS (1 MAJOR doc wording → D-029; nits applied; D-030 known limitation; D-031 fictional phones).
- Done-when: manifest counts = configured counts exactly ✔; lifecycle + sequence invariants tested ✔.

#### D5 · Clickstream and keyed idempotent producer
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W1.D5.1 | Clickstream model | funnel ordering of frequencies; late (10–60 min), duplicates, null session, malformed payloads at exact counts | `pytest tests/unit/generator/test_clickstream.py -q` | ✅ `03d02b4` |
| W1.D5.2 | Producer | idempotence + `acks=all`; key `{brand_id}:{entity_id}`; headers `brand_id`, `schema_version`, `event_type`; delivery metrics; flush on shutdown | `python -m lakehouse.generator produce --events 5000` | ✅ `03d02b4` |
| W1.D5.3 | Round-trip integration test | ≥ 5,000 click + ≥ 1,000 CDC, ≥ 3 brands; keys/headers asserted; one partition per customer key; lag 0 | `pytest tests/integration/test_producer_roundtrip.py -q` | ✅ `03d02b4` |

**Evidence** (2026-11-06, commit `03d02b4`, branch `feat/w1-producer`):
- D5.1: `pytest tests/unit/generator -q` (incl. `test_clickstream.py`, `test_messages.py`, `test_producer.py`) green inside the full run below; `clickstream --brands 3 --events 5000 --seed 42 --dry-run` twice → identical SHA-256 (`4ddfff4a…` events, `eac6adee…` manifest); counts base 5000, late 500, duplicate 200, null_session 50, malformed_json 25, emitted 5200 (= `defect_count` exactly); funnel 2562 / 1130 / 722 / 459 / 252.
- D5.2: `python -m lakehouse.generator produce --events 5000` (live broker) → `attempted=6220 delivered=6220 failed=0 undelivered=0`, exit 0; per topic customer_cdc 1020 / clickstream_events 5200; per brand alpha 3735 / beta 1864 / gamma 621. Console consumer shows partition, headers `brand_id,schema_version,event_type`, key `{brand_id}:{session_id}`. `confluent-kafka` 2.4.0 Windows wheel works (risk closed).
- D5.3: `pytest tests/integration/test_producer_roundtrip.py -q -rs` → 3 passed, 0 skipped (fresh 6-partition topics; counts, key format, `brand_id` header = key prefix, one partition per key, committed = high watermark and total records = sent, i.e. no retry duplicates). "Lag 0" is shown for the test's own group (every record consumed), not for an independent consumer.
- Gates: `pytest tests/unit -q --cov=lakehouse` → 797 passed, total 95.6 %, generator 95 %, contracts 96 %; ruff/format clean (57 files); mypy 24 files clean; `lint-imports` 2 kept; `pre-commit run --all-files` exit 0; smoke test 24/24.
- Review: `reviewer` (code, data + security) APPROVE WITH NITS, 0 blockers; 3 MAJOR applied (`produce` ValueError → exit 2 + test; flush 65 s > `delivery.timeout.ms`; DATA_CONTRACTS producer row + §2.5.1 manifest); nits applied (ADR wording, runbook prerequisites). Decisions D-034…D-036. Process deviation: no design review before implementation (interface fixed in the task cards); covered by the code review.
- Done-when: round trip green ✔; producer metrics logged with `brand_id` ✔.

#### D6 · Week 1 consolidation
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W1.D6.1 | Coverage + typing gates | ≥ 80% on `generator` + `contracts` in CI; mypy clean; import-linter active | CI run link | ✅ `12ac263` — CI runs 42218324701, 42218357517 success (PR #5, incl. per-package coverage step); merged `3da43d1` |
| W1.D6.2 | ADR-0002 | Kafka vs Kinesis, envelope, keys vs headers, JSON + schema vs registry, retention | file review | ✅ `12ac263` |
| W1.D6.3 | Week-1 demo + checkpoint | demo steps run; week checkpoint report below | `checkpoint week 1` | ✅ `12ac263` |

**Evidence** (2026-11-06, commit `12ac263`): D6.1 — CI step "Coverage gate per core package" added; locally `coverage report --include=*/lakehouse/generator/* --fail-under=80` → 95 %, contracts → 96 %; mypy clean; import-linter 2 kept. D6.2 — [ADR-0002](adr/0002-event-interface-kafka-envelope.md) reviewed by `reviewer` (no exactly-once claim; one wording nit fixed); D-038. D6.3 — [runbooks/week1-demo.md](runbooks/week1-demo.md) with recorded results; every demo step was run individually on 2026-11-06 (not timed as one end-to-end run).

**Week 1 checkpoint** (2026-11-06):
- Regression set: `pytest tests/unit -q` 797 passed · `python scripts/smoke_test.py` 24/24 · `pre-commit run --all-files` exit 0 · `lint-imports` 2 kept · integration round trip 3 passed.
- Acceptance: same seed → byte-identical output (CDC D4, clickstream D5) ✔ · manifest exact (CDC + clickstream) ✔ · ≥ 5,000 clickstream and ≥ 1,000 CDC events for 3 brands round-trip ✔ · per-key partition affinity asserted ✔ · CI job 1 green ✔ (PR #5 runs 42218324701 / 42218357517; merged `3da43d1`).
- Demo: steps 1–6 run end to end from a stopped stack in 105 s (see follow-ups); step 7 = stop.
- Week-gate follow-ups (2026-11-06, branch `fix/w1-week-gate`): **partitioner parity** — librdkafka default CRC32 ≠ Java murmur2 (live probe 157/200 keys on a different partition) → `partitioner=murmur2_random`, round trip asserts partition = Java murmur2 for every record (port validated with Kafka test vectors; probe 0/200) (D-039) · **broker outage** — unreachable broker → 20/20 `failed` via callbacks in 60.6 s, `ok=False` (`test_producer_failure_modes.py`) · **CLI end to end** — `produce` into fresh topics, topic counts = emitted (`test_produce_cli.py`) · accounting invariant on every exit path except a signal in the few bytecodes between enqueue and the counter increment (D-040) · **demo timed from a stopped stack**: 6/6 steps, 105 s · **stop command defect**: plain `docker compose stop` skips profiled services (earlier "stack stopped" claims were wrong) → `docker compose --profile core stop` in CLAUDE.md, rules, runbook. Gates: unit 799 passed, coverage 95.6 %; integration 6 passed. Review: `reviewer` (code, data) APPROVE WITH NITS — nits applied (queue-full unit test, comment, D-log order, runbook wording).
- Carry-overs: D-030 CDC lateness limitation (before SCD2 tests); tutor week gate (batched W1 learning session — summary delivered in chat 2026-11-06, quiz answers by the owner pending).

### Week 2

#### D7 · Spark job framework
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W2.D7.1 | SparkSession factory + env adapter | `local`/`aws` profiles from settings; no credentials in code; run IDs in logs | `pytest tests/spark/test_session.py -q` (in container) | ⏳ |
| W2.D7.2 | `spark-test` runner | pytest runs inside the Spark image; StructType derived from contracts tested | `docker compose --profile tools run --rm spark-test pytest tests/spark -q` | ⏳ |
| W2.D7.3 | Wheel + submit script | `scripts/submit_job.py hello_delta` writes/reads Delta | command output | ⏳ |

**Evidence**: _—_

#### D8 · Kafka → Bronze streaming
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W2.D8.1 | `ingestion.bronze_stream` | both topics; `_raw_value`, key, headers, `_source_ref`, `_ingest_ts`, `_batch_id`, `brand_id`, `_parse_error`; `availableNow`; one checkpoint per query | job run log | ⏳ |
| W2.D8.2 | Parse-function tests | valid, malformed, missing tenant → expected columns | `pytest tests/spark/test_bronze_parse.py -q` | ⏳ |
| W2.D8.3 | Count integration test | Bronze rows = produced messages incl. parse errors | `pytest tests/integration/test_bronze_stream.py -q` | ⏳ |

**Evidence**: _—_

#### D9 · 🔺 Restart and checkpoint loss
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W2.D9.1 | Restart / loss tests | (a) restart: every `(topic, partition, offset)` exactly once in Bronze; (b) checkpoint loss: re-reads observed and counted | `pytest tests/integration/test_stream_restart.py -q` | ⏳ |
| W2.D9.2 | ADR-0006 draft | per-stage semantics with test references; no exactly-once claim | file review | ⏳ |

**Evidence**: _—_

#### D10 · Validation gate and DLQ (stream entities)
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W2.D10.1 | Rules + DLQ writer | rules per contract → `_error_category`; DLQ envelope incl. `_bronze_ref`, `_retryable`, `_replay_status` | job run | ⏳ |
| W2.D10.2 | Rule tests | one positive + one negative per rule | `pytest tests/spark/test_rules.py -q` | ⏳ |
| W2.D10.3 | DLQ reconciliation | DLQ set = manifest DLQ set exactly; no duplicates/late events in DLQ | `pytest tests/integration/test_dlq_reconciliation.py -q` | ⏳ |

**Evidence**: _—_

#### D11 · Catalog and dbt capability spike
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W2.D11.1 | Hive metastore on Postgres | Bronze/DLQ tables registered; jobs use table names | `SHOW TABLES IN bronze` | ⏳ |
| W2.D11.2 | Thrift Server + dbt probe | `dbt debug` ok; probes for incremental merge, composite key, contracts, persist_docs | `dbt build --select spike` | ⏳ |
| W2.D11.3 | Decision recorded | ADR-0004 spike table with observed results; fallback chosen if needed | file review | ⏳ |

**Evidence**: _—_

#### D12 · M1 review and release
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W2.D12.1 | M1 demo runbook | `docs/runbooks/m1-streaming-demo.md` runs from a stopped stack | demo transcript | ⏳ |
| W2.D12.2 | Full regression | unit + spark + integration + smoke green; CI job 1 green on `main` | outputs + CI link | ⏳ |
| W2.D12.3 | Milestone review + tag | reviewer APPROVE, auditor PASS, README evidence links, claim note, `v0.1.0` (owner) | `checkpoint milestone M1` | ⏳ |

**M1 checkpoint**: _not run_ · **Can claim / cannot claim**: _filled at gate_

---

## Milestone 2 — Data Correctness & Analytics Products 🟩

### Week 3
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W3.D13.1 | Orders + products + landing writer | orders embed items; product seed per brand; boto3 writer with retries; manifest per batch; `sales_channel` from batch 10 | `pytest tests/unit/generator/test_landing.py -q` (moto) | ⏳ |
| W3.D13.2 | `ingestion.bronze_batch` | incremental file source; `_source_ref` file + line; `mergeSchema` | job run | ⏳ |
| W3.D13.3 | Batch count test | Bronze rows = landed rows; schema evolution visible | `pytest tests/integration/test_bronze_batch.py -q` | ⏳ |
| W3.D14.1 | `silver_customers` | dedup to max sequence; MERGE guarded by sequence; tombstones; HMAC token; phone mask; `stale_cdc_noop` metric | job run | ⏳ |
| W3.D14.2 | Merge unit tests | I/U/D, stale, duplicate on tiny frames | `pytest tests/spark/test_silver_customers.py -q` | ⏳ |
| W3.D15.1 | 🔺 CDC suite | 6 scenarios pass | `pytest tests/integration/test_cdc_correctness.py -q` | ⏳ |
| W3.D15.2 | PII tests | no raw email/phone in Silver; key from env; key change → token change | `pytest tests/integration/test_pii.py -q` | ⏳ |
| W3.D15.3 | ADR-0004 final | ownership + spike results | file review | ⏳ |
| W3.D16.1 | `silver_customers_history` | rebuild affected keys ordered by sequence | job run | ⏳ |
| W3.D16.2 | SCD2 scenarios | 4 scenarios incl. shuffled = identical hash | `pytest tests/integration/test_scd2_history.py -q` | ⏳ |
| W3.D17.1 | Silver orders + items | explode; dedup; composite FK; orphans → DLQ retryable | `pytest tests/integration/test_silver_orders.py -q` | ⏳ |
| W3.D17.2 | Silver clickstream | windowed dedup; `late_beyond_window` metric | 0 duplicates on `(brand_id, event_id)` | ⏳ |
| W3.D17.3 | DLQ replay | late parent recovered; second replay adds nothing | `pytest tests/integration/test_dlq_replay.py -q` | ⏳ |
| W3.D18.1 | Reconciliation | equation per batch = manifest; metrics in `ops.pipeline_metrics` | `pytest tests/integration/test_reconciliation.py -q` | ⏳ |
| W3.D18.2 | Idempotent re-run + crash | identical Silver content hash; no partial commit | `pytest tests/integration/test_idempotent_rerun.py -q` | ⏳ |
| W3.D18.3 | 🔺 Tenant isolation | per-tenant counts = manifest; joins keyed on `brand_id`; leaky fixture fails; scoped views | `pytest tests/integration/test_tenant_isolation.py -q` | ⏳ |
| W3.D18.4 | ADR-0003, ADR-0005 | written and linked | file review | ⏳ |

**Evidence**: _—_ · **Week 3 checkpoint**: _not run_

### Week 4
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W4.D19.1 | dbt project + staging | sources on Silver; profiles from env | `dbt build --select staging` | ⏳ |
| W4.D19.2 | Tenant-safe tests + CI parse | `relationships_by_brand`, `assert_no_cross_tenant_joins`; `dbt parse` in CI | CI link | ⏳ |
| W4.D20.1 | Dimensions | dim_customers, dim_customers_history, dim_products, dim_date; surrogate keys include `brand_id`; no raw PII | `dbt build --select dims` | ⏳ |
| W4.D20.2 | Facts | fct_orders, fct_order_items, fct_sessions; composite relationship tests | `dbt build --select facts` | ⏳ |
| W4.D21.1 | `obt_customer_360` | grain `(brand_id, customer_id)`; `as_of_date` var | `dbt build --select obt_customer_360` | ⏳ |
| W4.D21.2 | `agg_rfm_segments` | NTILE per brand; dbt unit tests; no 0%/100% segment | distribution query | ⏳ |
| W4.D21.3 | Docs + scoped Gold views | `dbt docs generate` complete descriptions | command output | ⏳ |
| W4.D22.1 | Airflow 3 image | Python 3.11; providers pinned; `orchestration` profile | `airflow version` | ⏳ |
| W4.D22.2 | `lakehouse_pipeline` DAG | mapped per brand; retries/timeouts; re-run same interval is a no-op | `airflow dags test lakehouse_pipeline <date>` | ⏳ |
| W4.D22.3 | ADR-0010 | orchestration ↔ compute interface | file review | ⏳ |
| W4.D23.1 | Tenant failure isolation | one brand fails, others complete | `pytest tests/integration/test_dag_failure_isolation.py -q` | ⏳ |
| W4.D23.2 | Quality gates | DLQ-rate threshold halts downstream | gate test | ⏳ |
| W4.D23.3 | Alert payload | JSON webhook payload on failure/threshold | payload in logs | ⏳ |
| W4.D23.4 | Idempotent backfill | past interval re-run → identical hashes | test output | ⏳ |
| W4.D24.1 | One-command e2e | `python scripts/run_e2e.py` green | output | ⏳ |
| W4.D24.2 | Full regression | all suites green | outputs + CI | ⏳ |
| W4.D24.3 | M2 review + tag | runbook, reviewer APPROVE, auditor PASS, `v0.2.0` (owner) | `checkpoint milestone M2` | ⏳ |
| W4.D24.4 | dbt contracts | enforced where the adapter supports it, else documented alternative | `dbt build` | ⏳ |

**Evidence**: _—_ · **M2 checkpoint**: _not run_

---

## Milestone 3 — Cloud Productionisation on AWS + Databricks 🟦

### Week 5
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W5.D25.1 | Account readiness (owner) | Free plan confirmed (ends ≈ 2027-03-22, USD 100 credits); root MFA; short-lived CLI access; tools installed | `aws sts get-caller-identity` (owner) | ⏳ |
| W5.D25.2 | Terraform bootstrap | state bucket: versioning, SSE, public-access block, native lockfile | `terraform plan` | ⏳ |
| W5.D25.3 | Budgets | 5/20/40 USD actual + forecast alerts | console (owner) | ⏳ |
| W5.D25.4 | IaC CI | fmt, validate, tflint, trivy config | CI link | ⏳ |
| W5.D26.1 | Storage + catalog | lifecycle, SSE-S3, `artifacts/` prefix, Glue DBs | `terraform plan` | ⏳ |
| W5.D26.2 | Identity | Glue job role prefix-scoped; tenant demo policy; OIDC role limited to repo/branch/env | reviewer APPROVE (`security`) | ⏳ |
| W5.D26.3 | Glue job definitions | Glue 5.0, G.1X × 2, timeout, max concurrency 1, Delta enabled | `terraform plan` | ⏳ |
| W5.D27.1 | Wheel artefact | built and uploaded; `aws` adapter via job arguments | upload listing | ⏳ |
| W5.D27.2 | Apply (owner) | resources created | `terraform output` | ⏳ |
| W5.D27.3 | Bronze on Glue | run SUCCEEDED; Glue tables; Athena query; counts = local; spend; teardown | `scripts/cloud/run_job.py bronze_batch` | ⏳ |
| W5.D28.1 | Silver + reconcile on Glue | reconcile = local | job output | ⏳ |
| W5.D28.2 | Tenant prefix denial | AccessDenied for `brand_alpha` role on `brand_beta/` | CLI evidence | ⏳ |
| W5.D28.3 | LOCAL_VS_CLOUD.md | every observed difference with evidence | file review | ⏳ |
| W5.D29.1 | CI job 2 | `core`-profile integration suites on PRs to `main` + manual | CI link | ⏳ |
| W5.D29.2 | Deploy-and-run workflow | OIDC only; environment approval; Glue run asserted | workflow run link | ⏳ |
| W5.D30.1 | Metrics report | counts, DLQ rate, late/stale, freshness, lag | command output | ⏳ |
| W5.D30.2 | Runbooks | replay, checkpoint recovery, teardown, incident | dry-run notes | ⏳ |
| W5.D30.3 | Cost report + teardown proof | spend per day and per run; nothing running | billing figures (owner) | ⏳ |
| W5.D30.4 | Airflow → Glue | cloud DAG variant triggers Glue jobs once | DAG run log | ⏳ |

**Evidence**: _—_ · **Week 5 checkpoint**: _not run_

### Week 6
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W6.D31.1 | Erasure CLI + log | rewrite subject in Bronze/Silver/Gold; `ops.erasure_log` | `pytest tests/integration/test_erasure.py -q` | ⏳ |
| W6.D31.2 | Erasure caveats | time travel, VACUUM retention, Kafka retention documented; ADR-0003 updated | file review | ⏳ |
| W6.D32.1 | Unity Catalog by hand (owner) | catalog, schemas, volume, tenant group + grants; lineage explored | screenshots + notes | ⏳ |
| W6.D32.2 | Grants as code | same grants as SQL in the bundle | `databricks bundle validate` | ⏳ |
| W6.D32.3 | Jobs on Databricks | wheel via Asset Bundle as serverless job; counts = local | `databricks bundle run` | ⏳ |
| W6.D33.1 | dbt-databricks Gold | same models on SQL warehouse; tests green | `dbt build --target databricks` | ⏳ |
| W6.D33.2 | AI/BI dashboard | Customer 360 / RFM dashboard | screenshot | ⏳ |
| W6.D33.3 | Table layout note | local OPTIMIZE before/after (measured) + Liquid Clustering vs Z-Order | `docs/benchmarks.md` | ⏳ |
| W6.D34.1 | Clean-machine run | fresh clone → e2e, timed; gaps fixed | timing record | ⏳ |
| W6.D34.2 | README rewrite | verified features only, evidence links, implemented-architecture diagram | auditor PASS | ⏳ |
| W6.D35.1 | ADRs + portfolio narrative | ADR-0002…0012 final; PORTFOLIO_NOTES final | file review | ⏳ |
| W6.D35.2 | Final regression | all suites + CI green | outputs | ⏳ |
| W6.D35.3 | Final AWS cycle | apply → deploy-and-run → verify → destroy; total spend | run link + spend | ⏳ |
| W6.D36.1 | M3 review + `v1.0.0` | reviewer APPROVE, auditor PASS, tag (owner) | `checkpoint milestone M3` | ⏳ |
| W6.D36.2 | Public-repo readiness | full-history gitleaks clean; LICENSE; secret scanning on when public | scan output | ⏳ |
| W6.D36.3 | Retrospective | what worked, what to change | file review | ⏳ |

**Evidence**: _—_ · **M3 checkpoint**: _not run_
