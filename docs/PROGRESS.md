# Progress Tracker — Tasks, Statuses and Evidence

> **Document Version**: 9.0
> **Last Updated**: 2026-11-13 (W4.D20 closed; v9.0 2026-11-05: rebuilt to match [DELIVERY_PLAN.md](DELIVERY_PLAN.md) v8 — the v8 day structure contradicted the milestone sequence; checkpoint audit added)
> **Status Conventions**: ✅ Verified (commit hash + verification output or CI link) · 🔄 In progress / implemented without evidence · ⏳ Not started · ❌ Blocked
> **Tiers**: 🟩 Core · 🟦 Extended · 🟨 Stretch — see [PROJECT_CHARTER.md §1](PROJECT_CHARTER.md)
> **Active**: **W4.D21** closed (D21.0–D21.3 ✅) → next **W4.D22** (Airflow 3 image, DAG, ADR-0010) · M1: 37/37 ✅ (`v0.1.0` tagged)

> [!IMPORTANT]
> **Evidence rule**: ✅ only when the Evidence line names a commit hash **and** the output of the task's verification command (or a CI run link). Files existing is not evidence. Checkpoints are run with the `checkpoint` skill (day / week / milestone) — protocol in DELIVERY_PLAN §1.

---

## Current State

- **Updated**: 2026-11-13 (W4.D21 closed) · **Branch**: `feat/w4-d21-customer360-rfm` (merge status in [DAY_CLOSE_REPORTS.md](DAY_CLOSE_REPORTS.md))
- **Last delivered**: W4.D21 — generator links identified sessions to same-brand CDC customers; shared clickstream rebuilt via audited `reset_clickstream` (owner yes); Gold `obt_customer_360`, `agg_rfm_segments` (NTILE per brand, `as_of_date` var), dbt unit tests, `gold_tenant` views, docs complete (D-097, contracts v8.1).
- **Unfinished**: W4.D22–D24 (Airflow 3 image build needs the owner's approval); V-28 orders reconcile mismatch (→ W4.D23.2); tutor gates M1 (V-11), W3, W4.D19–D21; V-19 `/security-review`. V-23 known limit.
- **Tests & results**: W4.D21 close: `dbt build --select staging marts` PASS=146 WARN=0 ERROR=0; analytics after the review fixes PASS=30 (4 unit tests); unit 1660 passed, 1 skipped, coverage ≈ 80.25 %; ruff/format/mypy clean.
- **Test cadence (D-059)**: only the relevant tests while developing, full suite once per day close; integration tests run sequentially (2 worker cores); broker chaos opt-in (`RUN_BROKER_CHAOS=1`).
- **Owner action** — [VERIFICATION_REGISTER.md](VERIFICATION_REGISTER.md) *Owner action*: V-11 learning gates · V-19 one `/security-review` run · V-13 (deferred) · approve/run the Airflow 3 image build (W4.D22.1) · `v0.2.0` tag at the M2 review.
- **Stack**: `core` + `sql` running at the W4.D21 close. Shared catalog: Bronze, DLQ, six Silver tables, `gold_staging` views, `gold_marts` (7 marts + `obt_customer_360`, `agg_rfm_segments`), `gold_tenant` views. Clickstream lineage rebuilt 2026-11-13 (5000 base events, project seed).
- **Decisions to remember** (full list: [DECISION_LOG.md](DECISION_LOG.md)): earlier list plus D-073 … D-087 (Week 3), D-088 staging conventions, D-089 tenant-safe dbt tests, D-090, D-091 Spark-owned `silver.products`, D-092 `v0.1.0`, D-093 Gold also on Athena, D-094, D-095 Gold marts conventions, D-096 clickstream customer-id defect, D-097 clickstream rebuild + Customer 360/RFM conventions.
- **Risks / blockers**: 7.6 GB Docker memory, 2 worker cores; coverage headroom ≈ 0.25 pt (V-24); full Gold build ≈ 10 min; Airflow image not built yet; Delta commit safety on S3 → W5.
- **Way of working**: Claude implements everything; every pipeline step is mapped in [DATA_FLOW.md](DATA_FLOW.md) (D-046); per-session trace in [SESSION_LOG.md](SESSION_LOG.md).
- **Next task ("Start today's task")**: **W4.D22** — Airflow 3 image (owner approves the build), `lakehouse_pipeline` DAG, ADR-0010.
- **Autonomous run**: not started (setup on `chore/autonomous-run`, D-041/D-042)

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
M1 (D1–D12)   [████████████████████] 37 / 37
M2 (D13–D24)  [██████████████░░░░░░] 26 / 37
M3 (D25–D36)  [░░░░░░░░░░░░░░░░░░░░]  0 / 36
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
| W2.D7.1 | SparkSession factory + env adapter | `local`/`aws` profiles from settings; no credentials in code; run IDs in logs | `pytest tests/spark/test_session.py -q` (in container) | ✅ |
| W2.D7.2 | `spark-test` runner | pytest runs inside the Spark image; StructType derived from contracts tested | `docker compose --profile tools run --rm spark-test pytest tests/spark -q` | ✅ |
| W2.D7.3 | Wheel + submit script | `scripts/submit_job.py hello_delta` writes/reads Delta | command output | ✅ |

**Evidence**:
- W2.D7.1: commits `e82f91b`, `44b5ea7`; `tests/spark/test_session.py` is part of the in-container run below; unit `pytest tests/unit` → 1045 passed, coverage 93 %. The `aws` profile is unit-tested as conf only and has never run on AWS (W5).
- W2.D7.2: commit `5836709`; `docker compose --profile tools run --rm spark-test pytest tests/spark -q` → 22 passed, exit 0, with the `core` profile not required.
- W2.D7.3: commit `80a6e63`; `python scripts/submit_job.py hello_delta --expect-packaged --rows 7` → exit 0, `rows_written` 7, `rows_read` 7, `delta_version` 0, on `s3a://lakehouse/ops/hello_delta/<run_id>`; `driver_code` and `executor_code` both inside `lakehouse_app.zip`. Negative: same run without `--py-files` → `RuntimeError: code not loaded from lakehouse_app.zip`, exit 1.
- Gates: ruff, ruff format, mypy (29 files), lint-imports clean; smoke 24/24; integration 6 passed (run before `44b5ea7`, which touches no Kafka code). Host venv has no PySpark.
- Limits: `--expect-packaged` proves code location on one executor (`spark.cores.max=1`), not freshness with `--skip-build`. W2.D7 review carry-overs are listed in [DAY_CLOSE_REPORTS.md](DAY_CLOSE_REPORTS.md).

#### D8 · Kafka → Bronze streaming
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W2.D8.1 | `ingestion.bronze_stream` | both topics; `_raw_value`, key, headers, `_source_ref`, `_ingest_ts`, `_batch_id`, `brand_id`, `_parse_error`; `availableNow`; one checkpoint per query | job run log | ✅ |
| W2.D8.2 | Parse-function tests | valid, malformed, missing tenant → expected columns | `docker compose --profile tools run --rm spark-test pytest tests/spark/test_bronze_parse.py -q` | ✅ |
| W2.D8.3 | Count integration test | Bronze rows = produced messages incl. parse errors | `pytest tests/integration/test_bronze_stream.py -q` | ✅ |

**Evidence**:
- W2.D8.1: commit `b67f155`; `python scripts/submit_job.py bronze_stream` → exit 0, run `bronze_stream_20261109T135757Z_76361045`: `raw_customers` rows_seen 3060, parse_errors 0; `raw_clickstream` rows_seen 15600, parse_errors 75; distinct query ids and one checkpoint per query under `checkpoints/bronze_stream/`. `bronze_report` → rows 3060 and 15600, `distinct_source_refs` equal to rows, all metadata null counts 0, parse errors `malformed_json` 75. These two counts come from whatever the topics held; the oracle comparison is W2.D8.3.
- W2.D8.2: commit `78f3d60`; `docker compose --profile tools run --rm spark-test pytest tests/spark -q` → 71 passed (22 before this day; includes `test_bronze_parse.py`).
- W2.D8.3: commit `78f3d60`; `pytest tests/integration -q -rs` → 7 passed, no skip. `test_bronze_stream.py` compares rows, per-code parse errors and per-brand counts with the generated messages and manifest, on throw-away topics. A second run from the same checkpoint: exit 0, 0 rows seen, 0 batches, same query ids, rows unchanged.
- Gates (final code, `0792283`): ruff, ruff format, mypy (33 files), lint-imports (2 kept) clean; unit 1066 passed, coverage 88 % (CI gate 80 %; the Spark-only code runs in the container and is not counted, which is why the figure fell from 93 %). Negative: `bronze_report` on a missing namespace → exit 1, reason visible in `job.failed` (D-048).
- Limits: tables are addressed by path until the metastore (W2.D11). Replay after a real mid-stream failure is D9 work: idempotence is proven at `write_bronze_batch` level and for a clean rerun. `_await_all` failure path is tested with fake queries only. Trailing garbage after a JSON object is not flagged (D-047).

#### D9 · 🔺 Restart and checkpoint loss
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W2.D9.1 | Restart / loss tests | (a) restart: every `(topic, partition, offset)` once per table in Bronze; (b) checkpoint loss: re-reads observed and counted | `pytest tests/integration/test_stream_restart.py -q` | ✅ |
| W2.D9.2 | ADR-0006 draft | per-stage semantics with test references; no exactly-once claim | file review | ✅ |

**Evidence**:
- W2.D9.1: commit `c23e322`; `$env:RUN_BROKER_CHAOS='1'; pytest tests/integration -q` → **21 passed** in 62 min, no skip (pytest summary line: `21 passed in 3755.39s (1:02:35)`) (S1–S9 in `test_stream_restart.py`, 4 live guards in `test_submit_guards.py`, D8 counts, producer tests, smoke 24/24). (a) S1–S4, S7, S8: per-partition proof `min/max/rows/distinct` equals the Kafka watermarks for every table. (b) S5: full checkpoint loss re-reads every offset once more (`rows = 2 × messages`, `distinct_source_refs = messages`, 2 lineages), counted by `bronze_report`. S9 (partial loss) found silent data loss before the fix (102 of 204 CDC, 208 of 416 click rows); now `exit=1`, "checkpoint rewound", rows unchanged `102/208 → 102/208`. S7: exit 1 after 77 s, rerun exact.
- W2.D9.2: commit `c23e322`; [ADR-0006](adr/0006-delivery-semantics-restart-and-checkpoint-loss.md) (draft): per-stage semantics with S1–S9 references, wording "offset committed once per checkpoint lineage", single-writer assumption, no exactly-once claim.
- Gates (`c23e322`): ruff, ruff format, mypy (34 files) clean; unit **1136 passed**; Spark container `pytest tests/spark -q` → **80 passed** (71 before; +9 `test_bronze_rewound.py`); lint-imports 2 kept, 0 broken. Review nits fixed in the next commit (unit 1137 passed).
- Open items moved to [VERIFICATION_REGISTER.md](VERIFICATION_REGISTER.md) (D-055).

#### D10 · Validation gate and DLQ (stream entities)
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W2.D10.1 | Rules + DLQ writer | rules per contract → `_error_category`; DLQ envelope incl. `_bronze_ref`, `_retryable`, `_replay_status` | job run | ✅ |
| W2.D10.2 | Rule tests | one positive + one negative per rule | `pytest tests/spark/test_rules.py -q` | ✅ |
| W2.D10.3 | DLQ reconciliation | DLQ set = manifest DLQ set exactly; no duplicates/late events in DLQ | `pytest tests/integration/test_dlq_reconciliation.py -q` | ✅ |

**Evidence**:
- W2.D10.1: commit `5f64ad4` (+ `c91e03f` docstring/test nit); `python scripts/submit_job.py dlq_gate` on the default namespace → CDC 3060 rows read, 210 dead-lettered (`email_invalid` 90, `email_null_or_wrong_type` 60, `signup_after_produced` 60); clickstream 15600 rows read, 75 parse errors kept in Bronze (not in the DLQ, D-056), 150 dead-lettered (`session_id_null_or_wrong_type`); rerun → `dlq_inserted=0`. Insert-only MERGE on `_dlq_id` + null-safe `brand_id`, independent post-write checks (`post_write_problems`, unit-tested to fail one by one) (D-057).
- W2.D10.2: Spark container `pytest tests/spark/test_rules.py -q` → **178 passed** (positive + negative per rule, plus frozen generator fixtures `seed_1`/`seed_42` and documented Spark-vs-Pydantic divergences, D-058).
- W2.D10.3: `pytest tests/integration/test_dlq_reconciliation.py -q` → **14 passed** (22.5 min): DLQ `(brand_id, _bronze_ref)` set = manifest DLQ set exactly, no duplicate/late/parse-error event in the DLQ, rerun inserts 0, NULL/invalid brand and invalid `event_id` handled null-safe and masked in logs.
- Day-close full suite on `5f64ad4`: ruff/format/mypy (38 files) clean; lint-imports 2 kept, 0 broken; unit **1146 passed** (coverage 79 %); Spark **258 passed**; integration **34 passed, 1 skipped** (`RUN_BROKER_CHAOS` opt-in, D-059) in 74 min; V-08 flaky count test did not recur. After `c91e03f`: unit **1154 passed**.
- Review: APPROVE WITH NITS after re-check; must-fix applied in `5f64ad4`, nits 1–3 in `c91e03f`.

#### D11 · Catalog and dbt capability spike
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W2.D11.1 | Hive metastore on Postgres | Bronze/DLQ tables registered; jobs use table names | `SHOW TABLES IN bronze` | ✅ |
| W2.D11.2 | Thrift Server + dbt probe | `dbt debug` ok; probes for incremental merge, composite key, contracts, persist_docs | `dbt build --select spike --full-refresh`, then `--vars "{spike_round: 2}"` (selects the singular test via `ref`) | ✅ |
| W2.D11.3 | Decision recorded | ADR-0004 spike table with observed results; fallback chosen if needed | file review | ✅ |

**Evidence**:
- W2.D11.1: commit `f2f07f9`; Postgres metastore schema auto-created (28 tables, no FKs — ADR-0004 W1 after an observed DataNucleus FK self-deadlock); `python scripts/submit_job.py register_tables` → `bronze.raw_clickstream`, `dead_letter.dlq_customers`, `dead_letter.dlq_clickstream` registered, `bronze.raw_customers` exists (spike entry, location check passed); rerun → all `exists`; `dlq_gate` on the default namespace reads Bronze by name → rows 3060 / 15600, DLQ 210 / 150, `dlq_inserted=0` (same as W2.D10); beeline via Thrift `SHOW TABLES IN bronze` → `raw_clickstream`, `raw_customers`; `SHOW TABLES IN dead_letter` → `dlq_clickstream`, `dlq_customers`; `SELECT count(*) FROM bronze.raw_customers` → 3060.
- W2.D11.2: commit `f2f07f9`; `dbt debug` → `Connection test: OK`; `dbt build --select spike --full-refresh` → PASS=8, then `--vars "{spike_round: 2}"` (incremental `MERGE INTO … ON brand_id AND customer_id`) → PASS=8; negatives: wrong-round expectation → FAIL 3, contract type mismatch → compile error, nothing written, `INSERT NULL` → `DELTA_VIOLATE_CONSTRAINT_WITH_VALUES`. Observed gaps: column-level `not_null` constraint fails on OSS Delta, column-level `check` ignored, `persist_docs(columns)` fails upstream → override macro.
- W2.D11.3: [ADR-0004](adr/0004-spark-dbt-ownership-and-adapter-capabilities.md) capability table C1–C12 with observed results; decision: keep Thrift (no `session` fallback); D-060, D-061.
- Day-close full suite on the feature tree (before review fixes): ruff/format/mypy (40 files) clean, lint-imports 2 kept / 0 broken, unit **1195 passed**, integration **34 passed, 1 skipped** (`RUN_BROKER_CHAOS` opt-in) in 71 min. After review fixes: unit **1203 passed, 1 skipped** (POSIX mode test, run in the image); after the CI coverage fix (offline fake-session tests for `tables.py`): unit **1215 passed, 1 skipped**, coverage 81 %; Spark container **269 passed** (`tests/spark`), render script **12 passed** in the image, `dlq_gate` rerun on the stack unchanged (3060/15600, inserted 0).
- Review: APPROVE WITH NITS; both major items (registration after the counter log with an explicit "ingest committed" error; D-061 wording) and the nits (location check on named reads, accepted→proposed wording, dir mode, test comment, Thrift auth as V-16, `main()` tests) applied in `f2f07f9`.

#### D12 · M1 review and release
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W2.D12.1 | M1 demo runbook | `docs/runbooks/m1-streaming-demo.md` runs from a stopped stack | demo transcript | ✅ |
| W2.D12.2 | Full regression | unit + spark + integration + smoke green; CI job 1 green on `main` | outputs + CI link | ✅ |
| W2.D12.3 | Milestone review + tag | reviewer APPROVE, auditor PASS, README evidence links, claim note, `v0.1.0` (owner) | `checkpoint milestone M1` | ✅ (tag `v0.1.0` on `ac239b3`, D-092) |

Evidence:
- W2.D12.1: runbook `2aaa8d0`, recorded 2026-11-10 from a stopped stack on `aa7f9e9` — steps 1–8 in 16 min 11 s; Bronze 4284 / 21840 rows = refs, one lineage, partitions contiguous; DLQ rerun inserted 0; table in [m1-streaming-demo.md](runbooks/m1-streaming-demo.md#recorded-run-2026-11-10-from-a-stopped-stack-commit-aa7f9e9).
- W2.D12.2: `0ba2cc2` (M1 review fixes, D-064–D-066), rebuilt pinned Spark image: ruff/format/mypy/import-linter clean; unit **1222 passed, 1 skipped**; Spark container **291 passed**; integration **35 passed** with `RUN_BROKER_CHAOS=1` (1 h 24 min), `it_*` purge ran without warnings; smoke after `up -d` 24/24 (waited 8 s). CI job 1: PR run and `main` run linked in [DAY_CLOSE_REPORTS.md](DAY_CLOSE_REPORTS.md) (W2.D12 merge status).
- W2.D12.2 (post-review fixes, `782ec39`, D-068): unit **1222 passed, 1 skipped**; Spark container **293 passed** (+`key_not_utf8` on both tables); `tests/integration/test_it_purge_glob.py` **9 passed**; ruff/format/mypy clean.
- W2.D12.3: milestone review (reviewer) findings applied in `0ba2cc2` (D-062–D-065); post-fix code review APPROVE WITH NITS (applied in `782ec39`, D-068) and `evidence-auditor` results in [DAY_CLOSE_REPORTS.md](DAY_CLOSE_REPORTS.md); README §"What Milestone 1 Can and Cannot Claim". Open: `v0.1.0` tag (owner, V-18).

**M1 checkpoint**: run 2026-11-11 (technical gate passed; learning gate V-11 open) · **Can claim / cannot claim**: [README.md](../README.md) §"What Milestone 1 Can and Cannot Claim"

---

## Milestone 2 — Data Correctness & Analytics Products 🟩

### Week 3
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W3.D13.1 | Orders + products + landing writer | orders embed items; product seed per brand; boto3 writer with retries; manifest per batch; `sales_channel` from batch 10 | `pytest tests/unit/generator/test_landing.py -q` (moto) | ✅ |
| W3.D13.2 | `ingestion.bronze_batch` | incremental file source; `_source_ref` file + line; `mergeSchema` | job run | ✅ |
| W3.D13.3 | Batch count test | Bronze rows = landed rows; schema evolution visible | `pytest tests/integration/test_bronze_batch.py -q` | ✅ |
| W3.D14.1 | `silver.customers` (job `silver_customers`) | dedup to max sequence; MERGE guarded by sequence; tombstones; HMAC token; phone mask; `stale_cdc_noop` metric | job run | ✅ |
| W3.D14.2 | Merge unit tests | I/U/D, stale, duplicate on tiny frames | `pytest tests/spark/test_silver_customers.py -q` | ✅ |
| W3.D15.1 | 🔺 CDC suite | 6 scenarios pass | `pytest tests/integration/test_cdc_correctness.py -q` | ✅ |
| W3.D15.2 | PII tests | no raw email/phone in Silver; key from env; key change → token change | `pytest tests/unit/common/test_pii.py -q`; `pytest tests/integration/test_cdc_correctness.py -q -k pii` (no separate integration `test_pii.py`) | ✅ |
| W3.D15.3 | ADR-0004 final | ownership + spike results | file review | ✅ |
| W3.D16.1 | `silver.customers_history` | rebuild affected keys ordered by sequence | job run | ✅ |
| W3.D16.2 | SCD2 scenarios | 4 scenarios incl. shuffled = identical hash | `pytest tests/integration/test_scd2_history.py -q` | ✅ |
| W3.D17.1 | Silver orders + items | explode; dedup; composite FK; orphans → DLQ retryable | `pytest tests/integration/test_silver_orders.py -q` | ✅ |
| W3.D17.2 | Silver clickstream | windowed dedup; `late_beyond_window` metric | 0 duplicates on `(brand_id, event_id)` | ✅ |
| W3.D17.3 | DLQ replay | late parent recovered; second replay adds nothing | `pytest tests/integration/test_dlq_replay.py -q` | ✅ |
| W3.D18.1 | Reconciliation | equation per batch = manifest; metrics in `ops.pipeline_metrics` | `pytest tests/integration/test_reconciliation.py -q` | ✅ |
| W3.D18.2 | Idempotent re-run + crash | identical Silver content hash; no partial commit | `pytest tests/integration/test_idempotent_rerun.py -q` | ✅ |
| W3.D18.3 | 🔺 Tenant isolation | per-tenant counts = manifest; joins keyed on `brand_id`; leaky fixture fails; scoped views | `pytest tests/integration/test_tenant_isolation.py -q` | ✅ |
| W3.D18.4 | ADR-0003, ADR-0005 | written and linked | file review | ✅ |

**Evidence**:
- W3.D13.1: `be2ae2b` — orders (embedded items, 4 defect kinds, run-level exact budget, duplicates within/across batches, `sales_channel` key from batch 10), product seed per brand, S3 landing writer (data first, manifest last, bounded retries), `land` CLI (D-069, D-070). `pytest tests/unit/generator -q` → 437 passed (incl. `test_landing.py` with moto: keys, write order, sha256/line count, idempotent rerun, retry, exhausted retries); same seed twice → byte-identical `--dry-run` trees (78 files).
- W3.D13.2: `be2ae2b` — `bronze_batch` (`availableNow` file source, `*.ndjson` only, `wholetext` + exact `#L<n>` refs, path brand + `payload_brand_id`, idempotent per micro-batch, `--namespace` isolates landing, D-071); `raw_orders`/`raw_products` in `CATALOG_TABLES`. Spark container `tests/spark/test_bronze_batch.py` **14 passed** (refs, blank lines, invalid UTF-8, evolution via `mergeSchema` append, namespaced landing, wrong-type JSON pinned, 0-byte file, rewound checkpoint); full Spark suite **307 passed** on the final tree (Kafka path unchanged by the `append_idempotent` refactor).
- W3.D13.3: `tests/integration/test_bronze_batch.py` against the core stack → **7 passed** (190 s; rerun on the final tree `a2f204b`: 7 passed, 173 s): rows per file = manifest line count, totals = landed lines (orders + products), refs unique and existing with path brand, `sales_channel` NULL before batch 10 and set from 10, 0 parse errors, rerun adds nothing, a new batch adds only its lines.
- Gates: ruff/format/mypy (46 files)/import-linter clean; unit **1293 passed, 1 skipped**, coverage **83 %**; smoke 24/24.
- Review: APPROVE WITH NITS; should-fix (mergeSchema docstring overclaim, untested run-level defect budget) and nits (vacuous cross-tenant test, wrong-type/empty-file/rewound tests, immutable landing keys note) applied in `be2ae2b`; contract wording aligned (DATA_CONTRACTS v7.8).

- W3.D14.1–D14.2: `505143c` — `silver_customers` (Delta stream on Bronze, parse/contract exclusion inline (Q-003), dedup to max `_cdc_sequence`, sequence-guarded MERGE with tombstones, multiple-match guard, counters checked against MERGE metrics, D-074), HMAC `email_token` + phone last 4 (D-075), generator late CDC `cdc-1.1.0` (D-073, closes V-01). Spark `test_silver_customers.py` (35 tests) in the full Spark suite **514 passed** (2026-11-12, 29 min).
- W3.D15.1–D15.2: `tests/integration/test_cdc_correctness.py` → **23 passed** (2026-11-12, tree `8f495d5`): the six scenarios (in-order, stale `U`, late `U` after `D`, duplicate, multi-event batch, same `customer_id` in two brands), exact round counters, generated run = manifest; PII: no raw email/phone in any string column, token 64-hex and not a plain hash, phone = last 4. Key from env / key change → token change: unit `tests/unit/common/test_pii.py` (`test_email_token_depends_on_key`, `load_pii_key` rejections without leaking the value). No separate `test_pii.py` integration file: the PII checks run on the same Silver as the CDC suite.
- W3.D15.3: ADR-0004 Accepted (ownership section 2026-11-11, `bbba657`) — file review.
- W3.D16.1–D16.2: `bbba657`, `51ca27b` — `silver_customers_history` rebuilds affected keys from the Bronze CDC log ordered by `_cdc_sequence` (D-076). `tests/integration/test_scd2_history.py` → **11 passed** (2026-11-12): expected versions per key, structurally sound and tenant-isolated, shuffled delivery = identical content hash, re-run adds nothing; Spark `test_customers_history.py` (21) in the 514.
- W3.D17.1–D17.3: `bbba657` — `silver_orders` + `order_items` (first copy wins, composite FKs with `brand_id`, orphans → `dlq_orders` retryable, D-078), `silver_clickstream` (in-batch dedup + lookback MERGE on `(brand_id, event_id, event_date)`, `late_beyond_window` from payload times, D-079/D-086), `replay` (D-080). `test_silver_orders.py` → **13 passed**, `test_dlq_replay.py` → **9 passed** (late parent recovered, second replay adds nothing); clickstream 0 duplicates: Spark `test_silver_clickstream.py` (16, incl. same events in one vs many batches = identical Silver) and the clickstream totals in `test_reconciliation.py`.
- W3.D18.1: `8f495d5` — `ops.pipeline_metrics` + `record_batch` in every Silver job, `reconcile` job (D-081/D-082). `test_reconciliation.py` → **9 passed** (every batch row balanced, data totals = sum of batch terms, totals per table and brand = manifests, tampered metrics row → non-zero exit). Limit: customers' data-level check is weak (V-23).
- W3.D18.2: `c6ab4b9` — `test_idempotent_rerun.py` → **5 passed, 0 skipped** (2026-11-12, 41 min): SIGKILL of `silver_customers` and `silver_orders` at batch 1, checkpoint shows planned > committed (mid-batch proven), re-submit → content hash and row multiset equal the reference namespace, no duplicate business keys. First runs failed on test defects, fixed in `04d5f20`/`c6ab4b9`: the S3 listing helper used an option `aws s3 ls` rejects (every checkpoint looked empty), and the comparison included `_bronze_batch_id`, which differs per Bronze load.
- W3.D18.3: `04d5f20` — `test_tenant_isolation.py` → **7 passed** (2026-11-12): per-brand counts = manifest, every brand view holds only its rows, real collisions present (10 % collision rate in the test world; the 1 % default gave 0 collisions, which made the leaky-join check vacuous), `isolation_check` passes on clean Silver and fails on wrong expectations. Found on the first stack run: `isolation_check` crashed with an ambiguous self-join (fixed, Spark regression test fails on the old code).
- W3.D18.4: ADR-0003 and ADR-0005 Accepted (`8f495d5`), linked from PROJECT_CHARTER and DATA_CONTRACTS — file review.
- Gates (final tree): ruff/format/mypy (66 files)/import-linter clean; unit **1597 passed, 1 skipped** → **1634 passed, 1 skipped, coverage 80.91 %** after the PR CI coverage fix (see below); Spark full suite **514 passed** (tree `8f495d5`) + `test_isolation.py` 6 passed after the fix; integration W3 set 77 tests: 71 passed on the first run, the 6 failing/erroring ones passed after the fixes above (tenant 7/7, re-run 5/5).
- Review: code review (W3 week gate) **APPROVE WITH NITS**, no must-fix; should-fix 2 and 3 applied (`04d5f20`), should-fix 1 recorded as V-23.

**Week 3 checkpoint** (2026-11-13): acceptance of W3.D14–D18 met (all W3.D14–D18 rows ✅). M1 regression on the final W3 tree: integration M1 set (Bronze stream/batch, DLQ reconciliation, purge, producer CLI/round-trip/failure modes, stream restart, submit guards) **50 passed, 1 skipped** (broker chaos opt-in, D-059); its wall time (18 h 12 min) is not representative: the host was idle or asleep during the run. Demo: not run separately; the integration suites exercise the same flow. Tutor week gate pending with the owner (TASKS Learning queue, V-11). PR #15 CI first failed the unit coverage gate (78.71 % < 80 %; the W3 Spark modules are tested in the Spark container, which does not count): 37 host unit tests for the pure-Python parts (reconcile equation, metrics rows, isolation helpers, contract export) raised it to 80.91 %; threshold and omit list unchanged. Headroom is thin (V-24).

### Week 4
| ID | Task | Acceptance | Verify | Status |
|---|---|---|---|---|
| W4.D19.1 | dbt project + staging | sources on Silver; profiles from env | `dbt build --select staging` | ✅ |
| W4.D19.2 | Tenant-safe tests + CI parse | `relationships_by_brand`, `assert_no_cross_tenant_joins`; `dbt parse` in CI | CI link | ✅ |
| W4.D20.0 | `silver.products` + `stg_products` (D-091) | Spark job: valid latest copy per `(brand_id, product_id)`, MERGE, rerun no-op; registered; staging view + tests | Spark tests + `dbt build --select stg_products` || ✅ |
| W4.D20.1 | Dimensions | dim_customers, dim_customers_history, dim_products, dim_date; surrogate keys include `brand_id`; no raw PII | `dbt build --select tag:dims` || ✅ |
| W4.D20.2 | Facts | fct_orders, fct_order_items, fct_sessions; composite relationship tests | `dbt build --select tag:facts` || ✅ |
| W4.D21.0 | Clickstream ↔ customers link (D-096) | identified sessions reference same-brand CDC customers; shared data rebuilt; `fct_sessions` customer test `error` and green | `dbt build --select fct_sessions` | ✅ `3e49b32` `6828076` |
| W4.D21.1 | `obt_customer_360` | grain `(brand_id, customer_id)`; `as_of_date` var | `dbt build --select obt_customer_360` | ✅ `f3dd0b2` |
| W4.D21.2 | `agg_rfm_segments` | NTILE per brand; dbt unit tests; no 0%/100% segment | distribution query | ✅ `f3dd0b2` |
| W4.D21.3 | Docs + scoped Gold views | `dbt docs generate` complete descriptions | command output | ✅ `f3dd0b2` |
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

**Evidence**:
- W4.D19.1 ✅: `7397428` — sources on the five Silver tables, `stg_*` views in `gold_staging`, profiles from env (unchanged, D-088). After the owner landed orders (V-25): `bronze_batch` 249 lines → `silver_orders` 225 accepted + 9 DLQ + 9 duplicates + 6 FK orphans = 249, 488 items. `dbt build --select staging --profiles-dir profiles` → **PASS=42 WARN=0 ERROR=0** (2026-11-13; 897 customers, 2604 history rows, 225 orders, 488 items, 12805 clickstream rows, 3 brands). The orders leg is not vacuous: 1 order references a customer id that exists in two brands.
- W4.D19.2 ✅: `7397428` — `relationships_by_brand`, `cross_tenant_key_matches` + `assert_no_cross_tenant_joins` (D-089). `pytest tests/integration/test_dbt_tenant_tests.py -q` → **5 passed** (leaky fixture: plain `relationships` 1 failure, `relationships_by_brand` 2, cross-tenant macro 1; fixtures selected only with the var; 3 consecutive runs green after seeds → views). CI [run 43427736811](https://github.com/dogruom/thelook-lakehouse-platform/actions/runs/43427736811): `dbt parse` (both var states, fixture gating check), unit, gitleaks → success.
- W4.D20.0 ✅: `ff84b4b` — Spark `silver_products` (D-094): Spark `tests/spark/test_tables.py` + `test_silver_products.py` → 11 + 53 passed; shared catalog run 1 → bronze_rows 120 = accepted 120, inserted 120; reruns (also after the review fix) → inserted 0, updated 0; `silver.products` registered. `dbt build --select stg_products+` PASS=13.
- W4.D20.1 ✅ / W4.D20.2 ✅: `ff84b4b` — `dbt build --select staging marts --profiles-dir profiles` (superset of `tag:dims` / `tag:facts`) → **PASS=117 WARN=1 ERROR=0** (2026-11-13, Thrift session UTC). Per brand alpha/beta/gamma: dim_customers 537/269/91, dim_customers_history 1566/778/260, dim_products 40/40/40, fct_orders 75/75/75 (net 58183.78/64424.53/51920.85), fct_order_items 168/165/155, fct_sessions 3866/1929/646; dim_date 1095 days. `assert_marts_reconcile_to_staging` PASS (counts and net sums per brand). WARN = `fct_sessions.customer_id` relationship, 3879 identified sessions with generator-random customer ids (D-096, V-26 → W4.D21.0); error-level cross-tenant check on sessions PASS. Contracts enforced on `dim_date`, `fct_orders` (data types, D-095). Negative check: `assert_dim_date_calendar` fails with a shifted `dim_date_start`.
- W4.D21.0 ✅: `3e49b32` (generator pools, D-097) + `6828076` (`reset_clickstream`). Owner yes 2026-11-13 → `reset_clickstream` dry run then `--apply --confirm clickstream`: 3 tables (31/19/7 files), 2 checkpoints, 1 metrics row removed; topic `clickstream_events` recreated; `produce --only clickstream --events 5000 --cdc-events 1000` → delivered 5200 (alpha 3124 / beta 1557 / gamma 519); `bronze_stream` → `silver_clickstream`: rows_read 5200 = accepted 4925 + DLQ 50 + duplicates 200 + parse 25. 26 sessions hit customers missing from Silver (shared CDC from an older generator version) → `produce --only cdc` additively: 905 duplicates, 34 stale no-op, 11 accepted; customers 897 → 903; then **0 orphans** of 1706/913/279 identified sessions. `relationships_by_brand_fct_sessions_customer_id` at severity error → PASS.
- W4.D21.1 ✅ / W4.D21.2 ✅ / W4.D21.3 ✅: `f3dd0b2` — `dbt build --select staging marts` → **PASS=146 WARN=0 ERROR=0** (before the contract column rename); after the rename `dbt build --select obt_customer_360+` → **PASS=29 ERROR=0** incl. 3 dbt unit tests (per-brand NTILE, tie-break + exclusions, every segment rule) and `assert_rfm_segments_non_degenerate`. obt_customer_360 903 rows (alpha 542 / beta 269 / gamma 92; buyers 45/35/17; with sessions 183/90/27). RFM 92 customers; segments per brand (at_risk/champions/hibernating/loyal/promising): alpha 10/6/11/8/8, beta 4/4/12/7/5, gamma 3/3/7/3/1 (min 5.9 %, max 41.2 %). Reproducibility: checksum `sum(hash(...))` of `agg_rfm_segments` = 21783813268 on two builds with `as_of_date` 2026-01-02 (and after the rename), c360 −16521825472 both times; `as_of_date` 2026-01-01 → c360 −9070750642. After the review fix (truncated hours, `whole_hours_between`): `dbt build --select obt_customer_360+` → **PASS=30** (4 unit tests incl. `obt_customer_360_cut_tenancy_and_revenue_rules`; negative check: with `dbt.datediff` the OBT unit test fails, `recency_hours` 0 → 1); segment counts unchanged; checksum rfm 5059524923 / c360 −14436000144 identical on two builds. `dbt docs generate` + `python scripts/check_dbt_docs.py` → 15 models, 0 missing descriptions. Views `gold_tenant.{obt_customer_360,agg_rfm_segments}__brand_{alpha,beta,gamma}` (6), `__brand_beta` returns only `brand_beta`.
- W4.D21 gates: ruff/format/mypy clean, lint-imports 2 kept; unit **1660 passed, 1 skipped, coverage ≈ 80.25 %** (887 of 4491 missed); after the review fixes 1668 passed (incl. 7 `run_e2e` tests that ship with W4.D24), 884 missed ≈ 80.32 %. Found: `reconcile` `batch_sum_vs_data` mismatch on `raw_orders` (V-28, orders untouched today).
- W4.D20 gates: ruff/format/mypy clean; unit **1646 passed, 1 skipped, coverage 80.41 %**; review APPROVE WITH NITS, all should-fix applied.
- Gates (W4.D19): ruff/format/mypy clean; unit **1634 passed, 1 skipped, coverage 80.91 %** (no `src/` change). V-15 closed (Spark job + dbt overlap, D-090). Review: code review **APPROVE WITH NITS**, should-fix 1–3 applied before the commit.

**M2 checkpoint**: _not run_

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
| W5.D28.4 | Gold on Athena (D-093) | `dbt-athena` target builds the same marts from Glue-catalog Silver; row counts = local | `dbt build --target athena --select marts` | ⏳ |
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
