# CLAUDE.md — Multi-Tenant CRM Lakehouse Platform

Portfolio-grade, local-first data platform: synthetic multi-tenant CRM events → Kafka (KRaft) →
Spark Structured Streaming → Delta Lake (Bronze/DLQ/Silver) → dbt Gold, with Airflow, Terraform/AWS.
Owner: Ömer Faruk Doğru (Senior Data / Analytics Engineer, Berlin). Status: **early — most components are planned.**

## Session protocol
- **Start:** read the `Current State` block at the top of [docs/PROGRESS.md](docs/PROGRESS.md), then
  `git status --short --branch`. Do not re-read all docs; open only what the task needs.
- **Non-trivial task:** use the `work-orchestrator` skill (classify → route model → plan → verify).
- **Git operations:** use the `git-workflow` skill.
- **End:** use the `session-handoff` skill (updates the PROGRESS `Current State` block, no chat logs).

## Source-of-truth order (when documents disagree)
1. [docs/PROJECT_CHARTER.md](docs/PROJECT_CHARTER.md) v7 — architecture invariants (§3), privacy (§5)
2. [docs/PROGRESS.md](docs/PROGRESS.md) v8 — task-level acceptance criteria and evidence
3. [docs/DELIVERY_PLAN.md](docs/DELIVERY_PLAN.md) — milestone roadmap
4. [docs/DATA_CONTRACTS.md](docs/DATA_CONTRACTS.md) v7 (schemas, DLQ envelope; draft, not yet enforced) and
   [docs/ENGINEERING_STANDARDS.md](docs/ENGINEERING_STANDARDS.md) v7 (coding/Spark/dbt/Airflow standards)
5. ADRs in [docs/adr/](docs/adr/). `docs/_archive/` is local-only and superseded — ignore it.
- Owner instructions: Turkish instructions override English ones; repository documents override
  conflicting prompt text. Fix critical doc inconsistencies directly and report them.
- Guides: [docs/GIT_CHEATSHEET.md](docs/GIT_CHEATSHEET.md), [docs/CLAUDE_CODE_CHEATSHEET.md](docs/CLAUDE_CODE_CHEATSHEET.md).

## Repository map
| Path | Content |
|---|---|
| `src/generator/` | Synthetic data generator (`config.py` only so far) |
| `src/streaming/`, `src/quality/`, `src/contracts/` | Spark jobs, DLQ/validation, contracts (empty) |
| `src/utils/logging.py` | structlog JSON logging |
| `dbt_project/` | dbt skeleton (no models yet) |
| `dags/` | Airflow DAGs (empty) |
| `docker/`, `docker-compose.yml`, `scripts/` | Local stack, init scripts, `smoke_test.py` |
| `tests/unit/`, `tests/integration/` | Empty — no tests exist yet |

## Commands (PowerShell, from repo root)
```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1          # project-local venv (not WS-Projects/.venv)
pip install -r requirements/dev.txt
ruff check src tests scripts; ruff format --check src tests scripts
mypy src/generator
pytest tests/unit -q                                           # addopts enforce --cov-fail-under=80
docker compose up -d; python scripts/smoke_test.py             # local stack (~7 containers)
docker compose stop                                            # stop without deleting volumes
```

## Non-negotiable engineering rules
- `brand_id` is mandatory and part of **every** business key and join. Partitioning is not a security boundary.
- Bronze is append-only (sole exception: audited erasure). No silent data loss: parse failures stay in
  Bronze, contract failures go to DLQ with `_bronze_ref`, duplicates/late events are counted.
- CDC is ordered by `_cdc_sequence`, never by wall clock. Kafka **key** = `{brand_id}:{entity_id}`;
  `brand_id` is additionally a **header**. Headers are not partition keys.
- Silver/SCD2 are Spark-owned; dbt owns Gold only (no dbt snapshots on customers).
- PII: `HMAC-SHA256` with a key from env/secret store. Plain SHA-256 is not anonymisation.

## Honest-claims policy (portfolio credibility)
Never write or imply, in code, docs, commits or README, unless demonstrated with linked evidence:
exactly-once delivery (say: at-least-once + idempotent MERGE + deterministic dedup) · GDPR compliance
(say: privacy controls) · `VACUUM` = erasure · Airflow = stream orchestration (it triggers
`availableNow` micro-batches) · SeaweedFS = AWS S3 · local Spark = Databricks runtime · production-ready.
A PROGRESS task is ✅ only with a commit hash **and** verification output.
Data: fully synthetic (Faker + Pydantic), domain inspired by TheLook. Never present it as real customer data.
Decision (owner, 2026-11-05: repository documents are authoritative): data is generated, not imported.
The Kaggle TheLook CSVs in `data/raw/` are unused local leftovers — do not use, commit or delete them.

## Safety
- Never read or print `.env`; document configuration in `.env.example` only.
- No `git reset --hard`, `git clean`, force push, history rewrite, branch deletion without explicit approval.
  Invoke `git` directly (no `-c` overrides) so permission rules apply.
- No `docker compose down -v`, `docker * prune`, `docker volume rm`. Use `docker compose stop`.
- Ask before `terraform apply/destroy`, any cloud spend, dataset downloads or large data regeneration.

## Conventions
- Conversation with the owner: **Turkish**. Code, comments, commits, docs: **professional English**.
- Conventional Commits (`feat(generator): …`). Branch from `main`: `feat/<scope>`, `fix/<scope>`, `docs/<scope>`.
- Python 3.11+, ruff (line 100), mypy, Pydantic v2, structlog, no `print()` in pipeline code.
- Unit tests run offline (S3 via `moto`); integration tests use the Docker stack.
- Teach briefly when introducing a Git/Claude Code concept: what, why, command, how to verify.
