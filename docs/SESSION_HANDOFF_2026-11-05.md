# Session Handoff — 2026-11-05

> **Purpose**: let the next Claude Code session (or a human reviewer) resume without re-reading the whole repository or this session's chat.
> **Legend**: **[V]** verified in this session by command output · **[A]** assumption / not verified · **[U]** unfinished or open.

---

## 1. Project and architecture context

- **Project**: Multi-Tenant CRM Lakehouse Platform — portfolio project of Ömer Faruk Doğru (Senior Data / Analytics Engineer, Berlin). Repository `C:\Users\admin\Desktop\WS-Projects\thelook-lakehouse-platform\thelook-lakehouse-platform`, remote `https://github.com/dogruom/thelook-lakehouse-platform.git`.
- **Architecture (design intent, mostly not implemented)**: synthetic multi-tenant generator (Faker + Pydantic v2) → Kafka 3.7.1 KRaft (`customer_cdc`, `clickstream_events`, composite keys `{brand_id}:{entity_id}`, `brand_id` header) → Spark 3.5.1 Structured Streaming → Delta Lake OSS 3.2 Bronze (append-only) + DLQ → Spark-owned Silver (sequence-ordered CDC MERGE, SCD2, dedup, HMAC PII tokens) → dbt Gold (Kimball marts, Customer 360, RFM) · Airflow 2.9.3 batch orchestration · SeaweedFS as local S3-compatible storage · Terraform/AWS in Milestone 3.
- **Authoritative documents** (order of precedence): `docs/PROJECT_CHARTER.md` v7.1 → `docs/PROGRESS.md` v8.1 → `docs/DELIVERY_PLAN.md` v7 → `docs/DATA_CONTRACTS.md` v7 / `docs/ENGINEERING_STANDARDS.md` v7 → `docs/adr/`. Summary and rules: `CLAUDE.md`.
- **Data**: fully synthetic. Kaggle TheLook CSVs exist locally in `data/raw/` (136 MB, gitignored) from an earlier approach; they are unused and must not be used, committed or deleted. **[V]** (files present, ignored)

## 2. Owner working preferences and approval requirements

- Chat in **Turkish**; every repository artefact (code, comments, docs, skills, rules, ADRs, tests, commits) in **professional English**.
- **Turkish instructions override English ones; repository documents override conflicting prompt text.** Critical documentation inconsistencies may be fixed without asking, then reported.
- Before each meaningful work item: short Turkish note (understanding, plan, risks, verification). After it: concise Turkish executive summary.
- Ask before material architectural, security, data-loss, cost or scope decisions. Routine, reversible work within approved scope proceeds without confirmation.
- Never read or print `.env`, credentials or keys. No destructive Git (`reset --hard`, `clean`, force push, history rewrite) or Docker operations (`down -v`, prune, volume removal). Preserve all data, containers, images and volumes. Avoid unnecessary large downloads/rebuilds and repeated repository-wide scans.
- Teach briefly while working (what, why, command, how to verify). Prefer Sonnet for routine work; Opus only for architecture, hard debugging and critical reviews.
- Honest claims only: no exactly-once, GDPR compliance, production-readiness or Databricks-compatibility claims without evidence.

## 3. Completed work this session

### 3.1 Audit findings **[V]**
- Implemented: Docker Compose stack definition (7 services + init containers), init scripts, `scripts/smoke_test.py`, `src/utils/logging.py`, `src/generator/config.py`.
- Not implemented: generator models, Kafka producer, Spark jobs, DLQ, Silver, dbt models, DAGs, CI workflow; **no tests exist**.
- PROGRESS Day 1 was marked ✅ without the evidence its own rule requires → corrected to 🔄.
- v6 documents contradicted Charter v7 (dbt-snapshot SCD2, SHA-256 as "GDPR compliance", `_cdc_timestamp` ordering, `brand_id`-only Kafka keys, hard-coded credentials in examples, fixed % performance targets) → aligned.
- Working-copy shell scripts were CRLF (would break in Linux containers) → `.gitattributes` added, files converted to LF (no content change).

### 3.2 Files created or changed
| File | Change |
|---|---|
| `CLAUDE.md` | New — project map, source-of-truth order, commands, invariants, honest-claims policy, safety (78 lines) |
| `.claude/rules/python.md`, `data-platform.md`, `docs-claims.md` | New — path-scoped rules |
| `.claude/skills/work-orchestrator/`, `git-workflow/`, `session-handoff/` (`SKILL.md`) | New — project skills |
| `.claude/settings.json` | New — deny destructive git/docker commands and `.env` reads; ask before push, rebase, `branch -D`, `terraform apply/destroy` |
| `.gitattributes` | New — LF for `*.sh`, Dockerfiles, YAML, TOML, `.conf` |
| `.gitignore` | Added `.claude/settings.local.json`, `CLAUDE.local.md` |
| `docs/GIT_CHEATSHEET.md` | New — Git quick reference with safety warnings |
| `docs/CLAUDE_CODE_CHEATSHEET.md` | Previously untracked; now committed with a project-skills and session-routine section |
| `docs/DATA_CONTRACTS.md` | Rewritten to v7 (aligned with Charter) |
| `docs/ENGINEERING_STANDARDS.md` | Rewritten to v7 |
| `docs/DELIVERY_PLAN.md` | Rewritten to v7 — three milestones + job-market evidence |
| `docs/PROJECT_CHARTER.md` | v7.1 — scope tiers mapped to milestones; benchmarks/backfill moved to Stretch |
| `docs/PROGRESS.md` | v8.1 — `Current State` block, Day 1 statuses corrected, audit note |
| `README.md` | Documentation links updated |
| Existing uncommitted work (docs move, SeaweedFS migration, generator config, downloader removal) | Committed in four logical commits (see §4) |

### 3.3 Job-market research **[V, with caveat]**
11 Berlin postings reviewed on 2026-11-05 (links and table in `docs/DELIVERY_PLAN.md` §5). Most were expired or blocked; only one still appeared open — directional evidence only. Outcome: AWS as primary hands-on cloud, Databricks as a bounded exercise, Airflow/dbt/modelling as the base layer, Kafka/streaming as senior differentiator.

### 3.4 Owner memory (outside the repository)
Claude user memory at `C:\Users\admin\.claude\projects\C--Users-admin\memory\` stores the working preferences and repository location.

## 4. Git state **[V]**

- Recovery point created first: local branch `backup/pre-consolidation-2026-11-05` → `e3f5b57` (state before consolidation). A patch/tar snapshot of the then-uncommitted work was written to a session scratchpad (temporary; may be cleaned by the OS).
- Commits added on top of `e3f5b57` (all fast-forwarded into `main`):
  `acf8ee2` infra SeaweedFS · `1f71400` docs move/re-baseline · `935c9cf` GeneratorConfig · `a43d8b5` downloader removal · `09897a2` LF enforcement · `851a88a` docs alignment · `573b399` Claude Code setup; plus this handoff commit (see `git log -1`).
- Consolidation: `origin/main` (`abe6c26`) was verified to be an ancestor; `git merge --ff-only` then a normal `git push origin main`. Result before the handoff commit: `main` = `origin/main` = `573b399` (confirmed with `git ls-remote`). No force push, no history rewrite.
- Other branches (untouched): local `feature/batch-ingestion` (= `573b399`), `develop` and `feature/docker-infrastructure` (stale, also on remote), `backup/pre-consolidation-2026-11-05` (local only).
- `git config --global safe.directory` was added for this repository path only (folder owner SID differs from the current Windows user).
- Not part of the project and left untouched: an empty Git repository at `C:\Users\admin\Desktop\WS-Projects` (no commits), a Cline `AGENTS.md` and a shared `.venv` there.

## 5. Tests and checks actually performed

| Check | Result |
|---|---|
| `git fetch`, ancestry check, ahead/behind, `ls-remote` | ✅ as described in §4 |
| Secret scan (regex for AWS keys, Anthropic/GitHub tokens, private keys, passwords) over unpushed commits and staged diffs | ✅ no matches (only documentation text) |
| `.env`, `*.docx`, `data/`, `docs/_archive/` ignored (`git check-ignore`) | ✅ |
| `.claude/settings.json` JSON validity | ✅ |
| Skill and rule frontmatter present | ✅ 6/6 |
| Relative Markdown links in changed docs | ✅ none broken |
| Stale-term scan (snapshots, SHA-256 GDPR, `_cdc_timestamp`, fixed % targets, marketing terms) | ✅ only negated/correct mentions remain |
| `GeneratorConfig` import (shared venv) | ❌ `ModuleNotFoundError: pydantic_settings` |
| `pytest` | ❌ cannot start: `--cov` options in `addopts`, `pytest-cov` not installed; no tests exist |
| `ruff check src scripts` | ⚠ 11 pre-existing findings (import order, `subprocess.run` without `check`, f-strings, ambiguous Unicode) |
| Docker stack / smoke test | Not run (7 containers exist, all exited; preserved) |
| Permission deny rules actually blocking commands | **[A]** not tested; may require a new session to load |

## 6. Remaining tasks, risks and unresolved decisions **[U]**

**Owner review requested**
1. `docs/DELIVERY_PLAN.md` v7 — milestone scope, cut lists and claim boundaries.
2. `docs/PROJECT_CHARTER.md` v7.1 — benchmarks and backfill moved from Extended to Stretch (scope decision).
3. `docs/DATA_CONTRACTS.md` v7 — new fields (`event_id`, `_cdc_sequence`, `_retryable`, `_replay_status`, `_raw_value`), DLQ error categories, `as_of_date` variable.
4. `CLAUDE.md` and `.claude/settings.json` — rules the agent will follow and the commands it will refuse.

**Open decisions**
- Whether to delete stale branches `develop` / `feature/docker-infrastructure` (local and remote) and local `feature/batch-ingestion` after review — not done.
- Databricks exercise: free/trial terms and cost must be checked when Milestone 3 starts.

**Risks / technical debt**
- No tests and no CI yet; coverage gate (`--cov-fail-under=80`) will fail until tests exist.
- `seaweedfs` and `aws-cli` images use `:latest` (reproducibility).
- Defaults inconsistent: `config.py` S3 keys `any` vs. `.env.example` `lakehouse_admin` / `lakehouse_secret_dev`; Kafka port 9094 (external) vs. 9092 (internal) — to be reconciled in M1 step 1.
- `GeneratorConfig` lacks tenant list, seed, CDC topic, out-of-order and cross-tenant collision rates.
- Permission rules are prefix-based; unusual command forms (e.g. `git -c ... reset --hard`) could bypass them — CLAUDE.md instructs plain `git` invocations.
- Milestone 2 is the densest milestone; follow its cut list.

## 7. Recommended next steps (next Claude Code session)

**Model**: Sonnet (routine environment setup and implementation). Use Opus only for ADR-0002/0006 review.

1. **M1 step 0 — environment baseline** (normal complexity, low risk):
   - `python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements/dev.txt`
   - make `pytest` start (verify `pytest-cov` in `requirements/dev.txt`; add a first test for `GeneratorConfig` so the coverage gate is meaningful)
   - fix the 11 ruff findings in `scripts/smoke_test.py`, `src/utils/logging.py`, `src/generator/config.py`
   - `docker compose up -d` → `python scripts/smoke_test.py`; record output, commit hash and host spec as W1.D1.1 evidence; `docker compose stop` afterwards
   - pin `seaweedfs` / `aws-cli` image versions (check current tags first)
2. **M1 step 1** — align `GeneratorConfig` with Charter v7 and DATA_CONTRACTS v7 (tenants, seed, topics, rates, consistent S3/Kafka defaults).
3. **M1 step 2** — customer CDC model (W1.D2.1–3) on branch `feat/m1-customer-cdc`.

## 8. Commands and file locations to resume

```powershell
cd C:\Users\admin\Desktop\WS-Projects\thelook-lakehouse-platform\thelook-lakehouse-platform
git status --short --branch
git log --oneline -10
```
- Start prompt: *"Resume: read Current State in docs/PROGRESS.md and continue with the next task."*
- Resume files: `CLAUDE.md`, `docs/PROGRESS.md` (`Current State`), `docs/DELIVERY_PLAN.md` §2 (Milestone 1), this file.
- Guides: `docs/GIT_CHEATSHEET.md`, `docs/CLAUDE_CODE_CHEATSHEET.md`.
- Skills: `/work-orchestrator <task>`, `/git-workflow`, `/session-handoff`.
- Recovery: `git branch --list "backup/*"`; `git reflog`.
