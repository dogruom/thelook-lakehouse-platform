# CLAUDE.md — Multi-Tenant CRM Lakehouse Platform

Portfolio-grade, local-first data platform: synthetic multi-tenant CRM events → Kafka (KRaft) →
Spark Structured Streaming → Delta Lake (Bronze/DLQ/Silver) → dbt Gold; Airflow 3; Terraform +
AWS Glue on AWS; bounded Databricks Free Edition track. Owner: Ömer Faruk Doğru (Senior Data / Analytics Engineer, Berlin).
Status: **re-planned 2026-11-05; Milestone 1 starts at W1.D1. Most components are planned.**

## Session protocol
- **"Start today's task" / "Bugünün görevine başla":** read `Current State` and the named next
  task's day card, give a 5-line Turkish brief (goal, deliverable, acceptance, learning outcome,
  owner approvals needed), then start — do not reopen settled decisions.
- **Start:** read only the `Current State` block of [docs/PROGRESS.md](docs/PROGRESS.md) and the active
  day card in [docs/DELIVERY_PLAN.md](docs/DELIVERY_PLAN.md); run `git status --short --branch`.
  Open other docs only when the task needs them. After a break, open with a 5-line recap from
  `Current State` (where we are, last delivered, unfinished, next, owner action needed).
- **Every substantive request:** `work-orchestrator` (scope check against the active milestone,
  over-engineering check, classify → route agents → task cards → verify → review). Trivial asks: answer directly.
- **End of a planned day / week / milestone:** `checkpoint` skill (evidence → [docs/PORTFOLIO_NOTES.md](docs/PORTFOLIO_NOTES.md)
  → `tutor` learning gate). **Git:** `git-workflow` skill.
- **End of session:** `session-handoff` skill (updates `Current State`; appends a git-derived entry to
  [docs/SESSION_LOG.md](docs/SESSION_LOG.md) — commits, files, decision IDs, learning items; no chat logs).
- **Day close report:** every planned day ends with a report appended to
  [docs/DAY_CLOSE_REPORTS.md](docs/DAY_CLOSE_REPORTS.md) (newest first) by the `checkpoint` skill.
- **Standing merge approval (owner, 2026-11-06, D-032):** after a day close, push the day branch,
  open the PR and merge it (merge commit) **only if** CI is green, the code review verdict is APPROVE /
  APPROVE WITH NITS with all must-fix applied, and the `evidence-auditor` reports no MISMATCH.
  Any red check or open must-fix → stop and report. Never force-push, never merge with failing checks.
- **Decisions:** every change to a contract, rate/threshold, default, boundary, tool or plan gets a
  row in [docs/DECISION_LOG.md](docs/DECISION_LOG.md) in the same commit (architecture-level → also an ADR).

## Standard reply format (every substantive reply to the owner, Turkish; trivial asks: answer only)
Keep each line short; omit a block only when it is genuinely empty. Spend tokens on the work, not the frame.
```
🎯 İstek    : [W?.D?.?] <owner's prompt rewritten as: goal · constraint · expected output>
🏷 Sınıf    : complexity / risk / mode · agents (parallel ‖ / sequential →)
⚠ Not      : wrong assumption, scope creep or cheaper path (1–3 lines) — only if any
📌 Yönetici özeti: 2–3 plain sentences — what is done, where it stands, what the owner must decide
🔧 Teknik özet: what changed and why · verified how (command → result) · what is NOT verified
📂 Dosyalar : one line per touched file as a clickable repo-relative link — [path](path) or
              [path:line](path#Lline) to the key spot — plus what changed there (new/edited/deleted)
👀 Kontrol et: 2–4 things the owner, as a data engineer/architect, must check or decide
🧠 Anla     : the concept in one sentence + interview line + 2–4 CV/ATS keywords
⏭ Sıradaki : next step · owner approval needed (image build, push, cloud spend…)
```
`🧠 Anla` always contains: what the session's work is for and why the concept exists (1–2 lines),
then **1–2 senior-level interview questions with model answers** (Senior Data Engineer bar, tutor
"Target bar"), then the interview line and CV/ATS keywords.
Glossary: `.claude/learning/GLOSSARY.md` is the owner's personal technical encyclopedia (Turkish,
terms in English). When the owner asks what a term means, **look it up there first**, answer from it,
and extend the entry if the answer adds something. New concepts from a session's `🧠 Anla` and every
"tutora ekle: <term>" get an entry (also listed under `## Owner questions` in `LEARNING_LOG.md` for
the next tutor session).
The owner may dictate loosely; the 🎯 line is where the prompt gets normalised — if it is wrong,
the owner corrects it in one line. Remind the owner of the playbook rule they skipped (no task ID,
new constraint unstated, approval pending) inside ⚠ instead of lecturing.

## Agents (`.claude/agents/`; workers on Sonnet or Haiku — the coordinator is the Opus main session)
| Agent | Writes | Use when |
|---|---|---|
| `implementer` | assigned production files | complex/multi-file implementation from a task card |
| `test-author` | `tests/**`, dbt tests | in parallel with implementer; 🔺 correctness suites |
| `reviewer` | nothing (read-only) | design review before / code review after complex, data-correctness, IAM, security work |
| `evidence-auditor` (Haiku) | nothing (read-only) | day close with 🔺 claims, week gate, milestone review |
| `Explore` (Haiku) | nothing | wide read-only searches only |
The main session is the coordinator; it owns docs, PROGRESS and acceptance. Trivial and normal
tasks need no agents. Subagents cannot spawn subagents. The `tutor` is a skill (it needs a dialogue).

## Model policy
Main session = coordinator on **Opus, medium effort** (project default): checks every prompt,
plans, sequences, chooses parallelism, reviews. It delegates implementation/tests/SQL/dbt/docs
drafting to Sonnet agents and search/evidence checks to Haiku agents; it writes only trivial edits
itself. `high` effort only for architecture checkpoints, hard debugging, material risk reviews.
Details: `work-orchestrator` §Model split; owner guide: [docs/AI_DEVELOPMENT_PLAYBOOK.md](docs/AI_DEVELOPMENT_PLAYBOOK.md).
Personal communication preferences live in `~/.claude/CLAUDE.md`; this file holds project rules only.

## Source-of-truth order (when documents disagree)
1. [docs/PROJECT_CHARTER.md](docs/PROJECT_CHARTER.md) v8 — invariants (§3), privacy (§5)
2. [docs/PROGRESS.md](docs/PROGRESS.md) v9 — task acceptance criteria, statuses, evidence
3. [docs/DELIVERY_PLAN.md](docs/DELIVERY_PLAN.md) v8 — day cards, checkpoints, milestone gates, market map
4. [docs/DATA_CONTRACTS.md](docs/DATA_CONTRACTS.md) v7.1, [docs/ENGINEERING_STANDARDS.md](docs/ENGINEERING_STANDARDS.md) v7.1
5. ADRs in [docs/adr/](docs/adr/) (0007 runtime/catalog, 0008 cost guardrails, 0009 boundaries, 0011 Glue, 0012 Databricks). `docs/_archive/` is superseded — ignore.
- Turkish owner instructions override English ones; repository documents override conflicting prompt text.

## Repository map
| Path | Content |
|---|---|
| `src/generator/config.py`, `src/utils/logging.py` | Only code so far; moves to `src/lakehouse/` in W1.D1 (ADR-0009) |
| `dbt_project/` | dbt skeleton (no `dbt_project.yml` yet) |
| `docker/`, `docker-compose.yml`, `scripts/` | Local stack (rebuild in W1.D2), init scripts, `smoke_test.py` |
| `tests/` | Empty — no tests exist yet |
| `.claude/hooks/guard_commands.py` | Blocks secret reads and destructive shell commands (`--self-test`) |

## Commands (PowerShell, repo root)
```powershell
py -3.11 -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -e .[dev]   # from W1.D1
ruff check src tests scripts; ruff format --check src tests scripts; mypy src
pytest tests/unit -q
docker compose --profile core up -d; python scripts/smoke_test.py; docker compose stop
docker compose --profile tools run --rm spark-test pytest tests/spark -q        # from W2.D7
```

## Non-negotiable engineering rules
- `brand_id` is mandatory and part of **every** business key and join. Partitioning is not a security boundary.
- Bronze is append-only (sole exception: audited erasure). No silent data loss: parse failures stay in
  Bronze, contract failures go to DLQ with `_bronze_ref`, duplicates/stale/late events are counted.
- CDC is ordered by `_cdc_sequence`, never by wall clock. Kafka **key** = `{brand_id}:{entity_id}`;
  `brand_id` is additionally a **header**. Headers are not partition keys.
- Silver/SCD2 are Spark-owned; dbt owns Gold only (no dbt snapshots on customers).
- PII: `HMAC-SHA256` with a key from env/secret store. Plain SHA-256 is not anonymisation.
- Python 3.11 in every runtime; Spark 3.5.9 / Delta 3.3.3 (no Spark 4-only APIs).

## Honest-claims policy (portfolio credibility)
Never write or imply, unless demonstrated with linked evidence: exactly-once delivery (say:
at-least-once + idempotent MERGE + deterministic dedup) · GDPR compliance (say: privacy controls) ·
`VACUUM` = erasure · Airflow = stream orchestration · SeaweedFS = AWS S3 · local Spark = Glue/Databricks
runtime · production-ready. A PROGRESS task is ✅ only with a commit hash **and** verification output.
Data is fully synthetic (Faker + Pydantic); the Kaggle TheLook CSVs in `data/raw/` are unused local
leftovers — do not use, commit or delete them.

## Safety
- Never read or print `.env`, state files, keys; never dump env or `docker compose config` (hook-enforced).
- No `git reset --hard`, `git clean`, force push, history rewrite, branch deletion without explicit approval.
- No `docker compose down -v`, prune, `docker rm/rmi`, volume removal. Use `docker compose stop`.
- Ask before `terraform apply/destroy`, any `aws` command, image builds, dataset downloads, cloud spend
  (project ceiling USD 50, ADR-0008).

## Conventions
- Chat with the owner: **Turkish**. Code, comments, commits, docs: **professional English**.
- Conventional Commits; branches from `main`: `feat/<scope>`, `fix/<scope>`, `docs/<scope>`.
- ruff (line 100), mypy, Pydantic v2, structlog, no `print()` in pipeline code.
- Keep tool output short (`-q`, `--tb=short`, `--tail 20`); read files with offsets; no repo-wide re-scans.
- Teach briefly when introducing a Git/Claude Code concept: what, why, command, how to verify.
