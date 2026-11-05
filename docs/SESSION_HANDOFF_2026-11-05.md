# Session Handoff — 2026-11-05

> **Purpose**: resume without re-reading the repository or the chat. Replaces the morning version of
> this file (still in Git history, commit `3a61d13`).
> **Legend**: **[V]** verified by command output in this session · **[A]** assumption · **[U]** open.

## 1. Where the project stands
- Planning is complete and committed: Charter v8, DELIVERY_PLAN v8.1 (36 day cards), PROGRESS v9,
  ADR-0007/0008/0009/0011/0012 — commit `23b0cb7` on `main` (pushed) **[V]**.
- Implementation started: **W1.D1** on branch `feat/w1-baseline`, commit `00df04e` **[V]** —
  `src/lakehouse` package, 10 unit tests (coverage 100 %), ruff/mypy clean, import-linter 2 contracts
  (+ negative probe), pre-commit 10/10 incl. gitleaks, CI workflow; full-history gitleaks clean;
  reviewer APPROVE WITH NITS, both MAJOR items fixed. W1.D1.4 waits for the first CI run result **[U]**.
- Nothing else is implemented; the Docker stack is defined but not verified (W1.D2).

## 2. Decisions taken today (do not reopen without new evidence)
| Topic | Decision | Record |
|---|---|---|
| Runtime | Spark 3.5.9, Delta 3.3.3, Python 3.11, Java 17 everywhere | ADR-0007 |
| Catalog / Gold | Postgres-backed Hive metastore; dbt-spark via Spark Thrift Server; spike at W2.D11 | ADR-0007 |
| Cloud compute | **AWS Glue 5.0** (EMR is Paid-plan only on the owner's account) | ADR-0011 |
| Databricks | Bounded Free Edition track W6.D32–D33 ($0) | ADR-0012 |
| Boundaries | One package `lakehouse`, components via contracts, import-linter in CI | ADR-0009 |
| Budget | ≤ USD 50 total; AWS account on Free plan (USD 100 credits, ends ≈ 2027-03-22) | ADR-0008/0011 |

## 3. Working system (Claude Code)
- Personal preferences: `~/.claude/CLAUDE.md` (Turkish, direct, prompt self-check, autonomy,
  context recovery) — loads in every local project **[V]**.
- Project: `CLAUDE.md`; agents `implementer`, `test-author`, `reviewer` (`data`/`security` focus),
  `evidence-auditor` (Sonnet), `Explore` override (Haiku); skills `work-orchestrator`,
  `checkpoint`, `tutor`, `session-handoff`, `git-workflow`; rules; guard hook.
- Main session default in this repo: Sonnet **[V]**. User-level default elsewhere: Opus (unchanged by request).

## 4. Owner actions
1. Start a **new** Claude Code session (current one still carries an old env variable that forces
   Sonnet on Explore).
2. Check the GitHub Actions run of `feat/w1-baseline` (no `gh` CLI here) → paste the run link or
   result; then merge via PR or let Claude fast-forward.
3. Next session starts W1.D2 (stack rebuild — needs approval for the image build, ~2–3 GB download).

## 5. Resume
```powershell
cd C:\Users\admin\Desktop\WS-Projects\thelook-lakehouse-platform\thelook-lakehouse-platform
.\.venv\Scripts\Activate.ps1
git status --short --branch
```
Prompt: *"Devam edelim"* — Claude reads `Current State` and the active day card.
