# Task Queue — Unattended Runs

> **Document Version**: 1.0
> **Last Updated**: 2026-11-06 (created with the autonomous-run setup, D-041)
> **Role**: machine-readable work queue for `scripts/autonomous/run_queue.py` and the
> `autonomous-run` skill. **Not** a source of truth for acceptance: task acceptance criteria,
> statuses with evidence and the Current State live in [PROGRESS.md](PROGRESS.md); day cards in
> [DELIVERY_PLAN.md](DELIVERY_PLAN.md).

## Rules
- One row = one unit an iteration can finish end to end (implement → verify → commit).
  `*.close` rows run the `checkpoint` skill (day close; tutor in batched mode) and the D-032
  push/PR/merge gate.
- Status: `todo` · `in_progress` · `blocked-owner` · `done` · `failed`. The runner picks the first
  `in_progress`, else the first `todo` whose dependencies are `done`.
- **The runner owns Attempts** (+1 for every iteration that does not end `done`/`blocked-owner`,
  including "no progress"; `failed` at 3). The agent may only move a row between `todo`,
  `in_progress`, `blocked-owner`, `done` (with commit hash) and `failed`, and edit Note; any other
  edit, or touching a `blocked-owner`/`done`/`failed` row, halts the run. Only the owner unblocks
  rows (between runs).
- `done` requires the commit hash in Note; the matching PROGRESS task follows the evidence rule.
- The table format is parsed by `scripts/autonomous/queue_file.py` — keep the six columns.

## How to run (owner)
```powershell
.\scripts\autonomous\run_queue.ps1 --dry-run              # shows the next row and command, changes nothing
.\scripts\autonomous\run_queue.ps1 --max-iterations 1     # pilot: one row, then stop
.\scripts\autonomous\run_queue.ps1                        # until queue done / blocked / limit > 24 h / halt
New-Item .claude\runs\STOP -ItemType File                 # stop after the current iteration
```
Exit codes: 0 done/paused · 2 config · 3 STOP · 4 waiting for owner · 5 limit > 24 h · 6 repeated
failures · 7 supervisor halt. Status: `Autonomous run` line in PROGRESS Current State; details in
`.claude/runs/` (gitignored). Keep the PC awake and on power (the runner blocks sleep while it runs).
Remote: start the interactive session with Remote Control to follow it from claude.ai / the app.
After a halt (exit 3/5/6/7) read `.claude/runs/STOP` and the log, fix the cause, then delete the
STOP file (`Remove-Item .claude\runs\STOP`) before restarting.

**Isolation — what this is and is not (D-041).** A policy layer: permission allow/deny rules, the
guard hook (path guard, network/delete/push rules, guardrail self-protection) and runner-side
checks (queue transitions, guardrail diff, supervisor). It is **not** an OS sandbox: an allowed
`python`/`git` process runs as your Windows user and could read `~/.aws` or `~/.ssh` or reach the
network. Residual-risk reducers (owner, optional): run unattended sessions when no AWS session is
active; enable GitHub branch protection on `main` (required CI checks, no admin bypass) — the
only server-side control for the D-032 merge gate.

## Queue
| ID | Task | Status | Depends on | Attempts | Note |
|---|---|---|---|---|---|
| W2.D7.1 | `lakehouse.common.spark` SparkSession factory + environment adapter (local/aws) + job CLI base with run IDs | done | — | 0 | `e82f91b`, `44b5ea7` (interactive session) |
| W2.D7.2 | `spark-test` service (tools profile) running pytest in the Spark image; first Spark tests | done | W2.D7.1 | 0 | `5836709` (no image rebuild needed) |
| W2.D7.3 | Wheel build + `scripts/submit_job.py` (spark-submit in the master container) | done | W2.D7.2 | 0 | `80a6e63` |
| W2.D7.close | Day close W2.D7 (checkpoint, report, D-032 merge gate) | done | W2.D7.3 | 0 | `879cd9e`, PR #8 |
| W2.D8.1 | `ingestion.bronze_stream` (both topics, raw value, key, headers, source ref, availableNow, one checkpoint per query) | done | W2.D7.close | 0 | `b67f155` |
| W2.D8.2 | Spark tests for the parse function (valid, malformed, missing tenant) | done | W2.D8.1 | 0 | `78f3d60` |
| W2.D8.3 | Integration: Bronze rows = produced messages | done | W2.D8.2 | 0 | `78f3d60` |
| W2.D8.close | Day close W2.D8 | done | W2.D8.3 | 0 | day-close docs, interactive session |
| W2.D9.1 | Restart tests: kill mid-stream → each offset once; delete checkpoint → replay quantified | done | W2.D8.close | 0 | `c23e322` |
| W2.D9.2 | ADR-0006 draft with measured outcomes | done | W2.D9.1 | 0 | `c23e322` |
| W2.D9.close | Day close W2.D9 (🔺 gate for M2) | done | W2.D9.2 | 0 | PR #10, merge `30a8f5c` |
| W2.D10.1 | `quality.rules` + DLQ writer | done | W2.D9.close | 0 | `5f64ad4` |
| W2.D10.2 | Spark tests per rule incl. negatives | done | W2.D10.1 | 0 | `5f64ad4`; 178 passed |
| W2.D10.3 | Integration: DLQ set = manifest DLQ-expected set (exact) | done | W2.D10.2 | 0 | `5f64ad4`; 14 passed |
| W2.D10.close | Day close W2.D10 | done | W2.D10.3 | 0 | DAY_CLOSE_REPORTS W2.D10 |
| W2.D11.1 | Postgres-backed Hive metastore; register Bronze/DLQ tables | done | W2.D10.close | 0 | `f2f07f9`; register_tables + SHOW TABLES |
| W2.D11.2 | Spark Thrift Server (`sql` profile); `dbt debug`; probe models | done | W2.D11.1 | 0 | `f2f07f9`; dbt build spike PASS=8 ×2 |
| W2.D11.3 | Decision recorded (Thrift or `session` fallback) in ADR-0004 spike section | done | W2.D11.2 | 0 | ADR-0004, D-060/D-061 |
| W2.D11.close | Day close W2.D11 | done | W2.D11.3 | 0 | DAY_CLOSE_REPORTS W2.D11 |
| W2.D12.1 | `docs/runbooks/m1-streaming-demo.md` | done | W2.D11.close | 0 | demo on `aa7f9e9`; runbook `2aaa8d0` |
| W2.D12.2 | Full regression | done | W2.D12.1 | 0 | `0ba2cc2`, `782ec39` |
| W2.D12.3 | Milestone review (reviewer + evidence-auditor), README status, can/cannot-claim note | blocked-owner | W2.D12.2 | 0 | `0ba2cc2`, `782ec39`; only the `v0.1.0` tag is open (owner, V-18) |
| W2.D12.close | Week/milestone close W2 (tutor deferred to owner session) | done | W2.D12.2 | 0 | `782ec39`, `2aaa8d0`; tag V-18 and tutor V-11 are owner items |
| W3.D13.1 | Orders + products generator, S3 landing writer, `land` CLI | done | W2.D12.close | 0 | `be2ae2b` |
| W3.D13.2 | `ingestion.bronze_batch` (file source, exact line refs, namespaced landing) | done | W3.D13.1 | 0 | `be2ae2b` |
| W3.D13.3 | Integration count test landing → Bronze | done | W3.D13.2 | 0 | `be2ae2b`; 7 passed |
| W3.D13.close | Day close W3.D13 (tutor queued) | done | W3.D13.3 | 0 | docs close commit |
| Q-001 | Run several Spark steps in one SparkSession / one submit (e.g. `bronze_stream` → `dlq_gate` → reports) to cut the ~30–60 s submit and JVM start per step in tests and demos | todo | W2.D12.close | 0 | Owner, 2026-11-09 (D-059): queued, not before D10; changes the job framework (D-044), so plan it with a design review |
| Q-002 | Gate rules `kafka_key_missing` and `brand_header_missing` (SCHEMA_VIOLATION) + Spark tests; manifest stays unchanged (producer always sets both) | done | W3.D13 | 0 | M1 review MINOR 4, D-063; before Silver relies on the key |
| Q-003 | Silver excludes `_parse_error IS NOT NULL` rows before any `brand_id` join; test with an unvalidated header brand | todo | W3.D14 | 0 | M1 review MINOR 5, D-063 |
| Q-004 | Small M1 review leftovers: `bronze_report --table` choices; shared log-safe brand key helper in `lakehouse.common`; glossary line `rejected_duplicates` vs manifest `duplicates_removed`; Airflow image 2.9.3 placeholder → 3.x in W4 | done | W3 | 0 | M1 review MINOR 6 + nits |

## Owner approvals
Open items the unattended run could not decide. The agent adds a row and stops that branch of
work; the owner answers in the Decision column (and a DECISION_LOG row when it changes a rule).

| # | Date | Task | Request (exact command / change) | Why needed | Risk | Decision |
|---|---|---|---|---|---|---|

## Learning queue (owner, when back)
Tutor gates deferred by the unattended run (D-041). Each row → one tutor session with a hands-on lab.

| Scope | Topics | Lab | Status |
|---|---|---|---|
| W1 | Kafka key/partition/headers, idempotent producer, contracts | [.claude/learning/labs/W1_LAB.md](../.claude/learning/labs/W1_LAB.md) | open |
| W2.D7 | Driver vs executor code shipping, config layering, run IDs, `from_json` leniency | [.claude/learning/labs/W2_LAB.md](../.claude/learning/labs/W2_LAB.md) | open |
| W2.D9 | Checkpoint anatomy (`offsets/`, `commits/`, `metadata`), Delta idempotent writes and their blind spot (S9), `failOnDataLoss`, fault injection, "offset committed once per checkpoint lineage" | [ADR-0006](adr/0006-delivery-semantics-restart-and-checkpoint-loss.md); `python scripts/submit_job.py bronze_report` (`offsets_by_partition`, `distinct_lineages`) | open |
| W2.D8 | Kafka source options, `foreachBatch` vs Delta sink, checkpoints, Delta `txnAppId`/`txnVersion`, parse errors vs contract failures | `python scripts/submit_job.py bronze_report`; [DATA_FLOW.md](DATA_FLOW.md) step 9 | open |
| W2.D10 | Validation as data (rules as columns), error taxonomies (code → category → retryable), quarantine/DLQ patterns, insert-only MERGE, null-safe join (`<=>`), independent post-write checks, log masking | `python scripts/submit_job.py dlq_report --list-refs`; [DATA_FLOW.md](DATA_FLOW.md) step 10 | open |
| W2.D11 | Hive metastore internals (DBS/TBLS/SDS in Postgres, embedded client vs metastore service), external Delta tables by name, Thrift/JDBC, dbt adapter capability limits (merge, composite `unique_key`, contracts vs Delta constraints, `persist_docs`) | `beeline … -e "SHOW TABLES IN bronze"`; `dbt build --select spike`; [ADR-0004](adr/0004-spark-dbt-ownership-and-adapter-capabilities.md) | open |
| W3.D13 | Incremental file ingestion (Auto Loader concept on OSS: file-source checkpoint tracks processed files), exact line lineage, path brand vs payload brand, schema evolution (contract projection vs `mergeSchema`), PERMISSIVE parsing of wrong types, landing manifests as completion markers | `python -m lakehouse.generator land --dry-run --out-dir build/landing`; [DATA_FLOW.md](DATA_FLOW.md) steps 6a, 9a | open |
