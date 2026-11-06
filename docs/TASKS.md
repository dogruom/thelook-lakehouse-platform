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
| W2.D7.1 | `lakehouse.common.spark` SparkSession factory + environment adapter (local/aws) + job CLI base with run IDs | todo | — | 0 | |
| W2.D7.2 | `spark-test` service (tools profile) running pytest in the Spark image; first Spark tests | todo | W2.D7.1 | 0 | image build pre-approved (D-041) |
| W2.D7.3 | Wheel build + `scripts/submit_job.py` (spark-submit in the master container) | todo | W2.D7.2 | 0 | |
| W2.D7.close | Day close W2.D7 (checkpoint, report, D-032 merge gate) | todo | W2.D7.3 | 0 | |
| W2.D8.1 | `ingestion.bronze_stream` (both topics, raw value, key, headers, source ref, availableNow, one checkpoint per query) | todo | W2.D7.close | 0 | |
| W2.D8.2 | Spark tests for the parse function (valid, malformed, missing tenant) | todo | W2.D8.1 | 0 | |
| W2.D8.3 | Integration: Bronze rows = produced messages | todo | W2.D8.2 | 0 | |
| W2.D8.close | Day close W2.D8 | todo | W2.D8.3 | 0 | |
| W2.D9.1 | Restart tests: kill mid-stream → each offset once; delete checkpoint → replay quantified | todo | W2.D8.close | 0 | |
| W2.D9.2 | ADR-0006 draft with measured outcomes | todo | W2.D9.1 | 0 | |
| W2.D9.close | Day close W2.D9 (🔺 gate for M2) | todo | W2.D9.2 | 0 | |
| W2.D10.1 | `quality.rules` + DLQ writer | todo | W2.D9.close | 0 | |
| W2.D10.2 | Spark tests per rule incl. negatives | todo | W2.D10.1 | 0 | |
| W2.D10.3 | Integration: DLQ set = manifest DLQ-expected set (exact) | todo | W2.D10.2 | 0 | |
| W2.D10.close | Day close W2.D10 | todo | W2.D10.3 | 0 | |
| W2.D11.1 | Postgres-backed Hive metastore; register Bronze/DLQ tables | todo | W2.D10.close | 0 | |
| W2.D11.2 | Spark Thrift Server (`sql` profile); `dbt debug`; probe models | todo | W2.D11.1 | 0 | |
| W2.D11.3 | Decision recorded (Thrift or `session` fallback) in ADR-0004 spike section | todo | W2.D11.2 | 0 | |
| W2.D11.close | Day close W2.D11 | todo | W2.D11.3 | 0 | |
| W2.D12.1 | `docs/runbooks/m1-streaming-demo.md` | todo | W2.D11.close | 0 | |
| W2.D12.2 | Full regression | todo | W2.D12.1 | 0 | |
| W2.D12.3 | Milestone review (reviewer + evidence-auditor), README status, can/cannot-claim note | todo | W2.D12.2 | 0 | `v0.1.0` tag is owner-only |
| W2.D12.close | Week/milestone close W2 (tutor deferred to owner session) | todo | W2.D12.3 | 0 | |

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
