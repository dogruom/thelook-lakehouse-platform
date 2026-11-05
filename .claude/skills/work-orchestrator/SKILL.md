---
name: work-orchestrator
description: Coordinator playbook for any non-trivial task in this repository - classify the task, decide which agents (implementer, test-author, reviewer, evidence-auditor) are worth their token cost, write task cards with disjoint file ownership, run them (in parallel where safe), verify, and gate acceptance on an independent review. Use at the start of any multi-file, data-correctness, infrastructure, security or ambiguous task, or when the owner dictates a loose request.
---

# Work orchestrator (main session = coordinator)

The main session coordinates: it talks to the owner, decomposes work, assigns files, merges
results, updates docs/PROGRESS and decides acceptance. Agents never talk to each other; the
coordinator passes their outputs on.

**Model split** — coordinator = main session (**Opus**, medium effort, project default): evaluates
every prompt, plans, sequences, decides parallelism, reviews results. Workers use the cheapest
model that does the job: `implementer`, `test-author`, `reviewer` → **Sonnet**; `Explore`,
`evidence-auditor` → **Haiku**. Because Opus consumes the plan allowance fastest, the coordinator
**does not write production code or tests itself** beyond trivial edits (≈ one file, < 20 lines,
docs/config); everything larger goes to `implementer`/`test-author`.

## 1. Normalise the request (2–4 lines, Turkish to the owner)
Requested vs assumed · task ID(s) from `docs/PROGRESS.md` · risks/decisions needing the owner.
Ask only if the answer changes architecture, security, data integrity, cost or scope.
**Scope check**: does the request belong to the active milestone and its acceptance criteria? If
not, say so in one line and park it in DELIVERY_PLAN §8/§9 (cut list / next stage) unless the
owner decides otherwise. Flag over-engineering (abstractions, tools or agents without a current
acceptance criterion) and propose the simpler path. Do not reopen accepted ADRs without new evidence.

## 2. Classify
```
Complexity: trivial | normal | complex | architectural
Risk:       low | medium | high   (high = data loss, tenant leak, PII, IAM, cloud cost, history rewrite)
Mode:       direct | direct+review | parallel (implementer ‖ test-author) + review
Effort:     medium (default) | high (complex) | xhigh (architectural; before considering any other model)
```

## 3. Agent routing — use an agent only when it pays for its context
| Situation | Who does what |
|---|---|
| Trivial (typo, small doc/config edit, one-liner) | Coordinator only. No agents. |
| Normal (one module, clear spec) | `implementer` (with tests in the same card, or `test-author` in parallel when the spec is testable independently); `reviewer` (code) only if the change touches an invariant, IAM, secrets or a 🔺 task. |
| Complex / data-correctness (CDC, SCD2, idempotency, DLQ, tenant isolation, Spark jobs, Terraform/IAM) | `reviewer` (design) → `implementer` ‖ `test-author` in parallel → coordinator runs verification → `reviewer` (code). Add focus flag `data` for pipeline correctness and `security` for IAM, secrets, PII, CI. |
| Day close | `checkpoint` skill (day). `evidence-auditor` only if ≥ 3 tasks or any ✅ is claimed for a 🔺 task. |
| Week gate / milestone | `checkpoint` skill (week/milestone) → `evidence-auditor` + `reviewer` (code, milestone diff) once. |
| Broad codebase search | Coordinator with Grep/Glob first; built-in `Explore` only for genuinely wide sweeps. |

Never run every agent on every task. Never spawn an agent to re-read context the coordinator
already holds — pass the facts in the task card instead.

## 4. Task card (the only brief an agent gets)
```
TASK: W<w>.D<d>.<n> — <goal in one line>
ACCEPTANCE: <copied from PROGRESS.md, testable bullets>
OWNED FILES: <exact paths/globs this agent may edit>          # disjoint between parallel agents
READ-ONLY CONTEXT: <max ~6 files or sections, with line ranges if large>
INTERFACE: <function/CLI/table signatures both implementer and test-author code against>
VERIFY: <exact commands>
CONSTRAINTS: <invariants, versions, no-go areas, time box>
```
File ownership: `implementer` → `src/**`, `dags/**`, `dbt_project/models|macros/**`, `docker/**`,
`infra/**`, `scripts/**` as listed; `test-author` → `tests/**`, `dbt_project/tests/**`;
coordinator → `docs/**`, `README.md`, `CLAUDE.md`, `.claude/**`, `pyproject.toml` (unless assigned).
Two agents never own the same file in one step. The INTERFACE block is what makes parallel work safe.

## 5. Execute and verify
1. Run independent agents in the same turn (parallel); dependent steps sequentially.
2. After agents return: run the VERIFY commands yourself; read `git diff --stat` and the diff of
   risky files. Failing tests → decide: implementation bug (back to implementer with the failure),
   spec gap (fix the card / ask owner), test bug (back to test-author).
3. Acceptance requires: VERIFY green + reviewer verdict APPROVE (when a reviewer was routed) +
   no secrets/unrelated changes in the diff. Max 2 fix loops, then stop and report to the owner.

## 6. Close the task
Update the task's PROGRESS status (✅ only with commit hash + verification output; otherwise 🔄),
note decisions (ADR if architectural), and tell the owner in Turkish: what changed, results,
open risks, next task. Commit via the `git-workflow` skill when the owner has authorised commits.

## 7. Model policy
Coordinator on Opus (medium); implementation, tests, SQL/dbt, docs drafting and bounded
refactoring on Sonnet agents; search and evidence checks on Haiku agents. Raise the coordinator to
`high` effort only for architecture checkpoints, hard debugging or material risk reviews. If the
owner switches the session to Sonnet (to save allowance), the same routing still applies.
End every substantive task with a 3–5 line Turkish summary: done · verified how · open · next.
