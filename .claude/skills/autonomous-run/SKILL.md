---
name: autonomous-run
description: One unattended iteration of the work queue - pick exactly one runnable row from docs/TASKS.md, execute it end to end through work-orchestrator (agents, verify, review, commit), update TASKS.md and PROGRESS.md, then stop. Used by scripts/autonomous/run_queue.py via `claude -p`; also when the owner says "kuyruktan bir iş yap" or "run one queue item".
---

# Autonomous run — one iteration

You are the coordinator in a **fresh context** without a human. Everything you need to continue
is in files; everything the next iteration needs must be in files when you stop. Do **one**
queue row, then stop. Reply format: short English log (no owner reply template — nobody reads
it live); the final line is `ITERATION: <task id> · <done|in_progress|blocked-owner|failed> · <commit or ->`.

## 1. Load state (read only what is needed)
1. `docs/TASKS.md` → the runnable row (first `in_progress`, else first `todo` with all
   dependencies `done`; the runner passes a task-id hint). If none: stop with `ITERATION: - · idle · -`.
2. `docs/PROGRESS.md` → `## Current State` block and the row's task section.
3. `docs/DELIVERY_PLAN.md` → the row's day card (Work, Verification, Done-when, Learn).
4. `git status --short --branch`. Branch per day: `feat/w<w>-d<d>-<slug>` from an up-to-date
   `main` (`git switch main; git pull --ff-only; git switch -c ...`), or continue the existing
   day branch. Uncommitted changes from the runner (the `Autonomous run` status line in PROGRESS)
   are expected: keep them and commit them with your work.
5. Set the row to `in_progress`. **Never edit Attempts, Depends on, Task or the set of rows, and
   never change a `blocked-owner` / `done` / `failed` row** — the runner owns those and halts the
   run on any such edit (`validate_transition`). `done` needs a commit hash in Note.

## 2. Execute through `work-orchestrator`
Follow the skill exactly (classify → task cards with disjoint file ownership → agents per the
model policy → verify → independent review for complex/data-correctness work). Contracts,
invariants and the honest-claims policy in `CLAUDE.md` apply unchanged.

## 3. Permission denials and decisions (no human to ask)
- A denied tool call is a **decision, not an obstacle**. Never reach the same effect another way
  (`python -c` network calls instead of `curl`, `shutil.rmtree` instead of `rm -rf`, editing
  settings/hooks, nested `claude`). Guardrail files (`.claude/` settings, hooks, agents, skills,
  rules, `CLAUDE.md`, `scripts/autonomous/`, `.github/`) are owner-only: the runner halts on any
  change to them and `run-supervisor` is instructed to halt on workarounds. If a guardrail file
  needs a change, that is an Owner approvals row.
- When a subagent reports a denial, you decide: (a) the work can be done with allowed tools and
  stays inside the task card → do it or re-brief the agent; (b) it needs an owner-only action
  (cloud spend, `aws`, `terraform apply/destroy`, deleting data/volumes, force push, a change to a
  contract/threshold/boundary not already in the plan, anything irreversible or outward-facing
  beyond D-032) → add a row to **Owner approvals** in `docs/TASKS.md` (exact command or change,
  why, risk), set the row `blocked-owner`, commit the docs, stop.
- Choices the plan already settles (day card, ADRs, DECISION_LOG) are not owner decisions —
  apply them. A new technical choice inside the card's scope: decide, record a DECISION_LOG row
  in the same commit, and flag it in the commit body as `Decision-for-review: D-0xx`.
- Standing approvals: local Docker image builds (D-041); push/PR/merge only under D-032.

## 4. Verify and record
- Run the day card's verification commands. ✅ in PROGRESS only with commit hash **and** output.
- Commit (Conventional Commits; attribution lines per the system reminder). Then update the
  TASKS row: `done` + commit hash in Note.
- Failure you cannot fix in this iteration: leave the row `in_progress`, write the failing command
  and the hypothesis in Note, commit work-in-progress only if tests that passed before still pass.
  The runner counts the attempt and sets `failed` after the third; then add nothing — the owner
  reviews `failed` rows.

## 5. `*.close` rows
Run the `checkpoint` skill for the day. The tutor runs in **batched mode** (no quiz): append
concept notes and question-bank items, and add/extend the row in **Learning queue** in TASKS.md
with a hands-on lab (tutor §6). Then the D-032 gate: push the day branch, open the PR, wait for
CI (`gh pr checks --watch`), merge with a merge commit only if CI is green, review verdict is
APPROVE / APPROVE WITH NITS with must-fix applied and `evidence-auditor` reports no MISMATCH.
Otherwise → Owner approvals row, `blocked-owner`. After a merge: `git switch main; git pull --ff-only`.
Append a SESSION_LOG entry (session-handoff format) and update `Current State`.

## 6. Stop
Make sure the working tree is committed (or only holds the runner's status line), TASKS.md and
PROGRESS.md agree, then print the `ITERATION:` line and end. Do not start a second row.
